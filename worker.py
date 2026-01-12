import os
import sys
import time
import gc
import logging
import traceback
from datetime import datetime, timedelta
from uuid import uuid4

import boto3
import pdfplumber
import requests
from botocore.client import Config
from sqlalchemy.orm import Session

from models import SessionLocal, Job  # User/Usage не нужны воркеру прямо сейчас


# ----------------------------
# Logging (timestamps + immediate flush)
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("pdf-worker")
logger.setLevel(LOG_LEVEL)

_handler = logging.StreamHandler(sys.stderr)
_handler.setLevel(LOG_LEVEL)
_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_handler.setFormatter(_formatter)
logger.handlers = [ _handler ]
logger.propagate = False

# Make stderr line-buffered where possible
try:
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass


# ----------------------------
# Config
# ----------------------------
R2_BUCKET = os.getenv("R2_BUCKET")

R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

ROUTELLM_API_KEY = os.getenv("ROUTELLM_API_KEY")
ROUTELLM_URL = os.getenv("ROUTELLM_URL", "https://routellm.abacus.ai/v1/chat/completions")
MODEL = os.getenv("MODEL", "gpt-4o-mini")

POLL_SECONDS = int(os.getenv("WORKER_POLL_SECONDS", "5"))
PDF_BATCH_SIZE = int(os.getenv("PDF_BATCH_SIZE", "20"))
CHECKPOINT_EVERY_PAGES = int(os.getenv("CHECKPOINT_EVERY_PAGES", "5"))

LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "90"))
LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))

# Very important to avoid sending gigantic page text in one shot
MAX_CHARS_PER_CHUNK = int(os.getenv("MAX_CHARS_PER_CHUNK", "12000"))

# If a job is "processing" for too long, re-queue it
STALE_PROCESSING_MINUTES = int(os.getenv("STALE_PROCESSING_MINUTES", "60"))

SYSTEM_PROMPT = os.getenv(
    "SYSTEM_PROMPT",
    (
        "You are a professional translator. Translate the following text from English to Russian. "
        "Keep the original formatting, line breaks, and structure exactly as they are. "
        "Do not add any explanations, only the translation."
    )
)

# ----------------------------
# R2 client
# ----------------------------
def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )

s3 = make_s3()


def env_sanity():
    missing = []
    for k, v in [
        ("R2_BUCKET", R2_BUCKET),
        ("R2_ENDPOINT", R2_ENDPOINT),
        ("R2_ACCESS_KEY", R2_ACCESS_KEY),
        ("R2_SECRET_KEY", R2_SECRET_KEY),
        ("ROUTELLM_API_KEY", ROUTELLM_API_KEY),
    ]:
        if not v:
            missing.append(k)
    if missing:
        logger.error(f"Missing env vars: {', '.join(missing)}")
        return False
    return True


# ----------------------------
# Text chunking helpers (preserve line breaks)
# ----------------------------
def chunk_text_preserving_lines(text: str, max_chars: int):
    """
    Split text by lines into chunks up to max_chars, preserving '\n'.
    """
    lines = text.splitlines(keepends=True)
    chunks = []
    buf = ""
    for line in lines:
        if len(buf) + len(line) > max_chars and buf:
            chunks.append(buf)
            buf = ""
        buf += line
    if buf:
        chunks.append(buf)
    return chunks


# ----------------------------
# RouteLLM call with retries
# ----------------------------
def translate_chunk(chunk: str, req_id: str) -> str:
    """
    Returns translated text. On failure returns original chunk (so pipeline doesn't break).
    """
    if not chunk or len(chunk.strip()) < 5:
        return ""

    headers = {
        "Authorization": f"Bearer {ROUTELLM_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": chunk},
        ],
        "temperature": 0.3,
    }

    last_err = None
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            t0 = time.time()
            resp = requests.post(
                ROUTELLM_URL,
                headers=headers,
                json=payload,
                timeout=LLM_TIMEOUT_SECONDS,
            )
            dt = time.time() - t0

            if resp.status_code != 200:
                logger.warning(
                    f"[{req_id}] LLM HTTP {resp.status_code} (attempt {attempt}/{LLM_MAX_RETRIES}) "
                    f"in {dt:.2f}s: {resp.text[:4000]}"
                )

            resp.raise_for_status()
            data = resp.json()

            out = data["choices"][0]["message"]["content"]
            logger.info(f"[{req_id}] LLM ok in {dt:.2f}s (chars_in={len(chunk)}, chars_out={len(out)})")
            return out

        except Exception as e:
            last_err = e
            logger.warning(f"[{req_id}] LLM error (attempt {attempt}/{LLM_MAX_RETRIES}): {e}")
            if attempt < LLM_MAX_RETRIES:
                time.sleep(1.5 * attempt)  # small backoff

    logger.error(f"[{req_id}] LLM failed after retries; returning original chunk. Last error: {last_err}")
    return chunk


def translate_text(text: str, req_id: str) -> str:
    chunks = chunk_text_preserving_lines(text, MAX_CHARS_PER_CHUNK)
    if len(chunks) == 1:
        return translate_chunk(chunks[0], req_id)

    out_parts = []
    for idx, ch in enumerate(chunks, start=1):
        part_id = f"{req_id}.c{idx}/{len(chunks)}"
        out_parts.append(translate_chunk(ch, part_id))
    return "".join(out_parts)


# ----------------------------
# Job claiming + stale handling
# ----------------------------
def requeue_stale_processing(db: Session):
    """
    Requeue jobs stuck in 'processing' too long (best-effort, schema-safe).
    """
    try:
        cutoff = datetime.utcnow() - timedelta(minutes=STALE_PROCESSING_MINUTES)

        # prefer updated_at if exists, else created_at
        ts_field = None
        if hasattr(Job, "updated_at"):
            ts_field = Job.updated_at
        elif hasattr(Job, "created_at"):
            ts_field = Job.created_at

        if ts_field is None:
            return

        stale = (
            db.query(Job)
            .filter(Job.status == "processing", ts_field < cutoff)
            .order_by(ts_field.asc())
            .first()
        )

        if stale:
            logger.warning(f"[job {stale.id}] Re-queuing stale processing job (older than {STALE_PROCESSING_MINUTES}m)")
            stale.status = "queued"
            db.commit()

    except Exception as e:
        logger.warning(f"Stale requeue check failed: {e}")


def claim_next_job(db: Session):
    """
    Claim oldest queued job in FIFO order. Uses FOR UPDATE SKIP LOCKED if possible.
    """
    q = db.query(Job).filter(Job.status == "queued").order_by(Job.created_at.asc())

    job = None
    try:
        job = q.with_for_update(skip_locked=True).first()
    except Exception:
        # fallback if dialect doesn't support it
        job = q.first()

    if not job:
        return None

    job.status = "processing"
    # if model has updated_at, touching status likely updates it; if not, it's fine
    db.commit()
    db.refresh(job)
    return job


# ----------------------------
# Core processing
# ----------------------------
def process_job(db: Session, job: Job):
    req_id = f"job{job.id}-{uuid4().hex[:8]}"
    logger.info(f"[{req_id}] Start job id={job.id} file='{job.filename}' key='{job.r2_key_input}'")

    local_input = f"tmp_in_{job.id}_{int(time.time())}_{job.filename}"
    local_output = f"tmp_out_{job.id}_{int(time.time())}_{job.filename}.txt"

    # checkpoint: we reuse word_count as "last processed page index" (1-based)
    last_page_done = int(job.word_count or 0)
    mode = "a" if last_page_done > 0 else "w"

    try:
        # Download input
        logger.info(f"[{req_id}] Downloading from R2 bucket='{R2_BUCKET}' key='{job.r2_key_input}' -> '{local_input}'")
        s3.download_file(R2_BUCKET, job.r2_key_input, local_input)

        # Count pages (fast open)
        with pdfplumber.open(local_input) as pdf:
            total_pages = len(pdf.pages)
        logger.info(f"[{req_id}] PDF pages={total_pages} resume_from_page={last_page_done + 1}")

        # Write output progressively
        with open(local_output, mode, encoding="utf-8") as out:
            # Process batches to reduce memory
            for batch_start in range(last_page_done, total_pages, PDF_BATCH_SIZE):
                batch_end = min(batch_start + PDF_BATCH_SIZE, total_pages)
                logger.info(f"[{req_id}] Batch {batch_start + 1}-{batch_end} / {total_pages}")

                with pdfplumber.open(local_input) as pdf:
                    for i in range(batch_start, batch_end):
                        page_no = i + 1
                        page_id = f"{req_id}.p{page_no}"

                        try:
                            page = pdf.pages[i]
                            text = page.extract_text() or ""
                        except Exception as e:
                            logger.warning(f"[{page_id}] extract_text failed: {e}")
                            text = ""

                        if not text.strip():
                            logger.info(f"[{page_id}] empty page text; writing marker only")
                            out.write(f"--- Page {page_no} ---\n\n")
                            out.flush()
                        else:
                            logger.info(f"[{page_id}] translating (chars={len(text)})")
                            translated = translate_text(text, page_id)
                            out.write(f"--- Page {page_no} ---\n{translated}\n\n")
                            out.flush()

                        # checkpoint
                        if page_no % CHECKPOINT_EVERY_PAGES == 0:
                            job.word_count = page_no
                            db.commit()
                            logger.info(f"[{req_id}] checkpoint saved: page={page_no}")

                # memory cleanup after each batch
                gc.collect()

        # Upload output
        r2_key_output = f"outputs/{job.user_id}/translated_{job.id}_{job.filename}.txt"
        logger.info(f"[{req_id}] Uploading output to R2 key='{r2_key_output}'")
        s3.upload_file(local_output, R2_BUCKET, r2_key_output)

        # Finalize job
        job.status = "completed"
        job.r2_key_output = r2_key_output
        job.word_count = total_pages
        db.commit()

        logger.info(f"[{req_id}] DONE status=completed pages={total_pages}")

    except Exception as e:
        logger.error(f"[{req_id}] FAILED: {e}")
        traceback.print_exc(file=sys.stderr)

        try:
            job.status = "failed"
            db.commit()
        except Exception as e2:
            logger.error(f"[{req_id}] failed to mark job failed: {e2}")

    finally:
        # Cleanup files
        for path, label in [(local_input, "input"), (local_output, "output")]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                    logger.info(f"[{req_id}] removed temp {label}: {path}")
                except Exception as e:
                    logger.warning(f"[{req_id}] failed to remove temp {label} '{path}': {e}")


# ----------------------------
# Worker loop
# ----------------------------
def run_worker():
    logger.info("=" * 70)
    logger.info("🚀 PDF Translation Worker Starting")
    logger.info(f"📦 Model: {MODEL}")
    logger.info(f"🪣 R2 Bucket: {R2_BUCKET}")
    logger.info(f"🌐 RouteLLM URL: {ROUTELLM_URL}")
    logger.info(f"⏱️ poll={POLL_SECONDS}s batch={PDF_BATCH_SIZE} checkpoint_every={CHECKPOINT_EVERY_PAGES} pages")
    logger.info("=" * 70)

    if not env_sanity():
        logger.error("Worker cannot start due to missing env vars.")
        while True:
            time.sleep(10)

    last_heartbeat = 0

    while True:
        db = SessionLocal()
        try:
            # best-effort: unstick stale jobs
            requeue_stale_processing(db)

            job = claim_next_job(db)
            if job:
                logger.info(f"[job {job.id}] claimed -> processing")
                process_job(db, job)
            else:
                # heartbeat every ~60s
                now = time.time()
                if now - last_heartbeat > 60:
                    try:
                        q = db.query(Job).filter(Job.status == "queued").count()
                        p = db.query(Job).filter(Job.status == "processing").count()
                        c = db.query(Job).filter(Job.status == "completed").count()
                        f = db.query(Job).filter(Job.status == "failed").count()
                        logger.info(f"⏳ idle. queued={q} processing={p} completed={c} failed={f}")
                    except Exception as e:
                        logger.info(f"⏳ idle. (counts unavailable: {e})")
                    last_heartbeat = now

                time.sleep(POLL_SECONDS)

        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            traceback.print_exc(file=sys.stderr)
            time.sleep(POLL_SECONDS)

        finally:
            db.close()


if __name__ == "__main__":
    run_worker()
