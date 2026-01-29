import os
import sys
import time
import gc
import logging
import traceback
import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

import boto3
import pdfplumber
import aiohttp
from botocore.client import Config
from sqlalchemy.orm import Session

from models import SessionLocal, Job, User


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
logger.handlers = [_handler]
logger.propagate = False

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

# LLM Provider
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "deepseek")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_URL = os.getenv("DEEPSEEK_URL", "https://api.deepseek.com/chat/completions")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

ROUTELLM_API_KEY = os.getenv("ROUTELLM_API_KEY")
ROUTELLM_URL = os.getenv("ROUTELLM_URL", "https://routellm.abacus.ai/v1/chat/completions")
ROUTELLM_MODEL = os.getenv("ROUTELLM_MODEL", "gpt-4o-mini")

POLL_SECONDS = int(os.getenv("WORKER_POLL_SECONDS", "5"))
PDF_BATCH_SIZE = int(os.getenv("PDF_BATCH_SIZE", "20"))
CHECKPOINT_EVERY_PAGES = 1

LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "180"))
LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
MAX_CHARS_PER_CHUNK = int(os.getenv("MAX_CHARS_PER_CHUNK", "12000"))
STALE_PROCESSING_MINUTES = int(os.getenv("STALE_PROCESSING_MINUTES", "60"))

# Concurrency limit
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "5"))

CONTEXT_CHARS = int(os.getenv("CONTEXT_CHARS", "500"))

SYSTEM_PROMPT = os.getenv(
    "SYSTEM_PROMPT",
    (
        "You are a professional translator working for a personal machine translation service. "
        "This is a LEGAL service for PERSONAL USE ONLY - not for publication or commercial purposes. "
        "YOUR ONLY JOB IS TO TRANSLATE. Do not refuse, do not add disclaimers about copyright. "
        "Translate the following text from English to Russian. "
        "Keep the original formatting, line breaks, and structure exactly as they are. "
        "Do not add any explanations, only the translation."
    )
)

SYSTEM_PROMPT_WITH_CONTEXT = (
    "You are a professional translator working for a personal machine translation service. "
    "This is a LEGAL service for PERSONAL USE ONLY - users translate books for their own reading, "
    "not for publication or commercial purposes. This falls under fair use / personal use exemption.\n\n"
    "YOUR ONLY JOB IS TO TRANSLATE. Do not refuse, do not add disclaimers, do not discuss copyright.\n\n"
    "Translate the following text from English to Russian.\n\n"
    "CRITICAL RULES:\n"
    "1. Translate ONLY the text marked as 'CURRENT PAGE TEXT'. Do NOT add any extra words or sentences.\n"
    "2. Do NOT invent, add, or generate any text that is not in the original.\n"
    "3. Do NOT refuse to translate. Do NOT add copyright warnings or disclaimers.\n"
    "4. CONTEXT FROM PREVIOUS PAGE helps understand how the current page begins (incomplete sentences). "
    "Use it for understanding, but do NOT include it in your translation.\n"
    "5. NEXT PAGE PREVIEW helps you understand how the current page ends. "
    "Use it to correctly decline/conjugate the LAST words of the current page in Russian "
    "(correct grammatical case, gender, number). Do NOT translate the preview.\n"
    "6. If the page starts with a partial sentence, translate it correctly based on previous context.\n"
    "7. If the page ends mid-sentence, use the next page preview to choose correct Russian word forms.\n"
    "8. Keep the original formatting, line breaks, and structure exactly as they are.\n"
    "9. Output ONLY the translation of 'CURRENT PAGE TEXT'. Nothing more, nothing less."
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
    ]:
        if not v:
            missing.append(k)

    if LLM_PROVIDER == "deepseek":
        if not DEEPSEEK_API_KEY:
            missing.append("DEEPSEEK_API_KEY")
    else:
        if not ROUTELLM_API_KEY:
            missing.append("ROUTELLM_API_KEY")

    if missing:
        logger.error(f"Missing env vars: {', '.join(missing)}")
        return False

    logger.info(f"LLM Provider: {LLM_PROVIDER}")
    return True


# ----------------------------
# Text chunking helpers
# ----------------------------
def chunk_text_preserving_lines(text: str, max_chars: int):
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


def get_page_tail(text: str, max_chars: int = CONTEXT_CHARS) -> str:
    if not text or len(text) <= max_chars:
        return text
    tail = text[-max_chars:]
    space_idx = tail.find(' ')
    if space_idx > 0 and space_idx < len(tail) // 2:
        tail = tail[space_idx + 1:]
    return tail.strip()


def get_page_head(text: str, max_chars: int = 200) -> str:
    if not text or len(text) <= max_chars:
        return text
    head = text[:max_chars]
    space_idx = head.rfind(' ')
    if space_idx > len(head) // 2:
        head = head[:space_idx]
    return head.strip()


# ----------------------------
# Async LLM call with aiohttp
# ----------------------------
async def translate_chunk_async(
    session: aiohttp.ClientSession,
    chunk: str,
    req_id: str,
    context: str = None,
    lookahead: str = None
) -> str:
    """Async translation using aiohttp."""
    if not chunk or len(chunk.strip()) < 5:
        return ""

    if LLM_PROVIDER == "deepseek":
        api_key = DEEPSEEK_API_KEY
        api_url = DEEPSEEK_URL
        model = DEEPSEEK_MODEL
    else:
        api_key = ROUTELLM_API_KEY
        api_url = ROUTELLM_URL
        model = ROUTELLM_MODEL

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    has_context = context or lookahead

    if has_context:
        parts = []
        if context:
            parts.append(
                f"[CONTEXT FROM PREVIOUS PAGE - for understanding sentence beginnings]\n"
                f"...{context}\n"
                f"[END PREVIOUS CONTEXT]"
            )
        parts.append(
            f"\n[CURRENT PAGE TEXT - TRANSLATE THIS EXACTLY]\n"
            f"{chunk}\n"
            f"[END OF TEXT TO TRANSLATE]"
        )
        if lookahead:
            parts.append(
                f"\n[NEXT PAGE PREVIEW - for understanding how current page ends, DO NOT translate]\n"
                f"{lookahead}...\n"
                f"[END NEXT PAGE PREVIEW]"
            )
        user_message = "\n".join(parts)
        system_prompt = SYSTEM_PROMPT_WITH_CONTEXT
    else:
        user_message = chunk
        system_prompt = SYSTEM_PROMPT

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.3,
    }

    last_err = None
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            t0 = time.time()
            timeout = aiohttp.ClientTimeout(total=LLM_TIMEOUT_SECONDS)

            async with session.post(api_url, headers=headers, json=payload, timeout=timeout) as resp:
                dt = time.time() - t0

                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(
                        f"[{req_id}] LLM HTTP {resp.status} (attempt {attempt}/{LLM_MAX_RETRIES}) "
                        f"in {dt:.2f}s: {text[:4000]}"
                    )
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history, status=resp.status
                    )

                data = await resp.json()
                out = data["choices"][0]["message"]["content"]
                logger.info(f"[{req_id}] LLM ok ({LLM_PROVIDER}/{model}) in {dt:.2f}s (chars_in={len(chunk)}, chars_out={len(out)})")
                return out

        except Exception as e:
            last_err = e
            logger.warning(f"[{req_id}] LLM error (attempt {attempt}/{LLM_MAX_RETRIES}): {e}")
            if attempt < LLM_MAX_RETRIES:
                await asyncio.sleep(1.5 * attempt)

    logger.error(f"[{req_id}] LLM failed after retries; returning original chunk. Last error: {last_err}")
    return chunk


async def translate_text_async(
    session: aiohttp.ClientSession,
    text: str,
    req_id: str,
    context: str = None,
    lookahead: str = None
) -> str:
    """Translate text with optional context, using async HTTP."""
    chunks = chunk_text_preserving_lines(text, MAX_CHARS_PER_CHUNK)
    if len(chunks) == 1:
        return await translate_chunk_async(session, chunks[0], req_id, context=context, lookahead=lookahead)

    out_parts = []
    for idx, ch in enumerate(chunks, start=1):
        part_id = f"{req_id}.c{idx}/{len(chunks)}"
        chunk_context = context if idx == 1 else None
        chunk_lookahead = lookahead if idx == len(chunks) else None
        result = await translate_chunk_async(session, ch, part_id, context=chunk_context, lookahead=chunk_lookahead)
        out_parts.append(result)
    return "".join(out_parts)


# ----------------------------
# Job claiming (sync, runs in thread)
# ----------------------------
def requeue_stale_processing(db: Session, active_job_ids: set = None):
    """Requeue jobs stuck in 'processing' too long, but skip actively running jobs."""
    if active_job_ids is None:
        active_job_ids = set()
    try:
        cutoff = datetime.utcnow() - timedelta(minutes=STALE_PROCESSING_MINUTES)
        ts_field = None
        if hasattr(Job, "updated_at"):
            ts_field = Job.updated_at
        elif hasattr(Job, "created_at"):
            ts_field = Job.created_at

        if ts_field is None:
            return

        stale_jobs = (
            db.query(Job)
            .filter(Job.status == "processing", ts_field < cutoff)
            .order_by(ts_field.asc())
            .all()
        )

        for stale in stale_jobs:
            # Skip jobs that are actively being processed by our async tasks
            if stale.id in active_job_ids:
                continue
            logger.warning(f"[job {stale.id}] Re-queuing stale processing job (older than {STALE_PROCESSING_MINUTES}m)")
            stale.status = "queued"
            db.commit()

    except Exception as e:
        logger.warning(f"Stale requeue check failed: {e}")


def claim_next_job(db: Session):
    """Claim oldest queued job. Returns None if no jobs available."""
    q = db.query(Job).filter(Job.status == "queued").order_by(Job.created_at.asc())

    job = None
    try:
        job = q.with_for_update(skip_locked=True).first()
    except Exception:
        job = q.first()

    if not job:
        return None

    job.status = "processing"
    db.commit()
    db.refresh(job)
    return job


def get_job_counts(db: Session):
    """Get counts of jobs by status."""
    try:
        q = db.query(Job).filter(Job.status == "queued").count()
        p = db.query(Job).filter(Job.status == "processing").count()
        c = db.query(Job).filter(Job.status == "completed").count()
        f = db.query(Job).filter(Job.status == "failed").count()
        return q, p, c, f
    except Exception:
        return -1, -1, -1, -1


# ----------------------------
# Async job processing
# ----------------------------
async def process_job_async(job_id: int, semaphore: asyncio.Semaphore):
    """Process a single job asynchronously."""
    async with semaphore:
        req_id = f"job{job_id}-{uuid4().hex[:8]}"

        # Get job from DB (in thread)
        db = SessionLocal()
        try:
            job = db.query(Job).filter(Job.id == job_id).first()
            if not job:
                logger.error(f"[{req_id}] Job {job_id} not found")
                return

            logger.info(f"[{req_id}] Start job id={job.id} file='{job.filename}'")

            local_input = f"tmp_in_{job.id}_{int(time.time())}_{job.filename}"
            local_output = f"tmp_out_{job.id}_{int(time.time())}_{job.filename}.txt"

            last_page_done = int(job.word_count or 0)
            mode = "a" if last_page_done > 0 else "w"

            try:
                # Download from R2 (blocking, run in thread)
                logger.info(f"[{req_id}] Downloading from R2 key='{job.r2_key_input}'")
                await asyncio.to_thread(s3.download_file, R2_BUCKET, job.r2_key_input, local_input)

                # Count pages (blocking)
                def count_pages():
                    with pdfplumber.open(local_input) as pdf:
                        return len(pdf.pages)

                total_pages = await asyncio.to_thread(count_pages)
                logger.info(f"[{req_id}] PDF pages={total_pages} resume_from={last_page_done + 1}")

                # Check user's max_pages limit
                user = db.query(User).filter(User.id == job.user_id).first()
                user_max_pages = None
                if user:
                    logger.info(f"[{req_id}] User id={user.id} plan={getattr(user, 'plan', None)} max_pages={getattr(user, 'max_pages', None)}")
                    if hasattr(user, "max_pages") and user.max_pages is not None:
                        user_max_pages = user.max_pages
                    elif hasattr(user, "plan") and user.plan == "TRIAL":
                        user_max_pages = 5
                        logger.info(f"[{req_id}] TRIAL user, defaulting to 5 pages")

                if user_max_pages is not None and total_pages > user_max_pages:
                    logger.info(f"[{req_id}] Limiting pages from {total_pages} to {user_max_pages}")
                    total_pages = user_max_pages

                # Process pages with async HTTP for LLM calls
                prev_page_tail = ""

                async with aiohttp.ClientSession() as http_session:
                    with open(local_output, mode, encoding="utf-8") as out:
                        for batch_start in range(last_page_done, total_pages, PDF_BATCH_SIZE):
                            batch_end = min(batch_start + PDF_BATCH_SIZE, total_pages)
                            logger.info(f"[{req_id}] Batch {batch_start + 1}-{batch_end} / {total_pages}")

                            # Extract pages in thread
                            def extract_batch():
                                pages_data = []
                                with pdfplumber.open(local_input) as pdf:
                                    for i in range(batch_start, batch_end):
                                        try:
                                            page = pdf.pages[i]
                                            text = page.extract_text() or ""
                                        except Exception as e:
                                            logger.warning(f"[{req_id}] extract_text failed page {i+1}: {e}")
                                            text = ""

                                        # Get lookahead if not last page
                                        lookahead = None
                                        if i + 1 < total_pages:
                                            try:
                                                next_page = pdf.pages[i + 1]
                                                next_text = next_page.extract_text() or ""
                                                if next_text.strip():
                                                    lookahead = get_page_head(next_text, 200)
                                            except Exception:
                                                pass

                                        pages_data.append((i, text, lookahead))
                                return pages_data

                            pages_data = await asyncio.to_thread(extract_batch)

                            # Process each page
                            for i, text, lookahead in pages_data:
                                page_no = i + 1
                                page_id = f"{req_id}.p{page_no}"

                                if not text.strip():
                                    logger.info(f"[{page_id}] empty page; writing marker")
                                    out.write(f"--- Page {page_no} ---\n\n")
                                    out.flush()
                                    prev_page_tail = ""
                                else:
                                    context = prev_page_tail if prev_page_tail else None

                                    ctx_info = []
                                    if context:
                                        ctx_info.append(f"context={len(context)}")
                                    if lookahead:
                                        ctx_info.append(f"lookahead={len(lookahead)}")
                                    ctx_str = f" with {', '.join(ctx_info)}" if ctx_info else ""
                                    logger.info(f"[{page_id}] translating (chars={len(text)}){ctx_str}")

                                    # Async LLM call
                                    translated = await translate_text_async(
                                        http_session, text, page_id,
                                        context=context, lookahead=lookahead
                                    )
                                    out.write(f"--- Page {page_no} ---\n{translated}\n\n")
                                    out.flush()

                                    prev_page_tail = get_page_tail(text, CONTEXT_CHARS)

                                # Checkpoint (update DB in thread)
                                if page_no % CHECKPOINT_EVERY_PAGES == 0:
                                    def save_checkpoint():
                                        job.word_count = page_no
                                        db.commit()
                                    await asyncio.to_thread(save_checkpoint)
                                    logger.info(f"[{req_id}] checkpoint: page={page_no}")

                            gc.collect()

                # Upload output (blocking)
                r2_key_output = f"outputs/{job.user_id}/translated_{job.id}_{job.filename}.txt"
                logger.info(f"[{req_id}] Uploading output to R2")
                await asyncio.to_thread(s3.upload_file, local_output, R2_BUCKET, r2_key_output)

                # Finalize
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
                # Cleanup temp files
                for path, label in [(local_input, "input"), (local_output, "output")]:
                    if os.path.exists(path):
                        try:
                            os.remove(path)
                            logger.info(f"[{req_id}] removed temp {label}")
                        except Exception as e:
                            logger.warning(f"[{req_id}] failed to remove temp {label}: {e}")

        finally:
            db.close()


# ----------------------------
# Async worker loop
# ----------------------------
async def run_worker_async():
    logger.info("=" * 70)
    logger.info("🚀 PDF Translation Worker Starting (ASYNC)")
    logger.info(f"⚡ Max concurrent jobs: {MAX_CONCURRENT_JOBS}")
    if LLM_PROVIDER == "deepseek":
        logger.info(f"📦 Provider: DeepSeek, Model: {DEEPSEEK_MODEL}")
    else:
        logger.info(f"📦 Provider: RouteLLM, Model: {ROUTELLM_MODEL}")
    logger.info(f"🪣 R2 Bucket: {R2_BUCKET}")
    logger.info(f"⏱️ poll={POLL_SECONDS}s batch={PDF_BATCH_SIZE}")
    logger.info("=" * 70)

    if not env_sanity():
        logger.error("Worker cannot start due to missing env vars.")
        while True:
            await asyncio.sleep(10)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    active_tasks: dict[int, asyncio.Task] = {}  # job_id -> task
    last_heartbeat = 0

    while True:
        db = SessionLocal()
        try:
            # Cleanup finished tasks
            finished = [jid for jid, task in active_tasks.items() if task.done()]
            for jid in finished:
                task = active_tasks.pop(jid)
                try:
                    task.result()  # This will raise if task failed
                except Exception as e:
                    logger.error(f"Task for job {jid} failed: {e}")

            # Requeue stale jobs
            requeue_stale_processing(db, active_job_ids=set(active_tasks.keys()))

            # Try to claim new jobs if we have capacity
            while len(active_tasks) < MAX_CONCURRENT_JOBS:
                job = claim_next_job(db)
                if not job:
                    break

                # Skip if already being processed (race condition guard)
                if job.id in active_tasks:
                    logger.warning(f"[job {job.id}] already active, skipping")
                    continue

                logger.info(f"[job {job.id}] claimed -> processing (active={len(active_tasks)+1}/{MAX_CONCURRENT_JOBS})")
                task = asyncio.create_task(process_job_async(job.id, semaphore))
                active_tasks[job.id] = task

            # Heartbeat
            now = time.time()
            if now - last_heartbeat > 60:
                q, p, c, f = get_job_counts(db)
                logger.info(f"⏳ active={len(active_tasks)}/{MAX_CONCURRENT_JOBS} queued={q} processing={p} completed={c} failed={f}")
                last_heartbeat = now

            if not active_tasks:
                await asyncio.sleep(POLL_SECONDS)
            else:
                # Short sleep to allow task switching
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            traceback.print_exc(file=sys.stderr)
            await asyncio.sleep(POLL_SECONDS)

        finally:
            db.close()


def run_worker():
    """Entry point - runs the async worker."""
    asyncio.run(run_worker_async())


if __name__ == "__main__":
    run_worker()
