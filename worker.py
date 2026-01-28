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

from models import SessionLocal, Job, User  # User needed for max_pages check


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

# LLM Provider: "deepseek" or "routellm"
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "deepseek")

# DeepSeek API
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_URL = os.getenv("DEEPSEEK_URL", "https://api.deepseek.com/chat/completions")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# RouteLLM API (fallback)
ROUTELLM_API_KEY = os.getenv("ROUTELLM_API_KEY")
ROUTELLM_URL = os.getenv("ROUTELLM_URL", "https://routellm.abacus.ai/v1/chat/completions")
ROUTELLM_MODEL = os.getenv("ROUTELLM_MODEL", "gpt-4o-mini")

POLL_SECONDS = int(os.getenv("WORKER_POLL_SECONDS", "5"))
PDF_BATCH_SIZE = int(os.getenv("PDF_BATCH_SIZE", "20"))
CHECKPOINT_EVERY_PAGES = 1  # Будет обновлять статус в базе после каждой страницы

LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "180"))
LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))

# Very important to avoid sending gigantic page text in one shot
MAX_CHARS_PER_CHUNK = int(os.getenv("MAX_CHARS_PER_CHUNK", "12000"))

# If a job is "processing" for too long, re-queue it
STALE_PROCESSING_MINUTES = int(os.getenv("STALE_PROCESSING_MINUTES", "60"))

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

# Улучшенный промпт с контекстом для склейки страниц
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

# Сколько символов контекста брать с предыдущей страницы
CONTEXT_CHARS = int(os.getenv("CONTEXT_CHARS", "500"))

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

    # Проверяем API ключ в зависимости от провайдера
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


def get_page_tail(text: str, max_chars: int = CONTEXT_CHARS) -> str:
    """
    Извлекает последние N символов текста для использования как контекст.
    Старается не обрезать слова посередине.
    """
    if not text or len(text) <= max_chars:
        return text

    # Берём последние max_chars символов
    tail = text[-max_chars:]

    # Пытаемся найти начало слова (пробел)
    space_idx = tail.find(' ')
    if space_idx > 0 and space_idx < len(tail) // 2:
        tail = tail[space_idx + 1:]

    return tail.strip()


def get_page_head(text: str, max_chars: int = 200) -> str:
    """
    Извлекает первые N символов текста для использования как lookahead.
    Старается не обрезать слова посередине.
    """
    if not text or len(text) <= max_chars:
        return text

    # Берём первые max_chars символов
    head = text[:max_chars]

    # Пытаемся найти конец слова (пробел с конца)
    space_idx = head.rfind(' ')
    if space_idx > len(head) // 2:
        head = head[:space_idx]

    return head.strip()


# ----------------------------
# RouteLLM call with retries
# ----------------------------
def translate_chunk(chunk: str, req_id: str, context: str = None, lookahead: str = None) -> str:
    """
    Returns translated text. On failure returns original chunk (so pipeline doesn't break).
    context: опциональный контекст с предыдущей страницы для понимания разорванных предложений
    lookahead: опциональное начало следующей страницы для правильного согласования окончаний
    """
    if not chunk or len(chunk.strip()) < 5:
        return ""

    # Выбираем провайдера
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

    # Формируем сообщение пользователя с контекстом и/или lookahead
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
            resp = requests.post(
                api_url,
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
            logger.info(f"[{req_id}] LLM ok ({LLM_PROVIDER}/{model}) in {dt:.2f}s (chars_in={len(chunk)}, chars_out={len(out)})")
            return out

        except Exception as e:
            last_err = e
            logger.warning(f"[{req_id}] LLM error (attempt {attempt}/{LLM_MAX_RETRIES}): {e}")
            if attempt < LLM_MAX_RETRIES:
                time.sleep(1.5 * attempt)  # small backoff

    logger.error(f"[{req_id}] LLM failed after retries; returning original chunk. Last error: {last_err}")
    return chunk


def translate_text(text: str, req_id: str, context: str = None, lookahead: str = None) -> str:
    """
    Переводит текст с опциональным контекстом предыдущей страницы и lookahead следующей.
    Контекст передаётся только в первый чанк, lookahead — только в последний.
    """
    chunks = chunk_text_preserving_lines(text, MAX_CHARS_PER_CHUNK)
    if len(chunks) == 1:
        return translate_chunk(chunks[0], req_id, context=context, lookahead=lookahead)

    out_parts = []
    for idx, ch in enumerate(chunks, start=1):
        part_id = f"{req_id}.c{idx}/{len(chunks)}"
        # Контекст передаём только в первый чанк
        chunk_context = context if idx == 1 else None
        # Lookahead передаём только в последний чанк
        chunk_lookahead = lookahead if idx == len(chunks) else None
        out_parts.append(translate_chunk(ch, part_id, context=chunk_context, lookahead=chunk_lookahead))
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

        # Check user's max_pages limit (for trial codes)
        user = db.query(User).filter(User.id == job.user_id).first()
        user_max_pages = None
        if user:
            logger.info(f"[{req_id}] User id={user.id} plan={getattr(user, 'plan', None)} max_pages={getattr(user, 'max_pages', None)}")
            # Check explicit max_pages limit
            if hasattr(user, "max_pages") and user.max_pages is not None:
                user_max_pages = user.max_pages
            # Fallback: TRIAL plan always limited to 5 pages (note: field is "plan" not "tier")
            elif hasattr(user, "plan") and user.plan == "TRIAL":
                user_max_pages = 5
                logger.info(f"[{req_id}] TRIAL user without max_pages, defaulting to 5")

        if user_max_pages is not None and total_pages > user_max_pages:
            logger.info(f"[{req_id}] Limiting pages from {total_pages} to {user_max_pages}")
            total_pages = user_max_pages

        # Write output progressively
        # Храним контекст (хвост) предыдущей страницы для склейки предложений
        prev_page_tail = ""

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
                            prev_page_tail = ""  # Сбрасываем контекст для пустых страниц
                        else:
                            # Передаём контекст предыдущей страницы для склейки предложений
                            context = prev_page_tail if prev_page_tail else None

                            # Получаем lookahead (начало следующей страницы) для правильного согласования
                            lookahead = None
                            if i + 1 < total_pages:
                                try:
                                    next_page = pdf.pages[i + 1]
                                    next_text = next_page.extract_text() or ""
                                    if next_text.strip():
                                        lookahead = get_page_head(next_text, 200)
                                except Exception as e:
                                    logger.warning(f"[{page_id}] failed to get lookahead: {e}")

                            # Логируем информацию о контексте
                            ctx_info = []
                            if context:
                                ctx_info.append(f"context={len(context)}")
                            if lookahead:
                                ctx_info.append(f"lookahead={len(lookahead)}")
                            ctx_str = f" with {', '.join(ctx_info)}" if ctx_info else ""
                            logger.info(f"[{page_id}] translating (chars={len(text)}){ctx_str}")

                            translated = translate_text(text, page_id, context=context, lookahead=lookahead)
                            out.write(f"--- Page {page_no} ---\n{translated}\n\n")
                            out.flush()

                            # Сохраняем хвост текущей страницы для следующей
                            prev_page_tail = get_page_tail(text, CONTEXT_CHARS)

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
    if LLM_PROVIDER == "deepseek":
        logger.info(f"📦 Provider: DeepSeek, Model: {DEEPSEEK_MODEL}")
        logger.info(f"🌐 API URL: {DEEPSEEK_URL}")
    else:
        logger.info(f"📦 Provider: RouteLLM, Model: {ROUTELLM_MODEL}")
        logger.info(f"🌐 API URL: {ROUTELLM_URL}")
    logger.info(f"🪣 R2 Bucket: {R2_BUCKET}")
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
