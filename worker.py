import os
import time
import sys
import gc
import boto3
import pdfplumber
import requests
from sqlalchemy.orm import Session
from models import SessionLocal, Job, User, Usage
from botocore.client import Config

# --- НАСТРОЙКИ ---
R2_BUCKET = os.getenv("R2_BUCKET")
ROUTELLM_API_KEY = os.getenv("ROUTELLM_API_KEY")
ROUTELLM_URL = "https://routellm.abacus.ai/v1/chat/completions"

# МЕНЯЕМ МОДЕЛЬ НА НАДЕЖНУЮ
MODEL = "gpt-4o-mini" 

# S3/R2 Клиент
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
    config=Config(signature_version='s3v4'),
    region_name='auto'
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def translate_text(text):
    """Отправляет текст в RouteLLM"""
    if not text or len(text.strip()) < 5:
        return ""
        
    headers = {
        "Authorization": f"Bearer {ROUTELLM_API_KEY}",
        "Content-Type": "application/json"
    }
    
    system_prompt = (
        "You are a professional translator. Translate the following text from English to Russian. "
        "Keep the original formatting, line breaks, and structure exactly as they are. "
        "Do not add any explanations, just the translation."
    )
    
    data = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text}
        ],
        "temperature": 0.3
    }
    
    try:
        resp = requests.post(ROUTELLM_URL, headers=headers, json=data, timeout=60)
        
        # --- ДИАГНОСТИКА ОШИБОК LLM ---
        if resp.status_code != 200:
            print(f"⚠️ LLM Error {resp.status_code}: {resp.text}", file=sys.stderr)
            
        resp.raise_for_status()
        return resp.json()['choices'][0]['message']['content']
    except Exception as e:
        print(f"❌ Translation Failed: {e}", file=sys.stderr)
        return text # Возвращаем оригинал, чтобы не ломать файл

def process_job(db: Session, job: Job):
    print(f"Processing Job {job.id}...", file=sys.stderr)
    
    local_filename = f"temp_{job.filename}"
    output_filename = f"{job.filename}.txt"
    
    try:
        s3.download_file(R2_BUCKET, job.r2_key_input, local_filename)
    except Exception as e:
        print(f"Download failed: {e}", file=sys.stderr)
        job.status = "failed"
        db.commit()
        return

    total_words = 0
    # Используем word_count как чекпойнт (номер последней обработанной страницы)
    last_page = job.word_count or 0
    
    try:
        # ОТКРЫВАЕМ ФАЙЛ ДЛЯ ЗАПИСИ (append mode, если возобновляем)
        mode = "a" if last_page > 0 else "w"
        with open(output_filename, mode, encoding="utf-8") as out_file:
            
            # Переоткрываем PDF батчами по 20 страниц для освобождения памяти
            with pdfplumber.open(local_filename) as pdf:
                total_pages = len(pdf.pages)
                print(f"Total pages: {total_pages} (resuming from page {last_page + 1})", file=sys.stderr)
            
            # Обрабатываем батчами по 20 страниц
            for batch_start in range(last_page, total_pages, 20):
                with pdfplumber.open(local_filename) as pdf:
                    batch_end = min(batch_start + 20, total_pages)
                    
                    for i in range(batch_start, batch_end):
                        page = pdf.pages[i]
                        text = page.extract_text()
                        
                        if text:
                            words = len(text.split())
                            total_words += words
                            
                            print(f"Translating page {i+1}/{total_pages}...", file=sys.stderr)
                            trans = translate_text(text)
                            
                            # ПИШЕМ СРАЗУ В ФАЙЛ (не в список!)
                            out_file.write(f"--- Page {i+1} ---\n{trans}\n\n")
                            out_file.flush()  # принудительно сбрасываем буфер
                        
                        # ЧЕКПОЙНТ каждые 5 страниц
                        if (i + 1) % 5 == 0:
                            job.word_count = i + 1  # сохраняем номер последней обработанной страницы
                            db.commit()
                
                # ОСВОБОЖДАЕМ ПАМЯТЬ после каждого батча
                gc.collect()
                print(f"🧹 Memory cleanup after batch ending at page {batch_end}", file=sys.stderr)
        
        # ЗАГРУЖАЕМ РЕЗУЛЬТАТ В R2
        r2_key_output = f"outputs/{job.user_id}/translated_{job.filename}.txt"
        s3.upload_file(output_filename, R2_BUCKET, r2_key_output)
        
        job.status = "completed"
        job.r2_key_output = r2_key_output
        job.word_count = total_pages  # финальное значение = общее количество страниц
        db.commit()
        print(f"✅ Job {job.id} COMPLETED! Total pages: {total_pages}", file=sys.stderr)
        
    except Exception as e:
        print(f"❌ Processing failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        job.status = "failed"
        db.commit()
    finally:
        # Удаляем временные файлы
        if os.path.exists(local_filename):
            try:
                os.remove(local_filename)
                print(f"🗑️ Removed temp input: {local_filename}", file=sys.stderr)
            except Exception as e:
                print(f"⚠️ Failed to remove {local_filename}: {e}", file=sys.stderr)
        
        # Удаляем output только если job завершён или провалился
        if job.status in ["completed", "failed"] and os.path.exists(output_filename):
            try:
                os.remove(output_filename)
                print(f"🗑️ Removed temp output: {output_filename}", file=sys.stderr)
            except Exception as e:
                print(f"⚠️ Failed to remove {output_filename}: {e}", file=sys.stderr)

def run_worker():
    print(f"Worker started with model {MODEL}... Waiting for jobs.", file=sys.stderr)
    while True:
        db = SessionLocal()
        try:
            # Берём самую старую задачу в очереди (FIFO)
            job = db.query(Job).filter(Job.status == "queued").order_by(Job.created_at.asc()).first()
            if job:
                job.status = "processing"
                db.commit()
                process_job(db, job)
            else:
                time.sleep(5)
        except Exception as e:
            print(f"Worker loop error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            time.sleep(5)
        finally:
            db.close()

if __name__ == "__main__":
    print("=" * 60, file=sys.stderr)
    print("🚀 PDF Translation Worker Starting", file=sys.stderr)
    print(f"📦 Model: {MODEL}", file=sys.stderr)
    print(f"🪣 R2 Bucket: {R2_BUCKET}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    run_worker()
