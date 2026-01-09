import os
import time
import sys
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

    translated_content = []
    total_words = 0
    
    try:
        with pdfplumber.open(local_filename) as pdf:
            total_pages = len(pdf.pages)
            print(f"Total pages: {total_pages}", file=sys.stderr)
            
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    words = len(text.split())
                    total_words += words
                    
                    print(f"Translating page {i+1}/{total_pages}...", file=sys.stderr)
                    trans = translate_text(text)
                    translated_content.append(f"--- Page {i+1} ---\n{trans}\n\n")
                
                if i % 5 == 0:
                    job.word_count = total_words
                    db.commit()

        with open(output_filename, "w", encoding="utf-8") as f:
            f.write("".join(translated_content))
            
        r2_key_output = f"outputs/{job.user_id}/translated_{job.filename}.txt"
        s3.upload_file(output_filename, R2_BUCKET, r2_key_output)
        
        job.status = "completed"
        job.r2_key_output = r2_key_output
        job.word_count = total_words
        db.commit()
        print(f"Job {job.id} COMPLETED!", file=sys.stderr)
        
    except Exception as e:
        print(f"Processing failed: {e}", file=sys.stderr)
        job.status = "failed"
        db.commit()
    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)
        if os.path.exists(output_filename):
             try: os.remove(output_filename)
             except: pass

def run_worker():
    print(f"Worker started with model {MODEL}... Waiting for jobs.", file=sys.stderr)
    while True:
        db = SessionLocal()
        try:
            job = db.query(Job).filter(Job.status == "queued").first()
            if job:
                job.status = "processing"
                db.commit()
                process_job(db, job)
            else:
                time.sleep(5)
        except Exception as e:
            print(f"Worker loop error: {e}", file=sys.stderr)
            time.sleep(5)
        finally:
            db.close()

if __name__ == "__main__":
    run_worker()
