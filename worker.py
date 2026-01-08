import os
import time
import json
import httpx  # <--- Добавили этот импорт
from sqlalchemy.orm import Session
from models import SessionLocal, Job, Usage, User
from openai import OpenAI
import boto3
from botocore.exceptions import NoCredentialsError
import pypdf
import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import io

# Настройки (можно брать из os.getenv напрямую для простоты воркера)
ROUTELLM_API_KEY = os.getenv("ROUTELLM_API_KEY")
ROUTELLM_BASE_URL = os.getenv("ROUTELLM_BASE_URL", "https://routellm.abacus.ai/v1")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")

# --- ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ ---

# 1. S3/R2 Клиент
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

# 2. OpenAI/RouteLLM Клиент (С "ЛЕЧЕНИЕМ" ОШИБКИ PROXIES)
# Мы создаем http_client вручную, чтобы библиотека OpenAI не пыталась 
# сама создавать его с неправильными параметрами.
client = OpenAI(
    api_key=ROUTELLM_API_KEY,
    base_url=ROUTELLM_BASE_URL,
    http_client=httpx.Client() # <--- Вот это лечит ошибку
)

def process_job(job_id: int):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    
    if not job:
        print(f"Job {job_id} not found")
        db.close()
        return

    try:
        print(f"Starting job {job_id} for file {job.filename}")
        job.status = "processing"
        db.commit()

        # 1. Скачиваем файл из R2
        file_stream = io.BytesIO()
        s3.download_fileobj(R2_BUCKET, job.r2_key_input, file_stream)
        file_stream.seek(0)
        
        # 2. Извлекаем текст (Упрощенно для MVP: только PDF)
        text_content = ""
        if job.filename.lower().endswith(".pdf"):
            reader = pypdf.PdfReader(file_stream)
            for page in reader.pages:
                text_content += page.extract_text() + "\n"
        elif job.filename.lower().endswith(".epub"):
             # Заглушка для EPUB, если вдруг загрузят
             text_content = "EPUB parsing not implemented in MVP-lite yet."
        else:
             text_content = "Unsupported format."

        # 3. Перевод (Упрощенно: берем первые 1000 символов для теста)
        # В реальности тут будет цикл по чанкам
        chunk = text_content[:2000] 
        
        response = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[
                {"role": "system", "content": "You are a professional translator. Translate the following text to Russian, keeping formatting."},
                {"role": "user", "content": chunk}
            ]
        )
        translated_text = response.choices[0].message.content

        # 4. Сохраняем результат (просто текстовый файл для MVP)
        output_filename = f"translated_{job.filename}.txt"
        output_stream = io.BytesIO(translated_text.encode('utf-8'))
        
        output_key = f"outputs/{job.user_id}/{output_filename}"
        s3.upload_fileobj(output_stream, R2_BUCKET, output_key)

        # 5. Обновляем статус
        job.status = "completed"
        job.r2_key_output = output_key
        db.commit()
        print(f"Job {job_id} completed successfully.")

    except Exception as e:
        print(f"Error processing job {job_id}: {e}")
        job.status = "failed"
        db.commit()
    finally:
        db.close()

def run_worker():
    print("Worker started. Polling for jobs...")
    while True:
        db = SessionLocal()
        # Ищем задачу со статусом 'queued'
        job = db.query(Job).filter(Job.status == "queued").first()
        
        if job:
            job_id = job.id
            # Сразу меняем статус, чтобы другие воркеры (если будут) не взяли
            # Но для надежности лучше делать это внутри транзакции с row locking, 
            # для MVP просто берем и закрываем сессию, передавая ID в функцию
            db.close()
            process_job(job_id)
        else:
            db.close()
            time.sleep(5) # Спим 5 секунд, если нет задач

if __name__ == "__main__":
    run_worker()
