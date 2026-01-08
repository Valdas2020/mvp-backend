import os
import time
from sqlalchemy.orm import Session
from models import init_db, SessionLocal, Job, User, Usage
import boto3
from openai import OpenAI
from datetime import datetime

# Setup
init_db()

# S3 / R2 Setup
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY")
)
BUCKET = os.getenv("R2_BUCKET")

# RouteLLM Setup
client = OpenAI(
    base_url="https://routellm.abacus.ai/v1",
    api_key=os.getenv("ROUTELLM_API_KEY")
)

def process_job(job_id):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    
    if not job:
        db.close()
        return

    try:
        print(f"Processing job {job_id}...")
        job.status = "processing"
        db.commit()

        # 1. Download Input
        local_input = f"/tmp/{job.input_filename}"
        s3.download_file(BUCKET, job.input_key, local_input)

        # 2. Extract Text (Placeholder)
        text_content = "This is a placeholder text extracted from the book."
        word_count = len(text_content.split())
        job.in_words = word_count
        
        # Update Usage
        period = datetime.utcnow().strftime("%Y-%m")
        usage = db.query(Usage).filter(Usage.user_id == job.user_id, Usage.period == period).first()
        if not usage:
            usage = Usage(user_id=job.user_id, period=period, words_used=0)
            db.add(usage)
        usage.words_used += word_count
        db.commit()

        # 3. Translate via RouteLLM
        response = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[
                {"role": "system", "content": "You are a translator. Translate to Russian."},
                {"role": "user", "content": f"Translate this: {job.input_filename}"}
            ]
        )
        translated_text = response.choices[0].message.content

        # 4. Generate Output
        output_filename = f"translated_{job.input_filename}.txt"
        local_output = f"/tmp/{output_filename}"
        with open(local_output, "w") as f:
            f.write(f"Translated Book: {translated_text}\n\n(Full content processing coming in update)")

        # 5. Upload Output
        output_key = f"outputs/{job.user_id}/{output_filename}"
        s3.upload_file(local_output, BUCKET, output_key)

        job.output_key_pdf = output_key
        job.status = "done"
        db.commit()
        print(f"Job {job_id} done.")

    except Exception as e:
        job.status = "error"
        job.error_msg = str(e)
        db.commit()
        print(f"Error processing job {job_id}: {e}")
    finally:
        db.close()

def run_worker():
    print("Worker started (Polling DB)...")
    while True:
        db = SessionLocal()
        # Find oldest queued job
        job = db.query(Job).filter(Job.status == "queued").order_by(Job.created_at.asc()).first()
        
        if job:
            job_id = job.id
            db.close() # Close session before processing to avoid locks
            process_job(job_id)
        else:
            db.close()
            time.sleep(5) # Sleep if no jobs

if __name__ == '__main__':
    run_worker()