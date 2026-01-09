from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from models import init_db, SessionLocal, User, OTP, InviteCode, Job, Usage
from datetime import datetime, timedelta
import os
import random
import smtplib
from email.mime.text import MIMEText
import jwt
import boto3
from botocore.client import Config # <--- ВАЖНЫЙ ИМПОРТ
import uuid
import sys

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# S3/R2 Клиент (инициализируем глобально с SigV4)
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
    config=Config(signature_version='s3v4'), # <--- ВКЛЮЧАЕМ SigV4
    region_name='auto'
)
BUCKET = os.getenv("R2_BUCKET")

@app.on_event("startup")
def on_startup():
    init_db()
    
    # --- ДИАГНОСТИКА R2 (Оставляем, полезно) ---
    print(f"\n--- R2 DIAGNOSTICS ---", file=sys.stderr)
    print(f"Endpoint: {os.getenv('R2_ENDPOINT')}", file=sys.stderr)
    try:
        response = s3.list_buckets()
        print("✅ Connection Successful!", file=sys.stderr)
    except Exception as e:
        print(f"❌ CONNECTION FAILED: {e}", file=sys.stderr)
    print(f"----------------------\n", file=sys.stderr)

    db = SessionLocal()
    try:
        if not db.query(InviteCode).first():
            print("Creating default invite codes...", file=sys.stderr)
            db.add(InviteCode(code="START_S_20", tier="S", quota_words=60000, max_uses=20))
            db.add(InviteCode(code="READER_M_20", tier="M", quota_words=200000, max_uses=20))
            db.commit()
    except Exception as e:
        print(f"Error creating invite codes: {e}", file=sys.stderr)
    finally:
        db.close()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

JWT_SECRET = os.getenv("JWT_SECRET", "secret_key_change_me")

# --- ЭНДПОИНТЫ ---

@app.post("/auth/send-otp")
def send_otp(email: str, db: Session = Depends(get_db)):
    code = str(random.randint(100000, 999999))
    db_otp = OTP(email=email, code=code)
    db.add(db_otp)
    db.commit()
    
    print(f"!!! LOGIN CODE FOR {email}: {code} !!!", file=sys.stderr)
    
    gmail_user = os.getenv("GMAIL_USER")
    gmail_password = os.getenv("GMAIL_APP_PASSWORD")
    
    if gmail_user and gmail_password:
        try:
            msg = MIMEText(f"Your login code: {code}")
            msg['Subject'] = "Your Login Code"
            msg['From'] = gmail_user
            msg['To'] = email
            server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
            server.login(gmail_user, gmail_password)
            server.send_message(msg)
            server.quit()
        except Exception as e:
            print(f"Email failed (check logs): {e}", file=sys.stderr)
            
    return {"message": "OTP generated", "debug_code": code}

@app.post("/auth/verify-otp")
def verify_otp(email: str, code: str, db: Session = Depends(get_db)):
    otp_record = db.query(OTP).filter(OTP.email == email, OTP.code == code).order_by(OTP.id.desc()).first()
    if not otp_record:
        raise HTTPException(status_code=400, detail="Invalid code")
    
    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(email=email)
        db.add(user)
        db.commit()
        db.refresh(user)
    
    token = jwt.encode({"user_id": user.id, "email": user.email}, JWT_SECRET, algorithm="HS256")
    return {"token": token, "user": {"email": user.email, "plan": user.plan}}

@app.post("/users/activate-invite")
def activate_invite(token: str, code: str, db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload["user_id"]
    except:
        raise HTTPException(status_code=401, detail="Invalid token")
        
    invite = db.query(InviteCode).filter(InviteCode.code == code).first()
    if not invite:
        raise HTTPException(status_code=400, detail="Invalid code")
    if invite.used_count >= invite.max_uses:
        raise HTTPException(status_code=400, detail="Code fully used")
        
    user = db.query(User).filter(User.id == user_id).first()
    user.plan = invite.tier
    user.quota_words = invite.quota_words
    invite.used_count += 1
    db.commit()
    return {"plan": user.plan, "quota": user.quota_words}

@app.post("/jobs/upload")
async def upload_file(token: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload["user_id"]
    except:
        raise HTTPException(status_code=401, detail="Invalid token")
        
    file_content = await file.read()
    filename = f"{uuid.uuid4()}_{file.filename}"
    r2_key = f"uploads/{user_id}/{filename}"
    
    s3.put_object(Bucket=BUCKET, Key=r2_key, Body=file_content)
    
    job = Job(
        user_id=user_id,
        filename=file.filename,
        r2_key_input=r2_key,
        status="queued"
    )
    db.add(job)
    db.commit()
    
    return {"job_id": job.id, "status": "queued"}

@app.get("/jobs")
def list_jobs(token: str, db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload["user_id"]
    except:
        raise HTTPException(status_code=401, detail="Invalid token")
    jobs = db.query(Job).filter(Job.user_id == user_id).order_by(Job.created_at.desc()).all()
    return jobs

@app.get("/jobs/{job_id}/download")
def download_job(job_id: int, token: str, db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except:
        raise HTTPException(status_code=401, detail="Invalid token")
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job or not job.r2_key_output:
        raise HTTPException(status_code=404, detail="File not ready")
    
    # Генерируем ссылку с SigV4
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': BUCKET, 'Key': job.r2_key_output},
        ExpiresIn=3600
    )
    return {"url": url}
