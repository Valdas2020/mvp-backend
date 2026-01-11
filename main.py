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
from botocore.client import Config
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
    config=Config(signature_version='s3v4'),
    region_name='auto'
)
BUCKET = os.getenv("R2_BUCKET")

@app.on_event("startup")
def on_startup():
    init_db()
    
    db = SessionLocal()
    try:
        # Проверяем, есть ли наш конкретный код
        target_code = "ALPHA_M_30"
        exists = db.query(InviteCode).filter(InviteCode.code == target_code).first()
        
        if not exists:
            print(f"Creating code {target_code}...", file=sys.stderr)
            db.add(InviteCode(
                code=target_code, 
                tier="M", 
                quota_words=200000, 
                max_uses=100  # Сделаем 100 использований, чтобы не мучаться
            ))
            db.commit()
            print(f"✅ Code {target_code} created successfully!", file=sys.stderr)
        else:
            print(f"ℹ️ Code {target_code} already exists in this DB.", file=sys.stderr)
            
    except Exception as e:
        print(f"❌ Error in on_startup: {e}", file=sys.stderr)
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

@app.post("/auth/alpha-login")
def alpha_login(invite_code: str = Form(...), name: str = Form(...), email: str = Form(...), db: Session = Depends(get_db)):
    raw = invite_code
    cleaned = invite_code.strip()
    normalized = cleaned.upper()

    print(f"[ALPHA LOGIN] raw='{raw}' cleaned='{cleaned}' normalized='{normalized}'", flush=True)

    # Диагностика: сколько кодов видит backend
    total_codes = db.query(InviteCode).count()
    print(f"[DB CHECK] Total invite_codes in backend DB: {total_codes}", flush=True)
    
    # Покажем первые 3 кода
    sample = db.query(InviteCode).limit(3).all()
    print(f"[DB CHECK] Sample codes: {[c.code for c in sample]}", flush=True)

    invite = db.query(InviteCode).filter(InviteCode.code == normalized).first()

    if not invite:
        raise HTTPException(status_code=400, detail="Неверный код приглашения")

    if invite.used_count >= invite.max_uses:
        raise HTTPException(status_code=400, detail="Код полностью использован")

    user = User(
        email=email,
        plan=invite.tier,
        quota_words=invite.quota_words
    )
    db.add(user)

    invite.used_count += 1
    db.commit()
    db.refresh(user)

    token = jwt.encode({
        "user_id": user.id,
        "email": user.email,
        "name": name,
        "exp": datetime.utcnow() + timedelta(days=1)
    }, JWT_SECRET, algorithm="HS256")

    return {"token": token, "user": {"email": user.email, "plan": user.plan, "name": name}}
  
    
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
