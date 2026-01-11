import os
import time
from datetime import datetime, timedelta
import jwt
import boto3
from botocore.client import Config
from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
import smtplib
from email.mime.text import MIMEText
import random
import requests

from models import SessionLocal, User, InviteCode, OTP, Job, Usage

# --- НАСТРОЙКИ ---
JWT_SECRET = os.getenv("JWT_SECRET", "your-secret-key-change-in-production")
GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# S3/R2 клиент
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='auto'
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def send_email(to_email: str, subject: str, body: str):
    """Отправка email через Gmail SMTP"""
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = GMAIL_USER
        msg["To"] = to_email

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"Email send failed: {e}")
        return False

def generate_otp():
    """Генерация 6-значного OTP"""
    return str(random.randint(100000, 999999))

def create_jwt(user_id: int, email: str, name: str):
    """Создание JWT токена"""
    payload = {
        "user_id": user_id,
        "email": email,
        "name": name,
        "exp": datetime.utcnow() + timedelta(days=30)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def decode_user_id(token: str):
    """Декодирование JWT и извлечение user_id"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return payload["user_id"]
    except:
        raise HTTPException(status_code=401, detail="Invalid token")

# --- PYDANTIC MODELS ---
class EmailRequest(BaseModel):
    email: str

class OTPVerifyRequest(BaseModel):
    email: str
    otp: str

class InviteActivateRequest(BaseModel):
    invite_code: str

class UploadRequest(BaseModel):
    filename: str

# --- ENDPOINTS ---

@app.get("/")
def root():
    return {"status": "ok", "service": "mvp-backend"}

@app.post("/auth/request-otp")
def request_otp(req: EmailRequest, db: Session = Depends(get_db)):
    """Шаг 1: Запрос OTP на email"""
    email = req.email.lower().strip()
    
    # Создаём или находим пользователя
    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(email=email, name=email.split("@")[0])
        db.add(user)
        db.commit()
        db.refresh(user)
    
    # Генерируем OTP
    otp_code = generate_otp()
    otp_entry = OTP(user_id=user.id, code=otp_code)
    db.add(otp_entry)
    db.commit()
    
    # Отправляем email
    send_email(
        email,
        "Your OTP Code",
        f"Your verification code is: {otp_code}\n\nValid for 10 minutes."
    )
    
    return {"message": "OTP sent"}

@app.post("/auth/verify-otp")
def verify_otp(req: OTPVerifyRequest, db: Session = Depends(get_db)):
    """Шаг 2: Проверка OTP и выдача JWT"""
    email = req.email.lower().strip()
    user = db.query(User).filter(User.email == email).first()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Проверяем OTP
    otp_entry = (
        db.query(OTP)
        .filter(
            OTP.user_id == user.id,
            OTP.code == req.otp,
            OTP.used == False,
            OTP.created_at > datetime.utcnow() - timedelta(minutes=10)
        )
        .first()
    )
    
    if not otp_entry:
        raise HTTPException(status_code=401, detail="Invalid or expired OTP")
    
    # Помечаем OTP как использованный
    otp_entry.used = True
    db.commit()
    
    # Создаём JWT
    token = create_jwt(user.id, user.email, user.name)
    
    return {
        "token": token,
        "user": {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "tier": user.tier
        }
    }

@app.post("/invite/activate")
def activate_invite(
    req: InviteActivateRequest,
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """Активация инвайт-кода"""
    user_id = decode_user_id(token)
    user = db.query(User).filter(User.id == user_id).first()
    
    if user.tier:
        raise HTTPException(status_code=400, detail="Already activated")
    
    invite = (
        db.query(InviteCode)
        .filter(
            InviteCode.code == req.invite_code,
            InviteCode.used_count < InviteCode.max_uses
        )
        .first()
    )
    
    if not invite:
        raise HTTPException(status_code=404, detail="Invalid or exhausted invite code")
    
    # Активируем тир
    user.tier = invite.tier
    invite.used_count += 1
    db.commit()
    
    return {"message": f"Tier {invite.tier} activated", "tier": invite.tier}

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """Загрузка файла и создание задачи"""
    user_id = decode_user_id(token)
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user.tier:
        raise HTTPException(status_code=403, detail="No active tier")
    
    # Проверка лимитов (упрощённо)
    # TODO: добавить проверку месячного лимита слов
    
    # Сохраняем в R2
    r2_key = f"inputs/{user_id}/{file.filename}"
    content = await file.read()
    
    try:
        s3.put_object(Bucket=R2_BUCKET, Key=r2_key, Body=content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")
    
    # Создаём задачу
    job = Job(
        user_id=user_id,
        filename=file.filename,
        r2_key_input=r2_key,
        status="queued"
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    return {"job_id": job.id, "status": "queued"}

@app.get("/jobs")
def list_jobs(token: str = Query(...), db: Session = Depends(get_db)):
    """Список задач пользователя"""
    user_id = decode_user_id(token)
    jobs = (
        db.query(Job)
        .filter(Job.user_id == user_id)
        .order_by(Job.created_at.desc())
        .all()
    )
    
    return {
        "jobs": [
            {
                "id": j.id,
                "filename": j.filename,
                "status": j.status,
                "word_count": j.word_count,
                "created_at": j.created_at.isoformat() if j.created_at else None
            }
            for j in jobs
        ]
    }

@app.get("/jobs/{job_id}/download")
def download_job(
    job_id: int,
    token: str = Query(...),
    db: Session = Depends(get_db),
):
    """Скачивание переведённого файла"""
    try:
        user_id = decode_user_id(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

    job = (
        db.query(Job)
        .filter(Job.id == job_id, Job.user_id == user_id)
        .first()
    )
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if not job.r2_key_output:
        raise HTTPException(status_code=404, detail="File not ready")

    try:
        # Генерируем presigned URL с форсированным скачиванием
        presigned_url = s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": R2_BUCKET,
                "Key": job.r2_key_output,
                "ResponseContentDisposition": f'attachment; filename="translated_{job.filename}.txt"'
            },
            ExpiresIn=3600,
        )
    except Exception as e:
        print(f"[DOWNLOAD] presign failed: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to generate download URL")

    # Вариант 1: Redirect (быстрый, но может открыть в браузере)
    # from fastapi.responses import RedirectResponse
    # return RedirectResponse(url=presigned_url, status_code=302)
    
    # Вариант 2: Proxy через backend (гарантированное скачивание)
    try:
        r = requests.get(presigned_url, stream=True, timeout=30)
        r.raise_for_status()
        
        return StreamingResponse(
            r.iter_content(chunk_size=8192),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="translated_{job.filename}.txt"'
            },
        )
    except Exception as e:
        print(f"[DOWNLOAD] streaming failed: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Download failed")

@app.get("/user/info")
def user_info(token: str = Query(...), db: Session = Depends(get_db)):
    """Информация о пользователе"""
    user_id = decode_user_id(token)
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "tier": user.tier
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
