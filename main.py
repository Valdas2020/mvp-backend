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
import uuid
import sys

app = FastAPI()

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ЗАПУСК ---
@app.on_event("startup")
def on_startup():
    init_db()
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

# S3/R2 Клиент
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY")
)
BUCKET = os.getenv("R2_BUCKET")

# --- ЭНДПОИНТЫ ---

@app.post("/auth/send-otp")
def send_otp(email: str, db: Session = Depends(get_db)):
    # 1. Генерируем код
    code = str(random.randint(100000, 999999))
    
    # 2. Сохраняем в базу
    db_otp = OTP(email=email, code=code)
    db.add(db_otp)
    db.commit()
    
    # 3. ПЕЧАТАЕМ КОД В ЛОГИ (Для отладки и входа без почты)
    print(f"!!! LOGIN CODE FOR {email}: {code} !!!", file=sys.stderr)
    
    # 4. Пытаемся отправить Email (в блоке try, чтобы не ронять сервер при ошибке)
    try:
        gmail_user = os.getenv("GMAIL_USER")
        gmail_password = os.getenv("GMAIL_APP_PASSWORD")
        
        if gmail_user and gmail_password:
            msg = MIMEText(f"Your login code: {code}")
            msg['Subject'] = "Your Login Code"
            msg['From'] = gmail_user
            msg
