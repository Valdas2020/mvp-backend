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

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def on_startup():
    init_db()
    db = SessionLocal()
    if not db.query(InviteCode).first():
        db.add(InviteCode(code="START_S_20", tier="S", quota_words=60000, max_uses=20))
        db.add(InviteCode(code="READER_M_20", tier="M", quota_words=200000, max_uses=20))
        db.commit()
    db.close()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

JWT_SECRET = os.getenv("JWT_SECRET", "secret")

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY")
)
BUCKET = os.getenv("R2_BUCKET")

# --- Auth Endpoints ---

@app.post("/auth/send-otp")
def send_otp(email: str, db: Session = Depends(get_db)):
    code = str(random.randint(100000, 999999))
    expires = datetime.utcnow() + timedelta(minutes=10)
    
    db_otp = OTP(email=email, code=code, expires_at=expires)
    db.add(db_otp)
    db.commit()
    
    try:
        msg = MIMEText(f"Ваш код входа: {code}")
        msg['Subject'] = "Код подтверждения MVP Translator"
        msg['From'] = os.getenv("GMAIL_USER")
        msg['To'] = email

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(os.getenv("GMAIL_USER"), os.getenv("GMAIL_APP_PASSWORD"))
            server.send_message(msg)
    except Exception as e:
        print(f"Email error: {e}")
        raise HTTPException(status_code=500, detail="Failed to send email")
        
    return {"message": "OTP sent"}

@app.post("/auth/verify-otp")
def verify_otp(email: str, code: str, db: Session = Depends(get_db)):
    otp_record = db.query(OTP).filter(OTP.email == email, OTP.code == code).first()
    if not otp_record or otp_record.expires_at < datetime.utcnow():
        raise HTTPException(status_code=400, detail="Invalid or expired code")
    
    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(email=email)
        db.add(user)
        db.commit()
    
    token = jwt.encode({"sub": user.email, "id": user.id}, JWT_SECRET, algorithm="HS256")
    
    db.delete(otp_record)
    db.commit()
    
    return {"token": token, "user": {"email": user.email, "plan": user.plan}}

# --- User & Invites ---

@app.get("/users/me")
def get_me(token: str, db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user = db.query(User).filter(User.id == payload["id"]).first()
        
        period = datetime.utcnow().strftime("%Y-%m")
        usage = db.query(Usage).filter(Usage.user_id == user.id, Usage.period == period).first()
        used = usage.words_used if usage else 0
        
        return {
            "email": user.email, 
            "plan": user.plan, 
            "quota": user.quota_words,
            "used": used
        }
    except:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/users/activate-invite")
def activate_invite(token: str, code: str, db: Session = Depends(get_db)):
    payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    user = db.query(User).filter(User.id == payload["id"]).first()
    
    invite = db.query(InviteCode).filter(InviteCode.code == code).first()
    if not invite:
        raise HTTPException(status_code=404, detail="Code not found")
    if invite.current_uses >= invite.max_uses:
        raise HTTPException(status_code=400, detail="Code fully used")
        
    user.plan = invite.tier
    user.quota_words = invite.quota_words
    invite.current_uses += 1
    
    db.commit()
    return {"status": "activated", "plan": user.plan}

# --- Jobs ---

@app.post("/jobs/upload")
def upload_book(
    file: UploadFile = File(...), 
    token: str = Form(...),
    glossary: str = Form(None),
    db: Session = Depends(get_db)
):
    payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    user_id = payload["id"]
    
    file_ext = file.filename.split('.')[-1]
    key = f"inputs/{user_id}/{uuid.uuid4()}.{file_ext}"
    s3.upload_fileobj(file.file, BUCKET, key)
    
    job = Job(
        user_id=user_id,
        input_filename=file.filename,
        input_key=key,
        status="queued"
    )
    db.add(job)
    db.commit()
    
    # No Redis enqueue needed, worker polls DB
    
    return {"job_id": job.id, "status": "queued"}

@app.get("/jobs")
def list_jobs(token: str, db: Session = Depends(get_db)):
    payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    jobs = db.query(Job).filter(Job.user_id == payload["id"]).order_by(Job.created_at.desc()).all()
    return jobs

@app.get("/jobs/{job_id}/download")
def download_link(job_id: int, token: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job or not job.output_key_pdf:
        raise HTTPException(status_code=404, detail="File not ready")
        
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': BUCKET, 'Key': job.output_key_pdf},
        ExpiresIn=3600
    )
    
    job.downloaded_at = datetime.utcnow()
    db.commit()
    
    return {"url": url}