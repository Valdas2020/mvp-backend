from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models import init_db, SessionLocal, User, OTP, InviteCode, Job, Usage
from datetime import datetime, timedelta
import os
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

# S3/R2 Client (SigV4)
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
    config=Config(signature_version="s3v4"),
    region_name="auto",
)
BUCKET = os.getenv("R2_BUCKET")

JWT_SECRET = os.getenv("JWT_SECRET", "secret_key_change_me")


@app.on_event("startup")
def on_startup():
    init_db()

    # R2 diagnostics (оставляем)
    print("\n--- R2 DIAGNOSTICS ---", file=sys.stderr)
    print(f"Endpoint: {os.getenv('R2_ENDPOINT')}", file=sys.stderr)
    try:
        s3.list_buckets()
        print("✅ Connection Successful!", file=sys.stderr)
    except Exception as e:
        print(f"❌ CONNECTION FAILED: {e}", file=sys.stderr)
    print("----------------------\n", file=sys.stderr)

    # bootstrap default invites if table is empty
    db = SessionLocal()
    try:
        if not db.query(InviteCode).first():
            print("Creating default invite codes...", file=sys.stderr)
            db.add(InviteCode(code="START_S_20", tier="S", quota_words=60000, max_uses=20))
            db.add(InviteCode(code="READER_M_20", tier="M", quota_words=200000, max_uses=20))
            db.commit()
    except Exception as e:
        print(f"Error creating invite codes: {e}", file=sys.stderr)
        db.rollback()
    finally:
        db.close()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- ENDPOINTS ---

@app.post("/auth/alpha-login")
def alpha_login(
    invite_code: str = Form(...),
    name: str = Form(...),
    email: str = Form(...),
    db: Session = Depends(get_db),
):
    code = (invite_code or "").strip().upper()
    email_norm = (email or "").strip().lower()
    name_norm = (name or "").strip()

    if not code:
        raise HTTPException(status_code=400, detail="Неверный код приглашения")
    if not email_norm:
        raise HTTPException(status_code=400, detail="Email обязателен")

    # 1) invite must exist (for gating)
    invite = db.query(InviteCode).filter(InviteCode.code == code).first()
    if not invite:
        raise HTTPException(status_code=400, detail="Неверный код приглашения")

    # 2) find existing user by unique email
    user = db.query(User).filter(User.email == email_norm).first()

    # If user exists: allow login (do not try to re-insert)
    if user:
        # Optional: upgrade plan/quota if invite is better AND invite still has uses
        needs_upgrade = (user.plan != invite.tier) or (user.quota_words != invite.quota_words)
        if needs_upgrade and invite.used_count < invite.max_uses:
            user.plan = invite.tier
            user.quota_words = invite.quota_words
            invite.used_count += 1
            db.commit()
            db.refresh(user)

        token = jwt.encode(
            {
                "user_id": user.id,
                "email": user.email,
                "name": name_norm,
                "exp": datetime.utcnow() + timedelta(days=1),
            },
            JWT_SECRET,
            algorithm="HS256",
        )
        return {"token": token, "user": {"email": user.email, "plan": user.plan, "name": name_norm}}

    # New user: require invite capacity
    if invite.used_count >= invite.max_uses:
        raise HTTPException(status_code=400, detail="Код полностью использован")

    # Create user + consume invite (transaction-safe)
    try:
        user = User(email=email_norm, plan=invite.tier, quota_words=invite.quota_words)
        db.add(user)
        invite.used_count += 1
        db.commit()
        db.refresh(user)
    except IntegrityError:
        # Race or prior creation: rollback and fetch existing user
        db.rollback()
        user = db.query(User).filter(User.email == email_norm).first()
        if not user:
            raise HTTPException(status_code=500, detail="DB error")

    token = jwt.encode(
        {
            "user_id": user.id,
            "email": user.email,
            "name": name_norm,
            "exp": datetime.utcnow() + timedelta(days=1),
        },
        JWT_SECRET,
        algorithm="HS256",
    )
    return {"token": token, "user": {"email": user.email, "plan": user.plan, "name": name_norm}}


@app.post("/users/activate-invite")
def activate_invite(token: str, code: str, db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload["user_id"]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    code_norm = (code or "").strip().upper()
    invite = db.query(InviteCode).filter(InviteCode.code == code_norm).first()
    if not invite:
        raise HTTPException(status_code=400, detail="Invalid code")
    if invite.used_count >= invite.max_uses:
        raise HTTPException(status_code=400, detail="Code fully used")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.plan = invite.tier
    user.quota_words = invite.quota_words
    invite.used_count += 1
    db.commit()
    return {"plan": user.plan, "quota": user.quota_words}


@app.post("/jobs/upload")
async def upload_file(token: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    print(f"[UPLOAD] Start uploading file: {file.filename}", file=sys.stderr)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload["user_id"]
        print(f"[UPLOAD] User ID: {user_id}", file=sys.stderr)
    except Exception as e:
        print(f"[UPLOAD] JWT Error: {e}", file=sys.stderr)
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        file_content = await file.read()
        filename = f"{uuid.uuid4()}_{file.filename}"
        r2_key = f"uploads/{user_id}/{filename}"

        print(f"[UPLOAD] Attempting R2 upload to key: {r2_key}", file=sys.stderr)
        
        s3.put_object(
            Bucket=BUCKET, 
            Key=r2_key, 
            Body=file_content,
            ContentType=file.content_type
        )
        print(f"[UPLOAD] R2 Upload Successful", file=sys.stderr)

        job = Job(
            user_id=user_id,
            filename=file.filename,
            r2_key_input=r2_key,
            status="queued",
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        
        print(f"[UPLOAD] Job created in DB with ID: {job.id}", file=sys.stderr)
        return {"job_id": job.id, "status": "queued"}

    except Exception as e:
        print(f"[UPLOAD] CRITICAL ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs")
def list_jobs(token: str, db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload["user_id"]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    jobs = db.query(Job).filter(Job.user_id == user_id).order_by(Job.created_at.desc()).all()
    return jobs


@app.get("/jobs/{job_id}/download")
def download_job(job_id: int, token: str, db: Session = Depends(get_db)):
    try:
        jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    job = db.query(Job).filter(Job.id == job_id).first()
    if not job or not job.r2_key_output:
        raise HTTPException(status_code=404, detail="File not ready")

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": job.r2_key_output},
        ExpiresIn=3600,
    )
    return {"url": url}
