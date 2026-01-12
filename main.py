import os
import time
from datetime import datetime, timedelta
from uuid import uuid4

import jwt
import boto3
from botocore.client import Config
import requests

from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel

from models import SessionLocal, User, InviteCode, OTP, Job, Usage  # noqa: F401


# ----------------------------
# CONFIG
# ----------------------------
JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

# Basic sanity (helps catch misconfigured env early)
if not JWT_SECRET:
    raise RuntimeError("JWT_SECRET is missing")
if not R2_BUCKET:
    raise RuntimeError("R2_BUCKET is missing")
if not R2_ENDPOINT:
    raise RuntimeError("R2_ENDPOINT is missing")
if not R2_ACCESS_KEY:
    raise RuntimeError("R2_ACCESS_KEY is missing")
if not R2_SECRET_KEY:
    raise RuntimeError("R2_SECRET_KEY is missing")

app = FastAPI()

# ----------------------------
# CORS (MVP-stable)
# - We do NOT use cookies, so allow_credentials MUST be False
# - "*" removes CORS as a variable while you stabilize API
# ----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# ----------------------------
# R2 (S3 compatible)
# ----------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="auto",
)


# ----------------------------
# DB
# ----------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ----------------------------
# JWT helpers
# ----------------------------
def create_jwt(user_id: int, email: str, name: str = ""):
    payload = {
        "user_id": user_id,
        "email": email,
        "name": name or "",
        "exp": datetime.utcnow() + timedelta(days=30),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def decode_user_id(token: str) -> int:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return int(payload["user_id"])
    except Exception as e:
        print(f"[AUTH] JWT decode error: {e}", flush=True)
        raise HTTPException(status_code=401, detail="Invalid token")


# ----------------------------
# Schemas
# ----------------------------
class InviteLoginRequest(BaseModel):
    invite_code: str
    device_id: str | None = None


class InviteActivateRequest(BaseModel):
    invite_code: str


class EmailRequest(BaseModel):
    email: str


class OTPVerifyRequest(BaseModel):
    email: str
    otp: str


# ----------------------------
# Endpoints
# ----------------------------
@app.get("/")
def root():
    return {"status": "ok", "service": "mvp-backend", "version": "1.2"}


# ========================================
# INVITE-ONLY AUTH (MVP)
# ========================================
@app.post("/api/auth/invite-login")
def invite_login(req: InviteLoginRequest, db: Session = Depends(get_db)):
    code = (req.invite_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="invite_code required")

    device_id = (req.device_id or "").strip()
    if not device_id:
        # Backward compatible fallback: creates a new user each time.
        # Better UX is to pass device_id from frontend (stored in localStorage).
        device_id = f"anon_{uuid4().hex}"

    print(f"[INVITE-LOGIN] code={code} device_id={device_id}", flush=True)

    invite = (
        db.query(InviteCode)
        .filter(InviteCode.code == code)
        .first()
    )
    if not invite:
        raise HTTPException(status_code=401, detail="Invalid invite code")

    # If we already bound this device_id to a user, do NOT consume another use
    user_email = f"device_{device_id}@local"
    user = db.query(User).filter(User.email == user_email).first()
    if user:
        token = create_jwt(user.id, user.email, getattr(user, "name", "") or "")
        return {
            "token": token,
            "user": {
                "id": user.id,
                "email": user.email,
                "name": getattr(user, "name", "") if hasattr(user, "name") else "",
                "tier": getattr(user, "tier", None) if hasattr(user, "tier") else None,
            },
        }

    # Consume invite capacity only on first bind
    used_count = getattr(invite, "used_count", None)
    max_uses = getattr(invite, "max_uses", None)
    if used_count is None or max_uses is None:
        raise HTTPException(status_code=500, detail="Invite schema mismatch (used_count/max_uses missing)")

    if used_count >= max_uses:
        raise HTTPException(status_code=401, detail="Invite code exhausted")

    # Create user WITHOUT invalid kwargs
    user = User(email=user_email)

    # Set tier if column exists on model
    if hasattr(user, "tier"):
        setattr(user, "tier", getattr(invite, "tier", None))

    db.add(user)

    # increment used_count
    invite.used_count = invite.used_count + 1

    db.commit()
    db.refresh(user)

    token = create_jwt(user.id, user.email, getattr(user, "name", "") or "")

    return {
        "token": token,
        "user": {
            "id": user.id,
            "email": user.email,
            "name": getattr(user, "name", "") if hasattr(user, "name") else "",
            "tier": getattr(user, "tier", None) if hasattr(user, "tier") else None,
        },
    }


# ========================================
# OTP AUTH (disabled for MVP)
# ========================================
@app.post("/api/auth/request-otp")
def request_otp(_req: EmailRequest, _db: Session = Depends(get_db)):
    raise HTTPException(status_code=503, detail="OTP auth disabled. Use invite code.")


@app.post("/api/auth/verify-otp")
def verify_otp(_req: OTPVerifyRequest, _db: Session = Depends(get_db)):
    raise HTTPException(status_code=503, detail="OTP auth disabled. Use invite code.")


# ========================================
# Invite activation (optional / future)
# ========================================
@app.post("/api/invite/activate")
def activate_invite(req: InviteActivateRequest, token: str = Query(...), db: Session = Depends(get_db)):
    user_id = decode_user_id(token)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if hasattr(user, "tier") and getattr(user, "tier", None):
        raise HTTPException(status_code=400, detail="Already activated")

    invite = (
        db.query(InviteCode)
        .filter(
            InviteCode.code == req.invite_code,
            InviteCode.used_count < InviteCode.max_uses,
        )
        .first()
    )
    if not invite:
        raise HTTPException(status_code=404, detail="Invalid or exhausted invite code")

    if hasattr(user, "tier"):
        user.tier = invite.tier

    invite.used_count += 1
    db.commit()

    return {"message": "Activated", "tier": getattr(invite, "tier", None)}


# ========================================
# Upload / Jobs / Download
# ========================================
@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    token: str = Query(...),
    db: Session = Depends(get_db),
):
    user_id = decode_user_id(token)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if hasattr(user, "tier") and not getattr(user, "tier", None):
        raise HTTPException(status_code=403, detail="No active tier")

    r2_key = f"inputs/{user_id}/{int(time.time())}_{file.filename}"
    content = await file.read()

    try:
        s3.put_object(Bucket=R2_BUCKET, Key=r2_key, Body=content)
    except Exception as e:
        print(f"[UPLOAD] R2 put_object failed: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Upload failed")

    job = Job(
        user_id=user_id,
        filename=file.filename,
        r2_key_input=r2_key,
        status="queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    return {"job_id": job.id, "status": job.status}


@app.get("/api/jobs")
def list_jobs(token: str = Query(...), db: Session = Depends(get_db)):
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
                "created_at": j.created_at.isoformat() if j.created_at else None,
            }
            for j in jobs
        ]
    }


@app.get("/api/jobs/{job_id}/download")
def download_job(job_id: int, token: str = Query(...), db: Session = Depends(get_db)):
    user_id = decode_user_id(token)

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
        presigned_url = s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": R2_BUCKET,
                "Key": job.r2_key_output,
                "ResponseContentDisposition": f'attachment; filename="translated_{job.filename}.txt"',
            },
            ExpiresIn=3600,
        )
    except Exception as e:
        print(f"[DOWNLOAD] presign failed: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to generate download URL")

    # Proxy (forces download reliably)
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


@app.get("/api/user/info")
def user_info(token: str = Query(...), db: Session = Depends(get_db)):
    user_id = decode_user_id(token)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "id": user.id,
        "email": user.email,
        "name": getattr(user, "name", "") if hasattr(user, "name") else "",
        "tier": getattr(user, "tier", None) if hasattr(user, "tier") else None,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
