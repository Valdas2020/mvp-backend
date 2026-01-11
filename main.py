import os
import sys
import uuid
from datetime import datetime, timedelta
from typing import Optional

import boto3
import jwt
from botocore.client import Config
from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    UploadFile,
    File,
    Form,
    Header,
    Query,
)
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models import init_db, SessionLocal, User, InviteCode, Job


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # MVP
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

JWT_SECRET = os.getenv("JWT_SECRET", "secret_key_change_me")

R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="auto",
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def extract_token(
    authorization: Optional[str],
    token_q: Optional[str],
    token_form: Optional[str],
) -> str:
    if authorization and authorization.startswith("Bearer "):
        return authorization[len("Bearer ") :].strip()
    if token_q:
        return token_q.strip()
    if token_form:
        return token_form.strip()
    raise HTTPException(
        status_code=401,
        detail="Missing token (send Authorization: Bearer ..., or ?token=..., or form field token).",
    )


def decode_user_id(token: str) -> int:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return int(payload["user_id"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


@app.on_event("startup")
def on_startup():
    init_db()

    # R2 diagnostics (полезно, оставим)
    print("\n--- R2 DIAGNOSTICS ---", file=sys.stderr)
    print(f"Endpoint: {R2_ENDPOINT}", file=sys.stderr)
    try:
        s3.list_buckets()
        print("✅ R2 Connection Successful!", file=sys.stderr)
    except Exception as e:
        print(f"❌ R2 CONNECTION FAILED: {e}", file=sys.stderr)
    print("----------------------\n", file=sys.stderr)

    # default invites (создаём только если таблица пустая)
    db = SessionLocal()
    try:
        if not db.query(InviteCode).first():
            db.add(InviteCode(code="START_S_20", tier="S", quota_words=60000, max_uses=20))
            db.add(InviteCode(code="READER_M_20", tier="M", quota_words=200000, max_uses=20))
            db.commit()
            print("Default invite codes created.", file=sys.stderr)
    except Exception as e:
        db.rollback()
        print(f"Error creating default invite codes: {e}", file=sys.stderr)
    finally:
        db.close()


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

    invite = db.query(InviteCode).filter(InviteCode.code == code).first()
    if not invite:
        raise HTTPException(status_code=400, detail="Неверный код приглашения")

    # если юзер уже есть — логиним без падения
    user = db.query(User).filter(User.email == email_norm).first()
    if user:
        # опционально: апгрейд по инвайту, если отличается, и есть лимит
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

    # новый юзер — тратим инвайт
    if invite.used_count >= invite.max_uses:
        raise HTTPException(status_code=400, detail="Код полностью использован")

    try:
        user = User(email=email_norm, plan=invite.tier, quota_words=invite.quota_words)
        db.add(user)
        invite.used_count += 1
        db.commit()
        db.refresh(user)
    except IntegrityError:
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


@app.get("/jobs")
def list_jobs(
    token: str = Query(...),
    db: Session = Depends(get_db),
):
    user_id = decode_user_id(token)
    jobs = db.query(Job).filter(Job.user_id == user_id).order_by(Job.created_at.desc()).all()
    return jobs


@app.post("/jobs/upload")
async def upload_file(
    # ВАЖНО: под твой page.js
    file: UploadFile = File(...),
    token: str = Form(...),
    # плюс запасной вход (если потом решишь перейти на Bearer или query)
    authorization: Optional[str] = Header(None),
    token_q: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    # если пришёл Bearer/query — они приоритетнее, иначе берём form token
    effective_token = None
    if authorization and authorization.startswith("Bearer "):
        effective_token = authorization[len("Bearer ") :].strip()
    elif token_q:
        effective_token = token_q.strip()
    else:
        effective_token = (token or "").strip()

    if not effective_token:
        raise HTTPException(status_code=401, detail="Missing token")

    user_id = decode_user_id(effective_token)

    # Лог для Render (чтобы больше не ловить немые 422)
    print(f"[UPLOAD] user_id={user_id} filename={file.filename} content_type={file.content_type}", flush=True)

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    r2_key = f"uploads/{user_id}/{uuid.uuid4()}_{file.filename}"

    try:
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=r2_key,
            Body=content,
            ContentType=file.content_type or "application/octet-stream",
        )
    except Exception as e:
        print(f"[UPLOAD] R2 put_object failed: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Upload to storage failed")

    job = Job(
        user_id=user_id,
        filename=file.filename,
        r2_key_input=r2_key,
        status="queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    print(f"[UPLOAD] job_created id={job.id} key={r2_key}", flush=True)
    return {"job_id": job.id, "status": job.status}


@app.get("/jobs/{job_id}/download")
def download_job(
    job_id: int,
    token: str = Query(...),
    db: Session = Depends(get_db),
):
    _user_id = decode_user_id(token)

    job = (
        db.query(Job)
        .filter(Job.id == job_id, Job.user_id == _user_id)
        .first()
    )
    if not job or not job.r2_key_output:
        raise HTTPException(status_code=404, detail="File not ready")

    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": R2_BUCKET, "Key": job.r2_key_output},
            ExpiresIn=3600,
        )
    except Exception as e:
        print(f"[DOWNLOAD] presign failed: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to generate download URL")

    return {"url": url}
