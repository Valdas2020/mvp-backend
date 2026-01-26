import os
import time
import hmac
import hashlib
from datetime import datetime, timedelta
from uuid import uuid4

import jwt
import boto3
from botocore.client import Config
import requests
import stripe

from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File, Request
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel

from models import (
    SessionLocal, User, InviteCode, OTP, Job, Usage,
    Payment, TIER_CONFIG, generate_invite_code
)  # noqa: F401


# ----------------------------
# CONFIG
# ----------------------------
JWT_SECRET = os.getenv("JWT_SECRET")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "admin-change-me")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

# Stripe config
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
STRIPE_SUCCESS_URL = os.getenv("STRIPE_SUCCESS_URL", "https://mvp-frontend-rho.vercel.app/payment/success")
STRIPE_CANCEL_URL = os.getenv("STRIPE_CANCEL_URL", "https://mvp-frontend-rho.vercel.app/payment/cancel")

# Wallet Pay config (Telegram) - DEPRECATED, use CryptoBot
WALLET_PAY_TOKEN = os.getenv("WALLET_PAY_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# CryptoBot config (crypto payments via @CryptoBot)
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN")
# Use testnet for testing: https://testnet-pay.crypt.bot/api
# Use production: https://pay.crypt.bot/api
CRYPTOBOT_API_URL = os.getenv("CRYPTOBOT_API_URL", "https://pay.crypt.bot/api")

# Frontend URL
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://mvp-frontend-rho.vercel.app")

# Initialize Stripe
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# Basic sanity (helps catch misconfigured env early)
if not JWT_SECRET or JWT_SECRET == "change-me":
    raise RuntimeError("JWT_SECRET must be set to a secure random value")
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


class CreateCheckoutRequest(BaseModel):
    tier: str  # "S", "M", "L"
    email: str | None = None  # Optional email for receipt


class WalletPayOrderRequest(BaseModel):
    tier: str
    telegram_user_id: str


class CryptoBotInvoiceRequest(BaseModel):
    tier: str  # "S", "M", "L"
    asset: str = "USDT"  # USDT or TON


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
    code = (req.invite_code or "").strip().upper()  # Приводим к верхнему регистру
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
    print(f"[DOWNLOAD] job_id={job_id} user_id={user_id}", flush=True)

    job = (
        db.query(Job)
        .filter(Job.id == job_id, Job.user_id == user_id)
        .first()
    )
    if not job:
        print(f"[DOWNLOAD] Job not found", flush=True)
        raise HTTPException(status_code=404, detail="Job not found")

    print(f"[DOWNLOAD] job.status={job.status} r2_key_output={job.r2_key_output}", flush=True)

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
        print(f"[DOWNLOAD] presigned URL generated, length={len(presigned_url)}", flush=True)
    except Exception as e:
        print(f"[DOWNLOAD] presign failed: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to generate download URL")

    # Proxy (forces download reliably)
    try:
        r = requests.get(presigned_url, stream=True, timeout=30)
        print(f"[DOWNLOAD] R2 response status={r.status_code}", flush=True)
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


# ========================================
# STRIPE PAYMENTS
# ========================================

@app.get("/api/pricing")
def get_pricing():
    """Return available tiers and prices"""
    return {
        "tiers": [
            {
                "id": tier_id,
                "words": config["words"],
                "price_eur": config["price_eur"],
                "price_usd": config["price_usd"],
                "price_ton": config["price_ton"],
            }
            for tier_id, config in TIER_CONFIG.items()
        ]
    }


@app.post("/api/stripe/create-checkout")
def create_stripe_checkout(req: CreateCheckoutRequest, db: Session = Depends(get_db)):
    """Create Stripe Checkout Session for one-time payment"""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")

    tier = req.tier.upper()
    if tier not in TIER_CONFIG:
        raise HTTPException(status_code=400, detail=f"Invalid tier: {tier}")

    config = TIER_CONFIG[tier]
    price_cents = int(config["price_eur"] * 100)

    # Generate unique invite code
    invite_code = generate_invite_code()

    # Ensure uniqueness
    while db.query(Payment).filter(Payment.invite_code == invite_code).first():
        invite_code = generate_invite_code()

    try:
        # Create Stripe Checkout Session
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": "eur",
                    "product_data": {
                        "name": f"PDF Translator - Tier {tier}",
                        "description": f"{config['words']:,} words translation quota",
                    },
                    "unit_amount": price_cents,
                },
                "quantity": 1,
            }],
            mode="payment",
            success_url=f"{STRIPE_SUCCESS_URL}?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=STRIPE_CANCEL_URL,
            customer_email=req.email if req.email else None,
            metadata={
                "tier": tier,
                "invite_code": invite_code,
                "quota_words": str(config["words"]),
            },
        )

        # Create pending payment record
        payment = Payment(
            invite_code=invite_code,
            tier=tier,
            quota_words=config["words"],
            amount=config["price_eur"],
            currency="EUR",
            payment_method="stripe",
            stripe_session_id=session.id,
            email=req.email,
            status="pending",
        )
        db.add(payment)
        db.commit()

        print(f"[STRIPE] Created checkout session={session.id} tier={tier} code={invite_code}", flush=True)

        return {
            "checkout_url": session.url,
            "session_id": session.id,
        }

    except stripe.error.StripeError as e:
        print(f"[STRIPE] Error creating checkout: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to create checkout session")


@app.post("/webhook/stripe")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    """Handle Stripe webhook events"""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not STRIPE_WEBHOOK_SECRET:
        print("[STRIPE WEBHOOK] Warning: STRIPE_WEBHOOK_SECRET not set, skipping signature verification", flush=True)
        event = stripe.Event.construct_from(
            stripe.util.json.loads(payload), stripe.api_key
        )
    else:
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, STRIPE_WEBHOOK_SECRET
            )
        except ValueError:
            print("[STRIPE WEBHOOK] Invalid payload", flush=True)
            raise HTTPException(status_code=400, detail="Invalid payload")
        except stripe.error.SignatureVerificationError:
            print("[STRIPE WEBHOOK] Invalid signature", flush=True)
            raise HTTPException(status_code=400, detail="Invalid signature")

    print(f"[STRIPE WEBHOOK] Received event: {event['type']}", flush=True)

    # Handle checkout.session.completed
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        session_id = session["id"]
        payment_status = session.get("payment_status")

        print(f"[STRIPE WEBHOOK] Session completed: {session_id}, payment_status={payment_status}", flush=True)

        if payment_status == "paid":
            # Find payment record
            payment = db.query(Payment).filter(Payment.stripe_session_id == session_id).first()

            if not payment:
                print(f"[STRIPE WEBHOOK] Payment not found for session {session_id}", flush=True)
                return {"status": "error", "message": "Payment not found"}

            if payment.status == "completed":
                print(f"[STRIPE WEBHOOK] Payment already completed: {session_id}", flush=True)
                return {"status": "ok", "message": "Already processed"}

            # Update payment status
            payment.status = "completed"
            payment.completed_at = datetime.utcnow()
            payment.stripe_payment_intent = session.get("payment_intent")

            # Also create an InviteCode record for backward compatibility
            existing_code = db.query(InviteCode).filter(InviteCode.code == payment.invite_code).first()
            if not existing_code:
                invite = InviteCode(
                    code=payment.invite_code,
                    tier=payment.tier,
                    quota_words=payment.quota_words,
                    max_uses=1,
                    used_count=0,
                )
                db.add(invite)

            db.commit()

            print(f"[STRIPE WEBHOOK] Payment completed! code={payment.invite_code} tier={payment.tier}", flush=True)

            # TODO: Send email with invite code if email provided
            if payment.email:
                print(f"[STRIPE WEBHOOK] Should send code to {payment.email}", flush=True)

    return {"status": "ok"}


@app.get("/api/stripe/session/{session_id}")
def get_stripe_session(session_id: str, db: Session = Depends(get_db)):
    """Get payment status and invite code after successful payment"""
    payment = db.query(Payment).filter(Payment.stripe_session_id == session_id).first()

    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    return {
        "status": payment.status,
        "tier": payment.tier,
        "quota_words": payment.quota_words,
        "invite_code": payment.invite_code if payment.status == "completed" else None,
    }


# ========================================
# WALLET PAY (TELEGRAM CRYPTO PAYMENTS)
# ========================================

@app.post("/api/wallet-pay/create-order")
def create_wallet_pay_order(req: WalletPayOrderRequest, db: Session = Depends(get_db)):
    """Create Wallet Pay order for crypto payment"""
    if not WALLET_PAY_TOKEN:
        raise HTTPException(status_code=503, detail="Wallet Pay not configured")

    tier = req.tier.upper()
    if tier not in TIER_CONFIG:
        raise HTTPException(status_code=400, detail=f"Invalid tier: {tier}")

    config = TIER_CONFIG[tier]

    # Generate unique invite code
    invite_code = generate_invite_code()
    while db.query(Payment).filter(Payment.invite_code == invite_code).first():
        invite_code = generate_invite_code()

    # Create order via Wallet Pay API
    # https://docs.wallet.tg/pay/
    order_id = f"order_{int(time.time())}_{invite_code}"

    try:
        response = requests.post(
            "https://pay.wallet.tg/wpay/store-api/v1/order",
            headers={
                "Wpay-Store-Api-Key": WALLET_PAY_TOKEN,
                "Content-Type": "application/json",
            },
            json={
                "amount": {
                    "currencyCode": "USDT",
                    "amount": str(config["price_usd"]),
                },
                "description": f"PDF Translator - Tier {tier} ({config['words']:,} words)",
                "externalId": order_id,
                "timeoutSeconds": 1800,  # 30 minutes
                "customerTelegramUserId": int(req.telegram_user_id),
                "returnUrl": f"{FRONTEND_URL}/payment/success?order_id={order_id}",
                "failReturnUrl": f"{FRONTEND_URL}/payment/cancel",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "SUCCESS":
            print(f"[WALLET PAY] Order creation failed: {data}", flush=True)
            raise HTTPException(status_code=500, detail="Failed to create order")

        pay_link = data["data"]["payLink"]

        # Create pending payment record
        payment = Payment(
            invite_code=invite_code,
            tier=tier,
            quota_words=config["words"],
            amount=config["price_usd"],
            currency="USDT",
            payment_method="wallet_pay",
            wallet_pay_order_id=order_id,
            telegram_user_id=req.telegram_user_id,
            status="pending",
        )
        db.add(payment)
        db.commit()

        print(f"[WALLET PAY] Created order={order_id} tier={tier} code={invite_code}", flush=True)

        return {
            "pay_link": pay_link,
            "order_id": order_id,
        }

    except requests.RequestException as e:
        print(f"[WALLET PAY] Request error: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to connect to Wallet Pay")


@app.post("/webhook/wallet-pay")
async def wallet_pay_webhook(request: Request, db: Session = Depends(get_db)):
    """Handle Wallet Pay webhook events"""
    payload = await request.body()

    # Verify webhook signature
    # https://docs.wallet.tg/pay/#section/Webhooks
    timestamp = request.headers.get("WalletPay-Timestamp")
    signature = request.headers.get("WalletPay-Signature")

    if WALLET_PAY_TOKEN and timestamp and signature:
        # Compute expected signature
        message = f"{timestamp}.{payload.decode()}"
        expected_sig = hmac.new(
            WALLET_PAY_TOKEN.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_sig):
            print("[WALLET PAY WEBHOOK] Invalid signature", flush=True)
            raise HTTPException(status_code=400, detail="Invalid signature")

    try:
        import json
        data = json.loads(payload)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    print(f"[WALLET PAY WEBHOOK] Received: {data}", flush=True)

    # Handle payment completed
    event_type = data.get("type")
    if event_type == "ORDER_PAID":
        order_data = data.get("payload", {})
        order_id = order_data.get("externalId")

        if not order_id:
            print("[WALLET PAY WEBHOOK] Missing externalId", flush=True)
            return {"status": "error"}

        payment = db.query(Payment).filter(Payment.wallet_pay_order_id == order_id).first()

        if not payment:
            print(f"[WALLET PAY WEBHOOK] Payment not found for order {order_id}", flush=True)
            return {"status": "error"}

        if payment.status == "completed":
            print(f"[WALLET PAY WEBHOOK] Payment already completed: {order_id}", flush=True)
            return {"status": "ok"}

        # Update payment status
        payment.status = "completed"
        payment.completed_at = datetime.utcnow()

        # Create InviteCode record
        existing_code = db.query(InviteCode).filter(InviteCode.code == payment.invite_code).first()
        if not existing_code:
            invite = InviteCode(
                code=payment.invite_code,
                tier=payment.tier,
                quota_words=payment.quota_words,
                max_uses=1,
                used_count=0,
            )
            db.add(invite)

        db.commit()

        print(f"[WALLET PAY WEBHOOK] Payment completed! code={payment.invite_code} tier={payment.tier}", flush=True)

        # TODO: Send code to user via Telegram Bot
        if payment.telegram_user_id and TELEGRAM_BOT_TOKEN:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": payment.telegram_user_id,
                        "text": f"✅ Payment successful!\n\nYour invite code: `{payment.invite_code}`\n\nTier: {payment.tier}\nWords: {payment.quota_words:,}\n\nUse this code at {FRONTEND_URL}",
                        "parse_mode": "Markdown",
                    },
                    timeout=10,
                )
            except Exception as e:
                print(f"[WALLET PAY WEBHOOK] Failed to send Telegram message: {e}", flush=True)

    return {"status": "ok"}


@app.get("/api/wallet-pay/order/{order_id}")
def get_wallet_pay_order(order_id: str, db: Session = Depends(get_db)):
    """Get payment status and invite code after successful payment"""
    payment = db.query(Payment).filter(Payment.wallet_pay_order_id == order_id).first()

    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    return {
        "status": payment.status,
        "tier": payment.tier,
        "quota_words": payment.quota_words,
        "invite_code": payment.invite_code if payment.status == "completed" else None,
    }


# ========================================
# CRYPTOBOT PAYMENTS (@CryptoBot)
# ========================================

@app.post("/api/cryptobot/create-invoice")
def create_cryptobot_invoice(req: CryptoBotInvoiceRequest, db: Session = Depends(get_db)):
    """Create CryptoBot invoice for crypto payment (USDT/TON)"""
    if not CRYPTOBOT_API_TOKEN:
        raise HTTPException(status_code=503, detail="CryptoBot not configured")

    tier = req.tier.upper()
    if tier not in TIER_CONFIG:
        raise HTTPException(status_code=400, detail=f"Invalid tier: {tier}")

    asset = req.asset.upper()
    if asset not in ["USDT", "TON"]:
        raise HTTPException(status_code=400, detail="Asset must be USDT or TON")

    config = TIER_CONFIG[tier]

    # Determine amount based on asset
    if asset == "USDT":
        amount = str(config["price_usd"])
    else:  # TON
        amount = str(config["price_ton"])

    # Generate unique invite code
    invite_code = generate_invite_code()
    while db.query(Payment).filter(Payment.invite_code == invite_code).first():
        invite_code = generate_invite_code()

    # Create invoice via CryptoBot API
    try:
        print(f"[CRYPTOBOT] Creating invoice: tier={tier} asset={asset} amount={amount}", flush=True)
        print(f"[CRYPTOBOT] API URL: {CRYPTOBOT_API_URL}/createInvoice", flush=True)

        response = requests.post(
            f"{CRYPTOBOT_API_URL}/createInvoice",
            headers={
                "Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN,
                "Content-Type": "application/json",
            },
            json={
                "asset": asset,
                "amount": amount,
                "description": f"PDF Translator - Tier {tier} ({config['words']:,} words)",
                "hidden_message": f"Your activation code: {invite_code}",
                "paid_btn_name": "openBot",
                "paid_btn_url": f"{FRONTEND_URL}/payment/success?crypto_invoice={invite_code}",
                "payload": f"{tier}:{invite_code}",
                "allow_comments": False,
                "allow_anonymous": True,
            },
            timeout=30,
        )

        print(f"[CRYPTOBOT] Response status: {response.status_code}", flush=True)
        print(f"[CRYPTOBOT] Response body: {response.text}", flush=True)

        data = response.json()

        if not data.get("ok"):
            error_msg = data.get("error", {})
            print(f"[CRYPTOBOT] Invoice creation failed: {data}", flush=True)
            raise HTTPException(status_code=500, detail=f"CryptoBot error: {error_msg}")

        invoice = data["result"]
        invoice_id = str(invoice["invoice_id"])
        pay_url = invoice["pay_url"]

        # Create pending payment record
        payment = Payment(
            invite_code=invite_code,
            tier=tier,
            quota_words=config["words"],
            amount=float(amount),
            currency=asset,
            payment_method="cryptobot",
            cryptobot_invoice_id=invoice_id,
            status="pending",
        )
        db.add(payment)
        db.commit()

        print(f"[CRYPTOBOT] Created invoice={invoice_id} tier={tier} code={invite_code} asset={asset}", flush=True)

        return {
            "pay_url": pay_url,
            "invoice_id": invoice_id,
        }

    except requests.RequestException as e:
        print(f"[CRYPTOBOT] Request error: {e}", flush=True)
        if hasattr(e, 'response') and e.response is not None:
            print(f"[CRYPTOBOT] Error response: {e.response.text}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to connect to CryptoBot")


@app.post("/webhook/cryptobot")
async def cryptobot_webhook(request: Request, db: Session = Depends(get_db)):
    """Handle CryptoBot webhook events"""
    payload = await request.body()

    # Verify webhook signature
    # CryptoBot sends signature in Crypto-Pay-Api-Signature header
    signature = request.headers.get("Crypto-Pay-Api-Signature")

    if CRYPTOBOT_API_TOKEN and signature:
        # Compute expected signature: HMAC-SHA256 of request body with API token
        expected_sig = hmac.new(
            hashlib.sha256(CRYPTOBOT_API_TOKEN.encode()).digest(),
            payload,
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_sig):
            print("[CRYPTOBOT WEBHOOK] Invalid signature", flush=True)
            raise HTTPException(status_code=400, detail="Invalid signature")

    try:
        import json
        data = json.loads(payload)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    print(f"[CRYPTOBOT WEBHOOK] Received: {data}", flush=True)

    # Handle invoice_paid event
    update_type = data.get("update_type")
    if update_type == "invoice_paid":
        invoice_data = data.get("payload", {})
        invoice_id = str(invoice_data.get("invoice_id"))
        payload_str = invoice_data.get("payload", "")  # "TIER:INVITE_CODE"

        # Parse payload to get tier and invite_code
        if ":" in payload_str:
            tier, invite_code = payload_str.split(":", 1)
        else:
            print(f"[CRYPTOBOT WEBHOOK] Invalid payload format: {payload_str}", flush=True)
            return {"status": "error"}

        # Find payment by invoice_id
        payment = db.query(Payment).filter(Payment.cryptobot_invoice_id == invoice_id).first()

        if not payment:
            # Try to find by invite_code as fallback
            payment = db.query(Payment).filter(
                Payment.invite_code == invite_code,
                Payment.payment_method == "cryptobot"
            ).first()

        if not payment:
            print(f"[CRYPTOBOT WEBHOOK] Payment not found for invoice {invoice_id}", flush=True)
            return {"status": "error"}

        if payment.status == "completed":
            print(f"[CRYPTOBOT WEBHOOK] Payment already completed: {invoice_id}", flush=True)
            return {"status": "ok"}

        # Update payment status
        payment.status = "completed"
        payment.completed_at = datetime.utcnow()

        # Create InviteCode record for backward compatibility
        existing_code = db.query(InviteCode).filter(InviteCode.code == payment.invite_code).first()
        if not existing_code:
            invite = InviteCode(
                code=payment.invite_code,
                tier=payment.tier,
                quota_words=payment.quota_words,
                max_uses=1,
                used_count=0,
            )
            db.add(invite)

        db.commit()

        print(f"[CRYPTOBOT WEBHOOK] Payment completed! code={payment.invite_code} tier={payment.tier}", flush=True)

    return {"status": "ok"}


@app.get("/api/cryptobot/invoice/{invoice_code}")
def get_cryptobot_invoice(invoice_code: str, db: Session = Depends(get_db)):
    """Get payment status and invite code by invite_code (from success URL)"""
    payment = db.query(Payment).filter(
        Payment.invite_code == invoice_code,
        Payment.payment_method == "cryptobot"
    ).first()

    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    return {
        "status": payment.status,
        "tier": payment.tier,
        "quota_words": payment.quota_words,
        "invite_code": payment.invite_code if payment.status == "completed" else None,
    }


# ========================================
# ADMIN PANEL
# ========================================
@app.get("/api/admin/codes")
def admin_list_codes(admin_secret: str = Query(...), db: Session = Depends(get_db)):
    """Список всех инвайт-кодов с их статусом"""
    if admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret")

    codes = db.query(InviteCode).order_by(InviteCode.tier.desc(), InviteCode.code).all()

    return {
        "total": len(codes),
        "codes": [
            {
                "code": c.code,
                "tier": c.tier,
                "quota_words": c.quota_words,
                "max_uses": c.max_uses,
                "used_count": c.used_count,
                "available": c.max_uses - c.used_count > 0,
            }
            for c in codes
        ],
        "stats": {
            "tier_M_total": sum(1 for c in codes if c.tier == "M"),
            "tier_M_used": sum(1 for c in codes if c.tier == "M" and c.used_count > 0),
            "tier_S_total": sum(1 for c in codes if c.tier == "S"),
            "tier_S_used": sum(1 for c in codes if c.tier == "S" and c.used_count > 0),
        }
    }


@app.get("/api/admin/stats")
def admin_stats(admin_secret: str = Query(...), db: Session = Depends(get_db)):
    """Общая статистика по пользователям и заданиям"""
    if admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret")

    total_users = db.query(User).count()
    total_jobs = db.query(Job).count()
    jobs_queued = db.query(Job).filter(Job.status == "queued").count()
    jobs_processing = db.query(Job).filter(Job.status == "processing").count()
    jobs_completed = db.query(Job).filter(Job.status == "completed").count()
    jobs_failed = db.query(Job).filter(Job.status == "failed").count()

    total_words = db.query(Job).filter(Job.status == "completed").with_entities(
        db.func.sum(Job.word_count)
    ).scalar() or 0

    return {
        "users": {
            "total": total_users,
        },
        "jobs": {
            "total": total_jobs,
            "queued": jobs_queued,
            "processing": jobs_processing,
            "completed": jobs_completed,
            "failed": jobs_failed,
        },
        "words_translated": total_words,
    }


@app.get("/api/admin/payments")
def admin_list_payments(admin_secret: str = Query(...), db: Session = Depends(get_db)):
    """Список всех платежей"""
    if admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret")

    payments = db.query(Payment).order_by(Payment.created_at.desc()).limit(100).all()

    return {
        "total": len(payments),
        "payments": [
            {
                "id": p.id,
                "invite_code": p.invite_code,
                "tier": p.tier,
                "amount": p.amount,
                "currency": p.currency,
                "payment_method": p.payment_method,
                "status": p.status,
                "email": p.email,
                "telegram_user_id": p.telegram_user_id,
                "created_at": p.created_at.isoformat() if p.created_at else None,
                "completed_at": p.completed_at.isoformat() if p.completed_at else None,
            }
            for p in payments
        ],
        "stats": {
            "total_completed": sum(1 for p in payments if p.status == "completed"),
            "total_pending": sum(1 for p in payments if p.status == "pending"),
            "revenue_eur": sum(p.amount for p in payments if p.status == "completed" and p.currency == "EUR"),
            "revenue_usd": sum(p.amount for p in payments if p.status == "completed" and p.currency in ["USD", "USDT"]),
        }
    }


# Initialize tables on startup
from models import init_db, Base, engine
try:
    Base.metadata.create_all(bind=engine)
    print("[STARTUP] Database tables initialized", flush=True)
except Exception as e:
    print(f"[STARTUP] DB init error: {e}", flush=True)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
