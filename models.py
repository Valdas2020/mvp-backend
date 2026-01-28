import os
import sys
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Float
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from datetime import datetime
import secrets
import string

# --- 1. НАСТРОЙКА ПОДКЛЮЧЕНИЯ К БД ---
database_url = os.getenv("DATABASE_URL")

if not database_url:
    print("CRITICAL ERROR: DATABASE_URL is missing in Environment Variables!", file=sys.stderr)
    sys.exit(1)

if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

try:
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()
    print("Database connection configured successfully.", file=sys.stderr)
except Exception as e:
    print(f"Error creating DB engine: {e}", file=sys.stderr)
    sys.exit(1)

def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        print("Tables initialized successfully.", file=sys.stderr)
    except Exception as e:
        print(f"Error initializing tables: {e}", file=sys.stderr)

# --- 2. МОДЕЛИ (ТАБЛИЦЫ) ---

class User(Base):
    __tablename__ = "app_users"  # Новое имя
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    plan = Column(String, nullable=True)
    quota_words = Column(Integer, default=0)
    max_pages = Column(Integer, nullable=True)  # Page limit for trial codes (None = unlimited)
    created_at = Column(DateTime, default=datetime.utcnow)

    jobs = relationship("Job", back_populates="user")
    usages = relationship("Usage", back_populates="user")

class InviteCode(Base):
    __tablename__ = "app_invite_codes" # Новое имя
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True)
    tier = Column(String)
    quota_words = Column(Integer)
    max_pages = Column(Integer, nullable=True)  # Page limit for trial codes (None = unlimited)
    max_uses = Column(Integer, default=1)
    used_count = Column(Integer, default=0)

class OTP(Base):
    __tablename__ = "app_otps" # Новое имя
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, index=True)
    code = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

class Job(Base):
    __tablename__ = "app_jobs" # Новое имя
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id")) # Ссылка на новое имя
    filename = Column(String)
    status = Column(String, default="queued")
    r2_key_input = Column(String)
    r2_key_output = Column(String, nullable=True)
    word_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="jobs")

class Usage(Base):
    __tablename__ = "app_usages" # Новое имя
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("app_users.id")) # Ссылка на новое имя
    words_deducted = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="usages")


# --- PAYMENT TRACKING ---

# Tier configuration (one-time packages)
TIER_CONFIG = {
    "S": {"words": 200_000, "price_usd": 3, "price_eur": 3, "price_ton": 2},
    "M": {"words": 500_000, "price_usd": 6, "price_eur": 6, "price_ton": 4},
    "L": {"words": 1_200_000, "price_usd": 9, "price_eur": 9, "price_ton": 6},
}


def generate_invite_code(length=8):
    """Generate a unique invite code like 'ABCD-1234'"""
    chars = string.ascii_uppercase + string.digits
    part1 = ''.join(secrets.choice(chars) for _ in range(4))
    part2 = ''.join(secrets.choice(chars) for _ in range(4))
    return f"{part1}-{part2}"


class Payment(Base):
    """Tracks all payment transactions for audit"""
    __tablename__ = "app_payments"

    id = Column(Integer, primary_key=True, index=True)

    # Generated invite code (sent to user after payment)
    invite_code = Column(String, unique=True, index=True, nullable=False)

    # Tier info
    tier = Column(String, nullable=False)  # "S", "M", "L"
    quota_words = Column(Integer, nullable=False)

    # Payment details
    amount = Column(Float, nullable=False)
    currency = Column(String, nullable=False)  # "USD", "EUR", "USDT", "TON"
    payment_method = Column(String, nullable=False)  # "stripe", "wallet_pay"

    # Provider-specific IDs
    stripe_session_id = Column(String, nullable=True, unique=True)
    stripe_payment_intent = Column(String, nullable=True)
    wallet_pay_order_id = Column(String, nullable=True, unique=True)
    cryptobot_invoice_id = Column(String, nullable=True, unique=True)  # CryptoBot invoice ID

    # User contact (for sending the code)
    email = Column(String, nullable=True)
    telegram_user_id = Column(String, nullable=True)

    # Status
    status = Column(String, default="pending")  # "pending", "completed", "failed", "refunded"

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
