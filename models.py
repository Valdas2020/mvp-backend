import os
import sys
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from datetime import datetime

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
    created_at = Column(DateTime, default=datetime.utcnow)

    jobs = relationship("Job", back_populates="user")
    usages = relationship("Usage", back_populates="user")

class InviteCode(Base):
    __tablename__ = "app_invite_codes" # Новое имя
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True)
    tier = Column(String)
    quota_words = Column(Integer)
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
