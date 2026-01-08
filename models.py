from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    plan = Column(String, default=None)  # "S", "M", or None
    quota_words = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    jobs = relationship("Job", back_populates="user")
    usages = relationship("Usage", back_populates="user")

class InviteCode(Base):
    __tablename__ = "invite_codes"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True)
    tier = Column(String)  # "S" or "M"
    quota_words = Column(Integer)
    max_uses = Column(Integer)
    current_uses = Column(Integer, default=0)

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    status = Column(String, default="queued")  # queued, processing, done, error
    input_filename = Column(String)
    input_key = Column(String) # S3 key
    output_key_pdf = Column(String, nullable=True)
    output_key_epub = Column(String, nullable=True)
    in_words = Column(Integer, default=0)
    error_msg = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    downloaded_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="jobs")

class Usage(Base):
    __tablename__ = "usage"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    period = Column(String) # "2024-01"
    words_used = Column(Integer, default=0)
    
    user = relationship("User", back_populates="usages")

class OTP(Base):
    __tablename__ = "otps"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, index=True)
    code = Column(String)
    expires_at = Column(DateTime)

# Database Setup
DATABASE_URL = os.getenv("DATABASE_URL")
engine = None
SessionLocal = None

def init_db():
    global engine, SessionLocal
    if DATABASE_URL:
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(bind=engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)