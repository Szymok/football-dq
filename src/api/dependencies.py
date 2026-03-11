"""Dependency Injection for FastAPI."""

from typing import Generator
from fastapi import Request
from src.storage.database import SessionLocal

def get_db() -> Generator:
    """Yields a database session and closes it after the request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
