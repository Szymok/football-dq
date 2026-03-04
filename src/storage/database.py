"""SQLAlchemy engine, session i Base."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from src.config.settings import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


def init_db():
    """Tworzy wszystkie tabele zdefiniowane w models.py."""
    from src.storage import models  # noqa: F401 – import potrzebny do rejestracji modeli
    Base.metadata.create_all(bind=engine)


def get_session():
    """Context manager zwracający sesję DB."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
