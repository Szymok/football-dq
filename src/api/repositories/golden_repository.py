"""Golden Record Repository layer."""

from sqlalchemy.orm import Session
from src.golden_record.engine import GoldenRecordEngine

class GoldenRepository:
    def __init__(self, session: Session):
        self.session = session
        self.engine = GoldenRecordEngine(session)

    def trigger_merge(self) -> dict:
        return self.engine.run_all()
