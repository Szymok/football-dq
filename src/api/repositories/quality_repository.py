"""Quality Repository layer."""

from sqlalchemy.orm import Session
from src.storage.models import DQCheckResult

class QualityRepository:
    """Handles direct database access for Quality domain variables."""
    def __init__(self, session: Session):
        self.session = session

    def get_latest_dq_results(self, limit: int = 100) -> list[DQCheckResult]:
        """Fetch the most recent DQ rules execution results."""
        return self.session.query(DQCheckResult).order_by(DQCheckResult.run_at.desc()).limit(limit).all()
