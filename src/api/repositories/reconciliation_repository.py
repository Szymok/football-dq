"""Reconciliation Repository layer."""

from sqlalchemy.orm import Session
from src.reconciliation.reconciler import MatchLinker

class ReconciliationRepository:
    """Handles data operations specific to reconciliation."""
    def __init__(self, session: Session):
        self.session = session
        self.linker = MatchLinker(session)

    def get_reconciliation_summary(self) -> dict:
        """Fetch the summary of match linkages across sources."""
        return self.linker.get_reconciliation_summary()
