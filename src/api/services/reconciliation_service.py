"""Reconciliation Service layer."""

from src.api.repositories.reconciliation_repository import ReconciliationRepository
from src.api.schemas.reconciliation import ReconciliationSummaryResponse

class ReconciliationService:
    def __init__(self, reconciliation_repository: ReconciliationRepository):
        self.reconciliation_repository = reconciliation_repository

    def get_summary(self) -> ReconciliationSummaryResponse:
        """Get the cross-source reconciliation summary metrics."""
        summary = self.reconciliation_repository.get_reconciliation_summary()
        
        return ReconciliationSummaryResponse(
            total=summary.get("total", 0),
            linked=summary.get("linked", 0),
            match=summary.get("match", 0),
            linked_no_scores=summary.get("linked_no_scores", 0),
            discrepancies=summary.get("discrepancies", 0),
            unmatched=summary.get("unmatched", 0),
            match_rate=summary.get("match_rate", 0.0),
        )
