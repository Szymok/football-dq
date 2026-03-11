"""Reconciliation Controller layer."""

import logging
from src.api.services.reconciliation_service import ReconciliationService
from src.api.schemas.reconciliation import ReconciliationSummaryResponse

logger = logging.getLogger(__name__)

class ReconciliationController:
    """Coordinates HTTP requests for the Reconciliation domain."""
    
    def __init__(self, reconciliation_service: ReconciliationService):
        self.reconciliation_service = reconciliation_service

    def get_summary(self) -> ReconciliationSummaryResponse:
        logger.info("Fetching reconciliation summary")
        return self.reconciliation_service.get_summary()
