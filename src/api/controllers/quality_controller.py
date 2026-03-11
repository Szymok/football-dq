"""Quality Controller layer."""

import logging
from src.api.services.quality_service import QualityService
from src.api.schemas.quality import QualityScorecardResponse
from fastapi import Response, status

logger = logging.getLogger(__name__)

class QualityController:
    """Coordinates HTTP requests for the Quality domain."""
    
    def __init__(self, quality_service: QualityService):
        self.quality_service = quality_service

    def get_scorecard(self) -> QualityScorecardResponse:
        """Retrieves the current data quality scorecard."""
        logger.info("Fetching current DQ scorecard")
        try:
            return self.quality_service.get_current_scorecard()
        except Exception as e:
            # Let the global/base exception handlers catch this, 
            # ensuring we re-raise to maintain strict error boundaries
            raise e
