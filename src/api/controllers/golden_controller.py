"""Golden Controller layer."""

import logging
from src.api.services.golden_service import GoldenService
from src.api.schemas.golden import GoldenMergeResponse

logger = logging.getLogger(__name__)

class GoldenController:
    """Coordinates HTTP requests for the Golden Record domain."""
    def __init__(self, service: GoldenService):
        self.service = service

    def trigger_merge(self) -> GoldenMergeResponse:
        logger.info("Triggering golden record merge")
        return self.service.execute_merge()
