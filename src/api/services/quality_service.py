"""Quality Service layer."""

from src.api.repositories.quality_repository import QualityRepository
from src.quality.scorer import calculate_dq_score
from src.api.schemas.quality import QualityScorecardResponse
from src.api.utils.errors import BaseAPIException

class QualityService:
    """Business logic for Data Quality operations."""
    
    def __init__(self, quality_repository: QualityRepository):
        self.quality_repository = quality_repository

    def get_current_scorecard(self) -> QualityScorecardResponse:
        """Calculates and returns the current scorecard based on latest check results."""
        # For simplicity, we take the last 100 results as the "current" run limits, 
        # or ideally we fetch by the exact last run_at timestamp.
        results = self.quality_repository.get_latest_dq_results(limit=100)
        
        if not results:
            # Throw strict domain exception if no DQ has ever been run
            raise BaseAPIException("No DQ results found. Run the pipeline first.", status_code=404)
        
        score_data = calculate_dq_score(results)
        
        return QualityScorecardResponse(
            overall_score=score_data["overall_score"],
            dimension_scores=score_data["dimension_scores"],
            vendor_scores=score_data.get("vendor_scores", {}),
            total_checks=score_data["total_checks"],
            passed_checks=score_data["passed_checks"],
            failed_checks=score_data["failed_checks"],
            run_at=results[0].run_at.isoformat() if results and hasattr(results[0], "run_at") and results[0].run_at else None
        )
