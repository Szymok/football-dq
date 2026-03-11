"""FastAPI Router for the Quality domain."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.api.dependencies import get_db
from src.api.repositories.quality_repository import QualityRepository
from src.api.services.quality_service import QualityService
from src.api.controllers.quality_controller import QualityController
from src.api.schemas.quality import QualityScorecardResponse

router = APIRouter()

# Dependency Injection Builders for the Quality Domain
def get_quality_controller(db: Session = Depends(get_db)) -> QualityController:
    repository = QualityRepository(db)
    service = QualityService(repository)
    return QualityController(service)

@router.get("/scorecard", response_model=QualityScorecardResponse)
def get_scorecard(controller: QualityController = Depends(get_quality_controller)):
    """
    Get the Data Quality Scorecard.
    
    Strictly delegates execution to the controller layer. Route contains NO business logic.
    """
    return controller.get_scorecard()
