"""FastAPI Router for the Golden Record domain."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.api.dependencies import get_db
from src.api.repositories.golden_repository import GoldenRepository
from src.api.services.golden_service import GoldenService
from src.api.controllers.golden_controller import GoldenController
from src.api.schemas.golden import GoldenMergeResponse

router = APIRouter()

def get_golden_controller(db: Session = Depends(get_db)) -> GoldenController:
    repository = GoldenRepository(db)
    service = GoldenService(repository)
    return GoldenController(service)

@router.post("/merge", response_model=GoldenMergeResponse)
def merge_records(controller: GoldenController = Depends(get_golden_controller)):
    """
    Triggers the Golden Record merging engine.
    """
    return controller.trigger_merge()
