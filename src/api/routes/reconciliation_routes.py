"""FastAPI Router for the Reconciliation domain."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.api.dependencies import get_db
from src.api.repositories.reconciliation_repository import ReconciliationRepository
from src.api.services.reconciliation_service import ReconciliationService
from src.api.controllers.reconciliation_controller import ReconciliationController
from src.api.schemas.reconciliation import ReconciliationSummaryResponse

router = APIRouter()

def get_reconciliation_controller(db: Session = Depends(get_db)) -> ReconciliationController:
    repository = ReconciliationRepository(db)
    service = ReconciliationService(repository)
    return ReconciliationController(service)

@router.get("/summary", response_model=ReconciliationSummaryResponse)
def get_reconciliation_summary(controller: ReconciliationController = Depends(get_reconciliation_controller)):
    """
    Get the Data Quality Reconciliation summary across sources.
    """
    return controller.get_summary()
