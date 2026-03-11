"""Pydantic schemas for the Reconciliation domain."""

from pydantic import BaseModel, Field

class ReconciliationSummaryResponse(BaseModel):
    total: int = Field(..., description="Total matches checked")
    linked: int = Field(..., description="Matches linked successfully")
    match: int = Field(..., description="Matches linked and scores matched")
    linked_no_scores: int = Field(..., description="Matches linked but no score to compare")
    discrepancies: int = Field(..., description="Matches linked but scores discrepant")
    unmatched: int = Field(..., description="Matches not linked across sources")
    match_rate: float = Field(..., description="Linking matching rate percentage")
