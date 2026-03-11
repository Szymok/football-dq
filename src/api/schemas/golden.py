"""Pydantic schemas for the Golden Record domain."""

from pydantic import BaseModel, Field

class GoldenMergeResponse(BaseModel):
    message: str = Field(..., description="Merge summary")
    players_merged: int
    matches_merged: int
