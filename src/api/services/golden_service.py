"""Golden Service layer."""

from src.api.repositories.golden_repository import GoldenRepository
from src.api.schemas.golden import GoldenMergeResponse

class GoldenService:
    def __init__(self, repository: GoldenRepository):
        self.repository = repository

    def execute_merge(self) -> GoldenMergeResponse:
        results = self.repository.trigger_merge()
        return GoldenMergeResponse(
            message="Golden record merging complete.",
            players_merged=results.get("players_merged", 0),
            matches_merged=results.get("matches_merged", 0)
        )
