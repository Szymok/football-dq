"""Golden Record Engine."""

import logging
from sqlalchemy.orm import Session
from src.storage.golden_models import GoldenPlayer, GoldenMatch
from src.storage.models import PlayerMatchStats, Match
from src.reconciliation.reconciler import MatchLinker

logger = logging.getLogger(__name__)

class GoldenRecordEngine:
    """Merges cross-source data into canonical Golden Records."""

    def __init__(self, session: Session):
        self.session = session
        self.linker = MatchLinker(session)

    def merge_players(self) -> int:
        """Naive merging of fbref and understat players based on fuzzy matches (exact name for now)."""
        logger.info("Starting GoldenPlayer merge process.")
        
        # Simple exact name match across sources to create a GoldenPlayer
        fbref_players = {r.player_name for r in self.session.query(PlayerMatchStats.player_name).filter_by(source="fbref").distinct()}
        understat_players = {r.player_name for r in self.session.query(PlayerMatchStats.player_name).filter_by(source="understat").distinct()}
        
        common_players = fbref_players & understat_players
        
        created = 0
        for name in common_players:
            # Check if canonical player exists
            existing = self.session.query(GoldenPlayer).filter_by(canonical_name=name).first()
            if not existing:
                gp = GoldenPlayer(
                    canonical_name=name,
                    # For a real system we'd persist actual mapping IDs, e.g. from reconciliation maps
                    fbref_id=f"mapped_{name}", 
                    understat_id=f"mapped_{name}"
                )
                self.session.add(gp)
                created += 1

        self.session.commit()
        logger.info(f"Created {created} Golden Players.")
        return created

    def merge_matches(self) -> int:
        """Merge match data combining fbref and understat metrics."""
        summary = self.linker.get_reconciliation_summary()
        # Stub: normally we would iterate the mapping table here and populate GoldenMatch.
        # Doing a simple fetch to show architectural intent.
        created = 0
        logger.info(f"Reconciliation provided {summary.get('linked')} linked matches. Ready to build GoldenMatches.")
        # GoldenMatch generation logic would reside here...
        
        return created

    def run_all(self):
        """Execute the full Golden Record pipeline."""
        return {
            "players_merged": self.merge_players(),
            "matches_merged": self.merge_matches()
        }
