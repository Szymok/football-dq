"""
Daily Data Pipeline – Pobieranie danych z wielu źródeł, ładowanie i uruchamianie reguł DQ.
Obsługa błędów za pomocą Sentry (jeśli podpięty w ustawieniach głównych).
"""

import logging
from src.database.connection import SessionLocal

# --- Extractors ---
from src.extractors.espn import ESPNExtractor
from src.extractors.fbref import FBrefExtractor
from src.extractors.understat import UnderstatExtractor
from src.extractors.sofascore import SofascoreExtractor
from src.extractors.whoscored import WhoScoredExtractor

# --- Loaders & Quality ---
from src.extractors.loader import DataLoader
from src.quality.checks import DQRunner

# --- Reconciliation ---
from src.golden_record.engine import GoldenRecordScorer

logger = logging.getLogger(__name__)

def run_daily_pipeline():
    logger.info("Rozpoczynam Daily Data Pipeline (w tym Vendor Scoring)...")
    
    # Inicjalizacja ekstraktorów
    extractors = [
        FBrefExtractor(),
        UnderstatExtractor(),
        ESPNExtractor(),
        SofascoreExtractor(),
        WhoScoredExtractor()
        # Opcjonalnie ClubElo
    ]

    with SessionLocal() as db:
        loader = DataLoader(db)
        
        # 1. Ekstrakcja i Ładowanie (Extract -> Load)
        logger.info("--- EKSTRAKCJA I ŁADOWANIE ---")
        for ext in extractors:
            try:
                data_dict = ext.extract()
                
                # Terminarze
                if "schedule" in data_dict and not data_dict["schedule"].empty:
                    # Generic loader schema, pass league/season as unknown or adapt if extractor returns it
                    # Uproszczenie dla schedule - wyciągamy pierwsze z brzegu
                    lg = ext.leagues[0] if ext.leagues else "Unknown"
                    ss = ext.seasons[0] if ext.seasons else "Unknown"
                    loader.load_schedule(data_dict["schedule"], source=ext.source_name, league=lg, season=ss)
                
                # Statystyki graczy (FBref, Understat, itp.)
                if "player_stats" in data_dict and not data_dict["player_stats"].empty:
                    ss = ext.seasons[0] if ext.seasons else "Unknown"
                    loader.load_player_season_stats(data_dict["player_stats"], source=ext.source_name, season=ss)

                # Dla WhoScored - missing players może być mapowane osobno w przyszłości
                if "missing_players" in data_dict and not data_dict["missing_players"].empty:
                    logger.info(f"Pobrano missing_players z WhoScored ({len(data_dict['missing_players'])})")
            
            except Exception as e:
                logger.error(f"Krytyczny błąd w ekstraktorze {ext.source_name}: {e}")

        # 2. Data Quality (Run DQ Checks & Vendor Scoring)
        logger.info("--- DATA QUALITY (Ground Truth: FBref) ---")
        dq_runner = DQRunner(db)
        try:
            # Używamy reguł z YAML dla standardowych (np. not_null, between)
            # Oraz wywołujemy logikę hardkodowaną (accuracy/consistency per vendor)
            results = dq_runner.run_all(yaml_rules_path="rules/xg_quality_rules.yaml")
        except Exception as e:
            logger.error(f"Błąd silnika DQ: {e}")

        # 3. Reconciliation (Golden Records)
        logger.info("--- GOLDEN RECORDS RECONCILIATION ---")
        golden_engine = GoldenRecordScorer(db)
        try:
            # Reconcile players using FBref as ground truth, Understat as secondary
            golden_engine.reconcile_players(primary_source="fbref", secondary_source="understat")
        except Exception as e:
            logger.error(f"Błąd silnika Golden Records: {e}")

    logger.info("Daily Data Pipeline ZAKOŃCZONY Pomyślnie.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    run_daily_pipeline()
