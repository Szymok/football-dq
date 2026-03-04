"""Konfiguracja aplikacji Football DQ."""

import os
from pathlib import Path

# Ścieżki
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "football_dq.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Źródła danych – soccerdata
LEAGUES = ["ENG-Premier League"]
SEASONS = ["2324"]

# Konfiguracja logowania
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s"

# DQ thresholds
DQ_THRESHOLDS = {
    "completeness_null_max_pct": 5.0,       # max 5% nulli
    "validity_xg_min": 0.0,
    "validity_xg_max": 7.0,
    "validity_minutes_min": 0,
    "validity_minutes_max": 120,
    "timeliness_max_days": 14,              # dane nie starsze niż 14 dni
    "consistency_min_match_pct": 80.0,      # min 80% entity match
    "accuracy_max_xg_delta": 0.15,          # max średnia delta xG
}
