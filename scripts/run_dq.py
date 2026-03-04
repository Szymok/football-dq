"""
Skrypt CLI: Uruchomienie pipeline'u Data Quality.

Użycie:
    python scripts/run_dq.py
"""

import sys
import os
import logging

# Dodaj root projektu do PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config.settings import LOG_FORMAT, LOG_LEVEL
from src.storage.database import SessionLocal
from src.quality.checks import DQRunner
from src.quality.scorer import calculate_dq_score, print_scorecard

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("run_dq")


def main():
    logger.info("=" * 60)
    logger.info("  ⚽ Football DQ – Pipeline sprawdzeń jakości")
    logger.info("=" * 60)

    session = SessionLocal()

    try:
        runner = DQRunner(session)
        results = runner.run_all()

        # Oblicz score
        score = calculate_dq_score(results)
        print_scorecard(score)

        # Wyświetl szczegóły nieudanych
        failed = [r for r in results if not r.passed]
        if failed:
            print("─── Nieudane sprawdzenia ───")
            for r in failed:
                print(f"  ❌ [{r.dimension}] {r.check_name}")
                if r.details:
                    print(f"     {r.details}")
                if r.value is not None and r.threshold is not None:
                    print(f"     wartość: {r.value}, próg: {r.threshold}")
            print()

    except Exception as e:
        logger.error(f"Błąd podczas sprawdzania DQ: {e}", exc_info=True)
    finally:
        session.close()


if __name__ == "__main__":
    main()
