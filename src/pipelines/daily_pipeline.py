"""Robust Daily DQ Pipeline Runner."""

import os
import logging
import sentry_sdk
from datetime import datetime

# Initialize Application Context
from src.api.instrument import init_sentry
from src.storage.database import SessionLocal
from src.quality.checks import DQRunner
from src.golden_record.engine import GoldenRecordEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline():
    """Main execution block for daily pipeline."""
    init_sentry()
    logger.info("Starting Daily Data Quality Pipeline")
    
    db = SessionLocal()
    try:
        # Extraction logic would live in a dedicated ingestion service, 
        # populated into `player_match_stats` table. Here we assume 
        # the db is populated and we run the DQ & Golden stages.

        # 1. Run Data Quality Rules
        logger.info("Executing DQ Runner...")
        dq_runner = DQRunner(db)
        
        rules_path = os.path.join(os.path.dirname(__file__), "..", "..", "rules", "xg_quality_rules.yaml")
        if os.path.exists(rules_path):
            dq_runner.run_all(yaml_rules_path=rules_path)
        else:
            logger.warning(f"No dynamic rules found at {rules_path}. Falling back to hardcoded.")
            dq_runner.run_all()
            
        # 2. Golden Record Generation
        logger.info("Executing Golden Record Engine...")
        golden_engine = GoldenRecordEngine(db)
        golden_results = golden_engine.run_all()
        
        logger.info(f"Pipeline completed successfully. Golden Records Merged: {golden_results}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sentry_sdk.capture_exception(e)
        raise e
    finally:
        db.close()

if __name__ == "__main__":
    run_pipeline()
