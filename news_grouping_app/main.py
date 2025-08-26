# main.py - UPDATED
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Database Setup & Migration ---
from news_grouping_app.db.database import setup_database, DEFAULT_DB_PATH
from news_grouping_app.datemigration import main as run_date_migration  # Keep date migration
from news_grouping_app.wiki_qid_migration import (
    main as run_wiki_qid_migration,
)

# --- Scrapers ---
from news_grouping_app.scrapers import bleepingcomputer
from news_grouping_app.scrapers import krebsonsecurityscraper
from news_grouping_app.scrapers import nist
from news_grouping_app.scrapers import cyberscoopscraper
from news_grouping_app.scrapers.register_scraper import RegisterScraper
from news_grouping_app.scrapers import schneier_scraper
from news_grouping_app.scrapers import Scrapinghackernews
from news_grouping_app.scrapers import securelist_scraper
from news_grouping_app.scrapers import Slashdotit
from news_grouping_app.scrapers import sophos
from news_grouping_app.scrapers import techcrunch
from news_grouping_app.scrapers import neowinscraper
from news_grouping_app.scrapers import techradar
from news_grouping_app.scrapers import darkreading_scraper

# --- Import UPDATED Pipeline Functions ---
from news_grouping_app.pipeline import (
    run_entity_extraction_pipeline,
    run_article_grouping_pipeline,
    run_trending_analysis_pipeline,
    run_full_pipeline_headless,  # Optional: Can use this instead of individual steps below
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def verify_table_exists(db_path=DEFAULT_DB_PATH, table_name="two_phase_article_groups"):
    """Helper function to check if a table exists."""
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    except Exception as e:
        logger.error(f"Error verifying table '{table_name}': {e}")
        return False


def run_scrapers_and_analysis():
    """
    Runs all scrapers and then the full analysis pipeline (Extraction -> Grouping -> Trending).
    """
    logger.info(
        f"--- Starting Scheduled Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---"
    )
    run_start_time = time.time()

    # 1. Ensure database is set up
    logger.info("Attempting to set up database schema...")
    try:
        setup_database()
        logger.info("Database schema setup call completed.")
        # *** ADD VERIFICATION STEP ***
        if verify_table_exists(table_name="two_phase_article_groups"):
            logger.info(
                "VERIFIED: 'two_phase_article_groups' table exists after setup."
            )
        else:
            logger.error(
                "VERIFICATION FAILED: 'two_phase_article_groups' table DOES NOT exist after setup. Aborting run."
            )
            return  # Stop the run if the table wasn't created
        # *****************************
    except Exception as db_setup_err:
        logger.exception("CRITICAL ERROR DURING DATABASE SETUP! Aborting run.")
        return  # Stop the run

    # 2. Run Scrapers in parallel
    logger.info("--- Starting Scrapers ---")
    scraper_start_time = time.time()
    scrapers = [
        bleepingcomputer,
        krebsonsecurityscraper,
        nist,
        schneier_scraper,
        Scrapinghackernews,
        securelist_scraper,
        Slashdotit,
        sophos,
        techcrunch,
        techradar,
        darkreading_scraper,
        neowinscraper,
        cyberscoopscraper,
    ]

    def run_scraper(scraper_module):
        try:
            logger.info(f"Running scraper: {scraper_module.__name__}")
            scraper_module.main()
            logger.info(f"Scraper {scraper_module.__name__} completed.")
        except Exception as e:
            logger.exception(f"Error running scraper {scraper_module.__name__}: {e}")

    def run_register_scraper():
        try:
            logger.info("Running scraper: RegisterScraper")
            register_scraper = RegisterScraper()
            register_scraper.process_register_articles(limit=100)
            logger.info("Scraper RegisterScraper completed.")
        except Exception as e:
            logger.exception(f"Error running RegisterScraper: {e}")

    max_workers = min(5, len(scrapers) + 1)  # Limit concurrency
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_scraper, s) for s in scrapers]
        futures.append(executor.submit(run_register_scraper))
        for future in as_completed(futures):
            pass  # results logged within tasks

    scraper_elapsed = time.time() - scraper_start_time
    logger.info(f"--- Scrapers Finished in {scraper_elapsed:.2f} seconds ---")

    # 3. Run Date Migration (if still needed after initial setup)
    logger.info("Running date format migration...")
    try:
        run_date_migration()
        logger.info("Date migration completed.")
    except Exception as e:
        logger.exception(f"Error during date migration: {e}")

    logger.info("Ensuring wiki_qid field exists...")
    try:
        run_wiki_qid_migration()
        logger.info("wiki_qid migration completed.")
    except Exception as e:
        logger.exception(f"Error during wiki_qid migration: {e}")

    # 4. Run Analysis Pipeline
    logger.info("--- Starting Analysis Pipeline ---")
    analysis_start_time = time.time()
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.error(
            "CRITICAL: OpenAI API Key not found. Analysis requires API key. Skipping analysis."
        )
    else:
        # Option 1: Run the full pipeline wrapper
        pipeline_logs = run_full_pipeline_headless(api_key)
        for log_msg in pipeline_logs:
            logger.info(log_msg)  # Log messages returned by the pipeline

        # Option 2: Run individual steps (more verbose logging control here)
        # logger.info("Running Entity/CVE Extraction...")
        # run_entity_extraction_pipeline(api_key)
        # logger.info("Running Similarity-Based Grouping...")
        # run_article_grouping_pipeline(api_key) # Uses default threshold
        # logger.info("Running Trending Analysis...")
        # run_trending_analysis_pipeline(api_key)

    analysis_elapsed = time.time() - analysis_start_time
    logger.info(f"--- Analysis Pipeline Finished in {analysis_elapsed:.2f} seconds ---")

    run_elapsed = time.time() - run_start_time
    logger.info(
        f"--- Scheduled Run Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (Total Duration: {run_elapsed:.2f} seconds) ---"
    )


def main():
    """
    Main execution function: runs once immediately, then schedules regular runs.
    """
    # Run once immediately at startup
    try:
        run_scrapers_and_analysis()
    except Exception as e:
        logger.exception("Error during initial run.")

    # Schedule regular runs
    interval_minutes = int(os.environ.get("SCHEDULE_INTERVAL_MINUTES", "15"))
    interval_seconds = interval_minutes * 60
    logger.info(
        f"Setting up scheduler to run every {interval_minutes} minutes."
    )

    while True:
        try:
            logger.info(f"Sleeping for {interval_minutes} minutes until next run...")
            time.sleep(interval_seconds)

            # Run the full process
            run_scrapers_and_analysis()

        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user. Exiting...")
            break
        except Exception as e:
            logger.exception(f"Unhandled error in scheduler loop: {e}")
            # Avoid tight loops on persistent errors
            logger.info("Sleeping for 60 seconds before retrying...")
            time.sleep(60)


if __name__ == "__main__":
    main()
