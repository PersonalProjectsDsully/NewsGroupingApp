
import os
import logging
import time
from datetime import datetime, timedelta

# --- Core Analysis Modules ---
from analysis.entity_extraction import extract_entities_for_all_articles
from analysis.company_extraction import extract_company_names_for_all_articles # Kept for now
from analysis.cve_extraction import process_cves_in_articles, update_cve_details_from_api

# --- Import the NEW grouping function and its default rules ---
from analysis.two_phase_grouping import run_grouping_update, DYNAMIC_THRESHOLD_RULES # <<< IMPORT RULES

# --- Trending Analysis ---
from analysis.trending_analysis import run_trending_analysis, cleanup_old_trends

# --- Optional Group Merging ---
try:
    from analysis.group_merging import merge_similar_groups
    GROUP_MERGING_ENABLED = True
    DEFAULT_MERGE_THRESHOLD = 0.60 # Configurable merge threshold
except ImportError:
    logger.warning("Group merging module ('analysis/group_merging.py') not found. Skipping merge step.")
    GROUP_MERGING_ENABLED = False

logger = logging.getLogger(__name__)


def run_entity_extraction_pipeline(api_key=None, db_path="db/news.db"):
    """
    Run the entity, company, and CVE extraction pipeline.
    These need to run BEFORE similarity-based grouping.
    """
    if api_key is None:
        api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("No API key found for entity extraction.")
        return ["Error: No API key for entity extraction"]

    logs = []
    start_time = time.time()
    logger.info("Starting Entity/CVE Extraction Pipeline...")
    logs.append("PHASE 1: Entity/CVE Extraction Started")

    try:
        # Extract Entities (relies on LLM)
        logger.info("Extracting entities...")
        logs.append("Extracting entities...")
        extract_entities_for_all_articles(api_key, db_path=db_path)
        logs.append("Done extracting entities.")

        # Extract Company Names (relies on LLM) - Consider if still needed alongside entity extraction
        logger.info("Extracting company names...")
        logs.append("Extracting company names...")
        extract_company_names_for_all_articles(api_key, db_path=db_path)
        logs.append("Done extracting company names.")

        # Extract CVE Mentions (Regex-based)
        logger.info("Processing CVE mentions in articles...")
        logs.append("Processing CVE mentions...")
        process_cves_in_articles(db_path=db_path)
        logs.append("Done processing CVE mentions.")

        # Update CVE Details (API call to MITRE)
        logger.info("Updating CVE details from API...")
        logs.append("Updating CVE details...")
        update_cve_details_from_api(db_path=db_path)
        logs.append("Done updating CVE details.")

    except Exception as e:
        logger.exception("Error during Entity/CVE Extraction Pipeline")
        logs.append(f"Error in Phase 1: {e}")

    elapsed = time.time() - start_time
    logs.append(f"PHASE 1 Finished in {elapsed:.2f} seconds.")
    logger.info(f"Entity/CVE Extraction Pipeline finished in {elapsed:.2f}s")
    return logs

# <<< MODIFIED Function Signature and Call >>>
def run_article_grouping_pipeline(
    api_key=None,
    db_path="db/news.db",
    threshold_rules=DYNAMIC_THRESHOLD_RULES # <<< Use rules dict, import default
):
    """
    Run the article grouping pipeline using the NEW similarity-based approach
    with dynamic thresholds and optional LLM checks.
    """
    if api_key is None:
        api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("No API key found. Cannot create new groups.")
        return ["Error: No API key for article grouping"]

    logs = []
    start_time = time.time()
    logger.info("Starting Similarity-Based Grouping Pipeline...")
    logs.append("PHASE 2: Similarity-Based Grouping Started")

    try:
        # Call the main function from the refactored two_phase_grouping
        logger.info(f"Running grouping update with dynamic threshold rules...")
        logs.append(f"Running grouping update with dynamic threshold rules...")
        run_grouping_update(
            threshold_rules=threshold_rules, # <<< Pass the rules dict
            api_key=api_key,
            db_path=db_path
            # batch_delay can be added here if needed, defaults defined in run_grouping_update
        )
        logs.append("Grouping update process completed.")

    except Exception as e:
        logger.exception("Error during Similarity-Based Grouping Pipeline")
        logs.append(f"Error in Phase 2: {e}")

    elapsed = time.time() - start_time
    logs.append(f"PHASE 2 Finished in {elapsed:.2f} seconds.")
    logger.info(f"Similarity-Based Grouping Pipeline finished in {elapsed:.2f}s")
    return logs
# <<< END MODIFIED Function >>>


def run_trending_analysis_pipeline(api_key=None, db_path="db/news.db"):
    """
    Run the 48-hour trending analysis pipeline.
    """
    if api_key is None:
        api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("No API key provided for trending analysis.")
        return ["Error: No API key for trending analysis"]

    logs = []
    start_time = time.time()
    logger.info("Starting Trending Analysis Pipeline...")
    logs.append("PHASE 3: Trending Analysis Started")

    try:
        # Cleanup old trends first
        logger.info("Cleaning up old trending data...")
        logs.append("Cleaning up old trending data...")
        cleanup_old_trends(db_path=db_path)
        logs.append("Done cleaning up old trends.")

        # Run the trending analysis
        logger.info("Running 48-hour trending analysis...")
        logs.append("Running 48-hour trending analysis...")
        from analysis.two_phase_grouping import PREDEFINED_CATEGORIES # Import here if needed
        run_trending_analysis(api_key, categories=PREDEFINED_CATEGORIES, db_path=db_path)
        logs.append("Done running trending analysis.")

    except Exception as e:
        logger.exception("Error during Trending Analysis Pipeline")
        logs.append(f"Error in Phase 3: {e}")

    elapsed = time.time() - start_time
    logs.append(f"PHASE 3 Finished in {elapsed:.2f} seconds.")
    logger.info(f"Trending Analysis Pipeline finished in {elapsed:.2f}s")
    return logs


def run_full_pipeline_headless(api_key=None, db_path="db/news.db"):
    """
    Run the full analysis pipeline (Extraction -> Grouping -> Trending) in headless mode.
    Returns logs.
    """
    overall_start_time = time.time()
    logger.info("--- Running Full Analysis Pipeline ---")

    if api_key is None:
        api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("Critical Error: API Key not found. Aborting pipeline.")
        return ["Critical Error: No API key found. Pipeline aborted."]

    all_logs = ["Pipeline Started at " + datetime.now().strftime('%Y-%m-%d %H:%M:%S')]

    # PHASE 1: Entity/CVE Extraction
    entity_logs = run_entity_extraction_pipeline(api_key, db_path)
    all_logs.extend(entity_logs)
    if any("Error:" in log for log in entity_logs):
         logger.error("Stopping pipeline due to errors in Entity/CVE Extraction.")
         all_logs.append("Pipeline stopped due to errors in Phase 1.")
         return all_logs

    # PHASE 2: Similarity-Based Grouping
    # <<< Pass default rules or allow customization >>>
    grouping_logs = run_article_grouping_pipeline(api_key, db_path, threshold_rules=DYNAMIC_THRESHOLD_RULES)
    all_logs.extend(grouping_logs)
    if any("Error:" in log for log in grouping_logs):
         logger.error("Stopping pipeline due to errors in Grouping.")
         all_logs.append("Pipeline stopped due to errors in Phase 2.")
         return all_logs

    # PHASE 2.5: Group Merging (Optional)
    if GROUP_MERGING_ENABLED:
        try:
            logger.info(f"Starting Group Merging with threshold {DEFAULT_MERGE_THRESHOLD}...")
            all_logs.append(f"PHASE 2.5: Group Merging Started (Threshold: {DEFAULT_MERGE_THRESHOLD})")
            merge_stats = merge_similar_groups(DEFAULT_MERGE_THRESHOLD, api_key, db_path)
            all_logs.append(f"Group Merging completed: {merge_stats.get('merged_pairs', 0)} pairs merged, {merge_stats.get('errors', 0)} errors.")
            logger.info(f"Group Merging completed: {merge_stats.get('merged_pairs', 0)} pairs merged.")
        except Exception as merge_err:
            logger.exception("Error during Group Merging")
            all_logs.append(f"Error in Phase 2.5: {merge_err}")
    else:
        all_logs.append("PHASE 2.5: Skipped (module not found or disabled).")


    # PHASE 3: Trending Analysis
    trending_logs = run_trending_analysis_pipeline(api_key, db_path)
    all_logs.extend(trending_logs)
    if any("Error:" in log for log in trending_logs):
         logger.warning("Errors occurred during Trending Analysis, but pipeline finished Phases 1 & 2.")
         all_logs.append("Warnings occurred during Phase 3.")

    overall_elapsed = time.time() - overall_start_time
    all_logs.append(f"\n--- Full Pipeline Completed in {overall_elapsed:.2f} seconds ---")
    logger.info(f"--- Full Analysis Pipeline Completed in {overall_elapsed:.2f} seconds ---")

    return all_logs


def schedule_regular_cleanup(db_path="db/news.db"):
    """
    Schedule regular cleanup of trending data.
    Placeholder - integrate with your scheduler (APScheduler, cron, etc.).
    """
    logger.info("Scheduled cleanup check: Running cleanup_old_trends.")
    try:
        cleanup_old_trends(db_path=db_path)
        logger.info("Scheduled cleanup completed successfully.")
        return True
    except Exception as e:
        logger.exception("Error during scheduled cleanup.")
        return False