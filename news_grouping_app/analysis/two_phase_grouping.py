# analysis/two_phase_grouping.py
# REFACTORED VERSION - Incorporates LLM checks, dynamic thresholds, context for new groups.

import json
import re
import logging
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os
import time
import sqlite3
from typing import Optional, List, Dict, Tuple, Any  # Added Any

# --- Database & Utility Imports ---
from news_grouping_app.db.database import get_connection, setup_database
from news_grouping_app.utils import approximate_tokens  # Assuming this exists

# --- LLM & Signature/Grouping Imports ---
try:
    from news_grouping_app.llm_calls import call_gpt_api
    from news_grouping_app.article_signature import generate_article_signature

    # Assumes enhanced_grouping.py has the UPDATED versions of these functions
    from news_grouping_app.enhanced_grouping import (
        get_existing_groups as get_existing_groups_enhanced,
        generate_group_signature,
        calculate_article_to_group_similarity,
        add_article_to_group,
    )

    # Optional: For consistency check after adding
    from news_grouping_app.analysis.consistency_checker import evaluate_group_consistency
except ImportError as e:
    logging.error(f"Failed to import necessary modules: {e}", exc_info=True)
    # Decide if you want to raise e or try to continue partially
    raise  # Stop execution if core components are missing

logger = logging.getLogger(__name__)

PREDEFINED_CATEGORIES = [
    "Science & Environment",
    "Business, Finance & Trade",
    "Artificial Intelligence & Machine Learning",
    "Software Development & Open Source",
    "Cybersecurity & Data Privacy",
    "Politics & Government",
    "Consumer Technology & Gadgets",
    "Automotive, Space & Transportation",
    "Enterprise Technology & Cloud Computing",
    "Other",
]

# --- Configuration ---
# LLM Check Configuration
ENABLE_LLM_MATCH_ASSESSMENT = (
    True  # Set to False to disable LLM checks for ambiguous cases
)
from news_grouping_app.config import OPENAI_MODEL
LLM_CHECK_MODEL = OPENAI_MODEL
AMBIGUITY_ZONE_BELOW_THRESHOLD = 0.10  # How far below threshold triggers check
AMBIGUITY_ZONE_ABOVE_THRESHOLD = 0.05  # How far above threshold triggers check
MAX_SCORE_GAP_FOR_AMBIGUITY = 0.08  # If second best is this close, trigger check

# Dynamic Threshold Rules (Example - customize as needed)
DEFAULT_SIMILARITY_THRESHOLD = 0.40  # Base threshold if no rules match
DYNAMIC_THRESHOLD_RULES = {
    "base": DEFAULT_SIMILARITY_THRESHOLD,
    "category_adjust": {  # Additive adjustments
        "Cybersecurity & Data Privacy": +0.05,
        "Artificial Intelligence & Machine Learning": +0.03,
        "Other": -0.03,
    },
    "size_adjust": {  # Additive adjustments based on article count in group
        "breakpoints": [1, 5, 10],  # Sizes at which adjustments change
        "adjustments": [
            +0.05,
            0.0,
            -0.03,
            -0.05,
        ],  # Adjustments for <=1, <=5, <=10, >10
    },
}

# --- Core Database Functions ---


def get_ungrouped_articles_for_processing(db_path: str = "db/news.db") -> pd.DataFrame:
    """Gets ungrouped articles (ID, title, content, date) ordered by date."""
    query = """
        SELECT a.id AS article_id, a.title, a.content, a.published_date
        FROM articles a
        WHERE NOT EXISTS (
            SELECT 1 FROM two_phase_article_group_memberships tgm WHERE tgm.article_id = a.id
        )
        ORDER BY a.published_date DESC
    """
    conn = None
    try:
        conn = get_connection(db_path)
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        logger.error(f"Error fetching ungrouped articles: {e}", exc_info=True)
        return pd.DataFrame()  # Return empty dataframe on error
    finally:
        if conn:
            conn.close()


def create_new_group_for_single_article(
    article_id: int,
    article_title: str,
    article_content: str,
    api_key: str,
    db_path: str = "db/news.db",
    closest_groups_info: Optional[List[Dict]] = None,  # <<< ADDED parameter
) -> Optional[Dict]:
    """
    Creates a new group, adds the article, considering near-miss groups for context.
    Returns the new group's basic dictionary or None on failure.
    """
    logger.info(f"Creating new group for Article ID: {article_id}")

    # --- Modify Prompt ---
    prompt = (
        f"Analyze this article and determine the most appropriate category, a concise group label, and a brief description (1-2 sentences).\n\n"
        f"Choose one category from this list:\n"
        + "\n".join(f"- {cat}" for cat in PREDEFINED_CATEGORIES)
        + f"\n\nIf none fit well, use 'Other'.\n\n"
        f"Article Title: {article_title}\nArticle Content (excerpt):\n{article_content[:3000]}...\n\n"
    )

    if closest_groups_info:
        prompt += "Context: This article did not strongly match existing groups based on automatic analysis. The closest groups found were:\n"
        for i, group_info in enumerate(
            closest_groups_info[:2]
        ):  # Show top 2 near misses
            label = group_info.get("label", "N/A")
            desc = group_info.get("desc", "N/A")
            score = group_info.get("score", -1.0)
            prompt += f"- Closest Group {i+1} (Score: {score:.2f}): '{label}' ({desc[:100]}...)\n"
        prompt += "\nConsidering this context, is the article discussing a truly new event/topic? If so, provide the category, label, and description for the NEW group. If it seems very related to one of the closest groups, still define a new group for now, perhaps using a label that reflects the specific nuance this article adds.\n\n"
    else:
        prompt += "\n"  # Ensure newline if no context

    prompt += (
        f"Return ONLY JSON in this exact format:\n"
        '{\n  "main_topic": "Chosen Category",\n  "group_label": "Concise Group Label",\n  "description": "Brief description."\n}'
    )
    # --- End Prompt Modification ---

    messages = [
        {
            "role": "system",
            "content": "You are an expert news analyst. Define a new group based on the provided article, considering the context of near-miss groups if provided. Respond only in JSON.",
        },
        {"role": "user", "content": prompt},
    ]

    response = call_gpt_api(
        messages, api_key, model=LLM_CHECK_MODEL
    )  # Use configured model
    if not response:
        logger.error(
            f"LLM call failed for creating new group for article {article_id}."
        )
        return None

    try:
        cleaned = response.strip().strip("```json").strip("```").strip()
        group_info = json.loads(cleaned)
        main_topic = group_info.get("main_topic", "Other")
        group_label = group_info.get("group_label", f"Group for Article {article_id}")
        description = group_info.get("description", article_title)
        if main_topic not in PREDEFINED_CATEGORIES:
            main_topic = "Other"
    except json.JSONDecodeError as e:
        logger.error(
            f"Failed to parse LLM JSON for article {article_id}: {e}\nResponse: {cleaned}"
        )
        main_topic, group_label, description = (
            "Other",
            f"Group for Article {article_id} (Auto)",
            article_title,
        )

    conn = get_connection(db_path)
    cursor = conn.cursor()
    new_group_dict = None
    try:
        cursor.execute("BEGIN")  # Use transaction
        cursor.execute(
            """
            INSERT INTO two_phase_article_groups (main_topic, sub_topic, group_label, description, consistency_score)
            VALUES (?, ?, ?, ?, ?)
            """,
            (main_topic, "", group_label, description, 0.7),  # Initial score
        )
        new_group_id = cursor.lastrowid
        logger.info(
            f"Created new group (ID: {new_group_id}, Label: '{group_label}') for article {article_id}."
        )

        # Use the existing cursor for add_article_to_group
        success = add_article_to_group(
            article_id, new_group_id, db_path=db_path, cursor=cursor
        )

        if not success:
            logger.error(
                f"Failed to add article {article_id} to its newly created group {new_group_id}"
            )
            conn.rollback()
            return None

        conn.commit()

        new_group_dict = {
            "group_id": new_group_id,
            "main_topic": main_topic,
            "sub_topic": "",
            "group_label": group_label,
            "description": description,
            "consistency_score": 0.7,
            "article_ids": [article_id],
            # Include other fields needed for signature generation if different from DB defaults
        }
        return new_group_dict

    except sqlite3.Error as e:
        logger.error(
            f"Database error creating new group for article {article_id}: {e}",
            exc_info=True,
        )
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


# --- Main Processing Logic ---


def _calculate_dynamic_threshold(
    group_dict: Dict, base_threshold: float, rules: Dict
) -> float:
    """Calculates the similarity threshold for a specific group based on rules."""
    threshold = base_threshold
    category = group_dict.get("main_topic")
    group_size = len(group_dict.get("article_ids", []))

    # Apply category adjustment
    if category and "category_adjust" in rules:
        threshold += rules["category_adjust"].get(category, 0.0)

    # Apply size adjustment
    if "size_adjust" in rules:
        breakpoints = rules["size_adjust"].get("breakpoints", [])
        adjustments = rules["size_adjust"].get("adjustments", [])
        if len(adjustments) == len(breakpoints) + 1:
            # Find the right adjustment based on size
            adj_index = 0
            for i, bp in enumerate(breakpoints):
                if group_size > bp:
                    adj_index = i + 1
                else:
                    break
            threshold += adjustments[adj_index]

    # Clamp threshold to a reasonable range (e.g., 0.1 to 0.9)
    return max(0.1, min(0.9, threshold))


def _get_group_details_for_prompt(
    group_id: int, groups_with_signatures: List[Tuple[Dict, Dict]]
) -> Optional[Dict]:
    """Helper to find group details needed for LLM prompts."""
    for group_dict, group_sig in groups_with_signatures:
        if group_dict.get("group_id") == group_id:
            # Extract key entity names (top N)
            key_entities = [
                e.get("entity_name")
                for e in group_sig.get("primary_entities", [])[:5]
                if e.get("entity_name")
            ]
            return {
                "id": group_id,
                "label": group_dict.get("group_label", "N/A"),
                "description": group_dict.get("description", "N/A"),
                "key_entities": key_entities,
            }
    return None


def process_single_ungrouped_article(
    article_id: int,
    article_title: str,
    article_content: str,
    existing_groups_with_signatures: List[
        Tuple[Dict, Dict]
    ],  # List of [group_dict, group_signature]
    threshold_rules: Dict,  # Use rules dictionary
    api_key: str,
    db_path: str = "db/news.db",
) -> Dict:
    """
    Processes a single article: matches to existing groups using dynamic thresholds
    and optional LLM checks, or creates a new one with context.
    Returns a result dictionary including new group info if created.
    """
    logger.debug(f"Processing article {article_id} ('{article_title[:50]}...')")
    try:
        article_sig = generate_article_signature(article_id, db_path)
        if not article_sig:
            logger.warning(
                f"Could not generate signature for article {article_id}. Skipping."
            )
            return {
                "status": "error",
                "message": "Signature generation failed",
                "article_id": article_id,
            }

        # Store similarity scores for all groups for context/LLM check
        group_scores = []

        base_threshold = threshold_rules.get("base", DEFAULT_SIMILARITY_THRESHOLD)

        # --- Compare article against all existing groups ---
        for group_data in existing_groups_with_signatures:
            group_dict, group_sig = group_data  # Unpack the tuple/list

            if not group_sig or not group_dict.get("article_ids"):
                continue

            # Calculate dynamic threshold for *this specific group*
            current_dynamic_threshold = _calculate_dynamic_threshold(
                group_dict, base_threshold, threshold_rules
            )

            # Calculate similarity (ASSUMES THIS FUNCTION IS UPDATED)
            # Pass api_key if calculate_... needs it for description similarity
            similarity_scores = calculate_article_to_group_similarity(
                article_sig, group_sig, api_key=api_key
            )
            composite_score = similarity_scores.get("composite_score", 0.0)

            group_scores.append(
                {
                    "group_id": group_dict["group_id"],
                    "group_label": group_dict["group_label"],
                    "description": group_dict.get("description", ""),
                    "score": composite_score,
                    "dynamic_threshold": current_dynamic_threshold,  # Store the threshold used for this comparison
                }
            )

        # --- Find Best Match and Check Ambiguity ---
        best_match_group = None
        best_match_score = -1.0
        best_match_threshold_used = base_threshold  # Default
        second_best_score = -1.0

        if group_scores:
            group_scores.sort(key=lambda x: x["score"], reverse=True)
            best_match_group = group_scores[0]
            best_match_score = best_match_group["score"]
            best_match_threshold_used = best_match_group["dynamic_threshold"]
            if len(group_scores) > 1:
                second_best_score = group_scores[1]["score"]

        logger.debug(
            f"Article {article_id}: Best match Group {best_match_group['group_id'] if best_match_group else 'None'} (Score: {best_match_score:.3f}, DynThr: {best_match_threshold_used:.3f}, 2ndBest: {second_best_score:.3f})"
        )

        # --- Decision Logic ---
        decision = "create_new"  # Default action
        final_group_id = None
        llm_check_triggered = False

        if best_match_group:
            is_match_above_threshold = best_match_score >= best_match_threshold_used
            is_in_ambiguity_zone = (
                best_match_threshold_used - AMBIGUITY_ZONE_BELOW_THRESHOLD
                <= best_match_score
                < best_match_threshold_used + AMBIGUITY_ZONE_ABOVE_THRESHOLD
            ) or (
                is_match_above_threshold
                and best_match_score - second_best_score < MAX_SCORE_GAP_FOR_AMBIGUITY
            )

            # Condition 1: Clear match above threshold, not ambiguous
            if is_match_above_threshold and not is_in_ambiguity_zone:
                decision = "add_to_existing"
                final_group_id = best_match_group["group_id"]

            # Condition 2: Ambiguous case - potentially use LLM
            elif ENABLE_LLM_MATCH_ASSESSMENT and is_in_ambiguity_zone:
                llm_check_triggered = True
                logger.info(
                    f"Article {article_id}: Ambiguous match score ({best_match_score:.3f} vs Thr {best_match_threshold_used:.3f}). Triggering LLM check."
                )

                # Prepare context for LLM
                article_entities = [
                    e.get("entity_name")
                    for e in article_sig.get("primary_entities", [])[:10]
                    if e.get("entity_name")
                ]
                candidate_group_ids = [
                    g["group_id"] for g in group_scores[:3]
                ]  # Top 3 candidates
                candidate_groups_details = [
                    _get_group_details_for_prompt(gid, existing_groups_with_signatures)
                    for gid in candidate_group_ids
                ]
                candidate_groups_details = [
                    d for d in candidate_groups_details if d
                ]  # Filter out None

                if candidate_groups_details:
                    prompt = (
                        f"Article ID {article_id} (Title: '{article_title}', Entities: {article_entities}) needs grouping.\n"
                        f"It has the following similarity scores to existing groups (higher is better):\n"
                    )
                    for cand_g in group_scores[:3]:
                        prompt += f"- Group {cand_g['group_id']} '{cand_g['group_label']}': Score = {cand_g['score']:.3f} (Threshold for this group was {cand_g['dynamic_threshold']:.3f})\n"

                    prompt += "\nBased on the *meaning and topic* described below, which group is the best fit? Or should it be in a 'None' (new) group?\n\n"
                    for i, details in enumerate(candidate_groups_details):
                        prompt += f"Group {details['id']}:\n  Label: {details['label']}\n  Description: {details['description'][:500]}\n  Key Entities: {details['key_entities']}\n\n"

                    prompt += "Respond with ONLY the best matching group ID number (e.g., '123') or the word 'None' if no group is a good semantic fit."

                    messages = [{"role": "user", "content": prompt}]
                    llm_decision_str = call_gpt_api(
                        messages, api_key, model=LLM_CHECK_MODEL
                    )

                    llm_group_id = None
                    if llm_decision_str:
                        try:
                            llm_group_id = int(llm_decision_str.strip())
                            if llm_group_id not in candidate_group_ids:
                                logger.warning(
                                    f"LLM returned group ID {llm_group_id} which was not in candidates {candidate_group_ids}. Ignoring."
                                )
                                llm_group_id = None
                        except (ValueError, TypeError):
                            if "none" in llm_decision_str.lower():
                                llm_group_id = "None"
                            else:
                                logger.warning(
                                    f"LLM assessment returned unparsable result: '{llm_decision_str}'. Falling back."
                                )
                                llm_group_id = None  # Treat as failure
                    else:
                        logger.error(f"LLM assessment failed for Article {article_id}.")
                        llm_group_id = None  # Treat as failure

                    if isinstance(llm_group_id, int):
                        logger.info(
                            f"LLM Assessment: Match Article {article_id} to Group {llm_group_id}."
                        )
                        decision = "add_to_existing"
                        final_group_id = llm_group_id
                    elif llm_group_id == "None":
                        logger.info(
                            f"LLM Assessment: Article {article_id} does not match candidates. Create new group."
                        )
                        decision = "create_new"
                    else:  # LLM failed or unclear response
                        logger.info(
                            "LLM Assessment failed or unclear. Falling back to threshold logic."
                        )
                        # Fallback: Use the original threshold comparison
                        if is_match_above_threshold:
                            decision = "add_to_existing"
                            final_group_id = best_match_group["group_id"]
                        else:
                            decision = "create_new"
                else:
                    logger.warning(
                        f"Could not get candidate group details for LLM check for article {article_id}. Falling back."
                    )
                    # Fallback: Use the original threshold comparison
                    if is_match_above_threshold:
                        decision = "add_to_existing"
                        final_group_id = best_match_group["group_id"]
                    else:
                        decision = "create_new"

            # Condition 3: Score is clearly below threshold
            elif not is_match_above_threshold:
                decision = "create_new"

        # --- Execute Action ---
        if decision == "add_to_existing":
            logger.info(
                f"Decision: Add Article {article_id} to Group {final_group_id} (Score: {best_match_score:.3f}, Thr: {best_match_threshold_used:.3f}, LLM Checked: {llm_check_triggered})"
            )
            # Use a dedicated connection/cursor for the write operation if add_article_to_group doesn't manage it
            conn_add = None
            cursor_add = None
            success = False
            try:
                conn_add = get_connection(db_path)
                cursor_add = conn_add.cursor()
                cursor_add.execute("BEGIN")
                success = add_article_to_group(
                    article_id, final_group_id, db_path=db_path, cursor=cursor_add
                )
                if success:
                    conn_add.commit()
                else:
                    conn_add.rollback()
            except Exception as add_err:
                logger.error(
                    f"Error during add_article_to_group transaction for article {article_id}: {add_err}"
                )
                if conn_add:
                    conn_add.rollback()
                success = False
            finally:
                if conn_add:
                    conn_add.close()

            if success:
                # Update local list for subsequent checks in this run
                for i, (grp_d, grp_s) in enumerate(existing_groups_with_signatures):
                    if grp_d["group_id"] == final_group_id:
                        existing_groups_with_signatures[i][0]["article_ids"].append(
                            article_id
                        )
                        # Re-generating signature here is too costly for a long run.
                        break

                # --- Optional: Post-Add Consistency Check ---
                try:
                    updated_group_data_dict = next(
                        (
                            g[0]
                            for g in existing_groups_with_signatures
                            if g[0]["group_id"] == final_group_id
                        ),
                        None,
                    )
                    if (
                        updated_group_data_dict
                        and len(updated_group_data_dict.get("article_ids", [])) > 1
                    ):
                        evaluation = evaluate_group_consistency(
                            updated_group_data_dict, api_key, db_path
                        )
                        consistency_score = evaluation.get("consistency_score", 0.5)
                        logger.info(
                            f"Consistency check for Group {final_group_id} after adding Article {article_id}: Score={consistency_score:.2f}"
                        )
                        # TODO: Potentially store this score or act on it if needed.
                except NameError:  # evaluate_group_consistency not imported
                    pass  # Silently skip if checker not available
                except Exception as cons_err:
                    logger.error(
                        f"Error during post-add consistency check: {cons_err}",
                        exc_info=False,
                    )
                # --- End Consistency Check ---

                return {
                    "status": "added_to_existing",
                    "article_id": article_id,
                    "group_id": final_group_id,
                    "group_label": best_match_group["group_label"],
                    "score": best_match_score,
                }
            else:
                return {
                    "status": "error",
                    "message": f"Failed to add article {article_id} to group {final_group_id}",
                    "article_id": article_id,
                }

        elif decision == "create_new":
            logger.info(
                f"Decision: Create new group for Article {article_id} (Best score: {best_match_score:.3f} vs Thr: {best_match_threshold_used:.3f}, LLM Checked: {llm_check_triggered})"
            )
            # Prepare context of near misses for the LLM
            near_miss_groups = []
            if group_scores:
                near_miss_groups = [
                    {
                        "label": g["group_label"],
                        "desc": g["description"],
                        "score": g["score"],
                    }
                    for g in group_scores[:2]  # Pass top 2 regardless of score
                ]

            new_group_info = create_new_group_for_single_article(
                article_id,
                article_title,
                article_content,
                api_key,
                db_path,
                closest_groups_info=near_miss_groups,  # Pass near-miss context
            )
            if new_group_info:
                return {
                    "status": "created_new",
                    "article_id": article_id,
                    "new_group_info": new_group_info,
                }
            else:
                return {
                    "status": "error",
                    "message": f"Failed to create new group for article {article_id}",
                    "article_id": article_id,
                }

        else:  # Should not happen
            logger.error(
                f"Invalid decision state '{decision}' for article {article_id}"
            )
            return {
                "status": "error",
                "message": "Internal error in decision logic",
                "article_id": article_id,
            }

    except Exception as e:
        logger.exception(f"Unexpected error processing article {article_id}: {e}")
        return {"status": "error", "message": str(e), "article_id": article_id}


def run_grouping_update(
    threshold_rules: Dict = DYNAMIC_THRESHOLD_RULES,  # Use rules dict
    api_key: Optional[str] = None,
    db_path: str = "db/news.db",
    batch_delay: float = 0.2,
):
    """
    Main function: processes ungrouped articles, matching or creating groups,
    using dynamic thresholds and optional LLM checks.
    """
    logger.info("--- Starting Grouping Update Run ---")
    if api_key is None:
        api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error(
            "API Key not found. LLM checks and new group creation will fail. Aborting grouping."
        )
        return

    logger.info("Fetching existing groups...")
    existing_groups = get_existing_groups_enhanced(db_path=db_path)
    logger.info(f"Found {len(existing_groups)} existing groups initially.")

    logger.info(f"Generating initial signatures for {len(existing_groups)} groups...")
    # This list will be MODIFIED during the run: List[Tuple[Dict, Dict]] -> List[[group_dict, group_sig]]
    existing_groups_with_signatures = []
    for group in existing_groups:
        # Ensure group has articles before generating signature
        if group.get("article_ids"):
            try:
                group_sig = generate_group_signature(
                    group, db_path
                )  # Assumes updated version
                if group_sig:
                    existing_groups_with_signatures.append(
                        [group, group_sig]
                    )  # Use list for mutability
                else:
                    logger.warning(
                        f"Could not generate signature for group {group.get('group_id')}"
                    )
            except Exception as sig_err:
                logger.error(
                    f"Error generating signature for group {group.get('group_id')}: {sig_err}",
                    exc_info=False,
                )
    logger.info(
        f"Finished generating initial signatures for {len(existing_groups_with_signatures)} groups."
    )

    logger.info("Fetching ungrouped articles...")
    ungrouped_df = get_ungrouped_articles_for_processing(db_path)
    if ungrouped_df.empty:
        logger.info("No ungrouped articles found to process.")
        logger.info("--- Grouping Update Run Finished ---")
        return
    logger.info(f"Found {len(ungrouped_df)} ungrouped articles to process.")

    results_summary = {"added_to_existing": 0, "created_new": 0, "errors": 0}
    total_articles = len(ungrouped_df)

    for index, row in ungrouped_df.iterrows():
        article_id = row["article_id"]
        article_title = row["title"]
        article_content = row["content"]

        logger.info(
            f"Processing article {index + 1}/{total_articles} (ID: {article_id})"
        )
        if not article_content:
            logger.warning(f"Article {article_id} has no content. Skipping.")
            results_summary["errors"] += 1
            continue

        # Pass the CURRENT list of signatures and threshold rules
        result = process_single_ungrouped_article(
            article_id,
            article_title,
            article_content,
            existing_groups_with_signatures,  # This list can now grow
            threshold_rules,  # Pass rules dict
            api_key,
            db_path,
        )

        # Process results and update local signature list if new group created
        if result["status"] == "added_to_existing":
            results_summary["added_to_existing"] += 1
        elif result["status"] == "created_new":
            results_summary["created_new"] += 1
            new_group_info = result.get("new_group_info")
            if new_group_info:
                logger.info(
                    f"Generating signature for newly created group {new_group_info.get('group_id')}..."
                )
                try:
                    new_group_sig = generate_group_signature(new_group_info, db_path)
                    if new_group_sig:
                        existing_groups_with_signatures.append(
                            [new_group_info, new_group_sig]
                        )  # Append mutable list
                        logger.info(
                            f"Added new group {new_group_info.get('group_id')} to comparison list ({len(existing_groups_with_signatures)} total)."
                        )
                    else:
                        logger.warning(
                            f"Failed to generate signature for new group {new_group_info.get('group_id')}. It won't be used for matching in this run."
                        )
                except Exception as new_sig_err:
                    logger.error(
                        f"Error generating signature for new group {new_group_info.get('group_id')}: {new_sig_err}",
                        exc_info=False,
                    )
            else:
                logger.error(
                    "Processing status was 'created_new' but no group info was returned."
                )
                results_summary["errors"] += 1
        elif result["status"] == "error":
            results_summary["errors"] += 1
            logger.error(
                f"Error processing article {article_id}: {result.get('message')}"
            )

        if batch_delay > 0:
            time.sleep(batch_delay)

    logger.info("--- Grouping Update Summary ---")
    logger.info(f"Total articles processed: {total_articles}")
    logger.info(f"Added to existing groups: {results_summary['added_to_existing']}")
    logger.info(f"Created new groups: {results_summary['created_new']}")
    logger.info(f"Errors encountered: {results_summary['errors']}")
    logger.info("--- Grouping Update Run Finished ---")
