# analysis/group_merging.py
import logging
import sqlite3
import json
import time
from typing import List, Dict, Tuple, Optional

# Assuming these are in the parent directory or PYTHONPATH is set correctly
from db.database import get_connection
from llm_calls import call_gpt_api

# Import necessary functions from enhanced_grouping (needs to be importable)
try:
    from enhanced_grouping import (
        get_existing_groups as get_existing_groups_enhanced,
        generate_group_signature,
        calculate_article_to_group_similarity,
    )
except ImportError as e:
    logging.error(f"Failed to import from enhanced_grouping for merging: {e}")
    # Define dummy functions or raise error if essential
    get_existing_groups_enhanced = None
    generate_group_signature = None
    calculate_article_to_group_similarity = None

logger = logging.getLogger(__name__)
MERGE_LLM_MODEL = "o3-mini"  # Model for generating merged labels/desc


def _calculate_group_similarity(
    group_sig_a: Dict, group_sig_b: Dict, api_key: Optional[str]
) -> float:
    """Calculate similarity between two group signatures."""
    if not calculate_article_to_group_similarity:
        return 0.0

    # Option 1: Treat A as article, B as group (and maybe average the reverse)
    sim_a_b_results = calculate_article_to_group_similarity(
        group_sig_a, group_sig_b, api_key
    )
    sim_b_a_results = calculate_article_to_group_similarity(
        group_sig_b, group_sig_a, api_key
    )

    sim_a_b = sim_a_b_results.get("composite_score", 0.0)
    sim_b_a = sim_b_a_results.get("composite_score", 0.0)

    # Average the two perspectives, or just take the max? Average is safer.
    avg_sim = (sim_a_b + sim_b_a) / 2.0

    # Option 2 (More Advanced): Add direct LLM comparison of labels/descriptions
    label_desc_sim = 0.0
    if api_key and group_sig_a.get("description") and group_sig_b.get("description"):
        prompt = (
            f"Rate the semantic similarity of these two group concepts on a scale of 0.0 to 1.0. Focus only on whether they describe the exact same core event or topic.\n\n"
            f"Group A:\nLabel: {group_sig_a.get('group_label', '')}\nDescription: {group_sig_a.get('description', '')[:500]}\n\n"
            f"Group B:\nLabel: {group_sig_b.get('group_label', '')}\nDescription: {group_sig_b.get('description', '')[:500]}\n\n"
            f"Similarity Score (0.0-1.0):"
        )
        messages = [{"role": "user", "content": prompt}]
        score_str = call_gpt_api(messages, api_key, model=MERGE_LLM_MODEL)
        try:
            label_desc_sim = float(score_str)
        except:
            label_desc_sim = 0.0

    # Combine scores (adjust weighting as needed)
    final_sim = (avg_sim * 0.7) + (label_desc_sim * 0.3)  # Example weighting

    logger.debug(
        f"Group Sim {group_sig_a.get('group_id')} vs {group_sig_b.get('group_id')}: AvgSigSim={avg_sim:.3f}, LLMLabelSim={label_desc_sim:.3f} -> Final={final_sim:.3f}"
    )
    return final_sim


def merge_similar_groups(
    merge_threshold: float, api_key: str, db_path: str = "db/news.db"
) -> Dict:
    """
    Finds and merges highly similar groups.

    Args:
        merge_threshold: Similarity score above which groups should be merged.
        api_key: OpenAI API key.
        db_path: Path to the database.

    Returns:
        Dictionary containing merge statistics.
    """
    if not all(
        [
            get_existing_groups_enhanced,
            generate_group_signature,
            calculate_article_to_group_similarity,
        ]
    ):
        logger.error(
            "Missing necessary functions from enhanced_grouping. Cannot perform merge."
        )
        return {"merged_pairs": 0, "errors": 1}

    logger.info(f"--- Starting Group Merging Pass (Threshold: {merge_threshold}) ---")
    start_time = time.time()
    merged_pairs_count = 0
    errors = 0
    processed_group_ids = set()  # Track IDs involved in a merge to avoid re-merging

    groups = get_existing_groups_enhanced(db_path)
    if len(groups) < 2:
        logger.info("Not enough groups (< 2) to perform merging.")
        return {"merged_pairs": 0, "errors": 0}

    logger.info(
        f"Generating signatures for {len(groups)} groups for merging analysis..."
    )
    groups_with_signatures = []
    for group in groups:
        if group.get("article_ids"):  # Only consider groups with articles
            group_sig = generate_group_signature(group, db_path)
            if group_sig:
                groups_with_signatures.append((group, group_sig))
            else:
                logger.warning(
                    f"Could not generate signature for group {group.get('group_id')} during merge prep."
                )

    logger.info("Comparing group pairs for potential merging...")
    # Iterate through unique pairs
    logger.info("Comparing group pairs for potential merging...")
    # Iterate through unique pairs
    for i in range(len(groups_with_signatures)):
        for j in range(i + 1, len(groups_with_signatures)):
            group_a_dict, group_a_sig = groups_with_signatures[i]
            group_b_dict, group_b_sig = groups_with_signatures[j]

            group_a_id = group_a_dict.get("group_id")
            group_b_id = group_b_dict.get("group_id")

            if group_a_id in processed_group_ids or group_b_id in processed_group_ids:
                continue

            # Calculate similarity between the two groups
            similarity = _calculate_group_similarity(group_a_sig, group_b_sig, api_key)

            # <<< --- ADDED LOGGING HERE --- >>>
            logger.info(
                f"Comparing Group {group_a_id} ('{group_a_dict.get('group_label', '')[:30]}...') vs "
                f"Group {group_b_id} ('{group_b_dict.get('group_label', '')[:30]}...'). "
                f"Similarity: {similarity:.4f}, Merge Threshold: {merge_threshold}"
            )
            # <<< --- END ADDED LOGGING --- >>>

            if similarity >= merge_threshold:
                logger.info(
                    f"Potential Merge Found: Group {group_a_id} ('{group_a_dict.get('group_label')}') and Group {group_b_id} ('{group_b_dict.get('group_label')}') - Similarity: {similarity:.3f}"
                )

                # --- Decide which group survives ---
                # Simple rule: Keep the one with more articles, or the older one if counts are equal
                # (Requires 'created_at' in group_dict or fetch it)
                # For now, let's assume group_a survives, group_b is merged into it. Need better logic later.
                surviving_group_id = group_a_id
                deleted_group_id = group_b_id
                surviving_group_dict = group_a_dict
                deleted_group_dict = group_b_dict

                # --- Get Merged Label/Description from LLM ---
                merged_label = f"{surviving_group_dict.get('group_label')} / {deleted_group_dict.get('group_label')}"  # Fallback
                merged_description = f"{surviving_group_dict.get('description')}\n---\n{deleted_group_dict.get('description')}"  # Fallback

                merge_prompt = (
                    f"These two article groups seem to cover the same topic. Suggest a concise, unified label and a brief description (1-2 sentences) for the merged group.\n\n"
                    f"Group A (ID {surviving_group_id}):\nLabel: {surviving_group_dict.get('group_label')}\nDescription: {surviving_group_dict.get('description')}\n\n"
                    f"Group B (ID {deleted_group_id}):\nLabel: {deleted_group_dict.get('group_label')}\nDescription: {deleted_group_dict.get('description')}\n\n"
                    f'Respond ONLY in JSON format: {{"merged_label": "New Label", "merged_description": "New Description"}}'
                )
                messages = [{"role": "user", "content": merge_prompt}]
                llm_response = call_gpt_api(messages, api_key, model=MERGE_LLM_MODEL)

                try:
                    if llm_response:
                        merge_data = json.loads(
                            llm_response.strip().strip("```json").strip("```")
                        )
                        merged_label = merge_data.get("merged_label", merged_label)
                        merged_description = merge_data.get(
                            "merged_description", merged_description
                        )
                        logger.info(f"LLM suggested merge label: '{merged_label}'")
                except Exception as llm_err:
                    logger.warning(
                        f"Could not parse LLM merge suggestion: {llm_err}. Using fallback label/description."
                    )

                # --- Perform Database Merge ---
                conn_merge = None
                try:
                    conn_merge = get_connection(db_path)
                    cursor_merge = conn_merge.cursor()
                    cursor_merge.execute("BEGIN TRANSACTION")

                    # 1. Update surviving group's label and description
                    cursor_merge.execute(
                        """
                        UPDATE two_phase_article_groups
                        SET group_label = ?, description = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE group_id = ?
                    """,
                        (merged_label, merged_description, surviving_group_id),
                    )

                    # 2. Reassign articles from the deleted group to the surviving group
                    # Use INSERT OR IGNORE to handle cases where an article might somehow
                    # exist in both (shouldn't happen but safer)
                    cursor_merge.execute(
                        """
                        INSERT OR IGNORE INTO two_phase_article_group_memberships (article_id, group_id)
                        SELECT article_id, ? FROM two_phase_article_group_memberships WHERE group_id = ?
                    """,
                        (surviving_group_id, deleted_group_id),
                    )

                    # 3. Delete the now-empty group (ON DELETE CASCADE should handle memberships if IGNORE wasn't used, but doing Step 2 first is safer)
                    # Make sure ON DELETE CASCADE is set on the FK in two_phase_article_group_memberships
                    cursor_merge.execute(
                        "DELETE FROM two_phase_article_groups WHERE group_id = ?",
                        (deleted_group_id,),
                    )

                    conn_merge.commit()
                    logger.info(
                        f"Successfully merged Group {deleted_group_id} into Group {surviving_group_id}. New Label: '{merged_label}'"
                    )
                    merged_pairs_count += 1
                    processed_group_ids.add(
                        group_a_id
                    )  # Mark both as processed for this pass
                    processed_group_ids.add(group_b_id)

                except sqlite3.Error as db_err:
                    logger.error(
                        f"Database error during merge of {deleted_group_id} into {surviving_group_id}: {db_err}",
                        exc_info=True,
                    )
                    if conn_merge:
                        conn_merge.rollback()
                    errors += 1
                finally:
                    if conn_merge:
                        conn_merge.close()

                # Since group B was deleted, we should ideally break from the inner loop
                # or handle indices carefully if continuing. Breaking is simpler.
                break  # Move to the next 'i'

    elapsed = time.time() - start_time
    logger.info(
        f"--- Group Merging Pass Finished in {elapsed:.2f} seconds ({merged_pairs_count} pairs merged, {errors} errors) ---"
    )
    return {"merged_pairs": merged_pairs_count, "errors": errors}
