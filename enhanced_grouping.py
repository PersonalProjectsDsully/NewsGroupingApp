#!/usr/bin/env python3
"""
enhanced_grouping.py

This module implements an enhanced article grouping system that can:
1. Group new articles with existing groups based on similarities
2. Create new groups for unrelated articles
3. Use multiple metrics for more accurate grouping decisions

Usage:
    python enhanced_grouping.py --threshold 0.5 --new-article-id 123
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from typing import Dict, List, Set, Tuple, Optional
import importlib.util
from collections import defaultdict  # <<< IMPORT ADDED
import pandas as pd  # <<< IMPORT ADDED


# Import the article_signature module (assuming it's in the same directory)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from article_signature import generate_article_signature, calculate_signature_similarity

try:
    # Adjust path if necessary based on your project structure
    # sys.path.append(os.path.dirname(os.path.abspath(__file__))) # If in the same dir
    from article_signature import generate_article_signature
except ImportError as e:
    logging.error(
        f"Failed to import generate_article_signature from article_signature.py: {e}",
        exc_info=True,
    )
    generate_article_signature = None  # Allow script to load, but fail if called

# Optional: Import LLM call function if using description similarity or LLM checks elsewhere
try:
    from llm_calls import call_gpt_api
except ImportError:
    call_gpt_api = None  # Define as None if not available

# Configure logging
# Ensure logging is configured in your main script (e.g., main.py or pipeline.py)
# logging.basicConfig(...) # Basic config for standalone testing if needed
logger = logging.getLogger(__name__)


def get_existing_groups(db_path: str = "db/news.db") -> List[Dict]:
    """
    Get all existing article groups with their members.

    Args:
        db_path: Path to the database

    Returns:
        List of group dictionaries with article IDs
    """
    # Use alias 'g' for groups table and 'm' for memberships for clarity
    query = """
        SELECT
            g.group_id,
            g.main_topic,
            g.sub_topic,
            g.group_label,
            g.description,
            g.consistency_score
        FROM two_phase_article_groups g
    """
    membership_query = """
        SELECT article_id
        FROM two_phase_article_group_memberships
        WHERE group_id = ?
    """
    groups = []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        group_rows = cursor.fetchall()

        for row in group_rows:
            (
                group_id,
                main_topic,
                sub_topic,
                group_label,
                description,
                consistency_score,
            ) = row
            cursor.execute(membership_query, (group_id,))
            article_ids = [r[0] for r in cursor.fetchall()]

            groups.append(
                {
                    "group_id": group_id,
                    "main_topic": main_topic,
                    "sub_topic": sub_topic,
                    "group_label": group_label,
                    "description": description,
                    "consistency_score": consistency_score,
                    "article_ids": article_ids,
                }
            )
    except sqlite3.Error as e:
        logger.error(f"Database error fetching existing groups: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return groups


def generate_group_signature(group: Dict, db_path: str = "db/news.db") -> Dict:
    """
    Generate a composite signature for a group based on its articles,
    including average entity relevance, latest date, and sources.

    Args:
        group: Group dictionary with at least 'group_id', 'article_ids'.
               Should also ideally contain 'group_label', 'description', 'main_topic'.
        db_path: Path to the database

    Returns:
        Group signature dictionary, or an empty dict if input is invalid.
    """
    if not generate_article_signature:
        logger.error(
            "generate_article_signature function not available. Cannot generate group signature."
        )
        return {}

    article_ids = group.get("article_ids", [])
    group_id = group.get("group_id")

    # Basic signature structure with group metadata
    group_signature = {
        "group_id": group_id,
        "group_label": group.get("group_label", ""),
        "description": group.get("description", ""),
        "main_topic": group.get("main_topic", ""),
        "primary_entities": [],
        "companies": [],
        "cves": [],
        "technologies": [],
        "products": [],
        # "references": [], # Keep commented unless aggregation logic is added
        "events": [],
        # "quotes": [], # Keep commented unless aggregation logic is added
        "latest_published_date": None,
        "member_sources": [],
    }

    if not article_ids:
        logger.warning(f"Group {group_id} has no articles. Returning basic signature.")
        return group_signature

    # --- Step 1: Generate signatures for all articles in the group ---
    article_signatures = []
    for article_id in article_ids:
        try:
            sig = generate_article_signature(
                article_id, db_path
            )  # Assumes this now returns date/source
            if sig:  # Ensure signature generation was successful
                article_signatures.append(sig)
            else:
                logger.warning(
                    f"Signature generation returned empty for article {article_id} in group {group_id}"
                )
        except Exception as e:
            logger.error(
                f"Error generating signature for article {article_id} in group {group_id}: {e}",
                exc_info=False,
            )
            continue  # Skip this article if signature fails

    if not article_signatures:
        logger.warning(
            f"No valid article signatures generated for group {group_id}. Returning basic signature."
        )
        return group_signature

    # --- Step 2: Aggregate data from article signatures ---
    all_entity_ids = set()
    entity_counts = defaultdict(int)
    entity_relevance_scores = defaultdict(list)  # Store relevance scores for averaging
    all_companies = set()
    company_counts = defaultdict(int)
    all_cves = set()
    cve_counts = defaultdict(int)
    all_tech_ids = set()
    all_product_ids = set()
    all_event_names = set()
    event_counts = defaultdict(int)
    latest_published_date = None  # Initialize
    all_sources = set()  # Initialize

    num_valid_articles = len(
        article_signatures
    )  # Use count of successfully generated signatures

    for sig in article_signatures:
        # Entities
        for entity in sig.get("primary_entities", []):
            entity_id = entity.get("entity_id")
            relevance = entity.get("relevance_score", 0.0)
            if entity_id is not None:
                all_entity_ids.add(entity_id)
                entity_counts[entity_id] += 1
                entity_relevance_scores[entity_id].append(relevance)
        # Companies
        for company in sig.get("companies", []):
            all_companies.add(company)
            company_counts[company] += 1
        # CVEs
        for cve in sig.get("cves", []):
            all_cves.add(cve)
            cve_counts[cve] += 1
        # Technologies
        for tech in sig.get("technologies", []):
            tech_id = tech.get("entity_id")
            if tech_id is not None:
                all_tech_ids.add(tech_id)
        # Products
        for product in sig.get("products", []):
            product_id = product.get("entity_id")
            if product_id is not None:
                all_product_ids.add(product_id)
        # Events
        for event in sig.get("events", []):
            event_name = event.get("event_name")
            if event_name:
                all_event_names.add(event_name)
                event_counts[event_name] += 1
        # Date Processing
        pub_date_str = sig.get("published_date")
        if pub_date_str:
            try:
                current_date = pd.to_datetime(pub_date_str, utc=True, errors="coerce")
                if pd.notna(current_date):
                    if (
                        latest_published_date is None
                        or current_date > latest_published_date
                    ):
                        latest_published_date = current_date
            except Exception as date_err:
                logger.debug(
                    f"Could not parse date '{pub_date_str}' for article {sig.get('article_id')}: {date_err}"
                )
        # Source Processing
        source = sig.get("source")
        if source:
            all_sources.add(source)

    # --- Step 3: Fetch details and calculate aggregates ---
    conn = None
    final_entity_details = []
    tech_details = []
    product_details = []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Entity details
        if all_entity_ids:
            placeholders = ",".join("?" for _ in all_entity_ids)
            query = f"SELECT entity_id, entity_name, entity_type FROM entity_profiles WHERE entity_id IN ({placeholders})"
            cursor.execute(query, list(all_entity_ids))
            fetched_entities = cursor.fetchall()
            for entity_id, entity_name, entity_type in fetched_entities:
                relevances = entity_relevance_scores.get(entity_id, [])
                avg_relevance = sum(relevances) / len(relevances) if relevances else 0.0
                frequency = entity_counts.get(entity_id, 0) / num_valid_articles
                final_entity_details.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity_name,
                        "entity_type": entity_type,
                        "frequency": frequency,
                        "avg_relevance": avg_relevance,
                    }
                )
        # Tech details
        if all_tech_ids:
            placeholders = ",".join("?" for _ in all_tech_ids)
            query = f"SELECT entity_id, entity_name FROM entity_profiles WHERE entity_id IN ({placeholders})"
            cursor.execute(query, list(all_tech_ids))
            tech_details = [
                {"entity_id": r[0], "entity_name": r[1]} for r in cursor.fetchall()
            ]
        # Product details
        if all_product_ids:
            placeholders = ",".join("?" for _ in all_product_ids)
            query = f"SELECT entity_id, entity_name FROM entity_profiles WHERE entity_id IN ({placeholders})"
            cursor.execute(query, list(all_product_ids))
            product_details = [
                {"entity_id": r[0], "entity_name": r[1]} for r in cursor.fetchall()
            ]
    except sqlite3.Error as e:
        logger.error(f"Database error fetching details for group {group_id}: {e}")
    finally:
        if conn:
            conn.close()

    company_details = [
        {"company_name": c, "frequency": cnt / num_valid_articles}
        for c, cnt in company_counts.items()
    ]
    cve_details = [
        {"cve_id": c, "frequency": cnt / num_valid_articles}
        for c, cnt in cve_counts.items()
    ]
    event_details = [
        {"event_name": n, "frequency": cnt / num_valid_articles}
        for n, cnt in event_counts.items()
    ]

    # --- Step 4: Populate final signature ---
    group_signature["primary_entities"] = sorted(
        final_entity_details,
        key=lambda x: (x["frequency"], x["avg_relevance"]),
        reverse=True,
    )
    group_signature["companies"] = sorted(
        company_details, key=lambda x: x["frequency"], reverse=True
    )
    group_signature["cves"] = sorted(
        cve_details, key=lambda x: x["frequency"], reverse=True
    )
    group_signature["technologies"] = tech_details
    group_signature["products"] = product_details
    group_signature["events"] = sorted(
        event_details, key=lambda x: x["frequency"], reverse=True
    )

    if latest_published_date is not None:
        group_signature["latest_published_date"] = latest_published_date.isoformat()
    group_signature["member_sources"] = sorted(list(all_sources))

    logger.debug(f"Generated signature for Group {group_id}")
    return group_signature


def calculate_article_to_group_similarity(
    article_signature: Dict,
    group_signature: Dict,
    api_key: Optional[
        str
    ] = None,  # Added optional api_key if needed for description similarity
) -> Dict[str, float]:
    """
    Calculate similarity between an article and a group, including
    temporal, source, and CORE ENTITY adjustments.

    Args:
        article_signature: Signature of the article (must include 'published_date', 'source', 'primary_entities')
        group_signature: Signature of the group (must include 'latest_published_date', 'member_sources', 'primary_entities')
        api_key: Optional API key if LLM description similarity is used.

    Returns:
        Dictionary with similarity scores and the final composite score.
    """
    results = {}
    CORE_ENTITY_BONUS = 0.20  # Configurable: How much to boost score for core match
    CORE_ENTITY_TYPES = {
        "product",
        "organization",
        "technology",
    }  # Types likely to be core topics

    # --- Calculate Base Similarities ---
    # Entity similarity (using avg_relevance and frequency)
    article_entities_dict = {
        e["entity_id"]: e.get("relevance_score", 0.7)
        for e in article_signature.get("primary_entities", [])
    }
    if article_entities_dict and group_signature.get("primary_entities"):
        entity_score = 0.0
        max_possible_score = 0.0
        for group_entity in group_signature["primary_entities"]:
            group_entity_id = group_entity["entity_id"]
            group_avg_relevance = group_entity.get("avg_relevance", 0.7)
            group_frequency = group_entity.get("frequency", 0.0)
            # Normalize based on potential contribution of each group entity
            max_possible_score += group_frequency * group_avg_relevance
            if group_entity_id in article_entities_dict:
                article_relevance = article_entities_dict[group_entity_id]
                # Weighted score for shared entity
                combined_weight = (
                    article_relevance * group_avg_relevance * group_frequency
                )
                entity_score += combined_weight
        results["entity_similarity"] = (
            entity_score / max_possible_score if max_possible_score > 0 else 0.0
        )
    else:
        results["entity_similarity"] = 0.0
    # Company similarity (Jaccard on names)
    article_companies = set(article_signature.get("companies", []))
    # Ensure group companies are extracted correctly if they are dicts now
    group_companies = {
        comp.get("company_name")
        for comp in group_signature.get("companies", [])
        if comp.get("company_name")
    }
    if article_companies and group_companies:
        intersection = len(article_companies.intersection(group_companies))
        union = len(article_companies.union(group_companies))
        results["company_similarity"] = intersection / union if union > 0 else 0.0
    else:
        results["company_similarity"] = 0.0
    # CVE similarity (Jaccard on IDs)
    article_cves = set(article_signature.get("cves", []))
    group_cves = {
        cve.get("cve_id")
        for cve in group_signature.get("cves", [])
        if cve.get("cve_id")
    }
    if article_cves and group_cves:
        intersection = len(article_cves.intersection(group_cves))
        union = len(article_cves.union(group_cves))
        results["cve_similarity"] = intersection / union if union > 0 else 0.0
    else:
        results["cve_similarity"] = 0.0
    # Event similarity (Jaccard on names, weighted by group frequency)
    article_event_names = {
        e.get("event_name")
        for e in article_signature.get("events", [])
        if e.get("event_name")
    }
    if article_event_names and group_signature.get("events"):
        event_score = 0.0
        max_possible_event_score = 0.0
        group_event_dict = {
            e["event_name"]: e.get("frequency", 0.0)
            for e in group_signature["events"]
            if e.get("event_name")
        }
        for event_name, group_freq in group_event_dict.items():
            max_possible_event_score += (
                group_freq  # Sum of frequencies as normalization factor
            )
            if event_name in article_event_names:
                event_score += group_freq  # Add frequency if event matches
        results["event_similarity"] = (
            event_score / max_possible_event_score
            if max_possible_event_score > 0
            else 0.0
        )
    else:
        results["event_similarity"] = 0.0
    # --- Calculate Weighted Composite Score (Before Adjustments) ---
    weights = {
        "entity_similarity": 0.40,
        "company_similarity": 0.25,
        "cve_similarity": 0.15,
        "event_similarity": 0.10,
        # Add other weights if more similarity metrics are added
    }
    composite_score = sum(
        results.get(metric, 0.0) * weight for metric, weight in weights.items()
    )
    # --- Apply Adjustments ---
    temporal_adjustment = 0.0
    source_bonus = 0.0
    core_entity_bonus = 0.0
    # Temporal Analysis
    article_pub_date_str = article_signature.get("published_date")
    group_latest_pub_date_str = group_signature.get("latest_published_date")
    if article_pub_date_str and group_latest_pub_date_str:
        try:
            article_pub_date = pd.to_datetime(
                article_pub_date_str, utc=True, errors="raise"
            )
            group_latest_pub_date = pd.to_datetime(
                group_latest_pub_date_str, utc=True, errors="raise"
            )
            time_diff = article_pub_date - group_latest_pub_date
            hours_diff = abs(time_diff.total_seconds()) / 3600
            if hours_diff <= 48:
                time_bonus = 0.05 * (1 - (hours_diff / 48))
                temporal_adjustment += time_bonus
            elif hours_diff > (7 * 24):
                penalty_factor = min((hours_diff / (7 * 24)) - 1, 1.0)
                time_penalty = -0.03 * penalty_factor
                temporal_adjustment += time_penalty
        except Exception as e:
            logger.warning(
                f"Could not parse/compare dates. Art: '{article_pub_date_str}', Grp: '{group_latest_pub_date_str}'. Err: {e}"
            )

    # Source Correlation
    article_source = article_signature.get("source")
    group_sources = group_signature.get("member_sources")  # This is a list
    if article_source and group_sources and article_source in group_sources:
        source_bonus = 0.03
    # Core Entity Match Bonus
    try:
        top_article_entity = None
        article_primary_entities = article_signature.get("primary_entities", [])
        if article_primary_entities:
            sorted_article_entities = sorted(
                article_primary_entities,
                key=lambda x: x.get("relevance_score", 0.0),
                reverse=True,
            )
            if sorted_article_entities:
                top_article_entity = sorted_article_entities[0]

        top_group_entity = None
        group_primary_entities = group_signature.get("primary_entities", [])
        if group_primary_entities:
            sorted_group_entities = sorted(
                group_primary_entities,
                key=lambda x: x.get("frequency", 0.0) * x.get("avg_relevance", 0.0),
                reverse=True,
            )
            if sorted_group_entities:
                top_group_entity = sorted_group_entities[0]

        if (
            top_article_entity
            and top_group_entity
            and top_article_entity.get("entity_id") == top_group_entity.get("entity_id")
            and top_article_entity.get("entity_type") in CORE_ENTITY_TYPES
        ):
            core_entity_bonus = CORE_ENTITY_BONUS
            logger.debug(
                f"Core entity bonus applied: +{core_entity_bonus:.3f} (Entity: '{top_article_entity.get('entity_name')}')"
            )
    except Exception as e:
        logger.warning(f"Error calculating core entity bonus: {e}")
    # --- Final Score Calculation ---
    adjusted_score = (
        composite_score + temporal_adjustment + source_bonus + core_entity_bonus
    )
    final_score = max(0.0, min(1.0, adjusted_score))  # Clamp final score
    results["composite_score"] = final_score
    results["_temporal_adjustment"] = temporal_adjustment
    results["_source_bonus"] = source_bonus
    results["_core_entity_bonus"] = core_entity_bonus
    results["_base_composite_score"] = composite_score
    logger.debug(
        f"Art {article_signature.get('article_id')} vs Grp {group_signature.get('group_id')}: Base={composite_score:.3f}, Temp={temporal_adjustment:.3f}, Src={source_bonus:.3f}, Core={core_entity_bonus:.3f} -> Final={final_score:.3f}"
    )
    return results


def add_article_to_group(
    article_id: int, group_id: int, db_path: str = "db/news.db", cursor=None
) -> bool:
    """Add article to group. Uses provided cursor if available."""
    conn_managed_here = False
    conn = None
    if cursor is None:
        conn = sqlite3.connect(db_path, timeout=10.0)
        cursor = conn.cursor()
        conn_managed_here = True

    try:
        cursor.execute(
            "SELECT group_id FROM two_phase_article_group_memberships WHERE article_id = ?",
            (article_id,),
        )
        existing_group = cursor.fetchone()

        if existing_group:
            if existing_group[0] == group_id:
                logger.info(f"Article {article_id} already in group {group_id}.")
                if conn_managed_here and conn:
                    conn.close()
                return True
            else:
                logger.warning(
                    f"Moving Article {article_id} from group {existing_group[0]} to {group_id}."
                )
                cursor.execute(
                    "DELETE FROM two_phase_article_group_memberships WHERE article_id = ?",
                    (article_id,),
                )

        cursor.execute(
            "INSERT INTO two_phase_article_group_memberships (article_id, group_id) VALUES (?, ?)",
            (article_id, group_id),
        )
        logger.info(f"Added/Moved article {article_id} to group {group_id}")

        if conn_managed_here and conn:
            conn.commit()
        return True

    except sqlite3.Error as e:  # Catch specific DB errors
        logger.error(
            f"DB Error in add_article_to_group ({article_id} -> {group_id}): {e}",
            exc_info=True,
        )
        if conn_managed_here and conn:
            conn.rollback()
        return False
    except Exception as e:  # Catch other unexpected errors
        logger.error(
            f"Unexpected Error in add_article_to_group ({article_id} -> {group_id}): {e}",
            exc_info=True,
        )
        if conn_managed_here and conn:
            conn.rollback()
        return False
    finally:
        if conn_managed_here and conn:
            conn.close()


def main():
    """
    Main function to execute the enhanced grouping process.
    """
    parser = argparse.ArgumentParser(description="Enhanced article grouping")
    parser.add_argument(
        "--threshold", type=float, default=0.5, help="Similarity threshold"
    )
    parser.add_argument(
        "--db-path", type=str, default="db/news.db", help="Path to the database"
    )
    parser.add_argument(
        "--new-article-id", type=int, help="ID of new article to process"
    )
    parser.add_argument(
        "--process-all", action="store_true", help="Process all ungrouped articles"
    )
    args = parser.parse_args()

    try:
        if args.new_article_id:
            logger.info(
                f"Processing article {args.new_article_id} with threshold {args.threshold}"
            )
            result = process_new_article(
                args.new_article_id, args.threshold, args.db_path
            )
            print(json.dumps(result, indent=2))

        elif args.process_all:
            # Find all ungrouped articles
            conn = sqlite3.connect(args.db_path)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id FROM articles
                WHERE id NOT IN (
                    SELECT article_id FROM two_phase_article_group_memberships
                )
            """
            )
            ungrouped_articles = [row[0] for row in cursor.fetchall()]
            conn.close()

            if not ungrouped_articles:
                logger.info("No ungrouped articles found")
                return

            logger.info(f"Processing {len(ungrouped_articles)} ungrouped articles")
            results = process_multiple_articles(
                ungrouped_articles, args.threshold, args.db_path
            )

            # Summarize results
            total = len(results)
            added_to_existing = sum(
                1
                for r in results.values()
                if r.get("action") == "added_to_existing_group"
            )
            created_new = sum(
                1 for r in results.values() if r.get("action") == "created_new_group"
            )
            failed = sum(1 for r in results.values() if not r.get("success"))

            print(f"Processed {total} articles:")
            print(f"  - Added to existing groups: {added_to_existing}")
            print(f"  - Created new groups: {created_new}")
            print(f"  - Failed: {failed}")

        else:
            print("Please specify either --new-article-id or --process-all")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
