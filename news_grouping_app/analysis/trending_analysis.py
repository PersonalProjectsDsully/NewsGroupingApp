# analysis/trending_analysis.py - UPDATED FOR CURSOR PASSING
# Implements 48-hour trending analysis of articles within their assigned categories.

import logging
import json
import re
import time
import sqlite3  # Import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import pytz

# Import necessary functions from news_grouping_app.db.database (assuming they are modified)
from news_grouping_app.db.database import (
    get_connection,
    link_entity_to_trend,
    insert_entity,  # Make sure this accepts optional cursor
)
from news_grouping_app.llm_calls import call_gpt_api
from news_grouping_app.utils import chunk_summaries, MAX_TOKEN_CHUNK

# Import entity/context functions (these primarily read, should be okay)
from news_grouping_app.analysis.entity_extraction import get_entities_for_article, get_trending_entities
from news_grouping_app.analysis.context_builder import build_grouping_context, format_context_for_prompt
from news_grouping_app.config import OPENAI_MODEL

logger = logging.getLogger(__name__)
MODEL = OPENAI_MODEL  # default model can be overridden via env var


def setup_trending_tables(db_path="db/news.db"):
    """
    Create (if not exists) the necessary tables for storing trending information.
    Uses its own connection.
    """
    conn = None
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        # trending_groups table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trending_groups (
                trend_id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                trend_label TEXT NOT NULL,
                summary TEXT NOT NULL,
                importance_score REAL DEFAULT 5.0,
                confidence_score REAL DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        # trending_group_memberships table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trending_group_memberships (
                article_id INTEGER NOT NULL,
                trend_id INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE, /* Add ON DELETE CASCADE */
                FOREIGN KEY (trend_id) REFERENCES trending_groups (trend_id) ON DELETE CASCADE, /* Add ON DELETE CASCADE */
                PRIMARY KEY (article_id, trend_id)
            )
        """
        )
        # trend_entities table (ensure it exists and has ON DELETE CASCADE)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trend_entities (
                trend_id INTEGER NOT NULL,
                entity_id INTEGER NOT NULL,
                relevance_score REAL DEFAULT 1.0,
                FOREIGN KEY (trend_id) REFERENCES trending_groups (trend_id) ON DELETE CASCADE, /* Add ON DELETE CASCADE */
                FOREIGN KEY (entity_id) REFERENCES entity_profiles (entity_id) ON DELETE CASCADE, /* Add ON DELETE CASCADE */
                PRIMARY KEY (trend_id, entity_id)
            )
        """
        )
        conn.commit()
        logger.info("Trending tables verified/created.")
    except sqlite3.Error as e:
        logger.error(f"Error setting up trending tables: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def get_articles_by_category_last_48h(category, db_path="db/news.db"):
    """
    Retrieve articles from the last 48 hours for a specific main_topic category.
    (Read operation - uses its own connection)
    """
    conn = get_connection(db_path)
    cutoff_time_utc = datetime.now(pytz.UTC) - timedelta(hours=48)
    cutoff_iso = cutoff_time_utc.strftime("%Y-%m-%d %H:%M:%S")

    query = """
        SELECT
            a.id AS article_id, a.title, a.content,
            a.title || ' - ' || a.content AS expanded_summary,
            a.published_date
        FROM articles a
        JOIN two_phase_article_group_memberships tgm ON a.id = tgm.article_id
        JOIN two_phase_article_groups tg ON tgm.group_id = tg.group_id
        WHERE tg.main_topic = ?
          AND a.published_date >= ? /* Direct comparison assuming standard format */
        ORDER BY a.published_date DESC
    """
    try:
        df = pd.read_sql_query(query, conn, params=(category, cutoff_iso))
    except Exception as e:
        logger.error(f"Error fetching articles for category '{category}': {e}")
        df = pd.DataFrame()  # Return empty DataFrame on error
    finally:
        conn.close()
    return df


def get_entity_co_occurrences(category, hours=48, limit=20, db_path="db/news.db"):
    """
    Get entity co-occurrence patterns within a specific category in recent articles.
    (Read operation - uses its own connection)
    """
    conn = get_connection(db_path)
    cutoff_time_utc = datetime.now(pytz.UTC) - timedelta(hours=hours)
    cutoff_iso = cutoff_time_utc.strftime("%Y-%m-%d %H:%M:%S")

    query = """
        SELECT
            e1.entity_id AS entity1_id, e1.entity_name AS entity1_name, e1.entity_type AS entity1_type,
            e2.entity_id AS entity2_id, e2.entity_name AS entity2_name, e2.entity_type AS entity2_type,
            COUNT(DISTINCT a.id) AS co_occurrence_count
        FROM articles a
        JOIN two_phase_article_group_memberships tgm ON a.id = tgm.article_id
        JOIN two_phase_article_groups tg ON tgm.group_id = tg.group_id
        JOIN article_entities ae1 ON a.id = ae1.article_id
        JOIN entity_profiles e1 ON ae1.entity_id = e1.entity_id
        JOIN article_entities ae2 ON a.id = ae2.article_id
        JOIN entity_profiles e2 ON ae2.entity_id = e2.entity_id
        WHERE tg.main_topic = ?
          AND a.published_date >= ?
          AND e1.entity_id < e2.entity_id /* Avoid duplicate pairs */
        GROUP BY e1.entity_id, e2.entity_id
        HAVING co_occurrence_count > 1
        ORDER BY co_occurrence_count DESC
        LIMIT ?
    """
    try:
        df = pd.read_sql_query(query, conn, params=(category, cutoff_iso, limit))
    except Exception as e:
        logger.error(f"Error fetching entity co-occurrences for '{category}': {e}")
        df = pd.DataFrame()  # Return empty DataFrame on error
    finally:
        conn.close()
    return df


def _prepare_trending_entity_context(trending_entities_df, max_items=10):
    """Build a textual context string listing recently trending entities."""
    if trending_entities_df.empty:
        return "RECENT TRENDING ENTITIES:\n(No trending entities found)\n"
    lines = ["RECENT TRENDING ENTITIES:"]
    for _, entity in trending_entities_df.head(max_items).iterrows():
        lines.append(
            f"- {entity['entity_name']} ({entity['entity_type']}): Mentioned in {entity['recent_mentions']} recent articles"
        )
    return "\n".join(lines) + "\n"


def _prepare_co_occurrence_context(co_occurrences_df, max_items=8):
    """Build a textual context string listing entity co-occurrences."""
    if co_occurrences_df.empty:
        return "\nENTITY CO-OCCURRENCES:\n(No co-occurrences found)\n"
    lines = ["\nENTITY CO-OCCURRENCES:"]
    for _, pair in co_occurrences_df.head(max_items).iterrows():
        lines.append(
            f"- {pair['entity1_name']} & {pair['entity2_name']}: Appear together in {pair['co_occurrence_count']} articles"
        )
    return "\n".join(lines) + "\n"


def _prepare_article_text(df, chunk_dict, db_path):
    """Build snippet text for articles, including entities and date."""
    article_text = ""
    for art_id, text in chunk_dict.items():
        # Fetch entities (Read operation, okay to use separate connection if get_entities_for_article does)
        entities_df = get_entities_for_article(art_id, db_path=db_path)
        entity_summary = ""
        if not entities_df.empty:
            top_entities = entities_df.sort_values(
                "relevance_score", ascending=False
            ).head(5)
            entity_summary = ", ".join(
                f"{row['entity_name']} ({row['entity_type']})"
                for _, row in top_entities.iterrows()
            )
        entity_text = f"\nKey entities: {entity_summary}" if entity_summary else ""

        date_str = ""
        try:
            date_row = df[df["article_id"] == art_id]["published_date"].iloc[0]
            if date_row:
                date_str = f"\nPublished: {pd.to_datetime(date_row).strftime('%Y-%m-%d %H:%M')}"
        except:
            pass

        snippet = text[:3000] + "..." if len(text) > 3000 else text
        article_text += f"Article ID={art_id}:{date_str}{entity_text}\n{snippet}\n\n"
    return article_text


def _build_trend_analysis_prompt(
    category, context_prompt, entity_context, co_occurrence_context, article_text
):
    """Construct the complete LLM user prompt for identifying trends."""
    return (
        f"Analyze these articles from the '{category}' category published in the last 48 hours. Identify significant trends or emerging stories. Group articles covering the same subject.\n\n"
        f"GENERAL CONTEXT:\n{context_prompt}\n"
        f"\n{entity_context}\n{co_occurrence_context}\n"
        "For each trend, provide:\n"
        "1. trend_label: A short, descriptive name\n2. summary: A 2-3 sentence summary\n3. importance_score: 1-10\n"
        '4. confidence_score: 0.1-1.0\n5. key_entities: Array of important entities [{"name": "...", "type": "..."}]\n6. articles: Array of article IDs\n\n'
        "Return valid JSON only:\n"
        '{ "trends": [ {"trend_label": "...", "summary": "...", "importance_score": X, "confidence_score": Y, "key_entities": [{"name": "...", "type": "..."}], "articles": [...]} ] }\n\n'
        f"Articles to analyze:\n\n{article_text}"
    )


def identify_trends_in_category(category, api_key, db_path="db/news.db"):
    """Identify trends using LLM analysis, entity context, etc."""
    logger.info(f"Identifying trends for category: {category}")
    df = get_articles_by_category_last_48h(category, db_path=db_path)
    if df.empty:
        logger.info(f"No recent articles found for category: {category}")
        return None

    summaries_dict = {
        row["article_id"]: row["expanded_summary"]
        for _, row in df.iterrows()
        if row["expanded_summary"]
    }
    if not summaries_dict:
        logger.info(f"No valid summaries for trend analysis in category: {category}")
        return None

    trending_entities = get_trending_entities(hours=48, limit=15, db_path=db_path)
    co_occurrences = get_entity_co_occurrences(
        category, hours=48, limit=15, db_path=db_path
    )
    result = {"trends": []}
    chunked_data = list(
        chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK)
    )  # Ensure list conversion

    for idx, chunk_dict in enumerate(chunked_data, start=1):
        logger.info(
            f"Processing chunk {idx}/{len(chunked_data)} for category: {category}"
        )
        # Build context (reads DB, likely ok with separate connection)
        context = build_grouping_context(chunk_dict, category, api_key, db_path=db_path)
        context_prompt = format_context_for_prompt(context)
        entity_context = _prepare_trending_entity_context(trending_entities)
        co_occurrence_context = _prepare_co_occurrence_context(co_occurrences)
        article_text = _prepare_article_text(df, chunk_dict, db_path)

        prompt = _build_trend_analysis_prompt(
            category,
            context_prompt,
            entity_context,
            co_occurrence_context,
            article_text,
        )
        messages = [
            {
                "role": "system",
                "content": f"Analyze recent articles to identify trends in '{category}'. Focus on meaningful patterns in the last 48 hours.",
            },
            {"role": "user", "content": prompt},
        ]
        response = call_gpt_api(messages, api_key)
        if not response:
            logger.warning(
                f"No response from GPT for chunk {idx} in category: {category}"
            )
            continue

        cleaned = response.strip().strip("```json").strip("```").strip()
        try:
            data = json.loads(cleaned)
            chunk_trends = data.get("trends", [])
            result["trends"].extend(chunk_trends)
        except json.JSONDecodeError as exc:
            logger.error(f"Error parsing trend identification JSON: {exc}\n{cleaned}")

    return result


def save_trends(category, trends_data, db_path="db/news.db"):
    """
    Save identified trend data. Uses a single connection and cursor
    and passes the cursor to helper functions.
    """
    if not trends_data or not trends_data.get("trends"):
        logger.info(f"No trends to save for category: {category}")
        return

    conn = None
    saved_count = 0
    entity_link_errors = 0
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()

        for trend in trends_data["trends"]:
            try:  # Inner try for individual trend saving
                # Insert trend group
                cursor.execute(
                    """
                    INSERT INTO trending_groups
                    (category, trend_label, summary, importance_score, confidence_score)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        category,
                        trend.get("trend_label", "Untitled Trend"),
                        trend.get("summary", ""),
                        trend.get("importance_score", 5.0),
                        trend.get("confidence_score", 0.7),
                    ),
                )
                trend_id = cursor.lastrowid

                # Link articles
                articles = trend.get("articles", [])
                for article_id in articles:
                    try:
                        cursor.execute(
                            "INSERT OR IGNORE INTO trending_group_memberships (article_id, trend_id) VALUES (?, ?)",
                            (int(article_id), trend_id),
                        )
                    except (ValueError, TypeError, sqlite3.Error) as article_err:
                        logger.warning(
                            f"Skipping invalid article ID {article_id} or DB error for trend {trend_id}: {article_err}"
                        )

                # Link key entities (PASS CURSOR)
                key_entities = trend.get("key_entities", [])
                for entity_data in key_entities:
                    entity_name = entity_data.get("name", "").strip()
                    entity_type = entity_data.get("type", "unknown").lower()
                    if entity_name:
                        try:
                            # Pass the existing cursor
                            entity_id = insert_entity(
                                entity_name, entity_type, db_path=db_path, cursor=cursor
                            )
                            if entity_id is None:  # Check if entity insertion failed
                                logger.error(
                                    f"Failed to insert/get entity '{entity_name}' for trend {trend_id}"
                                )
                                entity_link_errors += 1
                                continue  # Skip linking if entity_id is None
                            # Use a default relevance or calculate if possible
                            relevance = 0.8
                            link_entity_to_trend(
                                trend_id,
                                entity_id,
                                relevance,
                                db_path=db_path,
                                cursor=cursor,
                            )
                        # Catch potential OperationalError specifically if needed, though passing cursor should prevent most locks
                        except sqlite3.OperationalError as lock_err:
                            logger.error(
                                f"DATABASE LOCKED during entity linking for trend {trend_id}, entity '{entity_name}': {lock_err}"
                            )
                            entity_link_errors += 1
                        except Exception as exc:
                            logger.error(
                                f"Error processing entity '{entity_name}' for trend {trend_id}: {exc}",
                                exc_info=False,
                            )
                            entity_link_errors += 1
                saved_count += 1
            except sqlite3.Error as trend_err:
                logger.error(
                    f"Database error saving trend '{trend.get('trend_label')}': {trend_err}. Skipping this trend."
                )
                # Decide if rollback is needed here or continue with others
                # For now, we log and continue

        conn.commit()  # Commit all successfully processed trends at the end
        logger.info(
            f"Attempted to save {len(trends_data['trends'])} trends for category: {category}. Successfully saved: {saved_count}. Entity link errors: {entity_link_errors}."
        )

    except Exception as exc:
        logger.exception(
            f"Major error during save_trends for category {category}, rolling back transaction: {exc}"
        )
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def cleanup_old_trends(db_path="db/news.db"):
    """Remove trend data older than 48 hours. Uses its own connection."""
    logger.info("Running cleanup of old trending data.")
    conn = None
    deleted_count = 0
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        # Use CURRENT_TIMESTAMP and date modification directly in SQL
        cutoff_time_sql = "datetime('now', '-48 hours')"

        # Get IDs to delete first (optional, for logging count)
        cursor.execute(
            f"SELECT COUNT(*) FROM trending_groups WHERE created_at < {cutoff_time_sql}"
        )
        to_delete_count = cursor.fetchone()[0]
        logger.info(f"Found {to_delete_count} trends older than 48 hours to remove.")

        if to_delete_count > 0:
            # Rely on ON DELETE CASCADE for memberships and entities
            cursor.execute(
                f"DELETE FROM trending_groups WHERE created_at < {cutoff_time_sql}"
            )
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(
                f"Removed {deleted_count} trends (and associated data via CASCADE) older than 48 hours."
            )
        else:
            logger.info("No old trends found to remove.")

    except sqlite3.Error as exc:
        logger.error(f"Error cleaning up old trends: {exc}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def ensure_minimum_trends(min_count=6, api_key=None, db_path="db/news.db"):
    """
    Ensures minimum trending topics exist, creating some from popular groups if needed.
    Uses a single connection and passes cursor to helpers.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM trending_groups")
        current_count = cursor.fetchone()[0]

        if current_count >= min_count:
            logger.info(
                f"Have {current_count} trends (minimum {min_count} required). No action needed."
            )
            return

        needed_count = min_count - current_count
        logger.info(
            f"Need {needed_count} more trends to meet minimum of {min_count}. Checking popular groups..."
        )

        cutoff_time_utc = datetime.now(pytz.UTC) - timedelta(hours=48)
        cutoff_iso = cutoff_time_utc.strftime("%Y-%m-%d %H:%M:%S")

        # Query to find recent, popular groups not already trending
        group_query = """
            SELECT tg.group_id, tg.main_topic, tg.group_label, COUNT(tgm.article_id) AS article_count
            FROM two_phase_article_groups tg
            JOIN two_phase_article_group_memberships tgm ON tg.group_id = tgm.group_id
            JOIN articles a ON tgm.article_id = a.id
            WHERE a.published_date >= ?
              AND NOT EXISTS (SELECT 1 FROM trending_groups tr WHERE tr.trend_label = tg.group_label) /* Avoid duplicate labels */
            GROUP BY tg.group_id
            ORDER BY article_count DESC, tg.created_at DESC
            LIMIT ?
        """
        popular_groups_df = pd.read_sql_query(
            group_query, conn, params=(cutoff_iso, needed_count * 2)
        )  # Fetch extra

        if popular_groups_df.empty:
            logger.info(
                "No suitable recent groups found to generate additional trends."
            )
            return

        groups_to_use = popular_groups_df.head(needed_count)
        logger.info(f"Found {len(groups_to_use)} groups to convert to trends.")

        created_count = 0
        for _, group in groups_to_use.iterrows():
            article_query = "SELECT id FROM articles a JOIN two_phase_article_group_memberships tgm ON a.id = tgm.article_id WHERE tgm.group_id = ? ORDER BY a.published_date DESC LIMIT 10"
            article_ids = [
                row[0]
                for row in cursor.execute(
                    article_query, (group["group_id"],)
                ).fetchall()
            ]
            if not article_ids:
                continue

            summary = f"Recent developments related to {group['group_label']}"
            trend_data = {
                "category": group["main_topic"],
                "trend_label": group["group_label"],
                "summary": summary,
                "importance_score": 5.0,
                "confidence_score": 0.8,
                "articles": article_ids,
            }

            # Insert trend group (using cursor)
            cursor.execute(
                "INSERT INTO trending_groups (category, trend_label, summary, importance_score, confidence_score) VALUES (?, ?, ?, ?, ?)",
                (
                    trend_data["category"],
                    trend_data["trend_label"],
                    trend_data["summary"],
                    trend_data["importance_score"],
                    trend_data["confidence_score"],
                ),
            )
            trend_id = cursor.lastrowid

            # Link articles (using cursor)
            for article_id in trend_data["articles"]:
                cursor.execute(
                    "INSERT OR IGNORE INTO trending_group_memberships (article_id, trend_id) VALUES (?, ?)",
                    (article_id, trend_id),
                )

            # Link key entities (using cursor) - More complex, requires entity extraction for the group
            try:
                entity_query = f"""
                    SELECT e.entity_id, e.entity_name, e.entity_type, COUNT(*) AS count
                    FROM article_entities ae JOIN entity_profiles e ON ae.entity_id = e.entity_id
                    WHERE ae.article_id IN ({','.join('?'*len(article_ids))})
                    GROUP BY e.entity_id ORDER BY count DESC LIMIT 5
                """
                entities = cursor.execute(entity_query, article_ids).fetchall()
                for entity_row in entities:
                    link_entity_to_trend(
                        trend_id, entity_row[0], 0.8, db_path=db_path, cursor=cursor
                    )
            except Exception as entity_err:
                logger.warning(
                    f"Could not add entity relationships to generated trend {trend_id}: {entity_err}"
                )

            logger.info(
                f"Created additional trend from group: {trend_data['trend_label']} (ID: {trend_id})"
            )
            created_count += 1

        if created_count > 0:
            conn.commit()  # Commit all changes made within this function
        logger.info(f"Created {created_count} additional trends.")

    except Exception as e:
        logger.error(f"Error in ensure_minimum_trends: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def get_trending_topics(category=None, limit=10, db_path="db/news.db"):
    """
    Retrieve trending topics, optionally filtered.
    (Read operation - uses its own connection)
    """
    conn = get_connection(db_path)
    article_data_map = {}
    entity_data_map = {}
    df = pd.DataFrame()  # Initialize df

    try:
        base_query = """
            SELECT tg.trend_id, tg.category, tg.trend_label, tg.summary,
                   tg.importance_score, tg.confidence_score,
                   GROUP_CONCAT(DISTINCT tgm.article_id) AS article_ids
            FROM trending_groups tg
            LEFT JOIN trending_group_memberships tgm ON tg.trend_id = tgm.trend_id
        """
        params = []
        if category:
            base_query += " WHERE tg.category = ?"
            params.append(category)

        base_query += """
            GROUP BY tg.trend_id
            ORDER BY tg.importance_score DESC, tg.confidence_score DESC, tg.created_at DESC
            LIMIT ?
        """
        params.append(limit)

        df = pd.read_sql_query(base_query, conn, params=params)

        if not df.empty:
            df["article_ids"] = df["article_ids"].apply(
                lambda x: [int(aid) for aid in x.split(",")] if x else []
            )

            # Fetch articles and entities for each trend
            for _, row in df.iterrows():
                trend_id = row["trend_id"]
                article_ids = row["article_ids"]
                # Articles
                if article_ids:
                    placeholders = ",".join("?" * len(article_ids))
                    art_query = f"SELECT a.id AS article_id, a.title, a.link, a.published_date, a.source FROM articles a WHERE a.id IN ({placeholders}) ORDER BY a.published_date DESC"
                    article_df = pd.read_sql_query(art_query, conn, params=article_ids)
                    article_data_map[trend_id] = article_df.to_dict(orient="records")
                else:
                    article_data_map[trend_id] = []
                # Entities
                entity_query = "SELECT e.entity_id, e.entity_name, e.entity_type, te.relevance_score FROM entity_profiles e JOIN trend_entities te ON e.entity_id = te.entity_id WHERE te.trend_id = ? ORDER BY te.relevance_score DESC"
                entity_df = pd.read_sql_query(entity_query, conn, params=(trend_id,))
                entity_data_map[trend_id] = entity_df.to_dict(orient="records")

            df["articles"] = df["trend_id"].map(article_data_map)
            df["entities"] = df["trend_id"].map(entity_data_map)

    except Exception as e:
        logger.error(f"Error fetching trending topics: {e}")
        df = pd.DataFrame()  # Return empty on error
    finally:
        conn.close()
    return df


def run_trending_analysis(api_key, categories=None, db_path="db/news.db", min_trends=6):
    """Main function to run the 48-hour trending analysis."""
    logger.info("Starting enhanced trending analysis run.")
    if categories is None:
        from news_grouping_app.analysis.two_phase_grouping import (
            PREDEFINED_CATEGORIES,
        )  # Import if needed

        categories = PREDEFINED_CATEGORIES

    setup_trending_tables(db_path=db_path)  # Ensure tables exist
    cleanup_old_trends(db_path=db_path)  # Clean first

    for category in categories:
        logger.info(f"Processing category for trends: {category}")
        trends = identify_trends_in_category(category, api_key, db_path=db_path)
        if trends:
            save_trends(category, trends, db_path=db_path)
        time.sleep(1)  # Small delay

    # Ensure minimum trends exist AFTER attempting to generate new ones
    ensure_minimum_trends(min_count=min_trends, api_key=api_key, db_path=db_path)

    logger.info("Trending analysis run completed.")
    return True
