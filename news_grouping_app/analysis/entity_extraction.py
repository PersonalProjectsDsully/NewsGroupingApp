# analysis/entity_extraction.py

import sqlite3
import json
import re
import time
import pandas as pd
import logging

from news_grouping_app.db.database import get_connection, insert_entity, link_entity_to_article
from news_grouping_app.llm_calls import call_gpt_api
from news_grouping_app.utils import chunk_summaries, MAX_TOKEN_CHUNK
from news_grouping_app.config import OPENAI_MODEL

logger = logging.getLogger(__name__)
MODEL = OPENAI_MODEL  # default model can be overridden via env var

# Increase the token chunk size for faster processing
EXTRACTION_TOKEN_CHUNK = MAX_TOKEN_CHUNK * 1.5  # 50% larger chunks for extraction


def get_articles_missing_entity_extraction(db_path="db/news.db"):
    """
    Returns a DataFrame of articles that do NOT have any entry in article_entities.
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            a.id AS article_id,
            a.title || ' - ' || a.content AS expanded_summary
        FROM articles a
        WHERE NOT EXISTS (
            SELECT 1 FROM article_entities ae
            WHERE ae.article_id = a.id
        )
        ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def extract_entities_from_batch(article_batch, api_key, model=MODEL):
    """
    Extract entities from a batch of articles using the LLM.
    article_batch is a dict of {article_id: text}
    Returns a dict of {article_id: [entity_dicts]}
    """
    # Build a prompt that includes multiple articles for batch processing
    prompt = (
        "Extract important named entities from these articles. "
        "Include people, organizations, technologies, products, places, and key concepts. "
        "For each entity, determine its type and provide a brief description.\n\n"
        "Return only JSON with the format:\n"
        '{ "articles": [ '
        '{ "article_id": "...", "entities": ['
        '{ "name": "Entity Name", "type": "person|organization|technology|product|place|concept", '
        '"description": "Brief description", "relevance": 0.1-1.0, "context": "snippet where entity appears" },'
        "..."
        "] },"
        "..."
        "] }\n\n"
    )

    # Add each article to the prompt
    for art_id, text in article_batch.items():
        # Limit text length for each article
        prompt += f"Article ID={art_id}:\n{text[:3000]}...\n\n"

    messages = [
        {
            "role": "system",
            "content": "Extract named entities from multiple articles in batch mode.",
        },
        {"role": "user", "content": prompt},
    ]

    resp = call_gpt_api(messages, api_key, model=model)
    if not resp:
        logger.warning("No response from GPT for batch entity extraction.")
        return {}

    # Clean and parse the response
    cleaned = resp.strip().strip("```json").strip("```").strip()
    cleaned = re.sub(r"^json\s+", "", cleaned, flags=re.IGNORECASE)

    try:
        data = json.loads(cleaned)
        articles_data = data.get("articles", [])

        # Organize results by article_id
        results = {}
        for article in articles_data:
            art_id = article.get("article_id")
            entities = article.get("entities", [])
            if art_id and entities:
                results[art_id] = entities

        return results
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing batch entity extraction JSON: {e}\n{cleaned}")
        return {}


def extract_entities_for_all_articles(api_key, db_path="db/news.db"):
    """
    Identify articles with no entity extractions, extract entities with LLM,
    then store results in entity_profiles and article_entities.
    Enhanced with batch processing and better progress tracking.
    """
    df = get_articles_missing_entity_extraction(db_path=db_path)
    if df.empty:
        logger.info("All articles already have entity extractions.")
        return

    # Build a dict {article_id: expanded_summary}
    summaries_dict = {}
    for _, row in df.iterrows():
        art_id = row["article_id"]
        content = str(row["expanded_summary"]).strip()
        if content:
            summaries_dict[art_id] = content

    # Split into chunks for batch processing
    chunked_articles = list(
        chunk_summaries(summaries_dict, max_token_chunk=EXTRACTION_TOKEN_CHUNK)
    )
    total_articles = len(summaries_dict)
    processed_articles = 0
    total_extractions = 0

    logger.info(
        f"Starting entity extraction for {total_articles} articles in {len(chunked_articles)} batches"
    )

    for idx, chunk_dict in enumerate(chunked_articles, start=1):
        chunk_size = len(chunk_dict)
        logger.info(
            f"Processing batch {idx}/{len(chunked_articles)} with {chunk_size} articles. "
            f"Progress: {processed_articles}/{total_articles} articles ({processed_articles/total_articles*100:.1f}%)"
        )

        # Process the whole chunk at once
        batch_results = extract_entities_from_batch(chunk_dict, api_key)

        # Store the extracted entities
        for art_id, entities in batch_results.items():
            try:
                # Process each entity
                for entity in entities:
                    entity_name = entity.get("name", "").strip()
                    if not entity_name:
                        continue

                    entity_type = entity.get("type", "unknown").lower()
                    description = entity.get("description", "")
                    relevance = float(entity.get("relevance", 1.0))
                    context = entity.get("context", "")

                    # Insert or update the entity profile
                    entity_id = insert_entity(
                        entity_name=entity_name,
                        entity_type=entity_type,
                        description=description,
                        db_path=db_path,
                    )

                    # Link entity to the article
                    link_entity_to_article(
                        article_id=art_id,
                        entity_id=entity_id,
                        relevance_score=relevance,
                        context_snippet=context,
                        db_path=db_path,
                    )

                    total_extractions += 1
            except Exception as e:
                logger.error(f"Error processing entities for article {art_id}: {e}")

        # Update processed count and report progress
        processed_articles += len(batch_results)
        logger.info(
            f"Completed batch {idx}. "
            f"Processed {processed_articles}/{total_articles} articles ({processed_articles/total_articles*100:.1f}%). "
            f"Extracted {total_extractions} entities so far."
        )

        # Small delay between batches to avoid rate limiting
        time.sleep(0.5)

    logger.info(
        f"Finished entity extraction. Processed {processed_articles}/{total_articles} articles. "
        f"Extracted {total_extractions} entity-article relationships."
    )


def get_entities_for_article(article_id, db_path="db/news.db"):
    """
    Get all entities associated with a specific article.
    Returns a DataFrame with entity information.
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            e.entity_id,
            e.entity_name,
            e.entity_type,
            e.description,
            ae.relevance_score,
            ae.context_snippet
        FROM entity_profiles e
        JOIN article_entities ae ON e.entity_id = ae.entity_id
        WHERE ae.article_id = ?
        ORDER BY ae.relevance_score DESC
    """
    df = pd.read_sql_query(query, conn, params=(article_id,))
    conn.close()
    return df


def get_entities_for_category(category, limit=20, db_path="db/news.db"):
    """
    Get the most mentioned entities within a specific category.
    Returns a DataFrame with entity information.
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            e.entity_id,
            e.entity_name,
            e.entity_type,
            e.description,
            e.mention_count,
            COUNT(DISTINCT ae.article_id) AS article_count
        FROM entity_profiles e
        JOIN article_entities ae ON e.entity_id = ae.entity_id
        JOIN two_phase_article_group_memberships tgm ON ae.article_id = tgm.article_id
        JOIN two_phase_article_groups tg ON tgm.group_id = tg.group_id
        WHERE tg.main_topic = ?
        GROUP BY e.entity_id
        ORDER BY article_count DESC, e.mention_count DESC
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(category, limit))
    conn.close()
    return df


def get_related_entities(entity_id, limit=10, db_path="db/news.db"):
    """
    Get entities that frequently co-occur with the specified entity.
    Returns a DataFrame with entity information and co-occurrence count.
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            e.entity_id,
            e.entity_name,
            e.entity_type,
            e.description,
            COUNT(DISTINCT ae1.article_id) AS co_occurrence_count
        FROM entity_profiles e
        JOIN article_entities ae ON e.entity_id = ae.entity_id
        JOIN article_entities ae1 ON ae.article_id = ae1.article_id
        WHERE ae1.entity_id = ? AND ae.entity_id != ?
        GROUP BY e.entity_id
        ORDER BY co_occurrence_count DESC
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(entity_id, entity_id, limit))
    conn.close()
    return df


def get_trending_entities(hours=48, limit=20, db_path="db/news.db"):
    """
    Get entities that are trending in recent articles.
    Returns a DataFrame with entity information and recent mention count.
    MODIFIED: Correctly joins articles table to filter by date.
    """
    conn = get_connection(db_path)
    # Calculate cutoff time directly for the SQL query
    # Use the format SQLite expects: YYYY-MM-DD HH:MM:SS
    from datetime import datetime, timedelta, timezone  # Add timezone import

    cutoff_time_utc = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_time_str = cutoff_time_utc.strftime("%Y-%m-%d %H:%M:%S")

    # *** MODIFIED QUERY ***
    # Join with 'articles' table to use a.published_date for filtering
    # Removed the erroneous reference to tg.created_at
    query = """
        SELECT
            e.entity_id,
            e.entity_name,
            e.entity_type,
            e.description,
            COUNT(DISTINCT ae.article_id) AS recent_mentions
        FROM entity_profiles e
        JOIN article_entities ae ON e.entity_id = ae.entity_id
        JOIN articles a ON ae.article_id = a.id
        WHERE a.published_date >= ?  -- Filter using article's published date
        GROUP BY e.entity_id
        ORDER BY recent_mentions DESC
        LIMIT ?
    """
    # Use cutoff_time_str and limit as parameters
    df = pd.read_sql_query(query, conn, params=(cutoff_time_str, limit))
    conn.close()
    return df
