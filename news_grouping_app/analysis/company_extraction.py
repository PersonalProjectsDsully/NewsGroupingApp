# analysis/company_extraction.py

import sqlite3
import json
import re
import time
import pandas as pd
import logging

from news_grouping_app.db.database import get_connection
from news_grouping_app.llm_calls import call_gpt_api
from news_grouping_app.utils import chunk_summaries, MAX_TOKEN_CHUNK
from news_grouping_app.config import OPENAI_MODEL

logger = logging.getLogger(__name__)
MODEL = OPENAI_MODEL  # default model can be overridden via env var


def get_articles_missing_company_extraction(db_path="db/news.db"):
    """
    Returns a DataFrame of articles that do NOT have any entry in article_companies.
    Now selects articles by integer id instead of text link.
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            a.id AS article_id,
            a.title || ' - ' || a.content AS expanded_summary
        FROM articles a
        WHERE NOT EXISTS (
            SELECT 1 FROM article_companies ac
            WHERE ac.article_id = a.id
        )
        ORDER BY a.published_date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def extract_company_names_for_all_articles(api_key, db_path="db/news.db"):
    """
    Identify articles with no company extractions, parse them with LLM for company names,
    then store results in article_companies using article_id.
    """
    df = get_articles_missing_company_extraction(db_path=db_path)
    if df.empty:
        logger.info("All articles already have company extractions.")
        return

    # Build a dict {article_id: expanded_summary}
    summaries_dict = {}
    for _, row in df.iterrows():
        art_id = row["article_id"]
        content = str(row["expanded_summary"]).strip()
        if content:
            summaries_dict[art_id] = content

    chunked_articles = list(
        chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK)
    )
    total_extractions = 0

    for idx, chunk_dict in enumerate(chunked_articles, start=1):
        logger.info(
            f"Extracting company names for chunk {idx}/{len(chunked_articles)} "
            f"with {len(chunk_dict)} articles."
        )

        prompt = (
            "You are a named-entity recognition AI. For each article, extract all company names mentioned. "
            "Return only JSON with the format:\n"
            '{ "extractions": [ {"article_id": "...", "companies": ["CompanyA", "CompanyB"]}, ... ] }\n\n'
        )
        # Append the article texts
        for art_id, text in chunk_dict.items():
            snippet = text[:5000]  # limit if needed
            prompt += f"Article ID={art_id}:\n{snippet}\n\n"

        messages = [
            {
                "role": "system",
                "content": "Extract company names from the provided article texts.",
            },
            {"role": "user", "content": prompt},
        ]

        resp = call_gpt_api(messages, api_key, model=MODEL)
        if not resp:
            logger.warning("No response from GPT for this chunk.")
            continue

        cleaned = resp.strip().strip("```")
        cleaned = re.sub(r"^json\s+", "", cleaned, flags=re.IGNORECASE)
        try:
            data = json.loads(cleaned)
            extractions = data.get("extractions", [])
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing extraction JSON: {e}\n{cleaned}")
            extractions = []

        conn = get_connection(db_path)
        c = conn.cursor()
        try:
            for item in extractions:
                # We'll parse the article_id from the JSON. It's often a string,
                # so let's cast to int if needed:
                raw_article_id = item.get("article_id")
                try:
                    parsed_article_id = int(raw_article_id)
                except (TypeError, ValueError):
                    # If it's not a valid integer, skip
                    continue

                companies = item.get("companies", [])
                if not isinstance(companies, list):
                    continue

                for comp in companies:
                    comp_name = comp.strip()
                    if comp_name:
                        c.execute(
                            """
                            INSERT OR IGNORE INTO article_companies (article_id, company_name)
                            VALUES (?, ?)
                        """,
                            (parsed_article_id, comp_name),
                        )
                        total_extractions += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB error saving company extraction: {e}")
        finally:
            conn.close()

    logger.info(
        f"Finished extracting company names. Inserted {total_extractions} new (article_id, company) pairs."
    )


def get_companies_in_article_list(article_ids, db_path="db/news.db"):
    """
    Given a list of article IDs, return distinct company names from article_companies
    that match those article_ids.
    """
    if not article_ids:
        return []
    conn = get_connection(db_path)
    placeholders = ",".join("?" for _ in article_ids)
    query = f"""
        SELECT DISTINCT company_name
        FROM article_companies
        WHERE article_id IN ({placeholders})
        ORDER BY company_name ASC
    """
    rows = conn.execute(query, article_ids).fetchall()
    conn.close()
    return [r[0] for r in rows]


def filter_articles_by_company(df_articles, company_name, db_path="db/news.db"):
    """
    Return only articles that mention 'company_name' in article_companies.
    If company_name is (All) or empty, return the original df_articles.

    NOTE: This function now expects df_articles to have 'article_id' column.
    """
    if not company_name or company_name == "(All)":
        return df_articles

    if df_articles.empty:
        return df_articles

    # We'll rename the column from 'article_id' if your DataFrame still uses 'id'.
    # Just ensure you're consistent. Here we assume df_articles["article_id"].
    article_ids = df_articles["article_id"].tolist()

    conn = get_connection(db_path)
    placeholders = ",".join("?" for _ in article_ids)
    query = f"""
        SELECT article_id
        FROM article_companies
        WHERE company_name = ?
          AND article_id IN ({placeholders})
    """
    params = [company_name] + article_ids
    matched = conn.execute(query, params).fetchall()
    conn.close()

    valid_ids = {row[0] for row in matched}
    return df_articles[df_articles["article_id"].isin(valid_ids)].copy()
