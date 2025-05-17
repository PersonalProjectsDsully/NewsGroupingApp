# app.py
import os
import sqlite3
from flask import Flask, send_from_directory, jsonify, request
import pytz
from datetime import datetime, timedelta
import logging
import pandas as pd  # Keep pandas import needed elsewhere
import numpy as np

import threading
import time
import json

# --- Core Modules & Utilities ---
from news_grouping_app.analysis.cve_extraction import build_cve_table
from news_grouping_app.analysis.trending_analysis import (
    get_trending_topics,
    cleanup_old_trends,
    ensure_minimum_trends,
)
from news_grouping_app.analysis.entity_extraction import get_trending_entities, get_entities_for_category
from news_grouping_app.pipeline import schedule_regular_cleanup
from news_grouping_app.llm_calls import call_gpt_api
from pathlib import Path

# --- Database ---
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "db" / "news.db"


def get_connection():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


# Use frontend_build directory at the project root as the static folder
STATIC_DIR = Path(__file__).resolve().parent.parent / "frontend_build"
app = Flask(
    __name__,
    static_folder=str(STATIC_DIR),
    # static_url_path=""  # Keep default static path relative to static_folder
)

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


# --- Background Cleanup Scheduler ---
# (Keep the existing scheduler thread)
def cleanup_scheduler():
    while True:
        try:
            logger.info("Running scheduled trending data cleanup...")
            schedule_regular_cleanup(DB_PATH)
            logger.info("Scheduled cleanup finished.")
        except Exception as e:
            logger.error(f"Error in cleanup scheduler: {e}")
        time.sleep(60 * 60)


cleanup_thread = threading.Thread(target=cleanup_scheduler, daemon=True)
cleanup_thread.start()
logger.info("Background cleanup scheduler started.")


# --- Helper Function to Fetch PRIMARY Groups & Filtered Articles ---
# (Keep the existing fetch_groups_for_category function)
def fetch_groups_for_category(category_value, hours=None):
    cutoff_iso = None
    if hours is not None and hours > 0:
        cutoff_utc = datetime.now(pytz.UTC) - timedelta(hours=hours)
        cutoff_iso = cutoff_utc.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            f"Filtering articles for category '{category_value}' published after: {cutoff_iso}"
        )
    else:
        logger.info(
            f"Fetching all articles for category '{category_value}' (no time filter)."
        )

    conn = get_connection()
    c = conn.cursor()
    groups_data = []
    try:
        c.execute(
            "SELECT group_id, main_topic, group_label, description FROM two_phase_article_groups WHERE main_topic = ?",
            (category_value,),
        )
        group_definitions = c.fetchall()

        for group_id, _, group_label, description in group_definitions:
            description = description or "No description available."
            article_query = "SELECT a.id AS article_id, a.link, a.title, a.published_date, a.content FROM articles a JOIN two_phase_article_group_memberships tgm ON a.id = tgm.article_id WHERE tgm.group_id = ?"
            params = [group_id]
            if cutoff_iso:
                article_query += " AND a.published_date >= ?"
                params.append(cutoff_iso)
            article_query += " ORDER BY a.published_date DESC"

            c.execute(article_query, params)
            article_rows = c.fetchall()

            if not article_rows:
                continue

            articles_list = []
            latest_article_date = None
            for article_id, link, title, pubdate, content in article_rows:
                preview = (content or "")[:300] + "..." if content else ""
                articles_list.append(
                    {
                        "article_id": article_id,
                        "link": link,
                        "title": title,
                        "published_date": pubdate,
                        "preview": preview,
                    }
                )
                if pubdate and (
                    latest_article_date is None or pubdate > latest_article_date
                ):
                    latest_article_date = pubdate

            groups_data.append(
                {
                    "group_id": group_id,
                    "group_label": group_label,
                    "description": description,
                    "article_count": len(article_rows),
                    "articles": articles_list,
                    "latest_article_date": latest_article_date,
                }
            )
    except sqlite3.Error as e:
        logger.error(
            f"Error fetching groups/articles for category '{category_value}': {e}",
            exc_info=True,
        )
    finally:
        conn.close()

    groups_data.sort(
        key=lambda x: (x["article_count"], x.get("latest_article_date", "")),
        reverse=True,
    )
    for group in groups_data:
        group.pop("latest_article_date", None)
    logger.info(
        f"Found {len(groups_data)} groups with articles for category '{category_value}' in the time window."
    )
    return groups_data


# --- API Endpoints ---
# (Keep existing /api/... endpoints as they are)
# /api/home_groups
@app.route("/api/home_groups", methods=["GET"])
def get_home_groups():
    hours = request.args.get("hours", type=int, default=None)
    logger.info(f"API call to /api/home_groups with hours={hours}")
    categories_data = []
    try:
        from news_grouping_app.analysis.two_phase_grouping import PREDEFINED_CATEGORIES
    except ImportError:
        logger.warning("Could not import PREDEFINED_CATEGORIES. Using fallback.")
        PREDEFINED_CATEGORIES = ["Other"]  # Basic fallback

    for cat in PREDEFINED_CATEGORIES:
        groups_all = fetch_groups_for_category(cat, hours=hours)
        top_3 = groups_all[:3]
        if top_3:
            categories_data.append({"category": cat, "groups": top_3})

    return jsonify({"categories": categories_data})


# /api/category_groups
@app.route("/api/category_groups", methods=["GET"])
def get_category_groups():
    category = request.args.get("category")
    if not category:
        return jsonify({"error": "Missing 'category' parameter"}), 400
    hours = request.args.get("hours", type=int, default=None)
    logger.info(
        f"API call to /api/category_groups with category={category}, hours={hours}"
    )
    groups_data = fetch_groups_for_category(category_value=category, hours=hours)
    return jsonify({"category": category, "groups": groups_data})


# --- Specific Category Endpoints ---
@app.route("/api/science_environment_groups", methods=["GET"])
def get_science_environment_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category("Science & Environment", hours=hours)
    return jsonify({"category": "Science & Environment", "groups": groups})


@app.route("/api/business_finance_trade_groups", methods=["GET"])
def get_business_finance_trade_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category("Business, Finance & Trade", hours=hours)
    return jsonify({"category": "Business, Finance & Trade", "groups": groups})


@app.route("/api/ai_machine_learning_groups", methods=["GET"])
def get_ai_machine_learning_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category(
        "Artificial Intelligence & Machine Learning", hours=hours
    )
    return jsonify(
        {"category": "Artificial Intelligence & Machine Learning", "groups": groups}
    )


@app.route("/api/cybersecurity_data_privacy_groups", methods=["GET"])
def get_cybersecurity_data_privacy_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category("Cybersecurity & Data Privacy", hours=hours)
    return jsonify({"category": "Cybersecurity & Data Privacy", "groups": groups})


@app.route("/api/politics_government_groups", methods=["GET"])
def get_politics_government_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category("Politics & Government", hours=hours)
    return jsonify({"category": "Politics & Government", "groups": groups})


@app.route("/api/consumer_tech_gadgets_groups", methods=["GET"])
def get_consumer_tech_gadgets_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category("Consumer Technology & Gadgets", hours=hours)
    return jsonify({"category": "Consumer Technology & Gadgets", "groups": groups})


@app.route("/api/automotive_space_transportation_groups", methods=["GET"])
def get_automotive_space_transportation_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category(
        "Automotive, Space & Transportation", hours=hours
    )
    return jsonify({"category": "Automotive, Space & Transportation", "groups": groups})


@app.route("/api/enterprise_cloud_computing_groups", methods=["GET"])
def get_enterprise_cloud_computing_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category(
        "Enterprise Technology & Cloud Computing", hours=hours
    )
    return jsonify(
        {"category": "Enterprise Technology & Cloud Computing", "groups": groups}
    )


@app.route("/api/other_groups", methods=["GET"])
def get_other_groups():
    hours = request.args.get("hours", type=int, default=None)
    groups = fetch_groups_for_category("Other", hours=hours)
    return jsonify({"category": "Other", "groups": groups})


@app.route("/api/cve_table", methods=["GET"])
def cve_table_api():
    hours = request.args.get("hours", type=int, default=None)
    logger.info(f"API call to /api/cve_table with hours={hours}")
    try:
        df = build_cve_table(date_hours=hours, db_path=DB_PATH)
        logger.info(f"build_cve_table returned DataFrame with {len(df)} rows.")
        df = df.replace({np.nan: None})
        records = df.to_dict(orient="records")
        return jsonify(records)

    except Exception as e:
        logger.error(f"Error in CVE table API endpoint: {e}", exc_info=True)
        return jsonify({"error": "Failed to generate or process CVE data"}), 500


# /api/trending
@app.route("/api/trending", methods=["GET"])
def get_trending_api():
    category = request.args.get("category")
    requested_limit = request.args.get("limit", type=int, default=10)
    hours = request.args.get("hours", type=int, default=48)
    min_limit = 6
    limit = max(requested_limit, min_limit)
    logger.info(
        f"API call to /api/trending with category={category}, limit={limit}, hours={hours}"
    )
    try:
        api_key = os.environ.get("OPENAI_API_KEY")
        ensure_minimum_trends(min_count=min_limit, api_key=api_key, db_path=DB_PATH)
        df = get_trending_topics(category=category, limit=limit, db_path=DB_PATH)

        if df.empty:
            return jsonify([])

        if hours and hours > 0:
            cutoff_time_iso = (
                datetime.now(pytz.UTC) - timedelta(hours=hours)
            ).isoformat()
            logger.info(
                f"Filtering trending articles published after: {cutoff_time_iso}"
            )
            for idx in df.index:
                articles = df.loc[idx, "articles"]
                if isinstance(articles, list):
                    filtered_articles = [
                        a
                        for a in articles
                        if a.get("published_date")
                        and str(a["published_date"]) >= cutoff_time_iso
                    ]
                    df.loc[idx, "articles"] = filtered_articles
                else:
                    df.loc[idx, "articles"] = []
            df = df[
                df["articles"].apply(lambda x: isinstance(x, list) and len(x) > 0)
            ].copy()

        if df.empty:
            return jsonify([])

        records = df.to_dict(orient="records")
        # Format dates safely in the final list
        for record in records:
            for article in record.get("articles", []):
                pub_date = article.get("published_date")
                if pub_date:
                    try:
                        dt = pd.to_datetime(pub_date, errors="coerce")
                        if pd.notna(dt):
                            article["published_date"] = dt.isoformat()
                        else:
                            article["published_date"] = (
                                str(pub_date) if isinstance(pub_date, str) else None
                            )
                    except Exception as e:
                        logger.warning(
                            f"Could not format date {pub_date} for trending article {article.get('article_id')}: {e}"
                        )
                        article["published_date"] = (
                            str(pub_date) if isinstance(pub_date, str) else None
                        )
        return jsonify(records)
    except Exception as e:
        logger.error(f"Error fetching trending topics: {e}", exc_info=True)
        return jsonify({"error": "Failed to generate trending data"}), 500


# --- Other Endpoints ---
# (Keep /api/trending_entities, /api/category_entities, /api/prompt_tester/*, /api/debug/date_format as they were)
@app.route("/api/trending_entities", methods=["GET"])
def get_trending_entities_api():
    hours = request.args.get("hours", type=int, default=48)
    limit = request.args.get("limit", type=int, default=20)
    logger.info(f"API call to /api/trending_entities with hours={hours}, limit={limit}")
    try:
        df = get_trending_entities(hours=hours, limit=limit, db_path=DB_PATH)
        records = df.to_dict(orient="records")
        return jsonify(records)
    except Exception as e:
        logger.error(f"Error fetching trending entities: {e}", exc_info=True)
        return jsonify({"error": "Failed to generate trending entities"}), 500


@app.route("/api/category_entities", methods=["GET"])
def get_category_entities_api():
    category = request.args.get("category")
    if not category:
        return jsonify({"error": "Missing 'category' parameter"}), 400
    limit = request.args.get("limit", type=int, default=20)
    logger.info(
        f"API call to /api/category_entities with category={category}, limit={limit}"
    )
    try:
        df = get_entities_for_category(category, limit=limit, db_path=DB_PATH)
        records = df.to_dict(orient="records")
        return jsonify(records)
    except Exception as e:
        logger.error(
            f"Error fetching category entities for '{category}': {e}", exc_info=True
        )
        return (
            jsonify({"error": f"Failed to generate entities for category {category}"}),
            500,
        )


@app.route("/api/prompt_tester/articles", methods=["GET"])
def get_recent_articles_for_testing():
    hours = request.args.get("hours", type=int, default=24)
    limit = request.args.get("limit", type=int, default=20)
    logger.info(
        f"API call to /api/prompt_tester/articles with hours={hours}, limit={limit}"
    )
    cutoff_time = (datetime.now(pytz.UTC) - timedelta(hours=hours)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn = get_connection()
    articles = []
    try:
        query = "SELECT id, title, content, published_date, source FROM articles WHERE published_date >= ? ORDER BY published_date DESC LIMIT ?"
        rows = conn.execute(query, (cutoff_time, limit)).fetchall()
        for row in rows:
            articles.append(
                {
                    "article_id": row[0],
                    "title": row[1],
                    "content": (
                        row[2][:500] + "..." if row[2] and len(row[2]) > 500 else row[2]
                    ),
                    "full_content": row[2],
                    "published_date": row[3],
                    "source": row[4],
                }
            )
    except Exception as e:
        logger.error(f"Error fetching articles for prompt tester: {e}", exc_info=True)
    finally:
        conn.close()
    return jsonify(articles)


@app.route("/api/prompt_tester/test_prompt", methods=["POST"])
def test_grouping_prompt():
    logger.info("API call to /api/prompt_tester/test_prompt")
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400
        article_ids = data.get("articles", [])
        prompt_template = data.get("prompt_template", "")
        from news_grouping_app.config import OPENAI_MODEL
        model = data.get("model", OPENAI_MODEL)

        if not article_ids or not prompt_template:
            return jsonify({"error": "Missing 'articles' or 'prompt_template'"}), 400

        conn = get_connection()
        placeholders = ",".join("?" for _ in article_ids)
        query = f"SELECT id, title, content FROM articles WHERE id IN ({placeholders})"
        df = pd.read_sql_query(query, conn, params=article_ids)
        conn.close()

        if df.empty:
            return jsonify({"error": "No articles found for the given IDs"}), 404

        articles_text = ""
        for _, row in df.iterrows():
            articles_text += (
                f"Article ID={row['id']}:\nTitle: {row['title']}\n{row['content']}\n\n"
            )

        full_prompt = prompt_template.replace("{articles}", articles_text)
        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": full_prompt},
        ]

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            return jsonify({"error": "OpenAI API key not configured on server"}), 500

        response = call_gpt_api(messages, api_key, model=model)
        if response is None:
            return jsonify({"error": "Failed to get response from LLM API"}), 500

        return jsonify({"response": response})

    except Exception as e:
        logger.error(f"Error in /api/prompt_tester/test_prompt: {e}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route("/api/debug/date_format", methods=["GET"])
def debug_date_format():
    conn = get_connection()
    results = []
    try:
        c = conn.cursor()
        c.execute(
            "SELECT id, published_date FROM articles ORDER BY published_date DESC LIMIT 30"
        )
        date_samples = c.fetchall()
        target_format = "%Y-%m-%d %H:%M:%S"
        for article_id, date_str in date_samples:
            is_target_format = False
            parse_error = None
            pandas_parsed_utc_str = None
            original_value = date_str
            try:
                if date_str:
                    parsed_dt = pd.to_datetime(date_str, utc=True, errors="coerce")
                    if pd.notna(parsed_dt):
                        pandas_parsed_utc_str = parsed_dt.strftime(target_format)
                        try:
                            datetime.strptime(date_str, target_format)
                            is_target_format = True
                        except (ValueError, TypeError):
                            pass
                    else:
                        parse_error = "Pandas could not parse (became NaT)"
                else:
                    parse_error = "Date string is NULL or empty"
            except Exception as e:
                parse_error = str(e)

            results.append(
                {
                    "article_id": article_id,
                    "original": original_value,
                    "is_standard_format": is_target_format,
                    "pandas_parsed_utc": pandas_parsed_utc_str,
                    "parse_error": parse_error,
                }
            )
    except Exception as e:
        logger.error(f"Error in date debug endpoint: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
    return jsonify(results)


# --- Serve React App ---
# Serve the main index.html for the root path
@app.route("/")
def serve_index():
    return send_from_directory(app.static_folder, "index.html")


# Catch-all route for client-side navigation
# This MUST come AFTER your API routes
@app.route("/<path:path>")
def serve_react_app(path):
    # Construct the full path to the requested file/asset
    requested_path = os.path.join(app.static_folder, path)

    # Check if the requested path points to an existing file in the static folder
    # (e.g., CSS, JS, images, manifest.json, etc.)
    if os.path.exists(requested_path) and os.path.isfile(requested_path):
        # If it's an existing file, serve it directly
        logger.debug(f"Serving static file: {path}")
        return send_from_directory(app.static_folder, path)
    else:
        # If the path doesn't correspond to an existing file,
        # it's likely a client-side route. Serve the main index.html
        # to let React Router handle the routing.
        logger.debug(
            f"Path '{path}' not found as static file, serving index.html for client-side routing."
        )
        return send_from_directory(app.static_folder, "index.html")


# --- Run Server ---
if __name__ == "__main__":
    logger.info("Starting Flask application...")
    # Set debug=False for production/stable environments
    # Host 0.0.0.0 makes it accessible outside the container
    app.run(host="0.0.0.0", port=8501, debug=False)
