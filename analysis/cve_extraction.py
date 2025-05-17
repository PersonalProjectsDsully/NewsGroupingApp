import pandas as pd
import re
from datetime import datetime, timedelta
import pytz
import sqlite3
from urllib.parse import urlparse
import logging
from db.database import get_connection, insert_or_update_cve_info # Assuming this helper exists
from db.database import insert_article_cve # Assuming this helper exists
import json
import requests
import time
from utils import extract_cves, CVE_REGEX # <<< ADD CVE_REGEX HERE

logger = logging.getLogger(__name__)

def build_cve_table(date_hours=None, db_path="db/news.db"):
    """
    Returns a DataFrame with CVE mentions and metadata, for use in the React CVE Mentions table.
    Filters by `published_date` if `date_hours` is provided.

    The 'article_links' column is now an array/list of objects, each containing
    the 'url' and 'source' of the article mention.
    Example: [{'url': 'http://...', 'source': 'bleepingcomputer'}, {'url': 'http://...', 'source': 'theregister'}]
    """
    conn = get_connection(db_path)
    c = conn.cursor()
    logger.info(f"Building CVE table for date_hours: {date_hours}")

    # 1) Load references
    # Use alias 'article_published_date' to avoid potential conflict with cve_info dates
    query_ac = """
        SELECT
            ac.cve_id, ac.article_id, ac.published_date AS article_published_date,
            a.title AS article_title, a.link AS article_url, a.content, a.source
        FROM article_cves ac
        JOIN articles a ON ac.article_id = a.id
    """
    try:
        article_cves_df = pd.read_sql_query(query_ac, conn)
        logger.info(f"Initial fetch from article_cves yielded {len(article_cves_df)} rows.")
        if article_cves_df.empty:
            conn.close()
            logger.warning("No CVE mentions found in article_cves join. Returning empty table.")
            return pd.DataFrame(columns=[
                "cve_id", "times_seen", "first_mention", "last_mention",
                "article_links", "base_score", "vendor", "affected_products",
                "cve_page_link", "vendor_link", "solution", "sources"
            ])

        # Convert published_date to datetime
        article_cves_df["published_date_dt"] = pd.to_datetime(
            article_cves_df["article_published_date"], utc=True, errors='coerce'
        )
        failed_parses = article_cves_df["published_date_dt"].isna().sum()
        if failed_parses > 0:
            logger.warning(f"Failed to parse {failed_parses} article published dates.")

        # Drop rows where date parsing failed if date filtering is active
        if date_hours is not None and date_hours > 0:
            article_cves_df.dropna(subset=["published_date_dt"], inplace=True)
            if article_cves_df.empty:
                 logger.warning("DataFrame became empty after dropping rows with unparseable dates during initial load.")

    except Exception as e:
        logger.error(f"Error during initial data fetch or date conversion: {e}", exc_info=True)
        conn.close()
        return pd.DataFrame(columns=[
            "cve_id", "times_seen", "first_mention", "last_mention",
            "article_links", "base_score", "vendor", "affected_products",
            "cve_page_link", "vendor_link", "solution", "sources"
        ])

    # 2) Filter by date_hours if requested
    if date_hours is not None and date_hours > 0 and not article_cves_df.empty:
        cutoff_utc = datetime.now(pytz.UTC) - timedelta(hours=date_hours)
        logger.info(f"Applying date filter: published_date_dt >= {cutoff_utc}")
        article_cves_df = article_cves_df[article_cves_df["published_date_dt"] >= cutoff_utc].copy()
        logger.info(f"DataFrame shape after date filter: {article_cves_df.shape}")

    if article_cves_df.empty:
        conn.close()
        logger.warning("No CVE mentions found after applying date filter. Returning empty table.")
        return pd.DataFrame(columns=[
            "cve_id", "times_seen", "first_mention", "last_mention",
            "article_links", "base_score", "vendor", "affected_products",
            "cve_page_link", "vendor_link", "solution", "sources"
        ])

    # 3) Load CVE metadata
    try:
        cve_info_rows = c.execute("SELECT * FROM cve_info").fetchall()
        cve_info_cols = [desc[0] for desc in c.description]
        cve_info_df = pd.DataFrame(cve_info_rows, columns=cve_info_cols)
        logger.info(f"Fetched {len(cve_info_df)} rows from cve_info.")
    except Exception as e:
        logger.error(f"Error fetching cve_info: {e}", exc_info=True)
        cve_info_df = pd.DataFrame(columns=['cve_id'])

    conn.close() # Close connection after DB reads

    # 4) Merge
    if not cve_info_df.empty and 'cve_id' in cve_info_df.columns:
        if 'cve_id' in article_cves_df.columns:
             merged_df = pd.merge(article_cves_df, cve_info_df, on="cve_id", how="left")
             logger.info(f"Shape after merging with cve_info: {merged_df.shape}")
        else:
             logger.warning("Missing 'cve_id' column in article_cves_df, skipping merge.")
             merged_df = article_cves_df.copy()
             for col in ["base_score", "vendor", "affected_products", "cve_url", "vendor_link", "solution"]:
                  if col not in merged_df.columns: merged_df[col] = None
    else:
        logger.warning("cve_info table is empty or missing 'cve_id', proceeding without merge.")
        merged_df = article_cves_df.copy()
        for col in ["base_score", "vendor", "affected_products", "cve_url", "vendor_link", "solution"]:
            if col not in merged_df.columns: merged_df[col] = None

    if merged_df.empty:
        logger.warning("DataFrame is empty before grouping. Returning empty table.")
        return pd.DataFrame(columns=[
            "cve_id", "times_seen", "first_mention", "last_mention",
            "article_links", "base_score", "vendor", "affected_products",
            "cve_page_link", "vendor_link", "solution", "sources"
        ])

    # 5) Group by cve_id
    try:
        grouped = merged_df.groupby("cve_id").agg(
            article_id_list=pd.NamedAgg(column="article_id", aggfunc=lambda x: list(x.unique())),
            article_url_list=pd.NamedAgg(column="article_url", aggfunc=list),
            source_list=pd.NamedAgg(column="source", aggfunc=list),
            article_title_list=pd.NamedAgg(column="article_title", aggfunc=list),
            published_date_min=pd.NamedAgg(column="published_date_dt", aggfunc="min"),
            published_date_max=pd.NamedAgg(column="published_date_dt", aggfunc="max"),
            base_score=pd.NamedAgg(column="base_score", aggfunc="first"),
            vendor=pd.NamedAgg(column="vendor", aggfunc="first"),
            affected_products=pd.NamedAgg(column="affected_products", aggfunc="first"),
            cve_url=pd.NamedAgg(column="cve_url", aggfunc="first"),
            vendor_link=pd.NamedAgg(column="vendor_link", aggfunc="first"),
            solution=pd.NamedAgg(column="solution", aggfunc="first")
        ).reset_index()
        logger.info(f"Shape after grouping: {grouped.shape}")

    except KeyError as e:
        logger.error(f"Grouping failed - Missing column: {e}. Columns available: {merged_df.columns.tolist()}", exc_info=True)
        return pd.DataFrame(columns=[
            "cve_id", "times_seen", "first_mention", "last_mention",
            "article_links", "base_score", "vendor", "affected_products",
            "cve_page_link", "vendor_link", "solution", "sources"
        ])
    except Exception as e:
        logger.error(f"Error during grouping: {e}", exc_info=True)
        return pd.DataFrame(columns=[
            "cve_id", "times_seen", "first_mention", "last_mention",
            "article_links", "base_score", "vendor", "affected_products",
            "cve_page_link", "vendor_link", "solution", "sources"
        ])

    if grouped.empty:
        logger.warning("DataFrame is empty after grouping. Returning empty table.")
        return pd.DataFrame(columns=[
            "cve_id", "times_seen", "first_mention", "last_mention",
            "article_links", "base_score", "vendor", "affected_products",
            "cve_page_link", "vendor_link", "solution", "sources"
        ])

    # 6) Build final rows
    table_rows = []
    for _, row in grouped.iterrows():
        cve_id = row["cve_id"]
        article_urls = row["article_url_list"] or []
        article_sources = row["source_list"] or []

        times_seen = len(row["article_id_list"] or [])
        first_mention = row["published_date_min"] # Datetime object
        last_mention = row["published_date_max"] # Datetime object

        # --- This is where urlparse is needed ---
        sources_set = set()
        for url in article_urls:
            if url:
                try:
                    # Use urlparse here
                    hostname = urlparse(url).netloc
                    if hostname: sources_set.add(hostname)
                except Exception as parse_err:
                    logger.warning(f"Could not parse URL '{url}' for source extraction: {parse_err}")
        sources_str = ", ".join(sorted(list(sources_set)))
        # --- End of urlparse usage ---

        mitre_cve_link = f"https://cve.mitre.org/cgi-bin/cvename.cgi?name={cve_id}" if cve_id else None

        article_links_data = []
        num_links = min(len(article_urls), len(article_sources))
        for i in range(num_links):
            article_links_data.append({
                "url": article_urls[i],
                "source": article_sources[i] if article_sources[i] else "unknown"
            })

        table_rows.append({
            "cve_id": cve_id,
            "times_seen": times_seen,
            "first_mention": first_mention.isoformat() if pd.notna(first_mention) else None,
            "last_mention": last_mention.isoformat() if pd.notna(last_mention) else None,
            "article_links": article_links_data,
            "base_score": row["base_score"],
            "vendor": row["vendor"],
            "affected_products": row["affected_products"],
            "cve_page_link": mitre_cve_link,
            "vendor_link": row["vendor_link"],
            "solution": row["solution"],
            "sources": sources_str
        })

    result_df = pd.DataFrame(table_rows)
    result_df = result_df.where(pd.notnull(result_df), None)

    result_df['times_seen'] = pd.to_numeric(result_df['times_seen'], errors='coerce').fillna(0)
    result_df.sort_values(by=["times_seen", "cve_id"], ascending=[False, True], inplace=True)

    logger.info(f"Finished building CVE table. Returning {len(result_df)} rows.")
    return result_df

# --- Other functions like update_cve_details_from_api, process_cves_in_articles remain unchanged ---

def process_cves_in_articles(db_path="db/news.db"):
    """
    - Extract CVE numbers from articles using regex.
    - Store each CVE mention in `article_cves(article_id, cve_id, published_date)`.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Fetch all articles WITH their published date
    cursor.execute("SELECT id, published_date, content FROM articles")
    articles = cursor.fetchall()
    conn.close()

    total_found = 0
    from db.database import insert_article_cve # Assuming this helper exists
    from utils import extract_cves # Assuming this helper exists

    for (article_id, published_date, content) in articles:
        # Ensure published_date is passed along
        # It might be a string here, the insert_article_cve should handle format if needed,
        # but ideally, the migration script fixed the format in the articles table.
        found_cves = extract_cves(content or "")
        if not found_cves:
            continue

        for cve in found_cves:
            # Pass the published_date from the article record
            insert_success = insert_article_cve(article_id, cve, published_date, db_path=db_path)
            if insert_success:
                total_found += 1
            else:
                logger.warning(f"Failed to insert CVE mention for article {article_id}, CVE {cve}")

    logger.info(f"Finished processing CVEs. Attempted to insert {total_found} new CVE references.")


def update_cve_details_from_api(db_path="db/news.db"):
    """
    Fetches CVE details from Mitre API and updates `cve_info` table.
    """
    conn = get_connection(db_path)
    c = conn.cursor()

    # 1) Get all unique CVE IDs from article_cves
    try:
        c.execute("SELECT DISTINCT cve_id FROM article_cves")
        all_cves = [row[0] for row in c.fetchall()]
        logger.info(f"Found {len(all_cves)} unique CVE IDs mentioned in articles.")
    except Exception as e:
        logger.error(f"Error fetching distinct CVEs from article_cves: {e}", exc_info=True)
        conn.close()
        return

    # 2) Count occurrences for each CVE
    times_mentioned_map = {}
    try:
        mention_rows = c.execute("SELECT cve_id, COUNT(*) FROM article_cves GROUP BY cve_id").fetchall()
        times_mentioned_map = {row[0]: row[1] for row in mention_rows}
    except Exception as e:
        logger.error(f"Error counting CVE mentions: {e}", exc_info=True)
        # Continue, but times_mentioned might be inaccurate

    updated_count = 0
    skipped_count = 0
    failed_count = 0
    processed_count = 0

    from db.database import insert_or_update_cve_info # Assuming this helper exists
    import json # Ensure json is imported
    import requests # Ensure requests is imported

    total_cves = len(all_cves)
    for index, cve_id in enumerate(all_cves):
        processed_count += 1
        if index % 50 == 0: # Log progress
            logger.info(f"Updating CVE details: Processed {processed_count}/{total_cves}...")

        # Check if CVE info already exists and was updated recently (e.g., last 7 days)
        # This avoids hitting the API too often for the same CVE
        try:
             c.execute("SELECT updated_at FROM cve_info WHERE cve_id = ?", (cve_id,))
             result = c.fetchone()
             if result and result[0]:
                 last_updated_str = result[0]
                 last_updated_dt = pd.to_datetime(last_updated_str, errors='coerce') # Try parsing stored date
                 if pd.notna(last_updated_dt) and (datetime.now() - last_updated_dt).days < 7:
                     # logger.debug(f"Skipping recently updated CVE: {cve_id}")
                     skipped_count += 1
                     continue # Skip update if recent
        except Exception as check_err:
             logger.warning(f"Could not check last update time for {cve_id}: {check_err}")


        # Construct API URL carefully
        if not isinstance(cve_id, str) or not re.match(CVE_REGEX, cve_id):
            logger.warning(f"Skipping invalid CVE ID format: {cve_id}")
            failed_count += 1
            continue

        url = f"https://cveawg.mitre.org/api/cve/{cve_id}"
        try:
            resp = requests.get(url, timeout=15) # Increased timeout slightly
            resp.raise_for_status() # Check for HTTP errors
            data = resp.json()

            if not isinstance(data, dict):
                logger.warning(f"Invalid JSON response for {cve_id}: {data}")
                failed_count += 1
                continue
            if data.get("error") == "CVE_RECORD_NOT_FOUND" or data.get("message") == "CVE not found":
                 logger.info(f"CVE not found in API: {cve_id}")
                 # Optionally, insert a placeholder or mark as not found
                 failed_count += 1
                 continue
            if data.get("error") == "INTERNAL_SERVER_ERROR":
                 logger.warning(f"API internal server error for {cve_id}. Skipping.")
                 failed_count += 1
                 continue

        except requests.exceptions.Timeout:
             logger.warning(f"Timeout fetching details for {cve_id}")
             failed_count += 1
             continue
        except requests.exceptions.RequestException as req_err:
             logger.warning(f"Request error fetching details for {cve_id}: {req_err}")
             failed_count += 1
             continue
        except json.JSONDecodeError as json_err:
             logger.warning(f"JSON decode error for {cve_id}: {json_err}. Response text: {resp.text[:200]}")
             failed_count += 1
             continue
        except Exception as e:
            logger.error(f"Unexpected error fetching/parsing details for {cve_id}: {e}", exc_info=True)
            failed_count += 1
            continue

        # --- Parse CVE details ---
        cna_data = data.get("containers", {}).get("cna", {})
        if not cna_data: # Check if cna container exists
            logger.warning(f"No CNA container found in JSON for {cve_id}")
            # Treat as failure or insert minimal info? Let's count as failure for now.
            failed_count += 1
            continue

        affected_list = cna_data.get("affected", [])

        # Safely get vendor and product, handle potential None values
        vendor_str = ", ".join(
            sorted({aff.get("vendor", "") for aff in affected_list if aff and aff.get("vendor")})
        )
        products_str = ", ".join(
            sorted({aff.get("product", "") for aff in affected_list if aff and aff.get("product")})
        )

        # Safely get base score
        base_score = None
        metrics_list = cna_data.get("metrics", [])
        if metrics_list and isinstance(metrics_list, list):
            # Prefer CVSS v3.x if available
            cvss_v3 = next((m.get("cvssV3_1", {}).get("baseScore") or m.get("cvssV3_0", {}).get("baseScore")
                             for m in metrics_list if isinstance(m, dict) and (m.get("cvssV3_1") or m.get("cvssV3_0"))), None)
            if cvss_v3 is not None:
                 base_score = cvss_v3
            else:
                 # Fallback to CVSS v2 if v3 not found
                 cvss_v2 = next((m.get("cvssV2", {}).get("baseScore")
                                  for m in metrics_list if isinstance(m, dict) and m.get("cvssV2")), None)
                 base_score = cvss_v2


        # Safely get vendor link
        vendor_link = ""
        references_list = cna_data.get("references", [])
        if references_list and isinstance(references_list, list):
            vendor_link = next(
                (ref.get("url", "") for ref in references_list
                 if isinstance(ref, dict) and isinstance(ref.get("tags"), list) and "vendor-advisory" in ref["tags"]),
                ""
            )


        # Safely get solutions
        solution_str = ""
        solutions_list = cna_data.get("solutions", [])
        if solutions_list and isinstance(solutions_list, list):
            solution_str = "\n\n".join(
                sol.get("value", "") for sol in solutions_list if isinstance(sol, dict) and sol.get("value")
            ).strip()

        # Get current times mentioned count
        current_times_mentioned = times_mentioned_map.get(cve_id, 0)

        # Insert/Update using the database helper function
        insert_success = insert_or_update_cve_info(
            cve_id=cve_id,
            base_score=base_score,
            vendor=vendor_str,
            affected_products=products_str,
            # Construct API link if needed, or use link from response if available
            cve_url=f"https://cveawg.mitre.org/api/cve/{cve_id}", # Or use a specific URL from response if preferred
            vendor_link=vendor_link,
            solution=solution_str,
            times_mentioned=current_times_mentioned, # Use count from article_cves
            raw_json_str=json.dumps(data), # Store the raw JSON
            db_path=db_path
            # Let the helper handle the cursor/connection
        )

        if insert_success:
            updated_count += 1
        else:
            logger.warning(f"Failed to insert/update cve_info for {cve_id}")
            failed_count += 1

        # Optional: Small delay to be polite to the API
        time.sleep(0.1)


    logger.info(f"Finished updating CVE details. Total Processed: {processed_count}, Updated: {updated_count}, Skipped (Recent): {skipped_count}, Failed/Not Found: {failed_count}.")
    # No explicit commit/close needed if insert_or_update_cve_info handles its connection
