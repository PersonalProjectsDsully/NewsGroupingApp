#!/usr/bin/env python3
"""
article_signature.py

This module generates "signatures" for articles to enable efficient matching
of similar content. It uses the advanced filtering metrics to create a compact
representation of each article's key features.
"""

import sqlite3
import json
import hashlib
from typing import Dict, List, Set, Tuple, Any, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_primary_entities(
    article_id: int, min_relevance: float = 0.7, db_path: str = "db/news.db"
) -> List[Dict]:
    """
    Get the primary entities for an article (those with high relevance scores).

    Args:
        article_id: The article ID
        min_relevance: Minimum relevance score to include (0-1)
        db_path: Path to the database

    Returns:
        List of entity dictionaries
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            e.entity_id,
            e.entity_name,
            e.entity_type,
            ae.relevance_score
        FROM article_entities ae
        JOIN entity_profiles e ON ae.entity_id = e.entity_id
        WHERE ae.article_id = ? AND ae.relevance_score >= ?
        ORDER BY ae.relevance_score DESC
    """

    cursor.execute(query, (article_id, min_relevance))

    entities = []
    for row in cursor.fetchall():
        entity_id, entity_name, entity_type, relevance_score = row
        entities.append(
            {
                "entity_id": entity_id,
                "entity_name": entity_name,
                "entity_type": entity_type,
                "relevance_score": relevance_score,
            }
        )

    conn.close()
    return entities


def get_companies_for_article(
    article_id: int, db_path: str = "db/news.db"
) -> List[str]:
    """
    Get companies mentioned in an article.

    Args:
        article_id: The article ID
        db_path: Path to the database

    Returns:
        List of company names
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT company_name
        FROM article_companies
        WHERE article_id = ?
    """

    cursor.execute(query, (article_id,))

    companies = [row[0] for row in cursor.fetchall()]
    conn.close()
    return companies


def get_cves_for_article(article_id: int, db_path: str = "db/news.db") -> List[str]:
    """
    Get CVEs mentioned in an article.

    Args:
        article_id: The article ID
        db_path: Path to the database

    Returns:
        List of CVE IDs
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT cve_id
        FROM article_cves
        WHERE article_id = ?
    """

    cursor.execute(query, (article_id,))

    cves = [row[0] for row in cursor.fetchall()]
    conn.close()
    return cves


def get_entities_by_type(
    article_id: int, entity_type: str, db_path: str = "db/news.db"
) -> List[Dict]:
    """
    Get entities of a specific type for an article.

    Args:
        article_id: The article ID
        entity_type: Type of entity to retrieve
        db_path: Path to the database

    Returns:
        List of entity dictionaries
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            e.entity_id,
            e.entity_name,
            ae.relevance_score
        FROM article_entities ae
        JOIN entity_profiles e ON ae.entity_id = e.entity_id
        WHERE ae.article_id = ? AND e.entity_type = ?
        ORDER BY ae.relevance_score DESC
    """

    cursor.execute(query, (article_id, entity_type))

    entities = []
    for row in cursor.fetchall():
        entity_id, entity_name, relevance_score = row
        entities.append(
            {
                "entity_id": entity_id,
                "entity_name": entity_name,
                "relevance_score": relevance_score,
            }
        )

    conn.close()
    return entities


def get_external_references(article_id: int, db_path: str = "db/news.db") -> List[Dict]:
    """
    Get external references (URLs) for an article.

    Args:
        article_id: The article ID
        db_path: Path to the database

    Returns:
        List of reference dictionaries
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            normalized_url,
            domain,
            reference_type
        FROM article_external_references
        WHERE article_id = ?
    """

    try:
        cursor.execute(query, (article_id,))

        references = []
        for row in cursor.fetchall():
            normalized_url, domain, reference_type = row
            references.append(
                {"url": normalized_url, "domain": domain, "type": reference_type}
            )

        return references
    except sqlite3.OperationalError:
        # Table might not exist yet
        return []
    finally:
        conn.close()


def get_named_events(article_id: int, db_path: str = "db/news.db") -> List[Dict]:
    """
    Get named events mentioned in an article.

    Args:
        article_id: The article ID
        db_path: Path to the database

    Returns:
        List of event dictionaries
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            ne.event_id,
            ne.event_name,
            ne.event_type,
            ne.cve_ids
        FROM article_events ae
        JOIN named_events ne ON ae.event_id = ne.event_id
        WHERE ae.article_id = ?
    """

    try:
        cursor.execute(query, (article_id,))

        events = []
        for row in cursor.fetchall():
            event_id, event_name, event_type, cve_ids = row
            events.append(
                {
                    "event_id": event_id,
                    "event_name": event_name,
                    "event_type": event_type,
                    "cve_ids": cve_ids,
                }
            )

        return events
    except sqlite3.OperationalError:
        # Table might not exist yet
        return []
    finally:
        conn.close()


def get_quotes(article_id: int, db_path: str = "db/news.db") -> List[Dict]:
    """
    Get quotes from an article.

    Args:
        article_id: The article ID
        db_path: Path to the database

    Returns:
        List of quote dictionaries
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            q.quote_id,
            q.quote_text,
            q.speaker
        FROM article_quotes aq
        JOIN quotes q ON aq.quote_id = q.quote_id
        WHERE aq.article_id = ?
    """

    try:
        cursor.execute(query, (article_id,))

        quotes = []
        for row in cursor.fetchall():
            quote_id, quote_text, speaker = row
            quotes.append(
                {"quote_id": quote_id, "text": quote_text, "speaker": speaker}
            )

        return quotes
    except sqlite3.OperationalError:
        # Table might not exist yet
        return []
    finally:
        conn.close()


def get_author(article_id: int, db_path: str = "db/news.db") -> Optional[str]:
    """
    Get the author of an article.

    Args:
        article_id: The article ID
        db_path: Path to the database

    Returns:
        Author name or None
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT author_name
        FROM article_authors
        WHERE article_id = ?
        LIMIT 1
    """

    try:
        cursor.execute(query, (article_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        # Table might not exist yet
        return None
    finally:
        conn.close()


def generate_article_signature(
    article_id: int, db_path: str = "db/news.db"
) -> Dict[str, Any]:
    """
    Generate a comprehensive signature of key identifying elements for an article,
    including published_date and source.

    Args:
        article_id: The article ID
        db_path: Path to the database

    Returns:
        Dictionary containing the article signature
    """
    # --- Step 1: Fetch Basic Article Metadata (Date, Source) ---
    published_date_str: Optional[str] = None
    source: Optional[str] = None
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT published_date, source FROM articles WHERE id = ?", (article_id,)
        )
        row = cursor.fetchone()
        if row:
            published_date_str = row[0]
            source = row[1]
        else:
            logger.warning(f"Article {article_id} not found when fetching metadata.")
            # Return an empty dict or raise error if article must exist
            return {}
    except sqlite3.Error as e:
        logger.error(f"DB error fetching metadata for article {article_id}: {e}")
        # Decide if you want to continue without date/source or fail
        # For now, we'll continue but date/source will be None
    finally:
        if conn:
            conn.close()

    # --- Step 2: Gather data from other tables using existing functions ---
    # (Error handling within these functions is assumed)
    primary_entities = get_primary_entities(
        article_id, min_relevance=0.7, db_path=db_path
    )
    companies = get_companies_for_article(article_id, db_path=db_path)
    cves = get_cves_for_article(article_id, db_path=db_path)
    technologies = get_entities_by_type(article_id, "technology", db_path=db_path)
    products = get_entities_by_type(article_id, "product", db_path=db_path)
    references = get_external_references(article_id, db_path=db_path)
    events = get_named_events(article_id, db_path=db_path)
    quotes = get_quotes(article_id, db_path=db_path)
    author = get_author(article_id, db_path=db_path)  # Keep if using author similarity

    # --- Step 3: Build the signature dictionary ---
    signature = {
        "article_id": article_id,
        "published_date": published_date_str,  # Add published date
        "source": source,  # Add source
        "primary_entities": primary_entities,
        "companies": companies,
        "cves": cves,
        "technologies": technologies,
        "products": products,
        "references": references,
        "events": events,
        "quotes": quotes,
        "author": author,
    }

    logger.debug(f"Generated signature for article {article_id}")
    return signature


def calculate_signature_similarity(sig1: Dict, sig2: Dict) -> Dict[str, float]:
    """
    Calculate similarity between two article signatures.

    Args:
        sig1: First article signature
        sig2: Second article signature

    Returns:
        Dictionary with similarity scores for different dimensions
    """
    results = {}

    # Entity similarity
    entity_ids1 = {e["entity_id"] for e in sig1.get("primary_entities", [])}
    entity_ids2 = {e["entity_id"] for e in sig2.get("primary_entities", [])}

    if entity_ids1 and entity_ids2:
        entity_intersection = len(entity_ids1.intersection(entity_ids2))
        entity_union = len(entity_ids1.union(entity_ids2))
        results["entity_similarity"] = (
            entity_intersection / entity_union if entity_union > 0 else 0.0
        )
    else:
        results["entity_similarity"] = 0.0

    # Company similarity
    companies1 = set(sig1.get("companies", []))
    companies2 = set(sig2.get("companies", []))

    if companies1 and companies2:
        company_intersection = len(companies1.intersection(companies2))
        company_union = len(companies1.union(companies2))
        results["company_similarity"] = (
            company_intersection / company_union if company_union > 0 else 0.0
        )
    else:
        results["company_similarity"] = 0.0

    # CVE similarity
    cves1 = set(sig1.get("cves", []))
    cves2 = set(sig2.get("cves", []))

    if cves1 and cves2:
        cve_intersection = len(cves1.intersection(cves2))
        cve_union = len(cves1.union(cves2))
        results["cve_similarity"] = (
            cve_intersection / cve_union if cve_union > 0 else 0.0
        )
    else:
        results["cve_similarity"] = 0.0

    # Event similarity
    event_names1 = {e["event_name"] for e in sig1.get("events", [])}
    event_names2 = {e["event_name"] for e in sig2.get("events", [])}

    if event_names1 and event_names2:
        event_intersection = len(event_names1.intersection(event_names2))
        event_union = len(event_names1.union(event_names2))
        results["event_similarity"] = (
            event_intersection / event_union if event_union > 0 else 0.0
        )
    else:
        results["event_similarity"] = 0.0

    # Reference similarity
    urls1 = {r["url"] for r in sig1.get("references", [])}
    urls2 = {r["url"] for r in sig2.get("references", [])}

    if urls1 and urls2:
        url_intersection = len(urls1.intersection(urls2))
        url_union = len(urls1.union(urls2))
        results["reference_similarity"] = (
            url_intersection / url_union if url_union > 0 else 0.0
        )
    else:
        results["reference_similarity"] = 0.0

    # Author similarity
    results["author_similarity"] = (
        1.0
        if sig1.get("author") == sig2.get("author") and sig1.get("author") is not None
        else 0.0
    )

    # Calculate weighted composite score
    weights = {
        "entity_similarity": 0.3,
        "company_similarity": 0.2,
        "cve_similarity": 0.2,
        "event_similarity": 0.15,
        "reference_similarity": 0.1,
        "author_similarity": 0.05,
    }

    composite_score = sum(score * weights[metric] for metric, score in results.items())

    results["composite_score"] = composite_score

    return results


def find_candidate_matches(
    article_id: int,
    min_score: float = 0.4,
    limit: int = 10,
    db_path: str = "db/news.db",
) -> List[Dict]:
    """
    Find articles that might be related to the given article based on signature similarity.

    Args:
        article_id: The article ID to find matches for
        min_score: Minimum similarity score to include a match
        limit: Maximum number of matches to return
        db_path: Path to the database

    Returns:
        List of dictionaries with match information
    """
    # Generate signature for the target article
    target_signature = generate_article_signature(article_id, db_path)

    # Get all other articles
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM articles WHERE id != ?", (article_id,))
    other_article_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    matches = []

    for other_id in other_article_ids:
        other_signature = generate_article_signature(other_id, db_path)
        similarity = calculate_signature_similarity(target_signature, other_signature)

        if similarity["composite_score"] >= min_score:
            matches.append({"article_id": other_id, "similarity": similarity})

    # Sort by composite score (highest first) and limit results
    matches.sort(key=lambda x: x["similarity"]["composite_score"], reverse=True)
    return matches[:limit]


def generate_signatures_for_all_articles(
    db_path: str = "db/news.db",
) -> Dict[int, Dict]:
    """
    Generate signatures for all articles in the database.

    Args:
        db_path: Path to the database

    Returns:
        Dictionary mapping article IDs to signatures
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM articles")
    article_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    signatures = {}

    for i, article_id in enumerate(article_ids):
        if i % 10 == 0:
            logger.info(f"Generating signature for article {i+1}/{len(article_ids)}")
        signatures[article_id] = generate_article_signature(article_id, db_path)

    return signatures


def create_signature_index(db_path: str = "db/news.db"):
    """
    Create and populate the article_signatures table.

    Args:
        db_path: Path to the database
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the signatures table if it doesn't exist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS article_signatures (
            article_id INTEGER PRIMARY KEY,
            signature_json TEXT NOT NULL,
            entity_hash TEXT,
            company_hash TEXT,
            cve_hash TEXT,
            event_hash TEXT,
            reference_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (article_id) REFERENCES articles(id)
        )
    """
    )

    # Get all articles
    cursor.execute("SELECT id FROM articles")
    article_ids = [row[0] for row in cursor.fetchall()]

    for i, article_id in enumerate(article_ids):
        if i % 10 == 0:
            logger.info(f"Indexing article {i+1}/{len(article_ids)}")

        # Generate signature
        signature = generate_article_signature(article_id, db_path)
        signature_json = json.dumps(signature)

        # Generate hashes for quick matching
        entity_hash = hashlib.md5(
            json.dumps(
                sorted([e["entity_id"] for e in signature["primary_entities"]])
            ).encode()
        ).hexdigest()
        company_hash = hashlib.md5(
            json.dumps(sorted(signature["companies"])).encode()
        ).hexdigest()
        cve_hash = hashlib.md5(
            json.dumps(sorted(signature["cves"])).encode()
        ).hexdigest()
        event_hash = hashlib.md5(
            json.dumps(sorted([e["event_name"] for e in signature["events"]])).encode()
        ).hexdigest()
        reference_hash = hashlib.md5(
            json.dumps(sorted([r["url"] for r in signature["references"]])).encode()
        ).hexdigest()

        # Insert into database
        cursor.execute(
            """
            INSERT OR REPLACE INTO article_signatures
            (article_id, signature_json, entity_hash, company_hash, cve_hash, event_hash, reference_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                article_id,
                signature_json,
                entity_hash,
                company_hash,
                cve_hash,
                event_hash,
                reference_hash,
            ),
        )

    conn.commit()
    conn.close()

    logger.info(f"Created signature index for {len(article_ids)} articles")


def find_matches_by_entity_hash(
    entity_hash: str, db_path: str = "db/news.db"
) -> List[int]:
    """
    Find articles with matching entity hash.

    Args:
        entity_hash: The entity hash to match
        db_path: Path to the database

    Returns:
        List of matching article IDs
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT article_id FROM article_signatures
        WHERE entity_hash = ?
    """,
        (entity_hash,),
    )

    article_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    return article_ids


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate article signatures")
    parser.add_argument(
        "--db-path", type=str, default="db/news.db", help="Path to the database"
    )
    parser.add_argument(
        "--create-index", action="store_true", help="Create signature index"
    )
    parser.add_argument(
        "--article-id", type=int, help="Generate signature for specific article"
    )
    parser.add_argument(
        "--find-matches", action="store_true", help="Find matches for article"
    )
    args = parser.parse_args()

    if args.create_index:
        create_signature_index(args.db_path)

    if args.article_id:
        signature = generate_article_signature(args.article_id, args.db_path)
        print(json.dumps(signature, indent=2))

        if args.find_matches:
            matches = find_candidate_matches(args.article_id, db_path=args.db_path)
            print(f"\nFound {len(matches)} potential matches:")
            for match in matches:
                print(
                    f"Article {match['article_id']}: {match['similarity']['composite_score']:.2f} similarity"
                )
                for metric, score in match["similarity"].items():
                    if metric != "composite_score":
                        print(f"  - {metric}: {score:.2f}")
