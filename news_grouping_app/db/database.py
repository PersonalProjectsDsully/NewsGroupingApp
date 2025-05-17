# db/database.py - UPDATED with optional cursor passing for write operations

import sqlite3
import time
from datetime import datetime
from pathlib import Path
import logging  # Import logging

logger = logging.getLogger(__name__)  # Add logger for potential errors


# Determine the project root so the database path is consistent across modules.
# Using `parents[2]` gets the repository root when this file lives at
# `news_grouping_app/db/database.py`.
BASE_DIR = Path(__file__).resolve().parents[2]
# Place the database under the project "db" folder at the repository root.
# Many modules reference "db/news.db" directly, so compute the same path here
# to avoid mismatches when running inside a container or locally.
DEFAULT_DB_PATH = BASE_DIR / "db" / "news.db"


def get_connection(db_path=DEFAULT_DB_PATH):
    """
    Returns a new connection to the SQLite database.
    Includes a short timeout which might help occasionally with locking,
    though cursor passing is the primary solution.
    """
    # Consider increasing timeout if lock errors persist despite cursor passing
    return sqlite3.connect(str(db_path), timeout=10.0)


def setup_database(db_path=DEFAULT_DB_PATH):
    """
    Create all necessary tables with integer-based article IDs.
    Call this once at startup or whenever you need to ensure the schema exists.
    Includes ON DELETE CASCADE for relevant foreign keys.
    """
    conn = None  # Initialize conn
    # Ensure the database directory exists to avoid "unable to open database file" errors
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        logger.info(f"Setting up database schema in '{db_path}'...")

        # Articles Table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link TEXT,
            title TEXT,
            content TEXT,
            published_date TIMESTAMP, /* Stored as TEXT 'YYYY-MM-DD HH:MM:SS' */
            source TEXT,
            processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP /* Stored as TEXT 'YYYY-MM-DD HH:MM:SS' */
        )
        """
        )
        logger.debug("Table 'articles' checked/created.")

        # Two-phase grouping tables
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS two_phase_article_groups (
            group_id INTEGER PRIMARY KEY AUTOINCREMENT,
            main_topic TEXT NOT NULL,
            sub_topic TEXT NOT NULL,
            group_label TEXT NOT NULL,
            description TEXT, /* Ensure this exists */
            consistency_score FLOAT DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )
        logger.debug("Table 'two_phase_article_groups' checked/created.")

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS two_phase_article_group_memberships (
            article_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE,
            FOREIGN KEY (group_id) REFERENCES two_phase_article_groups (group_id) ON DELETE CASCADE,
            PRIMARY KEY (article_id, group_id)
        )
        """
        )
        logger.debug("Table 'two_phase_article_group_memberships' checked/created.")

        # Subgroup tables (Keep if still used, otherwise remove)
        # If removing, ensure app.py doesn't reference them anymore.
        # For now, assume they might be used by older logic or future features.
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS two_phase_subgroups (
            subgroup_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            group_label TEXT NOT NULL,
            summary TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )
        logger.debug("Table 'two_phase_subgroups' checked/created.")

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS two_phase_subgroup_memberships (
            article_id INTEGER NOT NULL,
            subgroup_id INTEGER NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE,
            FOREIGN KEY (subgroup_id) REFERENCES two_phase_subgroups (subgroup_id) ON DELETE CASCADE,
            PRIMARY KEY (article_id, subgroup_id)
        )
        """
        )
        logger.debug("Table 'two_phase_subgroup_memberships' checked/created.")

        # Company references
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS article_companies (
            article_id INTEGER NOT NULL,
            company_name TEXT NOT NULL,
            PRIMARY KEY(article_id, company_name),
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE
        )
        """
        )
        logger.debug("Table 'article_companies' checked/created.")

        # CVE references + CVE info
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS article_cves (
            article_id INTEGER NOT NULL,
            cve_id TEXT NOT NULL,
            published_date TIMESTAMP,
            PRIMARY KEY (article_id, cve_id),
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE
        )
        """
        )
        logger.debug("Table 'article_cves' checked/created.")

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS cve_info (
            cve_id TEXT PRIMARY KEY,
            base_score REAL,
            vendor TEXT,
            affected_products TEXT,
            cve_url TEXT,
            vendor_link TEXT,
            solution TEXT,
            times_mentioned INTEGER DEFAULT 0,
            raw_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )
        logger.debug("Table 'cve_info' checked/created.")

        # Trending group tables
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
        logger.debug("Table 'trending_groups' checked/created.")

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS trending_group_memberships (
            article_id INTEGER NOT NULL,
            trend_id INTEGER NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE,
            FOREIGN KEY (trend_id) REFERENCES trending_groups (trend_id) ON DELETE CASCADE,
            PRIMARY KEY (article_id, trend_id)
        )
        """
        )
        logger.debug("Table 'trending_group_memberships' checked/created.")

        # Entity profiles and relationships
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS entity_profiles (
            entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_name TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            description TEXT,
            first_seen TIMESTAMP,
            last_seen TIMESTAMP,
            mention_count INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(entity_name, entity_type)
        )
        """
        )
        logger.debug("Table 'entity_profiles' checked/created.")

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS article_entities (
            article_id INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            relevance_score REAL DEFAULT 1.0,
            context_snippet TEXT,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE,
            FOREIGN KEY (entity_id) REFERENCES entity_profiles (entity_id) ON DELETE CASCADE,
            PRIMARY KEY (article_id, entity_id)
        )
        """
        )
        logger.debug("Table 'article_entities' checked/created.")

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS group_entities (
            group_id INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            relevance_score REAL DEFAULT 1.0,
            FOREIGN KEY (group_id) REFERENCES two_phase_article_groups (group_id) ON DELETE CASCADE,
            FOREIGN KEY (entity_id) REFERENCES entity_profiles (entity_id) ON DELETE CASCADE,
            PRIMARY KEY (group_id, entity_id)
        )
        """
        )
        logger.debug("Table 'group_entities' checked/created.")

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS trend_entities (
            trend_id INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            relevance_score REAL DEFAULT 1.0,
            FOREIGN KEY (trend_id) REFERENCES trending_groups (trend_id) ON DELETE CASCADE,
            FOREIGN KEY (entity_id) REFERENCES entity_profiles (entity_id) ON DELETE CASCADE,
            PRIMARY KEY (trend_id, entity_id)
        )
        """
        )
        logger.debug("Table 'trend_entities' checked/created.")

        # Exemplar storage
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS exemplar_groups (
            exemplar_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            pattern_name TEXT NOT NULL,
            pattern_description TEXT,
            success_score REAL DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )
        logger.debug("Table 'exemplar_groups' checked/created.")

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS exemplar_articles (
            exemplar_id INTEGER NOT NULL,
            article_id INTEGER NOT NULL,
            FOREIGN KEY (exemplar_id) REFERENCES exemplar_groups (exemplar_id) ON DELETE CASCADE,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE,
            PRIMARY KEY (exemplar_id, article_id)
        )
        """
        )
        logger.debug("Table 'exemplar_articles' checked/created.")

        # --- New Tables (ensure these exist with correct schema and CASCADE) ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS article_external_references (
                article_id INTEGER,
                original_url TEXT,
                normalized_url TEXT,
                domain TEXT,
                reference_type TEXT, /* Added type */
                PRIMARY KEY (article_id, normalized_url),
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE
            )
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_references_domain ON article_external_references(domain)
        """
        )
        logger.debug("Table 'article_external_references' checked/created.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS named_events (
                event_id INTEGER PRIMARY KEY,
                event_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                cve_ids TEXT, /* Storing as comma-separated TEXT or JSON */
                first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(event_name, event_type)
            )
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_type ON named_events(event_type)
        """
        )
        logger.debug("Table 'named_events' checked/created.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS article_events (
                article_id INTEGER,
                event_id INTEGER,
                context_snippet TEXT,
                PRIMARY KEY (article_id, event_id),
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                FOREIGN KEY (event_id) REFERENCES named_events(event_id) ON DELETE CASCADE
            )
        """
        )
        logger.debug("Table 'article_events' checked/created.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS quotes (
                quote_id INTEGER PRIMARY KEY,
                quote_text TEXT NOT NULL,
                quote_hash TEXT NOT NULL,
                speaker TEXT,
                first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(quote_hash)
            )
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_quotes_speaker ON quotes(speaker)
        """
        )
        logger.debug("Table 'quotes' checked/created.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS article_quotes (
                article_id INTEGER,
                quote_id INTEGER,
                context_before TEXT,
                context_after TEXT,
                PRIMARY KEY (article_id, quote_id),
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                FOREIGN KEY (quote_id) REFERENCES quotes(quote_id) ON DELETE CASCADE
            )
        """
        )
        logger.debug("Table 'article_quotes' checked/created.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS article_authors (
                article_id INTEGER,
                author_name TEXT,
                PRIMARY KEY (article_id, author_name), /* Assuming one author per article for PK */
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE
            )
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_authors_name ON article_authors(author_name)
        """
        )
        logger.debug("Table 'article_authors' checked/created.")

        conn.commit()
        logger.info("Database schema setup: Commit successful.")

    except sqlite3.Error as e:
        logger.exception(f"Database setup failed during table creation: {e}")
        if conn:
            conn.rollback()  # Rollback changes on error
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database setup: Connection closed.")


# --- Modified Write Functions (Accepting Optional Cursor) ---


def _execute_write(sql, params, db_path, cursor=None):
    """Internal helper to execute a write query, managing connection if needed."""
    conn_managed_here = False
    conn = None
    if cursor is None:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        conn_managed_here = True
    try:
        cursor.execute(sql, params)
        last_row_id = cursor.lastrowid  # Get lastrowid *before* commit
        if conn_managed_here and conn:
            conn.commit()
        return last_row_id  # Return lastrowid for INSERTs
    except sqlite3.Error as e:
        # Check for lock errors specifically
        if "locked" in str(e).lower():
            logger.error(
                f"DATABASE LOCKED executing query. SQL: {sql[:100]}... Params: {params}"
            )
        else:
            logger.error(
                f"Database error executing query: {e}. SQL: {sql[:100]}... Params: {params}",
                exc_info=True,
            )
        if conn_managed_here and conn:
            conn.rollback()
        raise  # Re-raise to signal failure to the caller
    finally:
        if conn_managed_here and conn:
            conn.close()


def insert_entity(
    entity_name, entity_type, description=None, db_path=DEFAULT_DB_PATH, cursor=None
):
    """Insert or update an entity. Uses provided cursor if available. Returns entity_id."""
    conn_managed_here = False
    conn = None
    if cursor is None:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        conn_managed_here = True

    entity_id = None
    try:
        # Check if entity exists
        cursor.execute(
            "SELECT entity_id FROM entity_profiles WHERE entity_name = ? AND entity_type = ?",
            (entity_name, entity_type),
        )
        entity = cursor.fetchone()

        if entity:
            entity_id = entity[0]
            # Update existing - Use CURRENT_TIMESTAMP for SQLite internal handling
            cursor.execute(
                """
                UPDATE entity_profiles
                SET mention_count = mention_count + 1,
                    last_seen = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP,
                    description = COALESCE(?, description)
                WHERE entity_id = ?
            """,
                (description, entity_id),
            )
        else:
            # Insert new - Use CURRENT_TIMESTAMP
            cursor.execute(
                """
                INSERT INTO entity_profiles
                (entity_name, entity_type, description, first_seen, last_seen, mention_count)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
            """,
                (entity_name, entity_type, description),
            )
            entity_id = cursor.lastrowid

        if conn_managed_here and conn:
            conn.commit()
        return entity_id

    except sqlite3.Error as e:
        logger.error(
            f"Error in insert_entity for '{entity_name}': {e}", exc_info=False
        )  # Less verbose logging maybe
        if conn_managed_here and conn:
            conn.rollback()
        return None  # Return None on failure instead of raising? Depends on caller needs. Let's return None.
    finally:
        if conn_managed_here and conn:
            conn.close()


def link_entity_to_article(
    article_id,
    entity_id,
    relevance_score=1.0,
    context_snippet=None,
    db_path=DEFAULT_DB_PATH,
    cursor=None,
):
    """Link article and entity. Returns True on success, False on failure."""
    sql = "INSERT OR REPLACE INTO article_entities (article_id, entity_id, relevance_score, context_snippet) VALUES (?, ?, ?, ?)"
    params = (article_id, entity_id, relevance_score, context_snippet)
    try:
        _execute_write(sql, params, db_path, cursor)
        return True
    except Exception:  # Catch exceptions raised by _execute_write
        return False


def link_entity_to_group(
    group_id, entity_id, relevance_score=1.0, db_path=DEFAULT_DB_PATH, cursor=None
):
    """Link group and entity. Returns True on success, False on failure."""
    sql = "INSERT OR REPLACE INTO group_entities (group_id, entity_id, relevance_score) VALUES (?, ?, ?)"
    params = (group_id, entity_id, relevance_score)
    try:
        _execute_write(sql, params, db_path, cursor)
        return True
    except Exception:
        return False


def link_entity_to_trend(
    trend_id, entity_id, relevance_score=1.0, db_path=DEFAULT_DB_PATH, cursor=None
):
    """Link trend and entity. Returns True on success, False on failure."""
    sql = "INSERT OR REPLACE INTO trend_entities (trend_id, entity_id, relevance_score) VALUES (?, ?, ?)"
    params = (trend_id, entity_id, relevance_score)
    try:
        _execute_write(sql, params, db_path, cursor)
        return True
    except Exception:
        return False


# --- Exemplar Functions (Modified) ---
def create_exemplar(
    category,
    pattern_name,
    pattern_description=None,
    success_score=1.0,
    db_path=DEFAULT_DB_PATH,
    cursor=None,
):
    """Create a new exemplar group. Returns exemplar_id or None."""
    sql = "INSERT INTO exemplar_groups (category, pattern_name, pattern_description, success_score) VALUES (?, ?, ?, ?)"
    params = (category, pattern_name, pattern_description, success_score)
    try:
        return _execute_write(sql, params, db_path, cursor)  # Returns lastrowid
    except Exception:
        return None


def add_article_to_exemplar(exemplar_id, article_id, db_path=DEFAULT_DB_PATH, cursor=None):
    """Add an article to an exemplar group. Returns True on success, False on failure."""
    sql = "INSERT OR IGNORE INTO exemplar_articles (exemplar_id, article_id) VALUES (?, ?)"
    params = (exemplar_id, article_id)
    try:
        _execute_write(sql, params, db_path, cursor)
        return True
    except Exception:
        return False


# --- CVE Functions (Modified) ---
def insert_article_cve(
    article_id, cve_id, published_date, db_path=DEFAULT_DB_PATH, cursor=None
):
    """Insert or ignore an article_cve record. Returns True on success, False on failure."""
    # Ensure date is in correct format if needed, though migration should handle it
    # published_date_str = pd.to_datetime(published_date).strftime('%Y-%m-%d %H:%M:%S') if published_date else None
    sql = "INSERT OR IGNORE INTO article_cves (article_id, cve_id, published_date) VALUES (?, ?, ?)"
    params = (article_id, cve_id, published_date)  # Pass date as received
    try:
        _execute_write(sql, params, db_path, cursor)
        return True
    except Exception:
        return False


def insert_or_update_cve_info(
    cve_id,
    base_score,
    vendor,
    affected_products,
    cve_url,
    vendor_link,
    solution,
    times_mentioned,
    raw_json_str,
    db_path=DEFAULT_DB_PATH,
    cursor=None,
):
    """Insert or update cve_info. Returns True on success, False on failure."""
    sql = """
        INSERT INTO cve_info (
            cve_id, base_score, vendor, affected_products, cve_url,
            vendor_link, solution, times_mentioned, raw_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(cve_id) DO UPDATE SET
            base_score=excluded.base_score, vendor=excluded.vendor,
            affected_products=excluded.affected_products, cve_url=excluded.cve_url,
            vendor_link=excluded.vendor_link, solution=excluded.solution,
            times_mentioned=excluded.times_mentioned, raw_json=excluded.raw_json,
            updated_at=CURRENT_TIMESTAMP
    """
    params = (
        cve_id,
        base_score,
        vendor,
        affected_products,
        cve_url,
        vendor_link,
        solution,
        times_mentioned,
        raw_json_str,
    )
    try:
        _execute_write(sql, params, db_path, cursor)
        return True
    except Exception:
        return False
