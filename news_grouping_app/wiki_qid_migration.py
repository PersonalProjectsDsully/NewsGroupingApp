"""
Migration script to add external identifier fields to the entity_profiles table.

Adds two new columns:
  - wiki_qid: external Wikidata QID or Wikipedia page ID
  - aliases: JSON encoded list of alternative names

Also creates a unique index on wiki_qid to prevent duplicate entries.
Run this once when upgrading an existing database.
"""
from pathlib import Path
import sqlite3
from news_grouping_app.db.database import DEFAULT_DB_PATH

def column_exists(cur: sqlite3.Cursor, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())

def index_exists(cur: sqlite3.Cursor, table: str, index_name: str) -> bool:
    cur.execute(f"PRAGMA index_list({table})")
    return any(row[1] == index_name for row in cur.fetchall())

def main(db_path: Path = DEFAULT_DB_PATH) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    if not column_exists(cur, "entity_profiles", "wiki_qid"):
        cur.execute("ALTER TABLE entity_profiles ADD COLUMN wiki_qid TEXT")
    if not column_exists(cur, "entity_profiles", "aliases"):
        cur.execute("ALTER TABLE entity_profiles ADD COLUMN aliases TEXT")
    if not index_exists(cur, "entity_profiles", "idx_entity_profiles_wiki_qid"):
        cur.execute(
            "CREATE UNIQUE INDEX idx_entity_profiles_wiki_qid ON entity_profiles(wiki_qid)"
        )
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
