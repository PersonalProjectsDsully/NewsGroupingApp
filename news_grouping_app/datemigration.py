#!/usr/bin/env python3
"""
date_migration.py

Updates various date/time columns across multiple tables in `db/news.db`
so that each timestamp is stored in the 'YYYY-MM-DD HH:MM:SS' UTC format
that SQLite can parse easily with strftime().

Backup your database before running!
"""

import sqlite3
from datetime import timezone
from dateutil import parser

# List which tables & columns need date updates:
# (If you have additional tables/columns with dates, add them here)
TABLES_WITH_DATE_COLUMNS = {
    "articles": [
        "published_date",
        # If you also store 'processed_date', uncomment:
        # "processed_date",
    ],
    "article_cves": [
        "published_date",
    ],
    "cve_info": [
        "created_at",
        "updated_at",
    ],
    "entity_profiles": [
        "first_seen",
        "last_seen",
        "created_at",
        "updated_at",
    ],
    "two_phase_article_groups": [
        "created_at",
        "updated_at",
    ],
    "two_phase_article_group_memberships": [
        "added_at",
    ],
    "two_phase_subgroups": [
        "created_at",
        "updated_at",
    ],
    "two_phase_subgroup_memberships": [
        "added_at",
    ],
    "trending_groups": [
        "created_at",
        "updated_at",
    ],
    "trending_group_memberships": [
        "added_at",
    ],
    "exemplar_groups": [
        "created_at",
    ],
    # If you have other tables storing timestamps, add them too
}


def convert_to_utc_sqlite_format(date_str):
    """
    Parses a date string via dateutil, converts to UTC,
    and returns 'YYYY-MM-DD HH:MM:SS'.
    """
    dt = parser.parse(date_str)
    # If no timezone info, assume UTC:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Convert to UTC just in case:
    dt_utc = dt.astimezone(timezone.utc)
    # Format as 'YYYY-MM-DD HH:MM:SS' (which SQLite recognizes):
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S")


def main():
    db_path = "db/news.db"  # Adjust if needed
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for table_name, date_cols in TABLES_WITH_DATE_COLUMNS.items():
        for col_name in date_cols:
            print(f"\nProcessing {table_name}.{col_name} ...")
            # We'll select rowid and the date column so we can update row by row:
            select_sql = f"SELECT rowid, {col_name} FROM {table_name} WHERE {col_name} IS NOT NULL"
            try:
                rows = cur.execute(select_sql).fetchall()
            except sqlite3.Error as e:
                print(f"SKIP {table_name}.{col_name} (SQL error): {e}")
                continue

            for rowid, old_date_str in rows:
                if not old_date_str:
                    continue
                try:
                    new_date_str = convert_to_utc_sqlite_format(old_date_str)
                    # Update if it actually changed, to reduce churn:
                    if new_date_str != old_date_str:
                        update_sql = (
                            f"UPDATE {table_name} SET {col_name} = ? WHERE rowid = ?"
                        )
                        cur.execute(update_sql, (new_date_str, rowid))
                        print(
                            f"  {table_name} rowid={rowid}: '{old_date_str}' -> '{new_date_str}'"
                        )
                except Exception as parse_err:
                    print(
                        f"  WARNING: Could not parse '{old_date_str}' in {table_name}.{col_name}: {parse_err}"
                    )

            conn.commit()  # Commit after each column to keep changes so far

    conn.close()
    print("\nDone. Remember to verify with a SELECT or test queries.")


if __name__ == "__main__":
    main()
