from pathlib import Path
import sqlite3
import requests
from news_grouping_app.user_agents import RotatingUserAgentSession
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List
from news_grouping_app.db.database import DEFAULT_DB_PATH
import time
import xml.etree.ElementTree as ET
import logging


class CybersecurityScraper:
    def __init__(self, db_name: str = str(DEFAULT_DB_PATH), site_config: Dict[str, Any] = None):
        self.db_name = db_name
        self.site_config = site_config or {}
        self.session = RotatingUserAgentSession()
        self.setup_database()
        self.logger = logging.getLogger(__name__)  # add logger

    def setup_database(self):
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute(
                    """
                CREATE TABLE IF NOT EXISTS articles (
                    link TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    published_date TIMESTAMP,
                    content TEXT,
                    source TEXT NOT NULL,
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization error: {e}")
            raise

    def is_duplicate(self, link: str) -> bool:
        """Check if this link is already stored in the database."""
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("SELECT link FROM articles WHERE link = ?", (link,))
                row = c.fetchone()
                return bool(row)
        except sqlite3.Error as e:
            self.logger.error(f"Database error while checking duplicates: {e}")
            return False

    def parse_atom_feed(self, feed_content: str) -> List[Dict[str, Any]]:
        try:
            root = ET.fromstring(feed_content)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = []

            for entry in root.findall("atom:entry", ns):
                link_elem = entry.find('atom:link[@rel="alternate"]', ns)
                link = link_elem.get("href") if link_elem is not None else None

                title_elem = entry.find("atom:title", ns)
                title = title_elem.text if title_elem is not None else None

                published_elem = entry.find("atom:published", ns)
                published = published_elem.text if published_elem is not None else None

                # content extraction (now extracts plain text)
                content = ""
                content_elem = entry.find("atom:content", ns)
                if content_elem is not None:
                    if content_elem.get("type") == "html":
                        soup = BeautifulSoup(
                            content_elem.text or "", "html.parser"
                        )  # handle None case
                        # Directly get text, removing any additional divs.
                        content_parts = []
                        for tag in soup.find_all(
                            ["p", "blockquote"], recursive=False
                        ):  # check all available tags.
                            # Exclude specific classes like 'entry-tags' and 'posted'
                            if not any(
                                cls in (tag.get("class") or [])
                                for cls in ["entry-tags", "posted"]
                            ):  # class check
                                content_parts.append(tag.get_text().strip())
                        content = "\n".join(
                            filter(None, content_parts)
                        )  # filter out empty strings
                    else:
                        content = (
                            content_elem.text or ""
                        )  # Fallback to raw text if not HTML

                if link:  # Only add entries with a valid link
                    entries.append(
                        {
                            "link": link,
                            "title": title,
                            "published_date": published,
                            "content": content,
                        }
                    )
            return entries

        except (ET.ParseError, Exception) as e:
            self.logger.error(f"Error parsing ATOM feed: {e}")
            return []

    def process_feed(self, feed_url: str, source_name: str):
        try:
            response = self.session.get(feed_url, timeout=10)
            response.raise_for_status()
            articles = self.parse_atom_feed(response.text)

            seen_links = set()  # Keep track of seen links to avoid duplicates
            for article in articles:
                if article["link"] in seen_links:
                    continue  # skip duplicate
                seen_links.add(article["link"])
                # Check for all required values
                if not all(k in article for k in ["link", "title", "published_date"]):
                    continue

                if self.is_duplicate(article["link"]):  # simplified duplicate check
                    self.logger.info(
                        f"Skipping duplicate article (link): {article['link']}"
                    )
                    continue

                with sqlite3.connect(self.db_name) as conn:
                    c = conn.cursor()
                    c.execute(
                        """
                        INSERT OR REPLACE INTO articles (link, title, published_date, content, source)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            article["link"],
                            article["title"],
                            article["published_date"],
                            article["content"],
                            source_name,
                        ),
                    )
                    conn.commit()
                self.logger.info(f"Stored article: {article['link']}")

                time.sleep(1)  # Be nice to the server
        except requests.RequestException as e:
            self.logger.error(f"Error processing feed {feed_url}: {e}")
            pass


def main():
    site_configs = {"schneier": {"feed_url": "https://www.schneier.com/feed/atom/"}}
    scraper = CybersecurityScraper(site_config=site_configs)
    scraper.process_feed(site_configs["schneier"]["feed_url"], "schneier")


if __name__ == "__main__":
    main()
