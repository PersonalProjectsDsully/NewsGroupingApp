import sqlite3
import requests
from bs4 import BeautifulSoup
import feedparser
import time
import sys
from typing import Optional, Dict, Any, List
import logging  # Import logging


class KrebsScraper:
    def __init__(
        self,
        db_name: str = "db/news.db",
        feed_url: str = "https://krebsonsecurity.com/feed/",
    ):
        self.db_name = db_name
        self.feed_url = feed_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                )
            }
        )
        self.setup_database()
        self.logger = logging.getLogger(__name__)  # Add logger

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
                        content TEXT NOT NULL,
                        source TEXT NOT NULL,
                        processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization error: {e}")
            sys.exit(1)

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
            return False  # Assume not a duplicate on error

    def fetch_krebs_feed_entries(self) -> List[Dict[str, Any]]:
        try:
            feed = feedparser.parse(self.feed_url)
            entries = []
            for entry in feed.entries:
                link = getattr(entry, "link", None)
                title = getattr(entry, "title", "No Title")
                published = getattr(entry, "published", None)
                if link:
                    entries.append(
                        {"link": link, "title": title, "published_date": published}
                    )
            return entries
        except Exception as e:
            self.logger.error(f"Error fetching feed entries: {e}")
            return []

    def scrape_article(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            content_div = soup.find("div", class_="entry-content")
            if not content_div:
                return None

            paragraphs = content_div.find_all("p")
            article_text = "\n".join(
                p.get_text().strip() for p in paragraphs if p.get_text().strip()
            )
            return article_text if article_text else None
        except requests.RequestException as e:
            self.logger.error(f"Request error while scraping {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing {url}: {e}")
            return None

    def process_krebs_articles(self, limit: int = 100):
        feed_entries = self.fetch_krebs_feed_entries()
        if not feed_entries:
            self.logger.info("No feed entries found.")
            return

        processed_count = 0
        for entry in feed_entries:
            if processed_count >= limit:
                break

            if self.is_duplicate(entry["link"]):
                self.logger.info(f"Skipping duplicate article (link): {entry['link']}")
                continue

            self.logger.info(f"Processing article: {entry['title']}")
            content = self.scrape_article(entry["link"])
            if not content:
                self.logger.warning(f"Failed to scrape content for {entry['link']}\n")
                continue

            try:
                with sqlite3.connect(self.db_name) as conn:
                    c = conn.cursor()
                    c.execute(
                        """
                        INSERT OR REPLACE INTO articles (link, title, published_date, content, source)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            entry["link"],
                            entry["title"],
                            entry["published_date"],
                            content,
                            "krebs",
                        ),
                    )
                    conn.commit()

                self.logger.info(f"Stored article: {entry['title']}")
                processed_count += 1
                time.sleep(2)  # Add a small delay

            except sqlite3.Error as db_error:
                self.logger.error(f"Database error while storing article: {db_error}")
                continue


def main():
    scraper = KrebsScraper(
        db_name="db/news.db", feed_url="https://krebsonsecurity.com/feed/"
    )
    scraper.process_krebs_articles(limit=100)


if __name__ == "__main__":
    main()
