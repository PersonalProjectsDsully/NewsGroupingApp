from pathlib import Path
import sqlite3
import requests
from news_grouping_app.user_agents import RotatingUserAgentSession
from bs4 import BeautifulSoup
import feedparser
import time
import sys
from typing import Optional, Dict, Any, List
import logging


class SophosNewsScraper:
    def __init__(
        self,
        db_name: str = str(Path(__file__).resolve().parents[1] / "db" / "news.db"),
        feed_url: str = "https://news.sophos.com/en-us/feed/",
    ):
        self.db_name = db_name
        self.feed_url = feed_url
        self.session = RotatingUserAgentSession()
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

    def fetch_feed_entries(self) -> List[Dict[str, Any]]:
        try:
            feed = feedparser.parse(self.feed_url)
            entries = []
            for entry in feed.entries:
                entries.append(
                    {
                        "link": entry.link,
                        "title": entry.title,
                        "published_date": getattr(
                            entry, "published", entry.get("pubdate")
                        ),
                    }
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

            article_body = soup.find(
                "div", class_="entry-content lg:prose-lg mx-auto prose max-w-4xl"
            )
            if not article_body:
                return None

            paragraphs = article_body.find_all("p")
            article_text = "\n\n".join(
                p.get_text().strip() for p in paragraphs if p.get_text().strip()
            )
            return article_text if article_text else None
        except requests.RequestException as e:
            self.logger.error(f"Request error while scraping {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing {url}: {e}")
            return None

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

    def process_articles(self, limit: int = 100):
        feed_entries = self.fetch_feed_entries()
        if not feed_entries:
            self.logger.info("No feed entries found.")
            return

        new_entries = []
        for entry in feed_entries:
            if not self.is_duplicate(entry["link"]):  # Simplified duplicate check
                new_entries.append(entry)
            if len(new_entries) >= limit:
                break

        if not new_entries:
            self.logger.info("No new articles to process.")
            return

        for entry in new_entries:

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
                            "sophos",  # Consistent source name
                        ),
                    )
                    conn.commit()
                self.logger.info(f"Stored article: {entry['title']}")

                time.sleep(2)
            except sqlite3.Error as db_error:
                self.logger.error(f"Database error while storing article: {db_error}")
                continue


def main():
    scraper = SophosNewsScraper()
    scraper.process_articles(limit=100)


if __name__ == "__main__":
    main()
