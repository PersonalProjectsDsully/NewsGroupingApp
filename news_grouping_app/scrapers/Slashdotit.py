from pathlib import Path
import sqlite3
import requests
from bs4 import BeautifulSoup
import feedparser
import time
import sys
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging


class SlashdotITNewsScraper:
    def __init__(
        self,
        db_name: str = str(Path(__file__).resolve().parents[1] / "db" / "news.db"),
        feed_url: str = "https://rss.slashdot.org/Slashdot/slashdotit",
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

    def fetch_feed_entries(self) -> List[Dict[str, Any]]:
        try:
            feed = feedparser.parse(self.feed_url)
            entries = []
            for entry in feed.entries:
                # Use get to be safer, and handle dc:date for slashdot
                published = entry.get("published", entry.get("dc_date", None))
                entries.append(
                    {
                        "link": entry.link,
                        "title": entry.title,
                        "published_date": published,
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

            # Try finding the content in the 'body' div with class 'p'
            content_div = None
            body_div = soup.find("div", class_="body")
            if body_div:
                content_div = body_div.find("div", class_="p")
            if not content_div:
                # Fallback if the above structure isn't found
                content_div = soup.find("div", class_="p")

            if not content_div:
                print(f"Could not locate article content at {url}")
                return None

            # Now extract paragraphs, handling potential None cases
            paragraphs = content_div.find_all("p")
            article_text = "\n\n".join(
                p.get_text().strip() for p in paragraphs if p.get_text().strip()
            )

            if not article_text:
                article_text = content_div.get_text().strip()  # Use as a backup.

            return article_text if article_text else None  # Ensure text is returned.

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

    def process_articles(self, limit: int = 10):
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
                self.logger.warning(f"Failed to retrieve content for {entry['link']}\n")
                continue

            # Use current time if published_date is missing
            pub_date = entry["published_date"] or datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

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
                            pub_date,
                            content,
                            "slashdot_it",  # Use a consistent source name
                        ),
                    )
                    conn.commit()

                self.logger.info(f"Stored article: {entry['title']}")

                time.sleep(2)  # Add a small delay
            except sqlite3.Error as db_error:
                self.logger.error(f"Database error while storing article: {db_error}")
                continue


def main():
    scraper = SlashdotITNewsScraper()
    scraper.process_articles(limit=10)


if __name__ == "__main__":
    main()
