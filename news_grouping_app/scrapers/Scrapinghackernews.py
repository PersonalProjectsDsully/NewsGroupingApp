from pathlib import Path
#!/usr/bin/env python3
import argparse
import sqlite3
import requests
from news_grouping_app.user_agents import RotatingUserAgentSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import logging
import sys
import time
import xml.etree.ElementTree as ET
import re
from email.utils import parsedate_to_datetime
from typing import Optional, Dict, Any, List
from news_grouping_app.db.database import DEFAULT_DB_PATH


class THNScraper:
    def __init__(
        self,
        db_name: str,
        feed_url: str,
        batch_size: int,
        rate_limit: float,
        log_level: str,
    ):
        self.db_name = db_name
        self.feed_url = feed_url
        self.batch_size = batch_size
        self.rate_limit = rate_limit
        self.logger = self.setup_logging(log_level)
        self.setup_database()
        self.session = self.setup_http_session()

    def setup_logging(self, log_level: str):
        logger = logging.getLogger("THNScraper")  # Use a logger name
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler = logging.FileHandler("thn_scraper_no_desc.log")  # Log to a file
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler(sys.stdout)  # Log to console
        console_handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger

    def setup_database(self):
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.conn.row_factory = sqlite3.Row  # Use Row factory for easier access
            self.cursor = self.conn.cursor()
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    link TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    published_date TEXT,
                    content TEXT,
                    source TEXT NOT NULL,
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            self.conn.commit()
            self.logger.info("Database initialized successfully.")
        except sqlite3.Error as e:  # Catch and log the exception
            self.logger.exception("Database initialization error")
            sys.exit(1)  # Exit with a non-zero code to indicate failure

    def setup_http_session(self) -> requests.Session:
        session = RotatingUserAgentSession()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],  # Removed "POST" (not needed)
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def parse_rss_feed(self) -> List[Dict[str, Any]]:
        try:
            response = self.session.get(self.feed_url, timeout=10)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            root = ET.fromstring(response.content)
            entries = []
            for item in root.findall(".//item"):
                pub_date = None
                pub_date_elem = item.find("pubDate")
                if pub_date_elem is not None and pub_date_elem.text:
                    try:
                        dt = parsedate_to_datetime(pub_date_elem.text)
                        pub_date = dt.isoformat()
                    except Exception:
                        self.logger.warning(
                            f"Error parsing date {pub_date_elem.text}", exc_info=True
                        )

                entry = {
                    "title": (
                        item.find("title").text
                        if item.find("title") is not None
                        else None
                    ),
                    "link": (
                        item.find("link").text
                        if item.find("link") is not None
                        else None
                    ),
                    "published_date": pub_date,
                    # 'description': item.find('description').text  # Removed description
                }
                entries.append(entry)
            return entries
        except requests.RequestException as e:
            self.logger.exception("Error fetching RSS feed")
            return []
        except ET.ParseError as e:
            self.logger.exception("Error parsing RSS XML")
            return []

    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()  # Raise HTTPError for bad responses
            soup = BeautifulSoup(response.content, "html.parser")

            article_div = soup.find(
                "div", {"class": "articlebody", "id": "articlebody"}
            )
            if not article_div:
                self.logger.warning(f"Article content not found for URL: {url}")
                return None

            # Remove unwanted elements
            elements_to_remove = [
                ("div", {"class": ["dog_two", "note-b", "stophere"]}),
                ("div", {"id": ["hiddenH1"]}),
                ("center", {}),
                ("div", {"class": "separator"}),
            ]
            for tag, attrs in elements_to_remove:
                for element in article_div.find_all(tag, attrs=attrs):
                    element.decompose()

            paragraphs = [
                p.get_text().strip()
                for p in article_div.find_all("p")
                if p.get_text().strip()
            ]
            return {"content": "\n\n".join(paragraphs)}

        except requests.RequestException as e:
            self.logger.exception(f"Error fetching article URL: {url}")
            return None

        except Exception as e:
            self.logger.exception(f"Error processing article URL: {url}")
            return None

    def remove_emojis(self, text: Optional[str]) -> str:
        if not text:
            return ""
        emoji_pattern = re.compile(
            "["
            "\U0001f600-\U0001f64f"  # emoticons
            "\U0001f300-\U0001f5ff"  # symbols & pictographs
            "\U0001f680-\U0001f6ff"  # transport & map symbols
            "\U0001f1e0-\U0001f1ff"  # flags (iOS)
            "\U00002702-\U000027b0"
            "\U000024c2-\U0001f251"
            "]+",
            flags=re.UNICODE,
        )
        return emoji_pattern.sub(r"", text)

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

    def insert_article(
        self,
        link: str,
        title: str,
        published_date: Optional[str],
        content: str,
        source: str = "TheHackerNews",
    ):
        try:
            self.cursor.execute(
                """
                        INSERT OR REPLACE INTO articles (link, title, published_date, content, source)
                        VALUES (?, ?, ?, ?, ?)
            """,
                (link, title, published_date, content, source),
            )
            self.conn.commit()
            self.logger.info(f"Successfully inserted article: {title}")

        except sqlite3.Error as e:
            self.logger.exception(f"Database insertion error for article: {title}")

    def process_all_articles(self):
        entries = self.parse_rss_feed()
        if not entries:
            self.logger.info("No entries found in feed")
            return

        self.logger.info(f"Found {len(entries)} entries in feed")

        for i in range(0, len(entries), self.batch_size):
            batch = entries[i : i + self.batch_size]
            for entry in batch:
                if not entry.get("link"):
                    continue

                title = self.remove_emojis(entry.get("title"))
                self.logger.info(f"Processing article: {title}")

                if self.is_duplicate(entry["link"]):  # Simplified duplicate check
                    self.logger.info(
                        f"Skipping duplicate article (link): {entry['link']}"
                    )
                    continue

                article_data = self.scrape_article(entry["link"])

                if article_data:
                    content = self.remove_emojis(article_data.get("content", ""))
                    pub_date = entry.get("published_date")
                    self.insert_article(
                        entry["link"], title, pub_date, content
                    )  # Pass to insert function
                else:
                    self.logger.warning(f"Failed to scrape article: {entry['link']}")

                time.sleep(self.rate_limit)

    def close(self):
        try:
            self.conn.close()
        except Exception as e:
            self.logger.exception("Error closing database connection")
        self.session.close()


def main():
    parser = argparse.ArgumentParser(description="The Hacker News Scraper")
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB_PATH), help="SQLite database file name"
    )
    parser.add_argument(
        "--feed_url", type=str, default="https://feeds.feedburner.com/TheHackersNews"
    )
    parser.add_argument("--batch_size", type=int, default=5)  # Process in batches of 5
    parser.add_argument("--rate_limit", type=float, default=2.0)  # 2-second delay
    parser.add_argument("--log_level", type=str, default="INFO")  # Default log level
    args = parser.parse_args()

    scraper = THNScraper(
        db_name=args.db,
        feed_url=args.feed_url,
        batch_size=args.batch_size,
        rate_limit=args.rate_limit,
        log_level=args.log_level,
    )
    try:
        scraper.process_all_articles()
    except KeyboardInterrupt:
        scraper.logger.info("Processing interrupted by user.")
    except Exception as e:
        scraper.logger.exception("An unexpected error occurred.")
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
