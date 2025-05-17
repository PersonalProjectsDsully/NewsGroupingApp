from pathlib import Path
import sqlite3
import requests
from news_grouping_app.user_agents import RotatingUserAgentSession
from bs4 import BeautifulSoup
import feedparser
import time
import sys
from typing import Optional, Dict, Any, List
import logging  # Import logging
from news_grouping_app.db.database import DEFAULT_DB_PATH


class TechRadarScraper:
    def __init__(self, db_name: str = str(DEFAULT_DB_PATH), feed_urls: List[str] = None):
        if feed_urls is None:
            self.feed_urls = [
                "https://www.techradar.com/feeds/tag/software",
                "https://www.techradar.com/feeds/tag/computing",
                "https://www.techradar.com/feeds/articletype/news",
            ]
        else:
            self.feed_urls = feed_urls

        self.db_name = db_name
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
        all_entries = []
        seen_links = set()  # Keep track of links to avoid duplicates across feeds
        for feed_url in self.feed_urls:
            try:
                self.logger.info(f"Fetching feed: {feed_url}")  # Log which feed
                response = self.session.get(feed_url, timeout=10)
                response.raise_for_status()
                feed = feedparser.parse(response.text)  # Parse the text, not raw bytes
                for entry in feed.entries:
                    if entry.link not in seen_links:
                        seen_links.add(entry.link)
                        # Use .get() to avoid errors if a field is missing
                        content = ""
                        if hasattr(entry, "dc_content"):
                            content = entry.dc_content
                        elif hasattr(entry, "content"):
                            content = entry.content[0].value if entry.content else ""
                        elif hasattr(entry, "description"):  # backup incase
                            content = entry.description

                        all_entries.append(
                            {
                                "link": entry.link,
                                "title": entry.title,
                                "published_date": getattr(
                                    entry, "published", None
                                ),  # Use get
                                "content": self.clean_html_content(
                                    content
                                ),  # Clean HTML
                            }
                        )
            except Exception as e:
                self.logger.error(f"Error fetching feed {feed_url}: {e}")
        return all_entries

    def clean_html_content(self, html_content: str) -> str:
        """Remove HTML tags and extra whitespace from content."""
        if not html_content:
            return ""
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator=" ")  # Get text with spaces between elements
        return " ".join(text.split())  # Remove extra whitespace

    def scrape_article(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            article_body = soup.find("div", {"id": "article-body"})
            if not article_body:
                return None

            # Remove "You might also like" and related content
            you_might_like = article_body.find(
                "h3", string=lambda x: x and "You might also like" in x
            )
            if you_might_like:
                current = you_might_like
                while current:
                    next_el = current.next_sibling
                    current.decompose()
                    current = next_el

            # Remove other elements.
            for unwanted in article_body.find_all(
                ["div"], class_=["hawk-widget-insert", "see-more", "van_vid_carousel"]
            ):
                unwanted.decompose()

            # Select relevant elements (paragraphs and headers)
            content_elements = article_body.find_all(["p", "h2", "h3"])
            cleaned_paragraphs = []
            for element in content_elements:
                cleaned_text = self.clean_html_content(str(element))  # clean content
                if cleaned_text.strip():
                    cleaned_paragraphs.append(cleaned_text)
            return "\n\n".join(cleaned_paragraphs)
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
            return False  # Assume not a duplicate on database error

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
            content = entry["content"] or self.scrape_article(entry["link"])
            if not content:
                self.logger.warning(f"Failed to get content for {entry['link']}\n")
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
                            "techradar",
                        ),
                    )
                    conn.commit()
                self.logger.info(f"Stored article: {entry['title']}")
                time.sleep(2)
            except sqlite3.Error as db_error:
                self.logger.error(f"Database error while storing article: {db_error}")
                continue


def main():
    feed_urls = [
        "https://www.techradar.com/feeds/tag/software",
        "https://www.techradar.com/feeds/tag/computing",
        "https://www.techradar.com/feeds/articletype/news",
    ]
    scraper = TechRadarScraper(feed_urls=feed_urls)
    scraper.process_articles(limit=100)


if __name__ == "__main__":
    main()
