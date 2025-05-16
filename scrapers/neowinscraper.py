import sqlite3
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Optional, List, Dict, Any
import logging


class NeowinScraper:
    def __init__(self, db_name: str = "db/news.db", site_config: Dict[str, Any] = None):
        self.db_name = db_name
        self.site_config = site_config or {}
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
        self.logger = logging.getLogger(__name__)

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
            return False  # Assume not duplicate on error

    def remove_emojis(self, text: str) -> str:
        emoji_pattern = re.compile(
            "["
            "\U0001f600-\U0001f64f"
            "\U0001f300-\U0001f5ff"
            "\U0001f680-\U0001f6ff"
            "\U0001f1e0-\U0001f1ff"
            "\U00002702-\U000027b0"
            "\U000024c2-\U0001f251"
            "]+",
            flags=re.UNICODE,
        )
        return emoji_pattern.sub(r"", text)

    def parse_rss_feed(self, feed_content: str) -> List[Dict[str, Any]]:
        try:
            root = ET.fromstring(feed_content)
            channel = root.find("channel")
            if channel is None:
                return []

            articles = []
            for item in channel.findall("item"):
                title_elem = item.find("title")
                link_elem = item.find("link")
                pub_date_elem = item.find("pubDate")
                author_elem = item.find("author")

                title = title_elem.text if title_elem is not None else None
                link = link_elem.text if link_elem is not None else None
                pub_date = pub_date_elem.text if pub_date_elem is not None else None
                author = author_elem.text if author_elem is not None else None

                articles.append(
                    {
                        "link": link,
                        "title": title,
                        "published_date": pub_date,
                        "author": author,
                    }
                )
            return articles
        except (ET.ParseError, Exception) as e:
            self.logger.error(f"Error parsing RSS feed: {e}")
            return []

    def scrape_article(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Find the article content div
            article_div = soup.find("div", class_="article-content")
            if not article_div:
                return None

            # Extract paragraphs from the article
            paragraphs = article_div.find_all("p")

            # Filter out any unwanted elements
            for tag in article_div.find_all(["div"], class_=["ad"]):
                tag.decompose()

            # Extract the text from each paragraph
            article_text = "\n\n".join(
                p.get_text().strip() for p in paragraphs if p.get_text().strip()
            )

            # Extract headers if present
            headers_tags = article_div.find_all(["h1", "h2", "h3"])
            headers_text = "\n\n".join(
                h.get_text().strip() for h in headers_tags if h.get_text().strip()
            )

            # Combine headers and article text
            if headers_text:
                article_text = headers_text + "\n\n" + article_text

            return article_text

        except (requests.RequestException, Exception) as e:
            self.logger.error(f"Error scraping article {url}: {e}")
            return None

    def process_feed(self, feed_url: str, source_name: str):
        try:
            response = self.session.get(feed_url, timeout=10)
            response.raise_for_status()
            feed_content = response.text

            articles = self.parse_rss_feed(feed_content)
            seen_links = set()

            for article in articles:
                if not article.get("link"):
                    continue
                if article["link"] in seen_links:
                    continue
                seen_links.add(article["link"])

                if not all(k in article for k in ["link", "title", "published_date"]):
                    continue

                cleaned_title = self.remove_emojis(article["title"])
                content = self.scrape_article(article["link"])

                if not content:
                    self.logger.warning(f"No content extracted for {article['link']}")
                    continue

                if self.is_duplicate(article["link"]):
                    self.logger.info(
                        f"Skipping duplicate article (link): {article['link']}"
                    )
                    continue

                pub_date = None
                if article["published_date"]:
                    try:
                        pub_date = parsedate_to_datetime(article["published_date"])
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to parse date {article['published_date']}: {e}"
                        )
                        pub_date = article["published_date"]

                try:
                    with sqlite3.connect(self.db_name) as conn:
                        c = conn.cursor()
                        c.execute(
                            """
                            INSERT OR REPLACE INTO articles (link, title, published_date, content, source)
                            VALUES (?, ?, ?, ?, ?)
                        """,
                            (
                                article["link"],
                                cleaned_title,
                                pub_date,
                                content,
                                source_name,
                            ),
                        )
                        conn.commit()

                    self.logger.info(f"Stored article: {article['link']}")

                    # Rate limit to avoid overwhelming the server
                    time.sleep(1)
                except sqlite3.Error as e:
                    self.logger.error(f"Database error storing article: {e}")
                    continue

        except (requests.RequestException, Exception) as e:
            self.logger.error(f"Error processing feed {feed_url}: {e}")


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("neowin_scraper.log"), logging.StreamHandler()],
    )

    site_config = {"neowin": {"feed_url": "https://www.neowin.net/news/rss/"}}

    scraper = NeowinScraper(site_config=site_config)
    feed_url = site_config["neowin"]["feed_url"]
    scraper.process_feed(feed_url, "neowin")


if __name__ == "__main__":
    main()
