import sqlite3
import requests
import feedparser
from bs4 import BeautifulSoup
import logging
from typing import Optional, Dict, Any, List
import time
import sys
import re

class SecurelistProcessor:
    def __init__(self, db_name: str = 'db/news.db'):
        self.db_name = db_name
        self.feed_url = "https://securelist.com/feed/"
        self.logger = self.setup_logging()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/91.0.4472.124 Safari/537.36')
        })
        self.setup_database()

    def setup_logging(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler('securelist_scraper.log')
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        return logger


    def setup_database(self):
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("""
                    CREATE TABLE IF NOT EXISTS articles (
                        link TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        published_date TIMESTAMP,
                        content TEXT NOT NULL,
                        source TEXT NOT NULL,
                        processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
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



    def fetch_feed(self) -> List[Dict]:
        try:
            self.logger.info(f"Fetching RSS feed from {self.feed_url}")
            response = self.session.get(self.feed_url, timeout=10)
            response.raise_for_status()  # Raise HTTPError for bad responses
            feed = feedparser.parse(response.text)

            if feed.bozo != 0:  # Check for feed parsing errors
                self.logger.error(f"Feed parsing error: {feed.bozo_exception}")
                return []

            articles = []
            for entry in feed.entries:
                # Use get() method with a default value to handle missing fields gracefully
                article = {
                    'title': entry.get('title', ''),
                    'link': entry.get('link', ''),
                    'date': entry.get('published', '') # Use published date directly
                }
                articles.append(article)
            self.logger.info(f"Found {len(articles)} articles in feed")
            return articles

        except requests.RequestException as e:
            self.logger.error(f"Error fetching feed: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return []


    def scrape_article(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            article_div = soup.find('div', class_='js-reading-content')
            if not article_div:
                self.logger.warning(f"Could not find article content for {url}")
                return None

            content_div = article_div.find('div', class_='c-wysiwyg') #find content
            if not content_div:
                self.logger.warning(f"Could not find content div for {url}")
                return None
            # Remove any unwanted elements like image captions or infograms.
            for element in content_div.find_all('div', class_=['wp-caption', 'js-infogram-embed']):
                element.decompose()

            # combine headers with paragraphs to stop issues with grouping
            content_elements = content_div.find_all(['p','h1','h2','h3','h4','h5','h6'])
            article_text = ""
            for element in content_elements:
                text = element.get_text().strip()
                if text:
                     if element.name.startswith('h'):  # Check if it's a heading
                         article_text += f"\n\n{text}\n"
                     else:
                        article_text += f"{text}\n"

            return article_text.strip()
        except requests.RequestException as e:
             self.logger.error(f"Error fetching {url}: {e}")
             return None
        except Exception as e:
            self.logger.error(f"Error processing {url}: {e}")
            return None

    def process_article(self, article: Dict):
        link = article['link']
        title = article['title']
        published_date = article['date']

        self.logger.info(f"Processing: {title}")
        content = self.scrape_article(link)
        if not content:
            return # Skip if no content is retrieved

        if self.is_duplicate(link): #Simplified duplicate check.
            self.logger.info(f"Skipping duplicate article: {title}")
            return

        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("""
                        INSERT OR REPLACE INTO articles (link, title, published_date, content, source)
                        VALUES (?, ?, ?, ?, ?)
                """, (
                    link,
                    title,
                    published_date,
                    content,
                    "securelist"
                ))
                conn.commit()
            self.logger.info(f"Stored article: {title}")

        except sqlite3.Error as e:
            self.logger.error(f"Database error storing article {title}: {e}")

    def process_all_articles(self, limit: int = 100):
        try:
            articles = self.fetch_feed()
            if not articles:
                self.logger.info("No articles fetched from feed.")
                return

            processed_count = 0
            for article in articles:
                if processed_count >= limit:
                    self.logger.info(f"Reached processing limit of {limit} articles.")
                    break

                self.process_article(article)
                processed_count += 1
                time.sleep(2) # Add a small delay

            # Optional: Add some final statistics
            with sqlite3.connect(self.db_name) as conn:
              c = conn.cursor()
              c.execute("SELECT COUNT(*) FROM articles WHERE source = 'securelist'")
              total_count = c.fetchone()[0]
              self.logger.info("\nFinal Statistics:")
              self.logger.info(f"Total articles in database from Securelist: {total_count}")

        except Exception as e:
            self.logger.error(f"Error processing articles: {e}")


def main():
    processor = SecurelistProcessor()
    processor.process_all_articles(limit=100)

if __name__ == "__main__":
    main()