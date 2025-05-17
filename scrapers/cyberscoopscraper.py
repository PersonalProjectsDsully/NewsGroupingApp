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


class CyberScoopScraper:
    def __init__(self, db_name: str = 'db/news.db', site_config: Dict[str, Any] = None):
        self.db_name = db_name
        self.site_config = site_config or {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/91.0.4472.124 Safari/537.36')
        })
        self.setup_database()
        self.logger = logging.getLogger(__name__)
        
        # Set up namespaces for XML parsing
        self.namespaces = {
            'content': 'http://purl.org/rss/1.0/modules/content/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'atom': 'http://www.w3.org/2005/Atom'
        }

    def setup_database(self):
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute("""
                    CREATE TABLE IF NOT EXISTS articles (
                        link TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        published_date TIMESTAMP,
                        content TEXT,
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
            return False  # Assume not duplicate on error

    def remove_emojis(self, text: str) -> str:
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"
            "\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F6FF"
            "\U0001F1E0-\U0001F1FF"
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+",
            flags=re.UNICODE
        )
        return emoji_pattern.sub(r"", text)

    def parse_rss_feed(self, feed_content: str) -> List[Dict[str, Any]]:
        """Parse the CyberScoop RSS feed."""
        try:
            # Register namespaces
            for prefix, uri in self.namespaces.items():
                ET.register_namespace(prefix, uri)
                
            root = ET.fromstring(feed_content)
            channel = root.find('channel')
            if channel is None:
                self.logger.error("No channel element found in RSS feed")
                return []
                
            articles = []
            for item in channel.findall('item'):
                title_elem = item.find('title')
                link_elem = item.find('link')
                pub_date_elem = item.find('pubDate')
                creator_elem = item.find(f".//{{{self.namespaces['dc']}}}creator")
                description_elem = item.find('description')
                content_elem = item.find(f".//{{{self.namespaces['content']}}}encoded")
                
                title = title_elem.text if title_elem is not None else None
                link = link_elem.text if link_elem is not None else None
                pub_date = pub_date_elem.text if pub_date_elem is not None else None
                creator = creator_elem.text if creator_elem is not None else None
                description = description_elem.text if description_elem is not None else None
                content = content_elem.text if content_elem is not None else None
                
                # Get categories
                categories = []
                for cat_elem in item.findall('category'):
                    if cat_elem.text:
                        categories.append(cat_elem.text)
                
                if title and link:  # Only add if we have at least title and link
                    articles.append({
                        'link': link,
                        'title': title,
                        'published_date': pub_date,
                        'creator': creator,
                        'description': description,
                        'content': content,
                        'categories': categories
                    })
            
            return articles
        except (ET.ParseError, Exception) as e:
            self.logger.error(f"Error parsing RSS feed: {e}")
            return []

    def scrape_article(self, url: str) -> Optional[str]:
        """Scrape article content from CyberScoop."""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the main article content div
            article_div = soup.find('div', class_='single-article__content-inner')
            if not article_div:
                self.logger.warning(f"No article content found at {url}")
                return None

            # Remove ads and other unwanted elements
            for ad in article_div.find_all('div', class_=lambda c: c and 'ad' in c):
                ad.decompose()
                
            # Extract the paragraphs
            paragraphs = article_div.find_all('p')
            
            # Extract the text from each paragraph
            article_text = '\n\n'.join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
            
            # Get the title and any headers if present
            title = soup.find('h1', class_='single-article__title')
            title_text = title.get_text().strip() if title else ""
            
            # Get article excerpt/description
            excerpt = soup.find('div', class_='single-article__excerpt')
            excerpt_text = excerpt.get_text().strip() if excerpt else ""
            
            # Combine title, excerpt, and article text
            full_content = ""
            if title_text:
                full_content += title_text + "\n\n"
            if excerpt_text:
                full_content += excerpt_text + "\n\n"
            if article_text:
                full_content += article_text
                
            return full_content.strip()
            
        except (requests.RequestException, Exception) as e:
            self.logger.error(f"Error scraping article {url}: {e}")
            return None

    def process_feed(self, feed_url: str, source_name: str):
        """Process the CyberScoop RSS feed and store articles."""
        try:
            self.logger.info(f"Starting to process feed from {feed_url}")
            response = self.session.get(feed_url, timeout=10)
            response.raise_for_status()
            feed_content = response.text
            
            articles = self.parse_rss_feed(feed_content)
            self.logger.info(f"Found {len(articles)} articles in the feed")
            
            seen_links = set()
            processed_count = 0

            for article in articles:
                if not article.get('link'):
                    self.logger.warning("Skipping article with no link")
                    continue
                    
                if article['link'] in seen_links:
                    self.logger.info(f"Skipping duplicate article in feed: {article['link']}")
                    continue
                    
                seen_links.add(article['link'])

                if not all(k in article for k in ['link', 'title', 'published_date']):
                    self.logger.warning(f"Skipping article missing required fields: {article.get('link', 'unknown')}")
                    continue

                # Check if we already have this article in the database
                if self.is_duplicate(article['link']):
                    self.logger.info(f"Skipping duplicate article (already in DB): {article['link']}")
                    continue

                # Clean the title to remove emoji
                cleaned_title = self.remove_emojis(article['title'])
                
                # Try to use content from RSS feed first, if available and substantial
                content = None
                if article.get('content') and len(article['content']) > 100:
                    # Extract text content from HTML in RSS feed
                    content_soup = BeautifulSoup(article['content'], 'html.parser')
                    content = content_soup.get_text('\n\n', strip=True)
                
                # If content is not available or too short, scrape the full article
                if not content or len(content) < 100:
                    self.logger.info(f"Scraping full article content from {article['link']}")
                    content = self.scrape_article(article['link'])

                if not content:
                    self.logger.warning(f"No content could be extracted for {article['link']}")
                    continue

                # Parse the date
                pub_date = None
                if article['published_date']:
                    try:
                        pub_date = parsedate_to_datetime(article['published_date'])
                    except Exception as e:
                        self.logger.warning(f"Failed to parse date {article['published_date']}: {e}")
                        pub_date = article['published_date']

                # Store in database
                try:
                    with sqlite3.connect(self.db_name) as conn:
                        c = conn.cursor()
                        c.execute("""
                            INSERT OR REPLACE INTO articles (link, title, published_date, content, source)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            article['link'],
                            cleaned_title,
                            pub_date,
                            content,
                            source_name
                        ))
                        conn.commit()

                    self.logger.info(f"Successfully stored article: {article['link']}")
                    processed_count += 1

                    # Add a delay between requests to avoid overwhelming the server
                    time.sleep(1)
                except sqlite3.Error as e:
                    self.logger.error(f"Database error storing article: {e}")
                    continue
            
            self.logger.info(f"Processed {processed_count} articles from {source_name}")

        except (requests.RequestException, Exception) as e:
            self.logger.error(f"Error processing feed {feed_url}: {e}")


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("cyberscoop_scraper.log"),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting CyberScoop scraper")
    
    site_config = {
        "cyberscoop": {
            "feed_url": "https://cyberscoop.com/feed/"
        }
    }
    
    try:
        scraper = CyberScoopScraper(site_config=site_config)
        feed_url = site_config["cyberscoop"]["feed_url"]
        scraper.process_feed(feed_url, "cyberscoop")
        logger.info("CyberScoop scraper completed successfully")
    except Exception as e:
        logger.error(f"Scraper failed with error: {e}")


if __name__ == "__main__":
    main()
