import asyncio
import aiohttp
import random
import logging
from urllib.parse import urlparse, urljoin
import traceback
import time
import nest_asyncio
from bs4 import BeautifulSoup
import html2text
import re

nest_asyncio.apply()

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.social_media_domains = [
            'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
            'pinterest.com', 'reddit.com', 'tumblr.com', 'snapchat.com',
            'tiktok.com', 'youtube.com', 'whatsapp.com', 'telegram.org',
            'medium.com', 'quora.com'
        ]

    def is_social_media(self, url):
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        return any(social_domain in domain for social_domain in self.social_media_domains)

    async def fetch_and_process_url(self, session, url, headers):
        try:
            if self.is_social_media(url):
                self.logger.info(f"Skipping social media site: {url}")
                return None

            self.logger.info(f"Fetching URL: {url}")
            async with session.get(url, headers=headers, timeout=30) as response:
                # Check if the content is PDF
                content_type = response.headers.get('Content-Type', '').lower()
                if 'application/pdf' in content_type:
                    self.logger.info(f"Skipping PDF content: {url}")
                    return None
                
                content = await response.text()
                return self.extract_content(content, url)
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            self.logger.debug(traceback.format_exc())
            return None

    def extract_content(self, html_content, base_url):
        self.logger.info(f"Extracting content from {base_url}")
        
        def clean_text(text):
            return re.sub(r'\s+', ' ', text).strip()

        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract all visible text
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        all_text = clean_text(h.handle(str(soup)))
        
        # Extract links
        links = []
        seen_urls = set()
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            full_url = urljoin(base_url, href)
            if full_url not in seen_urls and not self.is_social_media(full_url):
                links.append({'text': clean_text(a.text), 'href': full_url})
                seen_urls.add(full_url)
        
        return {
            'link': base_url,
            'all_text': all_text,
            'links': links,
        }

    async def scrape_urls(self, items):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }

        total_urls = len(items)
        self.logger.info(f"Starting to scrape {total_urls} URLs")
        start_time = time.time()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for item in items:
                url = item['link']
                delay = random.uniform(1, 3)
                await asyncio.sleep(delay)
                task = asyncio.create_task(self.fetch_and_process_url(session, url, headers))
                tasks.append((item, task))

            successful_scrapes = 0
            failed_scrapes = 0
            skipped_pdfs = 0
            skipped_social_media = 0

            for item, task in tasks:
                try:
                    result = await task
                    if result:
                        item.update(result)
                        successful_scrapes += 1
                        self.logger.info(f"Successfully scraped: {item['link']}")
                    else:
                        if self.is_social_media(item['link']):
                            skipped_social_media += 1
                            self.logger.info(f"Skipped social media site: {item['link']}")
                        elif 'application/pdf' in item.get('content_type', '').lower():
                            skipped_pdfs += 1
                            self.logger.info(f"Skipped PDF: {item['link']}")
                        else:
                            failed_scrapes += 1
                            self.logger.warning(f"Failed to scrape: {item['link']}")
                except Exception as e:
                    failed_scrapes += 1
                    self.logger.error(f"Error scraping {item['link']}: {str(e)}")
                    self.logger.debug(f"Traceback for {item['link']}:\n{traceback.format_exc()}")

        end_time = time.time()
        total_time = end_time - start_time
        self.logger.info(f"Completed scraping {total_urls} URLs in {total_time:.2f} seconds")
        self.logger.info(f"Successful scrapes: {successful_scrapes}, Failed scrapes: {failed_scrapes}, Skipped PDFs: {skipped_pdfs}, Skipped social media: {skipped_social_media}")
        return items

    async def scrape_urls_from_list(self, items):
        return await self.scrape_urls(items)