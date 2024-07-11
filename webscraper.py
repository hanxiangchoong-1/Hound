import asyncio
import aiohttp
import random
import logging
from urllib.parse import urlparse
import traceback
import time

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def fetch_url(self, session, url, headers, delay):
        await asyncio.sleep(delay)
        try:
            self.logger.info(f"Fetching URL: {url}")
            async with session.get(url, headers=headers, timeout=30) as response:
                content = await response.text()
                self.logger.info(f"Successfully fetched URL: {url}")
                return content
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            self.logger.debug(traceback.format_exc())
            return None
        
    async def scrape_urls(self, items):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }

        total_urls = len(items)
        self.logger.info(f"Starting to scrape {total_urls} URLs")
        start_time = time.time()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for index, item in enumerate(items, 1):
                url = item['link']
                domain = urlparse(url).netloc
                delay = random.uniform(1, 3)
                self.logger.debug(f"Queuing URL {index}/{total_urls}: {url} (Delay: {delay:.2f}s)")
                task = asyncio.create_task(self.fetch_url(session, url, headers, delay))
                tasks.append((item, task))

            successful_scrapes = 0
            failed_scrapes = 0

            for index, (item, task) in enumerate(tasks, 1):
                try:
                    html_content = await task
                    if html_content:
                        url = item['link']
                        item.update({
                            'id': url,
                            'html_content': html_content
                        })
                        successful_scrapes += 1
                        self.logger.info(f"Successfully scraped {index}/{total_urls}: {url}")
                    else:
                        failed_scrapes += 1
                        self.logger.warning(f"Failed to scrape {index}/{total_urls}: {item['link']}")
                except Exception as e:
                    failed_scrapes += 1
                    self.logger.error(f"Error scraping {index}/{total_urls}: {item['link']} - {str(e)}")
                    self.logger.debug(f"Traceback for {item['link']}:\n{traceback.format_exc()}")

        end_time = time.time()
        total_time = end_time - start_time
        self.logger.info(f"Completed scraping {total_urls} URLs in {total_time:.2f} seconds")
        self.logger.info(f"Successful scrapes: {successful_scrapes}, Failed scrapes: {failed_scrapes}")
        
        return items

    def scrape_urls_from_list(self, items):
        return asyncio.get_event_loop().run_until_complete(self.scrape_urls(items))