import os
import requests
import traceback
import asyncio
import aiohttp
import random
import re
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class SearchEngine:
    def __init__(self):
        self.api_key = os.environ.get('GOOGLE_SE_API_KEY')
        self.search_engine_id = os.environ.get('GOOGLE_SE_ID')
        self.base_url = "https://www.googleapis.com/customsearch/v1"
        self.logger = logging.getLogger(__name__)

    def google_custom_search(self, query, num=10, **params):
        default_params = {
            'q': query,
            'key': self.api_key,
            'cx': self.search_engine_id,
            'num': num
        }
        default_params.update(params)
        
        try:
            self.logger.info(f"Sending API request for query: {query}")
            response = requests.get(self.base_url, params=default_params)
            response.raise_for_status()
            self.logger.info(f"API request successful for query: {query}")
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"API request error for query '{query}': {str(e)}")
            self.logger.debug(traceback.format_exc())
            return None
        except ValueError as e:
            self.logger.error(f"JSON decoding error for query '{query}': {str(e)}")
            self.logger.debug(traceback.format_exc())
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for query '{query}': {str(e)}")
            self.logger.debug(traceback.format_exc())
            return None

    def format_search_results(self, search_data):
        self.logger.info("Formatting search results")
        formatted_results = {
            "metadata": {
                "kind": search_data.get("kind"),
                "url": search_data.get("url"),
                "queries": search_data.get("queries"),
                "context": search_data.get("context"),
                "searchInformation": search_data.get("searchInformation")
            },
            "items": []
        }
        
        for item in search_data.get("items", []):
            formatted_item = {
                "title": item.get("title"),
                "link": item.get("link"),
                "snippet": item.get("snippet"),
                "htmlSnippet": item.get("htmlSnippet"),
                "displayLink": item.get("displayLink"),
                "formattedUrl": item.get("formattedUrl"),
                "htmlFormattedUrl": item.get("htmlFormattedUrl")
            }
            formatted_results["items"].append(formatted_item)
        
        self.logger.info(f"Formatted {len(formatted_results['items'])} search results")
        return formatted_results

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
            return None

    def extract_content(self, html_content, base_url):
        self.logger.info(f"Extracting content from {base_url}")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        def clean_text(text):
            return re.sub(r'\s+', ' ', text).strip()
        
        def dedupe_list(lst):
            return list(dict.fromkeys(lst))
        
        title = soup.title.string if soup.title else ''
        
        headers = {
            'h1': dedupe_list([clean_text(h.text) for h in soup.find_all('h1')]),
            'h2': dedupe_list([clean_text(h.text) for h in soup.find_all('h2')]),
            'h3': dedupe_list([clean_text(h.text) for h in soup.find_all('h3')])
        }
        
        paragraphs = dedupe_list([clean_text(p.text) for p in soup.find_all('p') if clean_text(p.text)])
        
        links = []
        seen_urls = set()
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            full_url = urljoin(base_url, href)
            if full_url not in seen_urls:
                links.append({'text': clean_text(a.text), 'href': full_url})
                seen_urls.add(full_url)
        
        meta_description = ''
        meta_desc_tag = soup.find('meta', attrs={'name': 'description'})
        if meta_desc_tag:
            meta_description = clean_text(meta_desc_tag.get('content', ''))
        
        main_content = ''
        main_tag = soup.find('main')
        if main_tag:
            main_content = clean_text(main_tag.text)
        
        if main_content:
            sentences = re.split(r'(?<=[.!?])\s+', main_content)
            main_content = '\n'.join(dedupe_list(sentences))
        
        all_text_components = [title, main_content, *paragraphs]
        all_text = '\n\n'.join(filter(None, all_text_components))
        
        self.logger.info(f"Content extraction completed for {base_url}")
        return {
            'title': title,
            'headers': headers,
            'paragraphs': paragraphs,
            'links': links,
            'meta_description': meta_description,
            'main_content': main_content,
            'all_text': all_text
        }

    async def scrape_urls(self, items):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }

        self.logger.info(f"Starting to scrape {len(items)} URLs")
        async with aiohttp.ClientSession() as session:
            tasks = []
            for item in items:
                url = item['link']
                domain = urlparse(url).netloc
                delay = random.uniform(1, 3)
                task = asyncio.create_task(self.fetch_url(session, url, headers, delay))
                tasks.append((item, task))

            for item, task in tasks:
                html_content = await task
                if html_content:
                    base_url = item['link']
                    extracted_content = self.extract_content(html_content, base_url)
                    item.update({
                        'extracted_content': extracted_content,
                        'html_content': html_content
                    })

        self.logger.info(f"Completed scraping {len(items)} URLs")
        return items

    def scrape_urls_from_list(self, items):
        return asyncio.get_event_loop().run_until_complete(self.scrape_urls(items))

    def search_and_scrape(self, query, num=10):
        self.logger.info(f"Starting search and scrape for query: {query}")
        results = self.google_custom_search(query, num)
        if results:
            formatted_results = self.format_search_results(results)
            htmls = self.scrape_urls_from_list(formatted_results['items'])
            self.logger.info(f"Completed search and scrape for query: {query}")
            return {
                'metadata': formatted_results['metadata'],
                'items': htmls
            }
        self.logger.warning(f"No results found for query: {query}")
        return None
