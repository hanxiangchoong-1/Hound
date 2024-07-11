import os
import requests
import traceback
import asyncio
import aiohttp
import random
import re
import logging
import html2text
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
load_dotenv()
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
    # def search_and_scrape(self, query, num=10):
    #     self.logger.info(f"Starting search and scrape for query: {query}")
    #     results = self.google_custom_search(query, num)
    #     if results:
    #         formatted_results = self.format_search_results(results)
    #         htmls = self.scrape_urls_from_list(formatted_results['items'])
    #         self.logger.info(f"Completed search and scrape for query: {query}")
    #         return {
    #             'metadata': formatted_results['metadata'],
    #             'items': htmls
    #         }
    #     self.logger.warning(f"No results found for query: {query}")
    #     return None
