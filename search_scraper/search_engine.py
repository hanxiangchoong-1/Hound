import os
import requests
import traceback
import logging
from dotenv import load_dotenv
import nest_asyncio
nest_asyncio.apply()
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

    def google_custom_search(self, query, num=10, site_restrict=None, **params):
        '''
        site_restrict (str or list, optional): Restricts the search to specific sites or domains. Defaults to None.
            Can be used in the following ways:
            - Single domain: "example.com"
            - Single subdomain: "blog.example.com"
            - Multiple domains: ["example.com", "anotherexample.com"]
            - Top-level domain: "gov"
            - Country-specific domains: "co.uk"
            - Specific directory: "example.com/blog"
            - Multiple specific paths: ["example.com/blog", "example.com/news"]
            - Combination: ["example.com", "blog.anotherexample.com", "gov"]
        '''
        default_params = {
            'q': query,
            'key': self.api_key,
            'cx': self.search_engine_id,
            'num': num
        }

        # Add site restriction if specified
        if site_restrict:
            if isinstance(site_restrict, str):
                # Single site restriction
                default_params['siteSearch'] = site_restrict
            elif isinstance(site_restrict, list):
                # Multiple site restriction
                default_params['siteSearch'] = '|'.join(site_restrict)
            
            # By default, include only these sites
            default_params['siteSearchFilter'] = 'i'
            
        # Update with any additional parameters
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
