import logging
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Union, Optional
from elastic_helpers import ESBulkIndexer
from search_engine import SearchEngine
from webscraper import WebScraper
from dataprocessor import DataProcessor
from es_config import BASIC_CONFIG
import nest_asyncio
nest_asyncio.apply()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

'''
Search Service
'''
class SearchRequest(BaseModel):
    entity: str
    query: str
    num: int = Field(default=10, ge=1, le=100)
    site_restrict: Optional[Union[str, List[str]]] = None

class SearchService:
    def __init__(self, search_engine: SearchEngine, es_bulk_indexer: ESBulkIndexer):
        self.search_engine = search_engine
        self.es_bulk_indexer = es_bulk_indexer
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _add_default_fields(item: Dict[str, Any]) -> None:
        """
        Add 'scraped' and 'processed' fields to the item if they don't exist.
        """
        if 'scraped' not in item:
            item['scraped'] = False
        if 'processed' not in item:
            item['processed'] = False

    def perform_search_and_index(self, entity: str, query: str, num: int, site_restrict: Union[str, List[str]] = None) -> Dict[str, int]:
        try:
            # Perform Google search
            results = self.search_engine.google_custom_search(query=query, num=num, site_restrict=site_restrict)
            
            if not results:
                self.logger.warning(f"No results found for query: {query}")
                raise ValueError("No results found")

            # Format search results
            formatted_results = self.search_engine.format_search_results(results)

            # Add explored and processed fields if they don't exist
            for item in formatted_results['items']:
                self._add_default_fields(item)

            # Prepare index name
            index_name = f"search__{entity}"

            # Check if index exists, create if not
            index_exists = self.es_bulk_indexer.check_index_existence(index_name=index_name)
            if not index_exists:
                self.logger.info(f"Creating new index: {index_name}")
                self.es_bulk_indexer.create_es_index(es_configuration=BASIC_CONFIG, index_name=index_name)

            # Upload documents to Elasticsearch
            success_count = self.es_bulk_indexer.bulk_upload_documents(
                index_name=index_name, 
                documents=formatted_results['items'], 
                id_col='link'
            )
            self.logger.info(f"Successfully uploaded {success_count} documents to index: {index_name}")

            return {"message": "Search completed and results indexed", "indexed_count": success_count}

        except ValueError as e:
            self.logger.warning(f"ValueError in search request: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing search request: {str(e)}")
            raise

'''
Scrape Service
'''

class ScrapeRequest(BaseModel):
    entity: str
    links: List[Dict[str, str]]

class ScrapeService:
    def __init__(self, es_bulk_indexer: ESBulkIndexer):
        self.es_bulk_indexer = es_bulk_indexer
        self.web_scraper = WebScraper()
        self.logger = logging.getLogger(__name__)

    
    @staticmethod
    def _add_default_fields(item: Dict[str, Any]) -> None:
        """
        Add 'scraped' and 'processed' fields to the item if they don't exist.
        """
        if 'scraped' not in item:
            item['scraped'] = True
        if 'processed' not in item:
            item['processed'] = False

    async def scrape_and_update(self, entity: str, items: List[Dict[str, Any]]) -> Dict[str, int]:
        try:
            # Scrape the URLs
            scraped_items = await self.web_scraper.scrape_urls_from_list(items)

            # Prepare index name
            index_name = f"explore__{entity}"

            # Check if index exists, create if not
            index_exists = self.es_bulk_indexer.check_index_existence(index_name=index_name)
            if not index_exists:
                self.logger.info(f"Creating new index: {index_name}")
                self.es_bulk_indexer.create_es_index(es_configuration=BASIC_CONFIG, index_name=index_name)


            # Add explored and processed fields if they don't exist
            for item in scraped_items:
                self._add_default_fields(item)

            # Update documents in Elasticsearch
            success_count = self.es_bulk_indexer.bulk_upload_documents(
                index_name=index_name, 
                documents=scraped_items, 
                id_col='link'
            )
            self.logger.info(f"Successfully updated {success_count} documents in index: {index_name}")

            return {"message": "Scraping completed and documents updated", "updated_count": success_count}

        except Exception as e:
            self.logger.error(f"Error processing scrape request: {str(e)}")
            raise

'''
Process Service
'''
class ProcessRequest(BaseModel):
    entity: str
    items: List[Dict[str, Any]]

class ProcessService:
    def __init__(self, es_bulk_indexer: ESBulkIndexer):
        self.es_bulk_indexer = es_bulk_indexer
        self.data_processor = DataProcessor()
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _add_default_fields(item: Dict[str, Any]) -> None:
        """
        Add 'scraped' and 'processed' fields to the item if they don't exist.
        """
        if 'scraped' not in item:
            item['scraped'] = True
        if 'processed' not in item:
            item['processed'] = True

    async def process_and_update(self, entity: str, items: List[Dict[str, Any]]) -> Dict[str, int]:
        try:
            processed_items = []
            for item in items:
                html_content = item.get('html_content')
                base_url = item.get('link')
                if html_content and base_url:
                    processed_item = self.data_processor.extract_content(html_content, base_url)
                    processed_items.append(processed_item)
                else:
                    self.logger.warning(f"Skipping item due to missing html_content or link: {item}")

            # Prepare index name
            index_name = f"processed__{entity}"

            # Check if index exists, create if not
            index_exists = self.es_bulk_indexer.check_index_existence(index_name=index_name)
            if not index_exists:
                self.logger.info(f"Creating new index: {index_name}")
                self.es_bulk_indexer.create_es_index(es_configuration=BASIC_CONFIG, index_name=index_name)

            # Add explored and processed fields if they don't exist
            for item in processed_items:
                self._add_default_fields(item)

            # Update documents in Elasticsearch
            success_count = self.es_bulk_indexer.bulk_upload_documents(
                index_name=index_name, 
                documents=processed_items, 
                id_col='link'
            )
            self.logger.info(f"Successfully updated {success_count} documents in index: {index_name}")

            return {"message": "Processing completed and documents updated", "updated_count": success_count}

        except Exception as e:
            self.logger.error(f"Error processing HTML content: {str(e)}")
            raise