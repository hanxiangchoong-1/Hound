import os
import sys
import logging
import traceback
import asyncio
import argparse
from dotenv import load_dotenv
from webscraper import WebScraper
from search_engine import SearchEngine

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)
from elastic_helpers import ESBulkIndexer
from elastic_config import BASIC_CONFIG
sys.path.pop(0)

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Initialize services
search_engine = SearchEngine()
webscraper = WebScraper()

# Initialize Elasticsearch
ELASTIC_CLOUD_ID = os.environ.get('ELASTIC_CLOUD_ID')
ELASTIC_USERNAME = os.environ.get('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD')
ELASTIC_CLOUD_AUTH = (ELASTIC_USERNAME, ELASTIC_PASSWORD)
es_bulk_indexer = ESBulkIndexer(cloud_id=ELASTIC_CLOUD_ID, credentials=ELASTIC_CLOUD_AUTH)

async def run(entity, query, skip_search=False, skip_scrape=False, skip_index=False):
    try:
        if not skip_search:
            logger.info(f"Performing search for query: {query}")
            search_result = search_engine.google_custom_search(query=query)
            logger.info(f"Search completed. Found {len(search_result['items'])} results.")
        else:
            logger.info("Skipping search step.")
            search_result = {'items': []}  # Placeholder for skipped search

        if not skip_scrape:
            logger.info("Starting web scraping.")
            scraped = await webscraper.scrape_urls_from_list(search_result['items'])
            logger.info(f"Web scraping completed. Scraped {len(scraped)} items.")
        else:
            logger.info("Skipping web scraping step.")
            scraped = search_result['items']  # Use search results if scraping is skipped

        if not skip_index:
            # Prepare index name
            index_name = f"raw__{entity}"

            # Check if index exists, create if not
            index_exists = es_bulk_indexer.check_index_existence(index_name=index_name)
            if not index_exists:
                logger.info(f"Creating new index: {index_name}")
                es_bulk_indexer.create_es_index(es_configuration=BASIC_CONFIG, index_name=index_name)

            # Update documents in Elasticsearch
            success_count = es_bulk_indexer.bulk_upload_documents(
                index_name=index_name, 
                documents=scraped, 
                id_col='link'
            )
            logger.info(f"Indexing completed. Successfully indexed {success_count} documents.")
        else:
            logger.info("Skipping indexing step.")

        logger.info("Process completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.debug(traceback.format_exc())

def main():
    parser = argparse.ArgumentParser(description="Search, scrape, and index web content.")
    parser.add_argument("entity", help="The entity to search for")
    parser.add_argument("query", help="The search query")
    parser.add_argument("--skip-search", action="store_true", help="Skip the search step")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip the web scraping step")
    parser.add_argument("--skip-index", action="store_true", help="Skip the indexing step")

    args = parser.parse_args()

    asyncio.run(run(args.entity, args.query, args.skip_search, args.skip_scrape, args.skip_index))

if __name__ == "__main__":
    main()