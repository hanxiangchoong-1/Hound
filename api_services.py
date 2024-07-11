import logging
from pydantic import BaseModel
from typing import Dict
from search_engine import SearchEngine
from elastic_helpers import ESBulkIndexer
from es_config import BASIC_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class SearchRequest(BaseModel):
    entity: str
    query: str
    num: int = 10

class SearchService:
    def __init__(self, search_engine: SearchEngine, es_bulk_indexer: ESBulkIndexer):
        self.search_engine = search_engine
        self.es_bulk_indexer = es_bulk_indexer
        self.logger = logging.getLogger(__name__)

    def perform_search_and_index(self, entity: str, query: str, num: int) -> Dict[str, int]:
        try:
            # Perform Google search
            results = self.search_engine.google_custom_search(query=query, num=num)
            
            if not results:
                self.logger.warning(f"No results found for query: {query}")
                raise ValueError("No results found")

            # Format search results
            formatted_results = self.search_engine.format_search_results(results)

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

        except Exception as e:
            self.logger.error(f"Error processing search request: {str(e)}")
            raise