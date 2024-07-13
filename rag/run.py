import os
import sys
import logging
import traceback
import asyncio
import argparse
from dotenv import load_dotenv
from llm import LLMProcessor
from tqdm import tqdm

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)
from elastic_helpers import ESBulkIndexer, ESQueryMaker
from elastic_config import BASIC_CONFIG
sys.path.pop(0)

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG,  # Changed to DEBUG to capture more detailed logs
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Initialize services
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
llm = LLMProcessor(api_key=OPENAI_API_KEY)

# Initialize Elasticsearch
ELASTIC_CLOUD_ID = os.environ.get('ELASTIC_CLOUD_ID')
ELASTIC_USERNAME = os.environ.get('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD')
ELASTIC_CLOUD_AUTH = (ELASTIC_USERNAME, ELASTIC_PASSWORD)
es_bulk_indexer = ESBulkIndexer(cloud_id=ELASTIC_CLOUD_ID, credentials=ELASTIC_CLOUD_AUTH)
es_query_maker = ESQueryMaker(cloud_id=ELASTIC_CLOUD_ID, credentials=ELASTIC_CLOUD_AUTH)

async def search_es(index_name, query_text, fields, n):
    try:
        logger.info(f"Searching index: {index_name} with query: {query_text}")
        
        # Perform the search
        results = es_query_maker.search_index(index_name, query_text, fields)
        await asyncio.sleep(1)  # 1 second delay

        # Extract the top n hits
        hits = results.get('hits', {}).get('hits', [])
        top_n_results = hits[:n]

        # Prepare processed results
        processed_results = []
        for hit in top_n_results:
            processed_hit = {
                'id': hit['_id'],
                'score': hit['_score'],
                'source': hit['_source']
            }
            processed_results.append(processed_hit)

        logger.info(f"Retrieved {len(processed_results)} results from index: {index_name}")
        return processed_results
    except Exception as e:
        logger.error(f"Error searching index {index_name} with query {query_text}: {str(e)}")
        logger.debug(traceback.format_exc())
        return []
    
async def run(raw_index_name, text_column, processed_index_name):
    try:
        # Check if processed index exists, create if not
        if not es_bulk_indexer.check_index_existence(index_name=processed_index_name):
            logger.info(f"Creating new index: {processed_index_name}")
            es_bulk_indexer.create_es_index(es_configuration=BASIC_CONFIG, index_name=processed_index_name)

        # Query all documents from raw index
        query = {"query": {"match_all": {}}}
        raw_docs = es_query_maker.conn.search(index=raw_index_name, body=query, scroll='2m', size=1000)
        scroll_id = raw_docs['_scroll_id']
        total_docs = raw_docs['hits']['total']['value']

        with tqdm(total=total_docs, desc="Processing documents") as pbar:
            while len(raw_docs['hits']['hits']) > 0:
                for doc in raw_docs['hits']['hits']:
                    try:
                        # Check if document already exists in processed index
                        exists_query = {"query": {"term": {"_id": doc['_id']}}}
                        exists_result = es_query_maker.conn.search(index=processed_index_name, body=exists_query)
                        
                        if exists_result['hits']['total']['value'] > 0:
                            logger.info(f"Document {doc['_id']} already processed. Skipping.")
                            pbar.update(1)
                            continue
                        
                        processed_doc = await process_document(doc, text_column)
                        
                        if processed_doc:
                            # Index single processed document
                            success = es_bulk_indexer.bulk_upload_documents(
                                index_name=processed_index_name,
                                documents=[processed_doc],
                                id_col='link'
                            )
                            if success:
                                logger.info(f"Indexed processed document: {processed_doc['link']}")
                            else:
                                logger.warning(f"Failed to index processed document: {processed_doc['link']}")
                        else:
                            logger.warning(f"Failed to process document: {doc['_id']}")
                    except Exception as e:
                        logger.error(f"Error processing or indexing document {doc['_id']}: {str(e)}")
                        logger.debug(traceback.format_exc())
                    finally:
                        pbar.update(1)

                # Get next batch
                raw_docs = es_query_maker.conn.scroll(scroll_id=scroll_id, scroll='2m')

        logger.info(f"All documents processed. Total: {total_docs}")

    except Exception as e:
        logger.error(f"An error occurred during the run: {str(e)}")
        logger.debug(traceback.format_exc())

def main():
    parser = argparse.ArgumentParser(description="Process and index text content using LLM.")
    parser.add_argument("raw_index_name", help="Index to draw from")
    parser.add_argument("text_column", help="Text data column to process")
    parser.add_argument("processed_index_name", help="Index to upload to")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.raw_index_name, args.text_column, args.processed_index_name))
    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}")
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main()