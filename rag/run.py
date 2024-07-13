import os
import sys
import logging
import traceback
import asyncio
import argparse
from dotenv import load_dotenv
from llm import LLMProcessor
from tqdm import tqdm
import json 

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
    
async def run(index_name, query_text, fields, n):
    try:
        results = await search_es(index_name, query_text, fields, n)
        
        if results:
            context_docs=['\n\n'.join([field+":\n\n"+j['source'][field] for field in fields]) for j in results]
            logger.info(f"Context:\n\n{context_docs}")
            response=await llm.basic_qa(context='\n\n'.join(context_docs), query=query_text)
            logger.info(f"Response:\n\n{response}")
            # logger.info("Search results:")
            # for i, result in enumerate(results, 1):
            #     logger.info(f"Result {i}:")
            #     logger.info(f"ID: {result['id']}")
            #     logger.info(f"Score: {result['score']}")
            #     logger.info(f"Source: {json.dumps(result['source'], indent=2)}")
            #     logger.info("---")
        else:
            logger.info("No results found.")

    except Exception as e:
        logger.error(f"An error occurred during the run: {str(e)}")
        logger.debug(traceback.format_exc())

def main():
    parser = argparse.ArgumentParser(description="Search Elasticsearch index and return results.")
    parser.add_argument("index_name", help="Index to search")
    parser.add_argument("query_text", help="Text to search for")
    parser.add_argument("fields", nargs='+', help="Fields to search in")
    parser.add_argument("--n", type=int, default=10, help="Number of results to return (default: 10)")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.index_name, args.query_text, args.fields, args.n))
    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}")
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main()