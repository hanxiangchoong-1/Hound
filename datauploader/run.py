import os
import sys
import logging
import traceback
import asyncio
import argparse
from dotenv import load_dotenv
from tqdm import tqdm
from llama_index.core import SimpleDirectoryReader

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

# Initialize Elasticsearch
ELASTIC_CLOUD_ID = os.environ.get('ELASTIC_CLOUD_ID')
ELASTIC_USERNAME = os.environ.get('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD')
ELASTIC_CLOUD_AUTH = (ELASTIC_USERNAME, ELASTIC_PASSWORD)
es_bulk_indexer = ESBulkIndexer(cloud_id=ELASTIC_CLOUD_ID, credentials=ELASTIC_CLOUD_AUTH)

def load_documents(folder_path):
    try:
        reader = SimpleDirectoryReader(folder_path)
        documents = reader.load_data()
        return [{"filename": doc.metadata['file_name'], "text": doc.text} for doc in documents]
    except Exception as e:
        logger.error(f"Error loading documents: {str(e)}")
        logger.debug(traceback.format_exc())
        return []

async def upload_documents(documents, index_name):
    try:
        # Check if index exists, create if not
        if not es_bulk_indexer.check_index_existence(index_name=index_name):
            logger.info(f"Creating new index: {index_name}")
            es_bulk_indexer.create_es_index(es_configuration=BASIC_CONFIG, index_name=index_name)

        # Bulk upload documents
        success = es_bulk_indexer.bulk_upload_documents(
            index_name=index_name,
            documents=documents,
            id_col='filename'
        )

        if success:
            logger.info(f"Successfully uploaded {len(documents)} documents to index: {index_name}")
        else:
            logger.warning(f"Failed to upload some or all documents to index: {index_name}")

    except Exception as e:
        logger.error(f"An error occurred during document upload: {str(e)}")
        logger.debug(traceback.format_exc())

async def run(folder_path, index_name):
    try:
        logger.info(f"Loading documents from: {folder_path}")
        documents = load_documents(folder_path)
        
        if not documents:
            logger.warning("No documents loaded. Exiting.")
            return

        logger.info(f"Uploading {len(documents)} documents to index: {index_name}")
        await upload_documents(documents, index_name)

    except Exception as e:
        logger.error(f"An error occurred during the run: {str(e)}")
        logger.debug(traceback.format_exc())

def main():
    parser = argparse.ArgumentParser(description="Upload documents to Elasticsearch index.")
    parser.add_argument("folder_path", help="Path to the folder containing documents")
    parser.add_argument("index_name", help="Elasticsearch index name to upload to")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.folder_path, args.index_name))
    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}")
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main()