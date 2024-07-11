import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pickle
from dotenv import load_dotenv
from search_engine import SearchEngine
from elastic_helpers import ESBulkIndexer
from api_services import SearchService, SearchRequest
from es_config import BASIC_CONFIG

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Initialize SearchEngine
search_engine = SearchEngine()

# Initialize Elasticsearch
ELASTIC_CLOUD_ID = os.environ.get('ELASTIC_CLOUD_ID')
ELASTIC_USERNAME = os.environ.get('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD')
ELASTIC_CLOUD_AUTH = (ELASTIC_USERNAME, ELASTIC_PASSWORD)
es_bulk_indexer = ESBulkIndexer(cloud_id=ELASTIC_CLOUD_ID, credentials=ELASTIC_CLOUD_AUTH)

# Initialize the service
search_service = SearchService(search_engine, es_bulk_indexer)

@app.post("/google_search")
async def google_search(request: SearchRequest):
    logger.info(f"Received search request for entity: {request.entity}, query: {request.query}")

    try:
        # result = search_service.perform_search_and_index(
        #     entity=request.entity,
        #     query=request.query,
        #     num=request.num
        # )
        # with open('./simulated_search_results.pkl', 'wb') as f: 
        #     pickle.dump(result, f)
        '''
        Reloading previous result to avoid excessive calls to google search api
        '''
        with open('./simulated_search_results.pkl', 'rb') as f: 
            result=pickle.load(f)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing search request: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)