import os
import logging
import traceback
from fastapi import FastAPI, HTTPException
import pickle
from dotenv import load_dotenv
from dataprocessor import DataProcessor
from webscraper import WebScraper
from search_engine import SearchEngine
from elastic_helpers import ESBulkIndexer
from api_services import SearchService, SearchRequest
from api_services import ScrapeService, ScrapeRequest
from api_services import ProcessService, ProcessRequest
import nest_asyncio
nest_asyncio.apply()
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

# # Initialize WebScraper
# webscraper = WebScraper()

# # Initialize DataProcessor
# dataprocessor = DataProcessor()



# Initialize Elasticsearch
ELASTIC_CLOUD_ID = os.environ.get('ELASTIC_CLOUD_ID')
ELASTIC_USERNAME = os.environ.get('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD')
ELASTIC_CLOUD_AUTH = (ELASTIC_USERNAME, ELASTIC_PASSWORD)
es_bulk_indexer = ESBulkIndexer(cloud_id=ELASTIC_CLOUD_ID, credentials=ELASTIC_CLOUD_AUTH)

# Initialize the service
search_service = SearchService(search_engine, es_bulk_indexer)
scrape_service = ScrapeService(es_bulk_indexer)
process_service = ProcessService(es_bulk_indexer)

@app.post("/google_search")
async def google_search(request: SearchRequest):
    logger.info(f"Received search request for entity: {request.entity}, query: {request.query}")

    try:
        result = search_service.perform_search_and_index(
            entity=request.entity,
            query=request.query,
            num=request.num
        )
        with open('./simulated_search_results.pkl', 'wb') as f: 
            pickle.dump(result, f)
        '''
        Reloading previous result to avoid excessive calls to google search api
        '''
        with open('./simulated_search_results.pkl', 'rb') as f: 
            result = pickle.load(f)
        return result
    except ValueError as e:
        logger.error(f"ValueError in google_search: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in google_search: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
@app.post("/scrape_links")
async def scrape_links(request: ScrapeRequest):
    logger.info(f"Received scrape request for entity: {request.entity}, {len(request.links)} links")
    try:
        result = await scrape_service.scrape_and_update(
            entity=request.entity,
            items=request.links
        )
        return result
    except ValueError as e:
        logger.error(f"ValueError in scrape_links: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in scrape_links: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/process_html")
async def process_html(request: ProcessRequest):
    logger.info(f"Received process request for entity: {request.entity}, {len(request.items)} items")
    try:
        result = await process_service.process_and_update(
            entity=request.entity,
            items=request.items
        )
        return result
    except ValueError as e:
        logger.error(f"ValueError in process_html: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in process_html: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)