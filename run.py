import requests
import json
from elastic_helpers import ESQueryMaker, NotFoundError
import os
from dotenv import load_dotenv
import time
import nest_asyncio
nest_asyncio.apply()

# Load environment variables
load_dotenv()

# Set up elasticsearch connection
ELASTIC_CLOUD_ID = os.getenv('ELASTIC_CLOUD_ID')
ELASTIC_USERNAME = os.getenv('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.getenv('ELASTIC_PASSWORD')

# API endpoint
API_BASE_URL = "http://localhost:8000"  # Adjust if your FastAPI is running on a different port or host

def call_google_search_endpoint(payload):
    url = f"{API_BASE_URL}/google_search"
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print("Google search successful")
        return response.json()
    else:
        print(f"Error in Google search: {response.status_code}")
        print(response.text)
        return None

def query_elasticsearch(index_name, query):
    es_query_maker = ESQueryMaker(ELASTIC_CLOUD_ID, (ELASTIC_USERNAME, ELASTIC_PASSWORD))
    try:
        results = es_query_maker.conn.search(index=index_name, body=query)
        print("Elasticsearch query successful")
        return results
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return None

def call_scrape_links_endpoint(links):
    url = f"{API_BASE_URL}/scrape_links"
    payload = {
        "entity": "govtech",
        "links": links
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print("Scrape links successful")
        return response.json()
    else:
        print(f"Error in scrape links: {response.status_code}")
        print(response.text)
        return None
    
def call_process_html_endpoint(entity, items):
    url = f"{API_BASE_URL}/process_html"
    payload = {
        "entity": entity,
        "items": items
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print("Process HTML successful")
        return response.json()
    else:
        print(f"Error in process HTML: {response.status_code}")
        print(response.text)
        return None


def wait_for_indexing(index_name, max_retries=5, delay=2):
    es_query_maker = ESQueryMaker(ELASTIC_CLOUD_ID, (ELASTIC_USERNAME, ELASTIC_PASSWORD))
    for attempt in range(max_retries):
        try:
            # Check if the index exists and has documents
            count = es_query_maker.conn.count(index=index_name)['count']
            if count > 0:
                print(f"Index {index_name} is ready with {count} documents.")
                return True
            else:
                print(f"Index {index_name} exists but has no documents yet. Retrying...")
        except NotFoundError:
            print(f"Index {index_name} not found. Retrying...")
        
        time.sleep(delay)
    
    print(f"Index {index_name} not ready after {max_retries} attempts.")
    return False


def main():
    # Step 1: Call Google search endpoint
    payload = {
        "entity": "govtech",
        "query": "govtech sg",
        # "site_restrict": "www.tech.gov.sg"
    }
    google_search_result = call_google_search_endpoint(payload=payload)
    if not google_search_result:
        return

    # # Wait for indexing to complete
    index_name = "search__govtech"
    if not wait_for_indexing(index_name):
        print("Indexing did not complete in time. Exiting.")
        return

    # Step 2: Query Elasticsearch
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"scraped": False}}
                ]
            }
        }
    }
    es_results = query_elasticsearch(index_name=index_name, query=query)
    if not es_results:
        return

    # Extract links from Elasticsearch results
    links_to_scrape = [
        {"link": hit["_source"]["link"], "title": hit["_source"].get("title", ""), "snippet": hit["_source"].get("snippet", "")}
        for hit in es_results.get("hits", {}).get("hits", [])
    ]

    if not links_to_scrape:
        print("No unexplored links found in Elasticsearch")
        return

    scrape_result = call_scrape_links_endpoint(links_to_scrape)
    if scrape_result:
        print(f"Scraping completed. Updated {scrape_result.get('updated_count', 0)} documents.")

    # Step 3: Process HTML content
    index_name="explore__govtech"
    if not wait_for_indexing(index_name):
        print("Indexing of scraped content did not complete in time. Exiting.")
        return
    
    # Query Elasticsearch again to get the scraped HTML content
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"scraped": True}},
                    {"term": {"processed": False}}
                ]
            }
        }
    }
    es_results = query_elasticsearch(index_name=index_name, query=query)
    if not es_results:
        return

    items_to_process = [hit["_source"] for hit in es_results.get("hits", {}).get("hits", [])]

    if not items_to_process:
        print("No items to process found in Elasticsearch")
        return

    process_result = call_process_html_endpoint("govtech", items_to_process)
    if process_result:
        print(f"Processing completed. Updated {process_result.get('updated_count', 0)} documents.")

    

if __name__ == "__main__":
    main()