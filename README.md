

# Hound

Conduct searches using a search engine API. Scrape the results, extract the text, use an LLM to clean the text, and upload it to Elastic

Goal: To perform research on a particular entity 


# To Run:


### Search Scraper
```
python3 ./search_scraper/run.py govtech "govtech sg directors"
```

### Data Processor
```
python3 ./dataprocessor/run.py raw__govtech all_text processed__govtech
```

