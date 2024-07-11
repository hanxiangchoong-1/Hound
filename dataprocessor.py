import yake
import logging
import re
import html2text
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("DataProcessor initialized")


    def extract_content(self, html_content, base_url):
        self.logger.info(f"Extracting content from {base_url}")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        def clean_text(text):
            return re.sub(r'\s+', ' ', text).strip()
        
        def dedupe_list(lst):
            return list(dict.fromkeys(lst))
        
        # Extract title
        title = soup.title.string if soup.title else ''
        
        # Extract headers
        headers = {}
        for i in range(1, 7):  # h1 to h6
            headers[f'h{i}'] = dedupe_list([clean_text(h.text) for h in soup.find_all(f'h{i}')])
        
        # Extract paragraphs
        paragraphs = dedupe_list([clean_text(p.text) for p in soup.find_all(['p', 'div', 'span']) if clean_text(p.text)])
        
        # Extract links
        links = []
        seen_urls = set()
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            full_url = urljoin(base_url, href)
            if full_url not in seen_urls:
                links.append({'text': clean_text(a.text), 'href': full_url})
                seen_urls.add(full_url)
        
        # Extract meta tags
        meta_tags = {}
        for meta in soup.find_all('meta'):
            name = meta.get('name', meta.get('property', ''))
            content = meta.get('content', '')
            if name and content:
                meta_tags[name] = clean_text(content)
        
        # Extract main content
        main_content = ''
        for tag in ['main', 'article', 'div[role="main"]', '#content', '.content']:
            main_tag = soup.select_one(tag)
            if main_tag:
                main_content = clean_text(main_tag.get_text(separator=' ', strip=True))
                break
        
        # If no main content found, use body
        if not main_content:
            main_content = clean_text(soup.body.get_text(separator=' ', strip=True))
        
        # Extract all visible text
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        all_text = clean_text(h.handle(str(soup)))
        
        # Extract structured data
        structured_data = []
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                structured_data.append(data)
            except json.JSONDecodeError:
                pass
        
        self.logger.info(f"Content extraction completed for {base_url}")
        return {
            'title': title,
            'headers': headers,
            'paragraphs': paragraphs,
            'links': links,
            'meta_tags': meta_tags,
            'main_content': main_content,
            'all_text': all_text,
            'structured_data': structured_data,
            'explored':False,
            'processed': False
        }


    def extract_keywords_and_phrases(self, text, num_keywords=15, num_phrases=10, max_ngram_size=5):
        self.logger.info(f"Extracting keywords and phrases. Params: num_keywords={num_keywords}, num_phrases={num_phrases}, max_ngram_size={max_ngram_size}")

        try:
            # Initialize YAKE keyword extractor
            language = "en"
            deduplication_threshold = 0.9
            deduplication_algo = 'seqm'
            windowSize = 5
            
            self.logger.debug("Initializing YAKE keyword extractor")
            custom_kw_extractor = yake.KeywordExtractor(
                lan=language, 
                n=max_ngram_size, 
                dedupLim=deduplication_threshold, 
                dedupFunc=deduplication_algo, 
                windowsSize=windowSize, 
                top=num_keywords + num_phrases
            )

            # Extract keywords and phrases
            self.logger.debug("Extracting keywords and phrases from text")
            keywords_phrases = custom_kw_extractor.extract_keywords(text)
            
            # Separate single-word keywords and multi-word phrases
            self.logger.debug("Separating keywords and phrases")
            keywords = []
            phrases = []
            
            for keyword, score in keywords_phrases:
                if len(keyword.split()) == 1:
                    keywords.append((keyword, score))
                else:
                    phrases.append((keyword, score))
                
                if len(keywords) == num_keywords and len(phrases) == num_phrases:
                    break
            
            # Ensure we have the requested number of keywords and phrases
            keywords = keywords[:num_keywords]
            phrases = phrases[:num_phrases]

            self.logger.info(f"Extraction complete. Found {len(keywords)} keywords and {len(phrases)} phrases")
            return keywords, phrases

        except Exception as e:
            self.logger.error(f"Error in keyword and phrase extraction: {str(e)}")
            raise