import yake
import logging

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("DataProcessor initialized")

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