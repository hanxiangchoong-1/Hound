import os
import logging
import os
from openai import AzureOpenAI
from prompts import BASIC_RAG_PROMPT
from dotenv import load_dotenv
load_dotenv()

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY_1"),  
    api_version="2024-06-01",
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    )
    
AZURE_OPENAI_DEPLOYMENT_NAME=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class LLMProcessor:
    def __init__(self, api_key=None, model="gpt-4o"):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.client = AzureOpenAI(
                            api_key=os.getenv("AZURE_OPENAI_KEY_1"),  
                            api_version="2024-06-01",
                            azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
                            )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"LLMProcessor initialized with model: {self.model}")

    async def _process_request(self, system_prompt, user_prompt):
        self.logger.info(f"Processing request with model: {self.model}")
        try:
            response = self.client.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=4096
            )
            self.logger.info("Request processed successfully")
            return response.choices[0].message.content.strip()
        except Exception as e:
            self.logger.error(f"Error processing request: {str(e)}")
            raise

    async def _execute_task(self, task_name, prompt, prompt_template):
        self.logger.info(f"Executing task: {task_name}")
        try:
            result = await self._process_request(prompt_template, prompt)
            self.logger.info(f"{task_name.capitalize()} completed successfully")
            return result
        except Exception as e:
            self.logger.error(f"Error in {task_name}: {str(e)}")
            raise

    async def basic_qa(self, context, query):
        prompt=f'''
        Context:
        {context}

        Query: 
        {query}
        '''
        return await self._execute_task("RAG answer generation", prompt, BASIC_RAG_PROMPT)

    # async def extract_entities(self, text, existing_entities=None):
    #     prompt = text
    #     if existing_entities:
    #         prompt += f"\n\nExisting entities: {existing_entities}"
    #         self.logger.info("Using existing entities in extraction")
    #     return await self._execute_task("extracting entities", prompt, EXTRACT_ENTITIES_PROMPT)

    # async def extract_relationships(self, text, entities):
    #     prompt = f"Text: {text}\n\nEntities: {entities}"
    #     return await self._execute_task("extracting relationships", prompt, EXTRACT_RELATIONSHIPS_PROMPT)
    

