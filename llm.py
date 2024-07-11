import os
import logging
from openai import AsyncOpenAI
from prompts import CLEAN_TEXT_PROMPT, EXTRACT_ENTITIES_PROMPT, EXTRACT_RELATIONSHIPS_PROMPT

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class LLMProcessor:
    def __init__(self, api_key=None, model="gpt-4o"):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.client = AsyncOpenAI(api_key=self.api_key)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"LLMProcessor initialized with model: {self.model}")

    async def _process_request(self, system_prompt, user_prompt):
        self.logger.info(f"Processing request with model: {self.model}")
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
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

    async def clean_text(self, text):
        return await self._execute_task("cleaning text", text, CLEAN_TEXT_PROMPT)

    async def extract_entities(self, text, existing_entities=None):
        prompt = text
        if existing_entities:
            prompt += f"\n\nExisting entities: {existing_entities}"
            self.logger.info("Using existing entities in extraction")
        return await self._execute_task("extracting entities", prompt, EXTRACT_ENTITIES_PROMPT)

    async def extract_relationships(self, text, entities):
        prompt = f"Text: {text}\n\nEntities: {entities}"
        return await self._execute_task("extracting relationships", prompt, EXTRACT_RELATIONSHIPS_PROMPT)