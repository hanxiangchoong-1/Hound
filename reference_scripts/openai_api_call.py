import os
from openai import AzureOpenAI
from dotenv import load_dotenv
load_dotenv()
'''
https://techcommunity.microsoft.com/t5/startups-at-microsoft/how-to-use-azure-openai-gpt-4o-with-function-calling/ba-p/4158612
'''
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY_1"),  
    api_version="2024-06-01",
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    )
    
AZURE_OPENAI_DEPLOYMENT_NAME=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
def call_OpenAI(messages):
    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT_NAME,
        messages=messages,
    )
    response_message = response.choices[0].message
    return response_message

messages = [
    {"role": "system", "content": "placeholder"},
    {"role": "user", "content": "hello"}
]
call_OpenAI(messages)