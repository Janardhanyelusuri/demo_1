import os
import json
import re
from openai import AzureOpenAI
from dotenv import load_dotenv
load_dotenv()  # ✅ REQUIRED to load variables from .env
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_DEPLOYMENT_NAME = os.getenv("AZURE_DEPLOYMENT_NAME")
AZURE_OPENAI_VERSION = os.getenv("AZURE_OPENAI_VERSION")


def llm_call(prompt: str) -> str: # FIX 1: Change function signature to expect a string prompt and return a string
 
    
    try:
        client = AzureOpenAI(
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_KEY,
            api_version=AZURE_OPENAI_VERSION
        )

        response = client.chat.completions.create(
            model=AZURE_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": "You are a helpful cloud optimization assistant. Your response must be in the specified JSON format only."},
                # FIX 2: Correctly pass the string prompt as the content of the user message.
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            #  max_tokens=800,
        )

        print(f"LLM Response: {response}")
        output_text = response.choices[0].message.content
        print(f"LLM Output: {output_text}")

        # Clean output_text to extract only the JSON
        # NOTE: I'm updating the regex to capture the single JSON object expected by your calling functions
        # The prompt generators in llm.py expect a single JSON object (starting with {{ and ending with }})
        match = re.search(r"(\{.*\})", output_text, re.DOTALL)
        if match:
            json_str = match.group(0)
        else:
            # Fallback to the whole output text if the pattern is not found
            json_str = output_text  

        # Do not call json.loads here. Return the string.
        # The calling function (get_compute_recommendation/get_storage_recommendation) expects a string and calls json.loads
        return json_str
        
    except Exception as e:
        print(f"Error during LLM processing: {e}")
        # FIX 3: Return an empty string on error, so the calling function's 
        # `json.loads("")` call will fail gracefully with a JSONDecodeError, 
        # which you are already handling with a `continue` statement.
        return ""