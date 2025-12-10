import os
import logging
import google.generativeai as genai
from dotenv import load_dotenv

def get_llm_model():
    """
    Configures and returns the Gemini model based on environment variables.
    """
    # Load environment variables
    load_dotenv()
    
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logging.critical("GEMINI_API_KEY not found in .env file.")
        raise ValueError("GEMINI_API_KEY not found.")

    genai.configure(api_key=api_key)

    # Allow model override via .env, default to 'gemini-2.0-flash-exp'
    model_name = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-exp")

    try:
        model = genai.GenerativeModel(model_name)
        logging.info(f"Successfully initialized model: {model_name}")
        return model
    except Exception as e:
        logging.warning(f"Primary model '{model_name}' failed ({e}). Falling back to 'gemini-1.5-flash'.")
        return genai.GenerativeModel('gemini-1.5-flash')