import sys

# Import from the app module
sys.path.insert(0, 'D:/Neuro_Data_Platform/Neuro-Agents/app')

from config import get_settings

settings = get_settings()
print(f"GROQ_QUERY_MODEL: {settings.GROQ_QUERY_MODEL}")
print(f"GROQ_API_KEY present: {bool(getattr(settings, 'GROQ_API_KEY', None))}")
print(f"GROQ_FALLBACK_MODEL: {settings.GROQ_FALLBACK_MODEL}")

# Test if we can create an LLMClient
from llm.client import LLMClient

try:
    client = LLMClient(model=settings.GROQ_QUERY_MODEL)
    print(f"\nLLMClient created successfully")
    print(f"Client model: {client._model}")
    
    # Try to make an API call
    print("\nTesting Groq API call...")
    result = client._chat("Test prompt", "Test user prompt", max_tokens=10)
    print(f"API call successful! Response: {result[:50]}...")
except Exception as e:
    print(f"\nError: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()