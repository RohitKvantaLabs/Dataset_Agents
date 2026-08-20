import sys
sys.path.insert(0, '.')

from app.config import get_settings
settings = get_settings()

print('Config loaded successfully')
print(f'Config GROQ_API_KEY: {settings.GROQ_API_KEY[:10]}...' if settings.GROQ_API_KEY else 'None')
print(f'Config GROQ_QUERY_MODEL: {settings.GROQ_QUERY_MODEL}')
print(f'Config GROQ_FALLBACK_MODEL: {settings.GROQ_FALLBACK_MODEL}')

# Test LLMClient initialization
from app.llm.client import LLMClient
print('\nTesting LLMClient initialization...')
try:
    client = LLMClient(model=settings.GROQ_QUERY_MODEL)
    print('LLMClient initialized successfully')
    print(f'Client model: {client._model}')
    
    # Test the actual chat method
    print('Testing actual Groq API call...')
    result = client._chat('Test prompt', 'Test user prompt', max_tokens=10)
    print(f'Chat succeeded: {result[:50]}...')
except Exception as e:
    print(f'Error: {type(e).__name__}: {e}')
    import traceback
    traceback.print_exc()