#!/usr/bin/env python
import sys
import os

# Add Neuro-Agents to Python path
sys.path.insert(0, 'D:/Neuro_Data_Platform/Neuro-Agents')

# Load environment variables
from dotenv import load_dotenv
load_dotenv('D:/Neuro_Data_Platform/Neuro-Agents/.env')

print("=== Testing Groq API Connection ===")
print(f"GROQ_API_KEY present: {bool(os.environ.get('GROQ_API_KEY'))}")

# Try to import and test the config
print("\n=== Testing Config Loading ===")
try:
    from app.config import get_settings
    settings = get_settings()
    print(f"GROQ_QUERY_MODEL: {settings.GROQ_QUERY_MODEL}")
    print(f"GROQ_API_KEY loaded: {bool(getattr(settings, 'GROQ_API_KEY', None))}")
except Exception as e:
    print(f"Config loading error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test Groq API directly
print("\n=== Testing Groq API Directly ===")
try:
    from groq import Groq
    client = Groq(api_key=os.environ.get('GROQ_API_KEY'))
    
    # List models
    print("Listing available Groq models...")
    models = client.models.list()
    
    print(f"Found {len(models.data)} models")
    
    # Check for our target model
    target_model = "openai/gpt-oss-20b"
    model_exists = any(m.id == target_model for m in models.data)
    
    print(f"\nTarget model '{target_model}' exists: {model_exists}")
    
    if not model_exists:
        print("\n=== Available models ===")
        for model in models.data:
            print(f"  - {model.id} (Created: {model.created})")
    
    # Test API call with a simple prompt
    print("\n=== Testing API Call ===")
    if model_exists:
        completion = client.chat.completions.create(
            model=target_model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "What is the capital of France?"},
            ],
            max_tokens=50,
        )
        print(f"Response: {completion.choices[0].message.content}")
    else:
        print("Skipping API call - model not available")
        
except Exception as e:
    print(f"Groq API error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n=== Test Complete ===")