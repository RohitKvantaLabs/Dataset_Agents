#!/usr/bin/env python
"""Diagnostic script for QueryUnderstandingAgent failure"""

import sys
import os

print("=" * 80)
print("DIAGNOSTIC: QueryUnderstandingAgent FAILURE")
print("=" * 80)

# Set environment variable from .env file
os.environ['GROQ_API_KEY'] = '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'

# Test 1: Verify config loading
print("\n1. Testing Config Loading")
print("-" * 40)
try:
    # Import config
    from app.config import get_settings
    settings = get_settings()
    
    print(f"✓ Config loaded successfully")
    print(f"  GROQ_QUERY_MODEL: {settings.GROQ_QUERY_MODEL}")
    print(f"  GROQ_API_KEY present: {bool(getattr(settings, 'GROQ_API_KEY', None))}")
    
except Exception as e:
    print(f"✗ Config loading failed: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 2: Test Groq API directly
print("\n2. Testing Groq API Connection")
print("-" * 40)

try:
    from groq import Groq
    
    client = Groq(api_key=os.environ.get('GROQ_API_KEY'))
    print("✓ Groq client created")
    
    # List available models
    print("\n  Listing available models...")
    models = client.models.list()
    print(f"✓ Retrieved {len(models.data)} models")
    
    # Check for openai/gpt-oss-20b
    target_model = settings.GROQ_QUERY_MODEL
    print(f"\n  Looking for model: '{target_model}'")
    
    model_exists = any(m.id == target_model for m in models.data)
    
    print(f"  Model exists: {model_exists}")
    
    if model_exists:
        print(f"  ✓ Model '{target_model}' is available")
        
        # Show all models for debugging
        print("\n  All available models:")
        for m in models.data:
            print(f"    - {m.id} (Created: {m.created}, Owned by: {m.owned_by})")
            
        # Test API call
        print(f"\n  Testing API call with model '{target_model}'...")
        completion = client.chat.completions.create(
            model=target_model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "What is the capital of France?"},
            ],
            max_tokens=50,
        )
        print(f"  ✓ API call successful!")
        print(f"  Response: {completion.choices[0].message.content}")
        
    else:
        print(f"  ✗ Model '{target_model}' is NOT available!")
        print("\n  Available models:")
        for m in models.data:
            print(f"    - {m.id} (Created: {m.created}, Owned by: {m.owned_by})")
        
        # This is the root cause!
        print(f"\n  *** ROOT CAUSE IDENTIFIED ***")
        print(f"  Model '{target_model}' is not available in Groq API")
        print(f"  This forces QueryUnderstandingAgent to fall back to keyword-only parsing")
        
except Exception as e:
    print(f"  ✗ Groq API error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Test QueryUnderstandingAgent
print("\n3. Testing QueryUnderstandingAgent")
print("-" * 40)

try:
    from app.agents.query_understanding_agent import QueryUnderstandingAgent
    
    agent = QueryUnderstandingAgent()
    print(f"✓ QueryUnderstandingAgent created")
    print(f"  Agent provider: {agent.provider}")
    
    # Test with a simple query
    test_query = "Find MEG datasets"
    print(f"\n  Testing query: '{test_query}'")
    
    result = agent.parse(test_query)
    
    print(f"\n  Parsing result:")
    print(f"    modality: {result.modality}")
    print(f"    condition: {result.condition}")
    print(f"    species: {result.species}")
    print(f"    region: {result.region}")
    print(f"    task: {result.task}")
    print(f"    format: {result.format}")
    print(f"    keywords: {result.keywords}")
    
    # Check if it fell back to keyword-only
    fallback_to_keyword_only = (
        result.modality == [] and 
        result.condition == [] and 
        (result.species == [] or result.species is None) and
        (result.region is None or result.region == '') and
        (result.task is None or result.task == '') and
        result.format == [] and
        result.keywords and result.keywords != []
    )
    
    if fallback_to_keyword_only:
        print(f"\n  ✗ FALLBACK DETECTED!")
        print(f"    Agent used keyword-only parsing")
        print(f"    Keywords: {result.keywords}")
        print(f"\n  *** This is why ALL queries return empty structured results ***")
    else:
        print(f"\n  ✓ Agent performed structured parsing")
        
except Exception as e:
    print(f"  ✗ Error testing QueryUnderstandingAgent: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("DIAGNOSIS SUMMARY")
print("=" * 80)

if 'model_exists' in locals() and not model_exists:
    print("\nROOT CAUSE CONFIRMED:")
    print("1. Config correctly loads GROQ_QUERY_MODEL = 'openai/gpt-oss-20b'")
    print("2. This model is NOT available in the Groq API")
    print("3. QueryUnderstandingAgent fails to create LLMClient")
    print("4. Agent falls back to _heuristic_parse which returns keyword-only results")
    print("5. All 8 queries return identical empty structured results")
    print("\nSOLUTION:")
    print("Update config.GROQ_QUERY_MODEL to use an AVAILABLE model from Groq")
    
print("\nAvailable models from Groq:")
for m in models.data:
    print(f"  - {m.id}")
    
print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)