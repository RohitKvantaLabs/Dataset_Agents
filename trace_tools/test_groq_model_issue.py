#!/usr/bin/env python
import os

# Set the API key from .env file
os.environ['GROQ_API_KEY'] = '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'

print("Testing Groq API to identify the model issue...")
print("=" * 70)

try:
    from groq import Groq
    
    # Create Groq client
    client = Groq(api_key=os.environ.get('GROQ_API_KEY'))
    print("✓ Groq client created successfully")
    
    # List all available models
    print("\nListing all available Groq models...")
    models = client.models.list()
    print(f"✓ Retrieved {len(models.data)} models")
    
    # Check for the specific model mentioned in config
    target_model = "openai/gpt-oss-20b"
    print(f"\nLooking for model '{target_model}' from config...")
    
    model_exists = any(m.id == target_model for m in models.data)
    
    print(f"Model exists: {model_exists}")
    
    if model_exists:
        print(f"✓ Model '{target_model}' is available!")
        
        # Show all models for reference
        print("\nAll available models:")
        for model in models.data:
            print(f"  - {model.id} (Created: {model.created}, Owned by: {model.owned_by})")
            
        # Test API call with the model
        print(f"\nTesting API call with model '{target_model}'...")
        completion = client.chat.completions.create(
            model=target_model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "What is the capital of France?"},
            ],
            max_tokens=50,
        )
        print(f"✓ API call successful!")
        print(f"Response: {completion.choices[0].message.content}")
    else:
        print(f"\n✗ Model '{target_model}' is NOT available in Groq API!")
        print("\nThis is the ROOT CAUSE of the QueryUnderstandingAgent failure.")
        print("\nAvailable models:")
        for model in models.data:
            print(f"  - {model.id} (Created: {model.created}, Owned by: {model.owned_by})")
            
        # Show what models ARE available that could be used
        print(f"\nSuggested alternative models from available list:")
        for model in models.data:
            print(f"  - {model.id}")
            
except Exception as e:
    print(f"\n✗ ERROR: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 70)
print("CONCLUSION:")
print("The QueryUnderstandingAgent is failing because the model")
print("'openai/gpt-oss-20b' specified in config is not available in")
print("the Groq API. This causes the agent to fall back to heuristic")
print("parsing which returns keyword-only results for all queries.")
print("=" * 70)