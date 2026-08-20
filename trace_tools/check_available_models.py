#!/usr/bin/env python
"""Quick test of Groq API to check available models"""

import os

# Set the API key from .env file
os.environ['GROQ_API_KEY'] = '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'

print("Checking available Groq models...")
print("=" * 50)

try:
    from groq import Groq
    
    # Create Groq client
    client = Groq(api_key=os.environ.get('GROQ_API_KEY'))
    print("✓ Groq client created successfully")
    
    # List all models
    models = client.models.list()
    print(f"✓ Retrieved {len(models.data)} models")
    
    # Check for specific models
    target_models = ["openai/gpt-oss-20b", "llama-3.1-8b-instant", "llama-3.3-70b-versatile"]
    
    print(f"\nChecking for target models:")
    for model_name in target_models:
        model_exists = any(m.id == model_name for m in models.data)
        status = "✓" if model_exists else "✗"
        print(f"  {status} '{model_name}': {'AVAILABLE' if model_exists else 'NOT AVAILABLE'}")
    
    # Show all available models
    print(f"\nAll available models:")
    for model in models.data:
        print(f"  - {model.id} (Created: {model.created}, Owned by: {model.owned_by})")
        
except Exception as e:
    print(f"\n✗ ERROR: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 50)
print("CONCLUSION:")
if any(m.id in ["llama-3.1-8b-instant", "llama-3.3-70b-versatile"] for m in models.data):
    print("✓ AVAILABLE MODELS FOUND - ready to update config")
else:
    print("✗ NO SUITABLE MODELS - cannot proceed with fix")
print("=" * 50)