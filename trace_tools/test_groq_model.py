#!/usr/bin/env python
import os

# Set API key
os.environ['GROQ_API_KEY'] = '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'

print("Testing Groq API connection...")

try:
    from groq import Groq
    client = Groq(api_key=os.environ['GROQ_API_KEY'])
    print("✓ Groq client created successfully")
    
    # List models
    print("Listing available models...")
    models = client.models.list()
    print(f"✓ Retrieved {len(models.data)} models")
    
    # Check for target model
    target_model = "openai/gpt-oss-20b"
    model_exists = any(m.id == target_model for m in models.data)
    
    print(f"\nTarget model '{target_model}' exists: {model_exists}")
    
    if model_exists:
        print(f"✓ Model is available!")
        
        # Test API call
        print("Testing API call with target model...")
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
        print(f"✗ Model '{target_model}' is NOT available!")
        print("\nAvailable models:")
        for m in models.data:
            print(f"  - {m.id}")
            
except Exception as e:
    print(f"✗ ERROR: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\nTest complete.")