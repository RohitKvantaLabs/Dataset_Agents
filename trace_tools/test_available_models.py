#!/usr/bin/env python
"""Quick check of available Groq models - no Unicode characters."""
import os
import sys

os.environ['GROQ_API_KEY'] = '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'

print("=" * 60)
print("GROQ AVAILABLE MODELS CHECK")
print("=" * 60)

try:
    from groq import Groq
    client = Groq(api_key=os.environ['GROQ_API_KEY'])
    print("[OK] Groq client created")

    models = client.models.list()
    print(f"[OK] Retrieved {len(models.data)} models")

    # Check for specific target models
    targets = [
        "openai/gpt-oss-20b",
        "llama-3.1-8b-instant",
        "llama-3.3-70b-versatile",
        "llama-3.1-70b-versatile",
        "llama-3.1-8b-instant",
        "mixtral-8x7b-32768",
    ]

    print("\nTarget model availability:")
    for t in targets:
        exists = any(m.id == t for m in models.data)
        status = "AVAILABLE" if exists else "NOT AVAILABLE"
        print(f"  [{status:14s}] {t}")

    print("\nAll available models:")
    for m in sorted(models.data, key=lambda x: x.id):
        print(f"  - {m.id}")

except Exception as e:
    print(f"[ERROR] {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# Now test if current config model works
print("\n" + "=" * 60)
print("TEST CURRENT CONFIG MODEL: llama-3.3-70b-versatile")
print("=" * 60)
try:
    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": 'Return: {"modality": ["fMRI"]}'},
        ],
        max_tokens=50,
        response_format={"type": "json_object"},
    )
    print(f"[OK] Response: {completion.choices[0].message.content}")
except Exception as e:
    print(f"[ERROR] {type(e).__name__}: {e}")

# Test llama-3.1-8b-instant specifically
print("\n" + "=" * 60)
print("TEST TARGET MODEL: llama-3.1-8b-instant")
print("=" * 60)
try:
    completion = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": 'Return: {"modality": ["EEG"]}'},
        ],
        max_tokens=50,
        response_format={"type": "json_object"},
    )
    print(f"[OK] Response: {completion.choices[0].message.content}")
except Exception as e:
    print(f"[ERROR] {type(e).__name__}: {e}")

# Test openai/gpt-oss-20b
print("\n" + "=" * 60)
print("TEST PREVIOUS MODEL: openai/gpt-oss-20b")
print("=" * 60)
try:
    completion = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": 'Return: {"modality": ["MEG"]}'},
        ],
        max_tokens=50,
        response_format={"type": "json_object"},
    )
    print(f"[OK] Response: {completion.choices[0].message.content}")
except Exception as e:
    print(f"[ERROR] {type(e).__name__}: {e}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
