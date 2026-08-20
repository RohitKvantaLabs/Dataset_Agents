#!/usr/bin/env python
"""Test available models with the actual query understanding prompt."""
import os
import sys
import json
import time

os.environ['GROQ_API_KEY'] = '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'
sys.path.insert(0, '.')

from groq import Groq

client = Groq(api_key=os.environ['GROQ_API_KEY'])

# Load the actual system prompt
from app.llm.prompts import QUERY_UNDERSTANDING_SYSTEM_PROMPT, query_understanding_user_prompt

# Test models that ARE available for text generation
test_models = [
    "openai/gpt-oss-20b",
    "openai/gpt-oss-120b",
    "qwen/qwen3.6-27b",
]

test_queries = [
    "Find MEG datasets",
    "Find fMRI datasets",
    "Find EEG datasets related to epilepsy",
    "Find diffusion MRI datasets",
    "Find datasets about Alzheimer's disease",
    "Find resting-state fMRI datasets",
    "Find human EEG datasets",
    "Find Parkinson's disease neuroimaging datasets",
]

for model_id in test_models:
    print("=" * 70)
    print(f"MODEL: {model_id}")
    print("=" * 70)

    # First test: simple JSON
    print("\n--- Simple JSON test ---")
    try:
        t0 = time.time()
        completion = client.chat.completions.create(
            model=model_id,
            messages=[
                {"role": "system", "content": "Return only valid JSON."},
                {"role": "user", "content": 'Return exactly: {"modality": ["fMRI"], "condition": []}'},
            ],
            max_tokens=100,
            temperature=0.1,
        )
        elapsed = time.time() - t0
        print(f"[OK] ({elapsed:.2f}s) {completion.choices[0].message.content}")
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        continue  # Skip to next model if basic test fails

    # Second test: JSON mode
    print("\n--- JSON mode test ---")
    try:
        t0 = time.time()
        completion = client.chat.completions.create(
            model=model_id,
            messages=[
                {"role": "system", "content": "Return only valid JSON."},
                {"role": "user", "content": 'Return exactly: {"modality": ["fMRI"], "condition": []}'},
            ],
            max_tokens=100,
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        elapsed = time.time() - t0
        print(f"[OK] ({elapsed:.2f}s) {completion.choices[0].message.content}")
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")

    # Third test: actual query understanding prompt
    print("\n--- Actual query understanding test ---")
    for q in test_queries[:2]:  # Just 2 queries to save time
        try:
            t0 = time.time()
            completion = client.chat.completions.create(
                model=model_id,
                messages=[
                    {"role": "system", "content": QUERY_UNDERSTANDING_SYSTEM_PROMPT},
                    {"role": "user", "content": query_understanding_user_prompt(q)},
                ],
                max_tokens=500,
                temperature=0.1,
                response_format={"type": "json_object"},
            )
            elapsed = time.time() - t0
            raw = completion.choices[0].message.content
            print(f"  Query: {q}")
            print(f"  Time: {elapsed:.2f}s")
            print(f"  Raw: {raw}")
            try:
                parsed = json.loads(raw.strip())
                print(f"  Parsed: {json.dumps(parsed, indent=2)}")
            except json.JSONDecodeError as je:
                print(f"  JSON DECODE ERROR: {je}")
        except Exception as e:
            print(f"  Query: {q}")
            print(f"  [ERROR] {type(e).__name__}: {e}")

    print()

print("=" * 70)
print("SUMMARY OF AVAILABLE MODELS:")
print("  - openai/gpt-oss-20b: available")
print("  - openai/gpt-oss-120b: available")
print("  - qwen/qwen3.6-27b: available")
print("  - llama-3.1-8b-instant: NOT available (404)")
print("  - llama-3.3-70b-versatile: NOT available (404)")
print("=" * 70)
