#!/usr/bin/env python
"""Test JSON mode reliability of openai/gpt-oss-120b."""
import os, sys, time, json

for mod in list(sys.modules.keys()):
    if 'app' in mod:
        del sys.modules[mod]

os.environ['GROQ_API_KEY'] = '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'
sys.path.insert(0, '.')

from groq import Groq
client = Groq(api_key=os.environ['GROQ_API_KEY'])

from app.llm.prompts import QUERY_UNDERSTANDING_SYSTEM_PROMPT, query_understanding_user_prompt

# Test 1: Simple JSON mode with explicit content
print("=== TEST 1: Simple explicit JSON ===")
for i in range(3):
    try:
        t0 = time.time()
        resp = client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=[
                {"role": "system", "content": "Return only valid JSON."},
                {"role": "user", "content": 'Return this exact JSON: {"modality": ["fMRI"], "condition": []}'},
            ],
            max_tokens=50,
            response_format={"type": "json_object"},
        )
        print(f"  Run {i+1}: {time.time()-t0:.2f}s -> {resp.choices[0].message.content}")
    except Exception as e:
        print(f"  Run {i+1}: FAIL {type(e).__name__}: {e}")
    time.sleep(0.5)

# Test 2: Actual query understanding prompt
print("\n=== TEST 2: Query understanding prompt (Find MEG datasets) ===")
for i in range(3):
    try:
        t0 = time.time()
        resp = client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=[
                {"role": "system", "content": QUERY_UNDERSTANDING_SYSTEM_PROMPT},
                {"role": "user", "content": query_understanding_user_prompt("Find MEG datasets")},
            ],
            max_tokens=500,
            response_format={"type": "json_object"},
        )
        elapsed = time.time() - t0
        raw = resp.choices[0].message.content
        parsed = json.loads(raw)
        print(f"  Run {i+1}: {elapsed:.2f}s -> {json.dumps(parsed)}")
    except Exception as e:
        print(f"  Run {i+1}: FAIL {type(e).__name__}: {e}")
    time.sleep(1)

# Test 3: Q8 which failed before
print("\n=== TEST 3: Parkinson's query ===")
for i in range(3):
    try:
        t0 = time.time()
        resp = client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=[
                {"role": "system", "content": QUERY_UNDERSTANDING_SYSTEM_PROMPT},
                {"role": "user", "content": query_understanding_user_prompt("Find Parkinson's disease neuroimaging datasets")},
            ],
            max_tokens=500,
            response_format={"type": "json_object"},
        )
        elapsed = time.time() - t0
        raw = resp.choices[0].message.content
        parsed = json.loads(raw)
        print(f"  Run {i+1}: {elapsed:.2f}s -> {json.dumps(parsed)}")
    except Exception as e:
        print(f"  Run {i+1}: FAIL {type(e).__name__}: {e}")
    time.sleep(1)

# Test 4: WITHOUT JSON mode - does it produce valid JSON anyway?
print("\n=== TEST 4: WITHOUT json_object mode ===")
for i in range(3):
    try:
        t0 = time.time()
        resp = client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=[
                {"role": "system", "content": QUERY_UNDERSTANDING_SYSTEM_PROMPT},
                {"role": "user", "content": query_understanding_user_prompt("Find MEG datasets")},
            ],
            max_tokens=500,
            temperature=0.1,
        )
        elapsed = time.time() - t0
        raw = resp.choices[0].message.content
        # Try parsing
        try:
            parsed = json.loads(raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip())
            print(f"  Run {i+1}: {elapsed:.2f}s -> VALID JSON: {json.dumps(parsed)}")
        except json.JSONDecodeError:
            print(f"  Run {i+1}: {elapsed:.2f}s -> NOT JSON: {raw[:200]}")
    except Exception as e:
        print(f"  Run {i+1}: FAIL {type(e).__name__}: {e}")
    time.sleep(1)
