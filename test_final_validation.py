#!/usr/bin/env python
"""Final validation: 8 queries, latency, fallback verification."""
import os, sys, time, json, logging

# Clear cached modules for clean import
for mod in list(sys.modules.keys()):
    if 'app' in mod:
        del sys.modules[mod]

os.environ['GROQ_API_KEY'] = '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'
sys.path.insert(0, '.')

# Clear lru_cache to pick up current config
from app.config import get_settings
get_settings.cache_clear()
settings = get_settings()

print("=" * 70)
print("PRE-FLIGHT CHECKS")
print("=" * 70)
print(f"1. Effective GROQ_QUERY_MODEL: {settings.GROQ_QUERY_MODEL}")
assert settings.GROQ_QUERY_MODEL == "openai/gpt-oss-120b", f"WRONG MODEL: {settings.GROQ_QUERY_MODEL}"
print("   [OK] Correct model configured")

# 2. Groq API connection
from groq import Groq
client = Groq(api_key=os.environ['GROQ_API_KEY'])
print("2. Groq API connection: [OK]")

# 3. Model exists
models = client.models.list()
model_exists = any(m.id == "openai/gpt-oss-120b" for m in models.data)
print(f"3. Model in available list: {'[OK] YES' if model_exists else '[FAIL] NO'}")
assert model_exists, "openai/gpt-oss-120b not found!"

# 4. JSON mode support
try:
    t0 = time.time()
    resp = client.chat.completions.create(
        model="openai/gpt-oss-120b",
        messages=[
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": '{"test": true}'},
        ],
        max_tokens=20,
        response_format={"type": "json_object"},
    )
    json_time = time.time() - t0
    print(f"4. JSON mode supported: [OK] ({json_time:.2f}s) -> {resp.choices[0].message.content}")
except Exception as e:
    print(f"4. JSON mode supported: [FAIL] {type(e).__name__}: {e}")
    sys.exit(1)

# 5 & 6 are implicit from above (no 404, no 400)
print("5. No 404 model-not-found: [OK]")
print("6. No 400 unsupported-json-mode: [OK]")

print("\n" + "=" * 70)
print("INITIALIZING QueryUnderstandingAgent")
print("=" * 70)

from app.agents.query_understanding_agent import QueryUnderstandingAgent

agent = QueryUnderstandingAgent()
print(f"Provider: {agent.provider}")
print(f"Model: {agent._llm._model if agent._llm else 'N/A'}")
assert agent.provider == "groq", f"Expected 'groq', got '{agent.provider}'"
print("[OK] Agent initialized with Groq provider\n")

# ---- 8 VALIDATION QUERIES ----
queries = [
    "Find MEG datasets",
    "Find fMRI datasets",
    "Find EEG datasets related to epilepsy",
    "Find diffusion MRI datasets",
    "Find datasets about Alzheimer's disease",
    "Find resting-state fMRI datasets",
    "Find human EEG datasets",
    "Find Parkinson's disease neuroimaging datasets",
]

def check_fallback(r):
    """Check if result is keyword-only fallback (no structured fields)."""
    return (
        r.modality == [] and
        r.condition == [] and
        (r.species == [] or r.species is None) and
        (r.region is None or r.region == "") and
        (r.task is None or r.task == "") and
        r.format == [] and
        r.keywords and r.keywords != []
    )

results = []

print("=" * 70)
print("COLD PASS — 8 Queries")
print("=" * 70)

for i, q in enumerate(queries, 1):
    t0 = time.time()
    r = agent.parse(q)
    elapsed = time.time() - t0
    is_fallback = check_fallback(r)
    
    results.append({
        "query": q,
        "modality": r.modality,
        "condition": r.condition,
        "species": r.species,
        "task": r.task,
        "region": r.region,
        "age_range": r.age_range,
        "format": r.format,
        "keywords": r.keywords,
        "latency": elapsed,
        "fallback": is_fallback,
        "provider": agent.provider,
    })
    
    fb_str = "KEYWORD-FALLBACK" if is_fallback else "STRUCTURED"
    print(f"Q{i}: {elapsed:.2f}s | {fb_str} | mod={r.modality} cond={r.condition} spec={r.species} task={r.task} kw={r.keywords}")

# Wait 2 seconds to let rate limit reset
print("\n[Waiting 3s for rate limit reset...]")
time.sleep(3)

print("\n" + "=" * 70)
print("WARM PASS — 8 Queries")
print("=" * 70)

warm_latencies = []
for i, q in enumerate(queries, 1):
    t0 = time.time()
    r = agent.parse(q)
    elapsed = time.time() - t0
    warm_latencies.append(elapsed)
    is_fallback = check_fallback(r)
    fb_str = "KEYWORD-FALLBACK" if is_fallback else "STRUCTURED"
    print(f"Q{i}: {elapsed:.2f}s | {fb_str} | mod={r.modality} cond={r.condition} spec={r.species} task={r.task}")

# Print summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
cold_latencies = [r["latency"] for r in results]
cold_fallbacks = sum(1 for r in results if r["fallback"])
warm_fallbacks = sum(1 for w in warm_latencies if w > 20)  # rough heuristic

print(f"Cold pass: avg={sum(cold_latencies)/len(cold_latencies):.2f}s, min={min(cold_latencies):.2f}s, max={max(cold_latencies):.2f}s")
print(f"Warm pass: avg={sum(warm_latencies)/len(warm_latencies):.2f}s, min={min(warm_latencies):.2f}s, max={max(warm_latencies):.2f}s")
print(f"Cold fallbacks: {cold_fallbacks}/8")
print(f"Provider: {agent.provider}")
print(f"Model: {agent._llm._model}")
print("\nDone.")
