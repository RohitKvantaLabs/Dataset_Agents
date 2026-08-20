#!/usr/bin/env python
"""Run QueryUnderstandingAgent against 8 validation queries."""
import os, sys, time, json, logging

# Clear cached modules
for mod in list(sys.modules.keys()):
    if 'app' in mod:
        del sys.modules[mod]

os.environ['GROQ_API_KEY'] = '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'
sys.path.insert(0, '.')

# Clear lru_cache
from app.config import get_settings
get_settings.cache_clear()
settings = get_settings()

print(f"Effective GROQ_QUERY_MODEL: {settings.GROQ_QUERY_MODEL}")

from app.agents.query_understanding_agent import QueryUnderstandingAgent

agent = QueryUnderstandingAgent()
print(f"Provider: {agent.provider}")
print(f"Model: {agent._llm._model if agent._llm else 'N/A'}")

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

def is_keyword_fallback(r):
    """True if result is keyword-only fallback with no structured fields."""
    return (
        r.modality == [] and
        r.condition == [] and
        (r.species == [] or r.species is None) and
        (r.region is None or r.region == "") and
        (r.task is None or r.task == "") and
        r.format == [] and
        r.keywords and r.keywords != []
    )

print("\n" + "=" * 70)
print("COLD PASS — 8 Queries (with 1.5s spacing)")
print("=" * 70)

cold_results = []
for i, q in enumerate(queries, 1):
    t0 = time.time()
    r = agent.parse(q)
    elapsed = time.time() - t0
    fb = is_keyword_fallback(r)
    
    cold_results.append({
        "query": q, "latency": elapsed, "fallback": fb,
        "modality": r.modality, "condition": r.condition,
        "species": r.species, "task": r.task, "region": r.region,
        "age_range": r.age_range, "format": r.format, "keywords": r.keywords,
    })
    
    status = "KEYWORD-FALLBACK" if fb else "STRUCTURED"
    print(f"\nQ{i}: {elapsed:.2f}s | {status}")
    print(f"  Query: \"{q}\"")
    print(f"  modality={r.modality}")
    print(f"  condition={r.condition}")
    print(f"  species={r.species}")
    print(f"  task={r.task}")
    print(f"  region={r.region}")
    print(f"  age_range={r.age_range}")
    print(f"  format={r.format}")
    print(f"  keywords={r.keywords}")
    
    if i < len(queries):
        time.sleep(1.5)

# Wait for rate limit reset
print("\n[Waiting 5s for rate limit reset before warm pass...]")
time.sleep(5)

print("\n" + "=" * 70)
print("WARM PASS — 8 Queries (with 1.5s spacing)")
print("=" * 70)

warm_results = []
for i, q in enumerate(queries, 1):
    t0 = time.time()
    r = agent.parse(q)
    elapsed = time.time() - t0
    fb = is_keyword_fallback(r)
    
    warm_results.append({"latency": elapsed, "fallback": fb})
    status = "KEYWORD-FALLBACK" if fb else "STRUCTURED"
    print(f"Q{i}: {elapsed:.2f}s | {status} | mod={r.modality} cond={r.condition}")
    
    if i < len(queries):
        time.sleep(1.5)

# Summary
print("\n" + "=" * 70)
print("LATENCY SUMMARY")
print("=" * 70)
cold_lat = [r["latency"] for r in cold_results]
warm_lat = [r["latency"] for r in warm_results]
cold_fb = sum(1 for r in cold_results if r["fallback"])
warm_fb = sum(1 for r in warm_results if r["fallback"])

print(f"Cold: avg={sum(cold_lat)/len(cold_lat):.2f}s min={min(cold_lat):.2f}s max={max(cold_lat):.2f}s")
print(f"Warm: avg={sum(warm_lat)/len(warm_lat):.2f}s min={min(warm_lat):.2f}s max={max(warm_lat):.2f}s")
print(f"Cold fallbacks: {cold_fb}/8")
print(f"Warm fallbacks: {warm_fb}/8")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
