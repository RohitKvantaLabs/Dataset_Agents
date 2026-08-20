#!/usr/bin/env python
"""Full validation of QueryUnderstandingAgent with openai/gpt-oss-120b."""
import os
import sys
import json
import time
import logging

# Force reimport by clearing any cached modules
for mod_name in list(sys.modules.keys()):
    if 'app' in mod_name:
        del sys.modules[mod_name]

os.environ['GROQ_API_KEY'] = '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'
sys.path.insert(0, '.')

# Clear lru_cache to pick up new config
from app.config import get_settings
get_settings.cache_clear()
settings = get_settings()

print("=" * 70)
print("CONFIGURATION VERIFICATION")
print("=" * 70)
print(f"GROQ_QUERY_MODEL: {settings.GROQ_QUERY_MODEL}")
assert settings.GROQ_QUERY_MODEL == "openai/gpt-oss-120b", "Config not updated!"
print("[OK] Config verified: openai/gpt-oss-120b")

# Import agent
from app.agents.query_understanding_agent import QueryUnderstandingAgent
from app.models.query_filters import QueryFilters

print("\n" + "=" * 70)
print("AGENT INITIALIZATION")
print("=" * 70)
agent = QueryUnderstandingAgent()
print(f"Provider: {agent.provider}")
print(f"Model: {agent._llm._model if agent._llm else 'None'}")
assert agent.provider == "groq", f"Expected 'groq', got '{agent.provider}'"
print("[OK] Agent initialized with Groq provider")

# Define test queries and expected values
test_queries = [
    {
        "query": "Find MEG datasets",
        "expect": {"modality_contains": "MEG"},
        "description": "MEG modality extraction",
    },
    {
        "query": "Find fMRI datasets",
        "expect": {"modality_contains": "fMRI"},
        "description": "fMRI modality extraction",
    },
    {
        "query": "Find EEG datasets related to epilepsy",
        "expect": {"modality_contains": "EEG", "condition_contains": "epilepsy"},
        "description": "EEG modality + epilepsy condition",
    },
    {
        "query": "Find diffusion MRI datasets",
        "expect": {"modality_any_of": ["DTI", "DWI", "diffusion"]},
        "description": "Diffusion MRI intent",
    },
    {
        "query": "Find datasets about Alzheimer's disease",
        "expect": {"condition_contains": "Alzheimer"},
        "description": "Alzheimer's condition extraction",
    },
    {
        "query": "Find resting-state fMRI datasets",
        "expect": {"modality_contains": "fMRI", "task_contains": "resting"},
        "description": "fMRI modality + resting-state task",
    },
    {
        "query": "Find human EEG datasets",
        "expect": {"modality_contains": "EEG", "species_contains": "human"},
        "description": "EEG modality + human species",
    },
    {
        "query": "Find Parkinson's disease neuroimaging datasets",
        "expect": {"condition_any_of": ["parkinson", "Parkinson"]},
        "description": "Parkinson's condition + neuroimaging intent",
    },
]

print("\n" + "=" * 70)
print("QUERY PARSING TESTS")
print("=" * 70)

results = []
all_pass = True

for i, tc in enumerate(test_queries, 1):
    query = tc["query"]
    expect = tc["expect"]
    desc = tc["description"]

    print(f"\n--- Test {i}: {desc} ---")
    print(f"Query: \"{query}\"")

    # Cold call latency
    t0 = time.time()
    result = agent.parse(query)
    elapsed = time.time() - t0

    print(f"Latency: {elapsed:.3f}s")
    print(f"Provider: {agent.provider}")
    print(f"Output: modality={result.modality}, condition={result.condition}, species={result.species}, task={result.task}, region={result.region}, format={result.format}, keywords={result.keywords}")

    # Validate expectations
    passed = True
    failures = []

    modality_str = json.dumps(result.modality).lower()
    condition_str = json.dumps(result.condition).lower()
    species_str = json.dumps(result.species).lower()
    task_str = (result.task or "").lower()

    if "modality_contains" in expect:
        val = expect["modality_contains"].lower()
        if val not in modality_str:
            passed = False
            failures.append(f"modality should contain '{expect['modality_contains']}', got {result.modality}")

    if "modality_any_of" in expect:
        if not any(v.lower() in modality_str for v in expect["modality_any_of"]):
            passed = False
            failures.append(f"modality should contain any of {expect['modality_any_of']}, got {result.modality}")

    if "condition_contains" in expect:
        val = expect["condition_contains"].lower()
        if val not in condition_str:
            passed = False
            failures.append(f"condition should contain '{expect['condition_contains']}', got {result.condition}")

    if "condition_any_of" in expect:
        if not any(v.lower() in condition_str for v in expect["condition_any_of"]):
            passed = False
            failures.append(f"condition should contain any of {expect['condition_any_of']}, got {result.condition}")

    if "species_contains" in expect:
        val = expect["species_contains"].lower()
        if val not in species_str:
            passed = False
            failures.append(f"species should contain '{expect['species_contains']}', got {result.species}")

    if "task_contains" in expect:
        val = expect["task_contains"].lower()
        if val not in task_str:
            passed = False
            failures.append(f"task should contain '{expect['task_contains']}', got {result.task}")

    status = "PASS" if passed else "FAIL"
    if not passed:
        all_pass = False
        for f in failures:
            print(f"  FAIL: {f}")

    print(f"Status: {status}")

    results.append({
        "query": query,
        "modality": result.modality,
        "condition": result.condition,
        "species": result.species,
        "task": result.task,
        "region": result.region,
        "format": result.format,
        "keywords": result.keywords,
        "latency": elapsed,
        "status": status,
        "provider": agent.provider,
    })

# Second pass for warm latency
print("\n" + "=" * 70)
print("WARM LATENCY (second pass)")
print("=" * 70)

warm_latencies = []
for i, tc in enumerate(test_queries, 1):
    t0 = time.time()
    result = agent.parse(tc["query"])
    elapsed = time.time() - t0
    warm_latencies.append(elapsed)
    print(f"Query {i}: {elapsed:.3f}s")

# Summary
print("\n" + "=" * 70)
print("FALLBACK VERIFICATION")
print("=" * 70)
print(f"Agent provider: {agent.provider}")
print(f"Agent LLM client: {agent._llm._model if agent._llm else 'None'}")

# Check if any result was keyword-only fallback
keyword_fallback_count = 0
for r in results:
    is_fallback = (
        r["modality"] == [] and
        r["condition"] == [] and
        (r["species"] == [] or r["species"] is None) and
        (r["region"] is None or r["region"] == "") and
        (r["task"] is None or r["task"] == "") and
        r["format"] == [] and
        r["keywords"] and r["keywords"] != []
    )
    if is_fallback:
        keyword_fallback_count += 1
        print(f"  FALLBACK detected for: {r['query']}")

if keyword_fallback_count == 0:
    print("[OK] No keyword-only fallback detected in any query")
else:
    print(f"[WARN] {keyword_fallback_count} queries fell back to keyword-only")

# Latency summary
print("\n" + "=" * 70)
print("LATENCY SUMMARY")
print("=" * 70)
cold_latencies = [r["latency"] for r in results]
print(f"Cold calls: min={min(cold_latencies):.3f}s, max={max(cold_latencies):.3f}s, avg={sum(cold_latencies)/len(cold_latencies):.3f}s")
print(f"Warm calls: min={min(warm_latencies):.3f}s, max={max(warm_latencies):.3f}s, avg={sum(warm_latencies)/len(warm_latencies):.3f}s")

print("\n" + "=" * 70)
print("OVERALL RESULT")
print("=" * 70)
if all_pass:
    print("[OK] ALL 8 QUERIES PASSED")
else:
    print("[FAIL] SOME QUERIES FAILED - see details above")

print("\nDone.")
