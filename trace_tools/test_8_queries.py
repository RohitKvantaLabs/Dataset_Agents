#!/usr/bin/env python
"""Run all 8 validation queries."""
import os, sys, time, json

# Clear cached modules
for mod in list(sys.modules.keys()):
    if 'app' in mod:
        del sys.modules[mod]

os.environ['GROQ_API_KEY'] = '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'
sys.path.insert(0, '.')

from app.config import get_settings
get_settings.cache_clear()
settings = get_settings()
print(f"Model: {settings.GROQ_QUERY_MODEL}")

from app.agents.query_understanding_agent import QueryUnderstandingAgent
agent = QueryUnderstandingAgent()
print(f"Provider: {agent.provider}, Model: {agent._llm._model if agent._llm else 'N/A'}")

queries = [
    ("Find MEG datasets", ["MEG"]),
    ("Find fMRI datasets", ["fMRI"]),
    ("Find EEG datasets related to epilepsy", ["EEG", "epilepsy"]),
    ("Find diffusion MRI datasets", ["DTI"]),
    ("Find datasets about Alzheimer's disease", ["Alzheimer"]),
    ("Find resting-state fMRI datasets", ["fMRI", "resting"]),
    ("Find human EEG datasets", ["EEG", "human"]),
    ("Find Parkinson's disease neuroimaging datasets", ["Parkinson"]),
]

cold_latencies = []
warm_latencies = []

print("\n=== COLD PASS ===")
for i, (q, checks) in enumerate(queries, 1):
    t0 = time.time()
    r = agent.parse(q)
    elapsed = time.time() - t0
    cold_latencies.append(elapsed)
    
    # Check results
    all_text = json.dumps(r.model_dump()).lower()
    passed = all(c.lower() in all_text for c in checks)
    
    print(f"Q{i}: {elapsed:.2f}s | {'PASS' if passed else 'FAIL'} | mod={r.modality} cond={r.condition} spec={r.species} task={r.task} kw={r.keywords}")

print("\n=== WARM PASS ===")
for i, (q, checks) in enumerate(queries, 1):
    t0 = time.time()
    r = agent.parse(q)
    elapsed = time.time() - t0
    warm_latencies.append(elapsed)
    
    all_text = json.dumps(r.model_dump()).lower()
    passed = all(c.lower() in all_text for c in checks)
    print(f"Q{i}: {elapsed:.2f}s | {'PASS' if passed else 'FAIL'}")

print(f"\nCold avg: {sum(cold_latencies)/len(cold_latencies):.2f}s")
print(f"Warm avg: {sum(warm_latencies)/len(warm_latencies):.2f}s")
print(f"Provider: {agent.provider}")
print("Done.")
