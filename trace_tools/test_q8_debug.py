#!/usr/bin/env python
"""Debug Q8 and check for intermittent failures."""
import os, sys, time, json, logging

for mod in list(sys.modules.keys()):
    if 'app' in mod:
        del sys.modules[mod]

os.environ['GROQ_API_KEY'] = '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'
sys.path.insert(0, '.')

logging.basicConfig(level=logging.DEBUG)

from app.config import get_settings
get_settings.cache_clear()
settings = get_settings()
print(f"Model: {settings.GROQ_QUERY_MODEL}")

from app.agents.query_understanding_agent import QueryUnderstandingAgent
agent = QueryUnderstandingAgent()
print(f"Provider: {agent.provider}")

def check_fallback(r):
    """Check if result is keyword-only fallback."""
    return (
        r.modality == [] and
        r.condition == [] and
        (r.species == [] or r.species is None) and
        (r.region is None or r.region == "") and
        (r.task is None or r.task == "") and
        r.format == [] and
        r.keywords and r.keywords != []
    )

# Run Q8 3 times
q8 = "Find Parkinson's disease neuroimaging datasets"
print(f"\nQuery: {q8}")
for run in range(1, 4):
    t0 = time.time()
    r = agent.parse(q8)
    elapsed = time.time() - t0
    fb = check_fallback(r)
    print(f"\nRun {run}: {elapsed:.2f}s | {'KEYWORD FALLBACK' if fb else 'STRUCTURED'}")
    print(f"  modality={r.modality} condition={r.condition} species={r.species} task={r.task} keywords={r.keywords}")

# Also test Q5 which had Alzheimer's
q5 = "Find datasets about Alzheimer's disease"
print(f"\n\nQuery: {q5}")
for run in range(1, 3):
    t0 = time.time()
    r = agent.parse(q5)
    elapsed = time.time() - t0
    fb = check_fallback(r)
    print(f"\nRun {run}: {elapsed:.2f}s | {'KEYWORD FALLBACK' if fb else 'STRUCTURED'}")
    print(f"  modality={r.modality} condition={r.condition} species={r.species} task={r.task} keywords={r.keywords}")
