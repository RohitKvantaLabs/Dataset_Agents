#!/usr/bin/env python
"""Quick test of single query."""
import os, sys, time, json

# Clear cached app modules
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

query = "Find MEG datasets"
print(f"\nQuery: {query}")
t0 = time.time()
result = agent.parse(query)
elapsed = time.time() - t0
print(f"Latency: {elapsed:.3f}s")
print(f"Result: modality={result.modality}, condition={result.condition}, species={result.species}, task={result.task}, keywords={result.keywords}")
