"""
Canonical NeuroSearch dataset catalog — isolated infrastructure.

This package builds and maintains the canonical dataset catalog
(``neurosearch_dataset_catalog`` collection) that will eventually back
NeuroSearch's unified search across multiple neuroscience repositories
(OpenNeuro, DANDI, NEMAR, NITRC, EBRAINS, ...).

Isolation guarantees
--------------------
- Nothing in this package reads or writes the production ``datasets``
  collection or any search / parser / ranking / orchestrator code.
- The catalog lives in its own MongoDB collection and is not wired into
  the production search pipeline (integration happens in a later phase).
- No dataset files are ever downloaded — metadata only.
- No values are fabricated: missing metadata stays null / empty, and every
  normalized value preserves its provenance (per-repository ``sources[]``
  plus untouched ``rawMetadata.<repository>``).

Modules
-------
- ``schema``       canonical schema constants, derived-field rules, validation
- ``normalize``    repository payload → per-source canonical record
- ``dedup``        deterministic cross-repository identity + merge rules
- ``persistence``  catalog collection indexes + upsert/merge
- ``ingest``       full-repository metadata ingestion orchestration
- ``report``       ingestion / dedup / coverage / normalization / integrity report
"""

__version__ = "0.1.0"
