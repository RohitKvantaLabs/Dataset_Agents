"""
Controlled vocabularies + SPDX license map — STRUCTURE ONLY (§3.3, §4.8).

Per execution instructions the vocabulary **data is intentionally
unimplemented**: no term is authored or inferred until the approved
lists are provided. This module pins down the expected shapes and the
consumption contract so Stage 3 of the quality pipeline and tests can
depend on it today; every structure below is empty and therefore a
no-op at runtime (exact-match only — empty vocab means no backfill).

Usage contract
--------------
- ``MODALITY_VOCAB``  ``dict[str, list[str]]``  label -> accepted raw tokens (lowercase)
- ``SPECIES_VOCAB``   ``dict[str, list[str]]``  label -> accepted raw tokens (lowercase)
- ``REGION_TERMS``    ``list[str]``             accepted region tokens (lowercase)
- ``AGE_TERMS``       ``dict[str, list[str]]``  label -> accepted raw tokens (lowercase)
- ``DISEASE_TERMS``   ``dict[str, list[str]]``  label -> accepted raw tokens (lowercase)
- ``LICENSE_SPDX_MAP`` ``dict[str, str]``       raw license string -> SPDX id

Stage 3 reads these via exact (word-boundary) token matching and only
sets a field when a vocabulary token matches — never LLM, never inferred.
"""

# UNIMPLEMENTED — pending approved vocabulary specification.
MODALITY_VOCAB: dict[str, list[str]] = {}

# UNIMPLEMENTED — pending approved vocabulary specification.
SPECIES_VOCAB: dict[str, list[str]] = {}

# UNIMPLEMENTED — pending approved vocabulary specification.
REGION_TERMS: list[str] = []

# UNIMPLEMENTED — pending approved vocabulary specification.
AGE_TERMS: dict[str, list[str]] = {}

# UNIMPLEMENTED — pending approved vocabulary specification.
DISEASE_TERMS: dict[str, list[str]] = {}

# UNIMPLEMENTED — pending approved SPDX license map.
LICENSE_SPDX_MAP: dict[str, str] = {}
