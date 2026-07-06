"""
Standalone runner: ensure MongoDB indexes exist on your Atlas cluster.

Run this once before taking any real fallback traffic:

    python -m scripts.ensure_indexes

Or, if you have a local Mongo instance configured in .env:

    python scripts/ensure_indexes.py

The index definitions live in app/db/indexes.py and are also called
automatically on every app startup (main.py lifespan hook), so this
script is mainly for one-off Atlas bootstrapping or CI pre-flight checks.
"""
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

from app.db.indexes import ensure_indexes  # noqa: E402 — after logging setup


if __name__ == "__main__":
    asyncio.run(ensure_indexes())
