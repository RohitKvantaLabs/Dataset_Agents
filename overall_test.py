# const datasetSchema = new mongoose.Schema({
#   title: { type: String, required: true },
#   description: { type: String, required: true },
#   source_repository: { type: String, required: true },
#   original_url: { type: String, required: true, unique: true },
#   modalities: [{ type: String }], 
#   subject_count: { type: Number },
#   species: { type: String },
#   license: { type: String },
#   is_bids_compliant: { type: Boolean, default: false },
#   quality_score: { type: Number },
#   flags: {
#     green: [{ type: String }],
#     red: [{ type: String }]
#   },
#   last_indexed: { type: Date, default: Date.now }
# });

# // Required for fast text-based searching
# datasetSchema.index({ title: 'text', description: 'text' });

# --- RAW TERMINAL OUTPUTS (pasted) ---
# --- DIR app\\db\\indexes.py ---
#
#
#     Directory: D:\\Neuro-Agents\\app\\db
#
# Mode                 LastWriteTime         Length Name                         
# ----                 -------------         ------ ----                         
# -a----          7/4/2026  10:27 AM           1344 indexes.py                   
# --- GIT LOG --follow -- app/db/indexes.py ---
# commit d456d304a43f274420e607c04a48d8865bb5cae2
# Author: Ankit10M <ankitbrijeshmishra10@gmail.com>
# Date:   Mon Jul 6 11:28:08 2026 +0530
#
#     first commit
# --- TYPE app\\db\\indexes.py ---
# """
# app/db/indexes.py â€” MongoDB index creation, called from main.py lifespan.
#
# The actual index definitions live here so they can be called on startup
# (ensuring a warm Vercel instance always has indexes) and also run
# standalone via scripts/ensure_indexes.py against Atlas before go-live.
# """
# import logging
#
# from app.db.mongo import get_db
# from app.db.repositories.dataset_repository import COLLECTION_NAME
#
# logger = logging.getLogger("neuro_platform.db.indexes")
#
#
# async def ensure_indexes() -> None:
#     """Create all required MongoDB indexes if they don't already exist.
#
#     Uses ``create_index`` which is idempotent â€” safe to call on every
#     startup. The unique index on (source, source_id) is the hard backstop
#     for concurrent fallback writes (the application-level upsert is the
#     first line of defence; this is belt-and-suspenders).
#     """
#     db = get_db()
#     collection = db[COLLECTION_NAME]
#
#     await collection.create_index(
#         [("source", 1), ("source_id", 1)],
#         unique=True,
#         name="uniq_source_source_id",
#     )
#     await collection.create_index([("url", 1)], name="url_lookup")
#     await collection.create_index(
#         [("trust_tier", 1), ("last_verified_at", 1)],
#         name="verification_cadence",
#     )
#     logger.info("MongoDB indexes verified/created on collection=%s", COLLECTION_NAME)
# --- TYPE scripts\\ensure_indexes.py ---
# """
# Standalone runner: ensure MongoDB indexes exist on your Atlas cluster.
#
# Run this once before taking any real fallback traffic:
#
#     python -m scripts.ensure_indexes
#
# Or, if you have a local Mongo instance configured in .env:
#
#     python scripts/ensure_indexes.py
#
# The index definitions live in app/db/indexes.py and are also called
# automatically on every app startup (main.py lifespan hook), so this
# script is mainly for one-off Atlas bootstrapping or CI pre-flight checks.
# """
# import asyncio
# import logging
#
# logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
#
# from app.db.indexes import ensure_indexes  # noqa: E402 â€” after logging setup
#
#
# if __name__ == "__main__":
#     asyncio.run(ensure_indexes())
# --- TYPE app\\main.py ---
# """
# FastAPI application factory.
#
# Lifecycle
# ---------
# startup  â†’ no eager startup work
# shutdown â†’ close the Motor client connection pool
#
# CORS is configured to allow the MERN frontend origin (adjust
# ALLOWED_ORIGINS in config if needed for production).
# """
# import logging
# from contextlib import asynccontextmanager
#
# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
#
# from app.api.v1.router import router as api_v1_router
# from app.config import get_settings
# from app.db.mongo import get_client
#
# logger = logging.getLogger("neuro_platform.main")
#
#
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     yield
#     logger.info("Shutting down â€” closing MongoDB connection pool...")
#     try:
#         get_client().close()
#     except Exception as exc:  # noqa: BLE001
#         logger.warning("Error closing Motor client: %s", exc)
#
#
# def create_app() -> FastAPI:
#     settings = get_settings()
#
#     app = FastAPI(
#         title=settings.APP_NAME,
#         description=(
#             "Aggregated search and discovery engine for global "
#             "neuroscience and neuroimaging datasets."
#         ),
#         version="1.0.0",
#         lifespan=lifespan,
#     )
#
#     # CORS â€” allow the MERN/Next.js frontend and Vercel preview URLs.
#     # Tighten allow_origins in production to your specific domain(s).
#     app.add_middleware(
#         CORSMiddleware,
#         allow_origins=["*"],   # tighten in prod
#         allow_credentials=True,
#         allow_methods=["*"],
#         allow_headers=["*"],
#     )
#
#     app.include_router(api_v1_router, prefix=settings.API_V1_PREFIX)
#
#     return app
#
#
# app = create_app()

# --- RECENT COMMAND OUTPUTS (git log --oneline and ensure_indexes run) ---
# 6b5cb13 (HEAD -> main) docs: remove false claims of automatic index creation on startup
# 6f037af docs: remove false claims of automatic index creation on startup
# f12b3dd (origin/main) fix: restore lifespan-only startup/shutdown, remove eager index creation and deprecated on_event handler
# d456d30 first commit
# INFO neuro_platform.db.indexes: MongoDB indexes verified/created on collection=datasets