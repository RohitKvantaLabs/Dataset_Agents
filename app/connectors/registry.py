"""
Connector Registry — ingestion + retrieval factory.

Maps string labels (used in cron endpoints, CLI scripts, and the
repository retrieval service) to connector class instances. Adding a
new source requires only:
  1. Create a connector in app/connectors/
  2. Register it here with a unique string key.

Example usage
-------------
    from app.connectors.registry import get_connector, get_enabled_sources
    connector = get_connector("dandi")
    records = await connector.fetch(limit=500)
    enabled = get_enabled_sources()
"""
import importlib
import logging

from app.config import get_settings
from app.core.circuit_breaker import CircuitBreaker

logger = logging.getLogger("neuro_platform.connectors.registry")
from app.connectors.base import BaseConnector
from app.connectors.dandi_connector import DandiConnector
from app.connectors.dryad_connector import DryadConnector
from app.connectors.ebrains_connector import EBRAINSConnector
from app.connectors.figshare_connector import FigshareConnector
from app.connectors.neurovault_connector import NeuroVaultConnector
from app.connectors.nitrc_connector import NITRCConnector
from app.connectors.openneuro_connector import OpenNeuroConnector
from app.connectors.osf_connector import OSFConnector
from app.connectors.zenodo_connector import ZenodoConnector

# Registry mapping: string label → connector class (not instance).
# Instances are created on-demand so that HTTP clients are not shared
# across async tasks unless explicitly injected.
_REGISTRY: dict[str, type[BaseConnector]] = {
    "openneuro": OpenNeuroConnector,
    "dandi": DandiConnector,
    "neurovault": NeuroVaultConnector,
    "ebrains": EBRAINSConnector,
    "zenodo": ZenodoConnector,
    "figshare": FigshareConnector,
    "dryad": DryadConnector,
    "osf": OSFConnector,
    "nitrc": NITRCConnector,
}


def get_connector(source: str) -> BaseConnector:
    """
    Return a fresh connector instance for *source*.

    Parameters
    ----------
    source:
        Case-insensitive repository label (e.g. ``"dandi"``, ``"openneuro"``).

    Raises
    ------
    KeyError
        If *source* is not registered.
    """
    cls = _REGISTRY.get(source.lower())
    if cls is None:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Unknown connector {source!r}. Available sources: {available}"
        )
    return cls()


def list_sources() -> list[str]:
    """Return the sorted list of registered source labels."""
    return sorted(_REGISTRY)


def get_enabled_sources() -> list[str]:
    """
    Return the sources enabled by ``REPOSITORY_ENABLED_SOURCES`` config.

    Defaults to all nine registered sources; operators can disable a repo
    via env without code changes (§2.13). Unknown entries are ignored.
    """
    settings = get_settings()
    configured = [s.lower().strip() for s in settings.REPOSITORY_ENABLED_SOURCES]
    return [s for s in configured if s in _REGISTRY]


def get_circuit_state(source: str) -> str:
    """
    Return the current circuit state for *source*'s connector breaker
    ("closed" | "open" | "half_open" | "unknown").

    Used by the §4.6 repository-health endpoint for ops/admin. Connectors
    own a module-level ``CircuitBreaker(name=f"{source}-api")``; the module
    is imported lazily so this helper never raises and never pulls the
    connector in at app boot.
    """
    try:
        module = importlib.import_module(f"app.connectors.{source.lower()}_connector")
        for attr in vars(module).values():
            if isinstance(attr, CircuitBreaker) and attr.name == f"{source.lower()}-api":
                return attr.state.value
    except Exception as exc:  # noqa: BLE001 — health must never raise
        logger.warning("Could not read circuit state for %r: %s", source, exc)
    return "unknown"
