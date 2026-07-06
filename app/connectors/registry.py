"""
Connector Registry — ingestion factory.

Maps string labels (used in cron endpoints and CLI scripts) to connector
class instances. Adding a new source requires only:
  1. Create a connector in app/connectors/
  2. Register it here with a unique string key.

Example usage
-------------
    from app.connectors.registry import get_connector
    connector = get_connector("dandi")
    records = await connector.fetch(limit=500)
"""
from app.connectors.base import BaseConnector
from app.connectors.dandi_connector import DandiConnector
from app.connectors.openneuro_connector import OpenNeuroConnector

# Registry mapping: string label → connector class (not instance).
# Instances are created on-demand so that HTTP clients are not shared
# across async tasks unless explicitly injected.
_REGISTRY: dict[str, type[BaseConnector]] = {
    "dandi": DandiConnector,
    "openneuro": OpenNeuroConnector,
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
