"""
Connector base interface.

All external data-source connectors (OpenNeuro, DANDI, etc.) must
implement this abstract class. The ingestion pipeline calls fetch()
and expects back a list of raw dicts that the normalizer will convert
into the canonical Dataset schema.
"""
from abc import ABC, abstractmethod


class BaseConnector(ABC):
    """Structural interface every source connector must satisfy."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """
        Human-readable repository name stored in Dataset.source_repository.
        Example: "openneuro" | "dandi"
        """
        ...

    @abstractmethod
    async def fetch(self, limit: int = 200) -> list[dict]:
        """
        Fetch raw metadata records from the upstream repository.

        Parameters
        ----------
        limit:
            Maximum number of records to pull in one call.

        Returns
        -------
        list[dict]
            Raw upstream records; field names vary by source. The
            normalizer handles the mapping to Common Schema.
        """
        ...
