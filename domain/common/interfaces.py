from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class RowError:
    index: int
    field: str
    message: str


@dataclass
class IngestionResult:
    page_id: str
    region: str
    schema_hash: str
    accepted_count: int
    rejected_count: int
    snapshot_ids: List[int]
    errors: List[RowError]


class IngestionStrategy(Protocol):
    async def ingest(
        self,
        *,
        page_id: str,
        rows: List[Dict[str, Any]],
        session: AsyncSession,
        client_metadata: Optional[Dict[str, Any]] = None,
    ) -> IngestionResult: ...
