from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from domain.common.interfaces import IngestionResult, RowError
from dynamic.registry import get_registered_page
from storage.repository import append_snapshot


class GlobalIngestor:
    REGION = "global"

    async def ingest(
        self,
        *,
        page_id: str,
        rows: List[Dict[str, Any]],
        session: AsyncSession,
        client_metadata: Optional[Dict[str, Any]] = None,
    ) -> IngestionResult:
        reg = get_registered_page(page_id)

        # Strict isolation: YAML must declare region=global
        if reg.page.region != self.REGION:
            raise ValueError(f"Page '{page_id}' is region='{reg.page.region}', not '{self.REGION}'.")

        settings = get_settings()
        max_rows = max(1, int(settings.snapshot_max_rows))
        rows_in = rows[:max_rows]

        model = reg.row_model
        errors: List[RowError] = []
        accepted: List[Dict[str, Any]] = []

        for i, row in enumerate(rows_in):
            try:
                obj = model.model_validate(row)
                accepted.append(obj.model_dump())
            except Exception as e:
                errors.append(RowError(index=i, field="*", message=str(e)))

        snapshot_payload = {
            "rows": accepted,
            "counts": {"received": len(rows), "validated": len(accepted)},
        }

        snapshot_id = await append_snapshot(
            session,
            page_id=page_id,
            region=self.REGION,
            schema_hash=reg.page.schema_hash,
            payload=snapshot_payload,
            metadata={
                "client": client_metadata or {},
                "rejected_count": len(errors),
            },
        )

        return IngestionResult(
            page_id=page_id,
            region=self.REGION,
            schema_hash=reg.page.schema_hash,
            accepted_count=len(accepted),
            rejected_count=len(errors),
            snapshot_ids=[snapshot_id],
            errors=errors[:50],
        )
