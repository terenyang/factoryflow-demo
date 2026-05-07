from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_base_event(
    event_type: str,
    correlation_id: str,
    source_service: str,
    session_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "correlation_id": correlation_id,
        "workflow_id": workflow_id,
        "session_id": session_id,
        "timestamp": utc_now(),
        "source": {
            "service": source_service,
            "version": "0.1.0",
        },
        "data": data or {},
    }
