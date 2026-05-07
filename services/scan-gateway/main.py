"""FactoryFlow Scan Gateway.

The gateway is intentionally thin:
- receives operator/device input
- creates workflow correlation metadata
- starts the distributed trace
- publishes the first workflow event

Business logic should not accumulate here.
"""

from __future__ import annotations

import hashlib
import logging
import os
from uuid import uuid4

from fastapi import FastAPI
from opentelemetry import trace
from pydantic import BaseModel

from shared.factoryflow.events import build_base_event
from shared.factoryflow.messaging import create_producer, publish_event
from shared.factoryflow.otel import setup_otel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scan-gateway")

setup_otel("scan-gateway")
tracer = trace.get_tracer(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
SCAN_TOPIC = "factory.scan.received"

producer = create_producer(KAFKA_BOOTSTRAP)
app = FastAPI(title="FactoryFlow Scan Gateway")


class ScanRequest(BaseModel):
    serial: str
    station: str
    operator: str | None = None


@app.post("/scans")
async def submit_scan(request: ScanRequest):
    correlation_id = str(uuid4())
    workflow_id = str(uuid4())

    # Store hashes in telemetry instead of raw serials.
    serial_hash = hashlib.md5(request.serial.encode()).hexdigest()

    with tracer.start_as_current_span("scan_received") as span:
        span.set_attribute("correlation_id", correlation_id)
        span.set_attribute("workflow_id", workflow_id)
        span.set_attribute("station", request.station)
        span.set_attribute("serial_hash", serial_hash)

        event = build_base_event(
            event_type=SCAN_TOPIC,
            correlation_id=correlation_id,
            workflow_id=workflow_id,
            session_id=request.station,
            source_service="scan-gateway",
            data={
                "serial": {
                    "hash": serial_hash,
                    "prefix": request.serial[:5],
                },
                "production": {
                    "station": request.station,
                },
                "operator": {
                    "name": request.operator,
                },
            },
        )

        # Kafka becomes the system integration boundary.
        publish_event(
            producer=producer,
            topic=SCAN_TOPIC,
            event=event,
            key=request.station,
        )

        logger.info(
            "Scan event published",
            extra={
                "correlation_id": correlation_id,
                "workflow_id": workflow_id,
                "topic": SCAN_TOPIC,
            },
        )

        return {
            "status": "accepted",
            "correlation_id": correlation_id,
            "workflow_id": workflow_id,
        }
