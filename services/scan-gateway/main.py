"""HTTP entry point for synthetic factory scans."""

from __future__ import annotations

import hashlib
import logging
import os
from uuid import uuid4

from fastapi import FastAPI
from opentelemetry import trace
from pydantic import BaseModel, Field

from factoryflow.events import build_base_event
from factoryflow.messaging import create_producer, publish_event
from factoryflow.otel import setup_otel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scan-gateway")

SERVICE_NAME = "scan-gateway"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")
SCAN_TOPIC = os.getenv("SCAN_TOPIC", "factory.scan.received")

setup_otel(SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT)
tracer = trace.get_tracer(__name__)
producer = create_producer(KAFKA_BOOTSTRAP)

app = FastAPI(title="FactoryFlow Scan Gateway")


class ScanRequest(BaseModel):
    serial: str = Field(min_length=1)
    station: str = Field(min_length=1)
    operator: str | None = None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/scans", status_code=202)
def submit_scan(request: ScanRequest) -> dict[str, str]:
    correlation_id = str(uuid4())
    workflow_id = str(uuid4())

    # Keep raw serials out of logs and traces; use a hash for lookup/debugging.
    serial_hash = hashlib.sha256(request.serial.encode("utf-8")).hexdigest()

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
            source_service=SERVICE_NAME,
            data={
                "serial": {"hash": serial_hash, "prefix": request.serial[:5]},
                "production": {"station": request.station},
                "operator": {"name": request.operator},
            },
        )

        # Kafka is the integration boundary between services.
        publish_event(producer, SCAN_TOPIC, event, key=request.station)

    logger.info("scan accepted", extra={"correlation_id": correlation_id})
    return {"status": "accepted", "correlation_id": correlation_id, "workflow_id": workflow_id}
