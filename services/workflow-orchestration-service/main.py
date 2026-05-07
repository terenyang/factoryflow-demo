"""Workflow orchestration service for FactoryFlow."""

from __future__ import annotations

import json
import logging
import os

from opentelemetry import trace
from opentelemetry.propagate import extract

from factoryflow.messaging import create_consumer, create_producer, headers_to_carrier, publish_event
from factoryflow.otel import setup_otel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("workflow-orchestration-service")

SERVICE_NAME = "workflow-orchestration-service"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")
SCAN_TOPIC = os.getenv("SCAN_TOPIC", "factory.scan.received")
VALIDATION_TOPIC = os.getenv("VALIDATION_TOPIC", "factory.validation.requested")
WORKFLOW_LIFECYCLE_TOPIC = os.getenv("WORKFLOW_LIFECYCLE_TOPIC", "factory.workflow.lifecycle")

setup_otel(SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT)
tracer = trace.get_tracer(__name__)

WORKFLOW_TEMPLATE = {
    "name": "standard-assembly-flow",
    "version": "1.0",
    "steps": [
        {
            "step": 1,
            "service": "errorproof-service",
            "request_topic": VALIDATION_TOPIC,
            "success_topic": "factory.errorproof.completed",
            "error_topic": "factory.errorproof.error",
        }
    ],
}


def handle_message(raw_value: bytes, raw_headers: list[tuple[str, bytes]] | None, producer) -> None:
    carrier = headers_to_carrier(raw_headers)
    parent_context = extract(carrier)

    with tracer.start_as_current_span("workflow_orchestration", context=parent_context) as span:
        event = json.loads(raw_value.decode("utf-8"))
        correlation_id = event.get("correlation_id", "")
        workflow_id = event.get("workflow_id", "")

        span.set_attribute("correlation_id", correlation_id)
        span.set_attribute("workflow_id", workflow_id)
        span.set_attribute("workflow.name", WORKFLOW_TEMPLATE["name"])

        # The orchestrator owns routing. Downstream services execute steps only.
        event["workflow"] = {**WORKFLOW_TEMPLATE, "state": "IN_PROGRESS", "current_step": 1}

        publish_event(producer, VALIDATION_TOPIC, event, key=event.get("session_id"))
        publish_event(
            producer,
            WORKFLOW_LIFECYCLE_TOPIC,
            {
                "event_type": "factory.workflow.dispatched",
                "correlation_id": correlation_id,
                "workflow_id": workflow_id,
                "workflow": {"name": WORKFLOW_TEMPLATE["name"], "current_step": 1},
            },
            key=event.get("session_id"),
        )

        logger.info("workflow dispatched", extra={"correlation_id": correlation_id})


def run() -> None:
    consumer = create_consumer(KAFKA_BOOTSTRAP, SERVICE_NAME, [SCAN_TOPIC])
    producer = create_producer(KAFKA_BOOTSTRAP)
    logger.info("workflow orchestration service started")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning("consumer error: %s", msg.error())
            continue
        handle_message(msg.value(), msg.headers(), producer)


if __name__ == "__main__":
    run()
