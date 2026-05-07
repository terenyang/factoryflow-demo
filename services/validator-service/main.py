"""Errorproof-style validation service for FactoryFlow."""

from __future__ import annotations

import json
import logging
import os

from opentelemetry import trace
from opentelemetry.propagate import extract

from factoryflow.messaging import create_consumer, create_producer, headers_to_carrier, publish_event
from factoryflow.otel import setup_otel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("validator-service")

SERVICE_NAME = "validator-service"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")
VALIDATION_TOPIC = os.getenv("VALIDATION_TOPIC", "factory.validation.requested")
SUCCESS_TOPIC = os.getenv("SUCCESS_TOPIC", "factory.errorproof.completed")
ERROR_TOPIC = os.getenv("ERROR_TOPIC", "factory.errorproof.error")

setup_otel(SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT)
tracer = trace.get_tracer(__name__)


def handle_message(raw_value: bytes, raw_headers: list[tuple[str, bytes]] | None, producer) -> None:
    carrier = headers_to_carrier(raw_headers)
    parent_context = extract(carrier)

    with tracer.start_as_current_span("errorproof_validation", context=parent_context) as span:
        event = json.loads(raw_value.decode("utf-8"))

        correlation_id = event.get("correlation_id", "")
        workflow_id = event.get("workflow_id", "")

        span.set_attribute("correlation_id", correlation_id)
        span.set_attribute("workflow_id", workflow_id)

        serial_prefix = event.get("data", {}).get("serial", {}).get("prefix", "")

        # Minimal deterministic validation rule for demo purposes.
        is_valid = serial_prefix.upper() != "ERROR"

        result_topic = SUCCESS_TOPIC if is_valid else ERROR_TOPIC

        result_event = {
            **event,
            "event_type": result_topic,
            "validation": {
                "status": "PASS" if is_valid else "FAIL",
                "service": SERVICE_NAME,
            },
        }

        publish_event(producer, result_topic, result_event, key=event.get("session_id"))

        logger.info(
            "validation completed",
            extra={
                "correlation_id": correlation_id,
                "result": result_event["validation"]["status"],
            },
        )


def run() -> None:
    consumer = create_consumer(KAFKA_BOOTSTRAP, SERVICE_NAME, [VALIDATION_TOPIC])
    producer = create_producer(KAFKA_BOOTSTRAP)

    logger.info("validator service started")

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
