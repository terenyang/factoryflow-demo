"""Validator service.

This service demonstrates:
- idempotent validation behavior
- trace continuation across Kafka consumers
- success/error event publishing

The logic is intentionally lightweight.
The goal is architectural clarity, not MES complexity.
"""

from __future__ import annotations

import json
import logging
import os
import time

from opentelemetry import trace
from opentelemetry.propagate import extract

from shared.factoryflow.messaging import (
    create_consumer,
    create_producer,
    headers_to_carrier,
    publish_event,
)
from shared.factoryflow.otel import setup_otel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("validator-service")

setup_otel("validator-service")
tracer = trace.get_tracer(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
VALIDATION_TOPIC = "factory.validation.requested"
SUCCESS_TOPIC = "factory.validation.completed"
ERROR_TOPIC = "factory.validation.error"

consumer = create_consumer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="validator-service",
    topics=[VALIDATION_TOPIC],
)

producer = create_producer(KAFKA_BOOTSTRAP)


while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        logger.warning(f"Kafka consumer error: {msg.error()}")
        continue

    carrier = headers_to_carrier(msg.headers())
    parent_context = extract(carrier)

    with tracer.start_as_current_span(
        "validate_scan",
        context=parent_context,
    ) as span:
        event = json.loads(msg.value().decode("utf-8"))

        correlation_id = event.get("correlation_id", "")
        workflow_id = event.get("workflow_id", "")

        span.set_attribute("correlation_id", correlation_id)
        span.set_attribute("workflow_id", workflow_id)

        serial_prefix = (
            event.get("data", {})
            .get("serial", {})
            .get("prefix", "")
        )

        # Demo rule only.
        # Real validation logic belongs in domain-specific services.
        is_valid = serial_prefix != "ERROR"

        next_topic = SUCCESS_TOPIC if is_valid else ERROR_TOPIC

        validation_event = {
            **event,
            "event_type": next_topic,
            "validation": {
                "status": "PASS" if is_valid else "FAIL",
            },
        }

        publish_event(
            producer=producer,
            topic=next_topic,
            event=validation_event,
            key=event.get("session_id"),
        )

        logger.info(
            "Validation completed",
            extra={
                "correlation_id": correlation_id,
                "result": validation_event["validation"]["status"],
            },
        )

    time.sleep(0.05)
