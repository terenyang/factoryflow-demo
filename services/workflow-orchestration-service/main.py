"""Workflow orchestration service.

This service demonstrates centralized workflow orchestration.
Services should not hardcode the next processing step.
The workflow definition determines routing behavior.
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
logger = logging.getLogger("workflow-orchestration-service")

setup_otel("workflow-orchestration-service")
tracer = trace.get_tracer(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
SCAN_TOPIC = "factory.scan.received"
VALIDATION_TOPIC = "factory.validation.requested"
WORKFLOW_COMPLETED_TOPIC = "factory.workflow.orchestration.completed"

consumer = create_consumer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="workflow-orchestration-service",
    topics=[SCAN_TOPIC],
)

producer = create_producer(KAFKA_BOOTSTRAP)

WORKFLOW_TEMPLATE = {
    "name": "standard-assembly-flow",
    "version": "1.0",
    "steps": [
        {
            "step": 1,
            "service": "validator-service",
            "success_topic": "factory.validation.completed",
            "error_topic": "factory.validation.error",
        }
    ],
}


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
        "workflow_orchestration",
        context=parent_context,
    ) as span:
        event = json.loads(msg.value().decode("utf-8"))

        correlation_id = event.get("correlation_id", "")
        workflow_id = event.get("workflow_id", "")

        span.set_attribute("correlation_id", correlation_id)
        span.set_attribute("workflow_id", workflow_id)

        # Workflow metadata is injected once and propagated downstream.
        event["workflow"] = WORKFLOW_TEMPLATE
        event["workflow"]["state"] = "IN_PROGRESS"
        event["workflow"]["current_step"] = 1

        publish_event(
            producer=producer,
            topic=VALIDATION_TOPIC,
            event=event,
            key=event.get("session_id"),
        )

        publish_event(
            producer=producer,
            topic=WORKFLOW_COMPLETED_TOPIC,
            event={
                "correlation_id": correlation_id,
                "workflow_id": workflow_id,
                "event_type": WORKFLOW_COMPLETED_TOPIC,
            },
            key=event.get("session_id"),
        )

        logger.info(
            "Workflow dispatched",
            extra={
                "correlation_id": correlation_id,
                "workflow": WORKFLOW_TEMPLATE["name"],
            },
        )

    time.sleep(0.05)
