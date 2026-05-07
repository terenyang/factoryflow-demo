"""Kafka helpers used by demo services.

The helper keeps service code focused on workflow logic while preserving
correlation and OpenTelemetry context across Kafka boundaries.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable

from confluent_kafka import Consumer, Producer
from opentelemetry.propagate import inject


def build_headers(correlation_id: str) -> list[tuple[str, bytes]]:
    """Build Kafka headers with W3C trace context and a business correlation id."""
    carrier: Dict[str, str] = {}
    inject(carrier)
    carrier["correlation_id"] = correlation_id
    return [(k, v.encode("utf-8")) for k, v in carrier.items()]


def headers_to_carrier(headers: Iterable[tuple[str, bytes]] | None) -> Dict[str, str]:
    """Convert Kafka byte headers into a carrier accepted by OTEL extract()."""
    if not headers:
        return {}
    return {k: v.decode("utf-8") if isinstance(v, bytes) else str(v) for k, v in headers}


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers})


def publish_event(producer: Producer, topic: str, event: Dict[str, Any], key: str | None = None) -> None:
    """Publish JSON event and flush quickly for demo reliability."""
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(event).encode("utf-8"),
        headers=build_headers(event.get("correlation_id", "")),
    )
    producer.poll(0)
    producer.flush(5)


def create_consumer(bootstrap_servers: str, group_id: str, topics: list[str]) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(topics)
    return consumer
