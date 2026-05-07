"""OpenTelemetry setup helpers for FactoryFlow demo services.

This is a sanitized, generic helper based on common OTEL patterns:
- service.name resource attribute
- OTLP gRPC exporter
- BatchSpanProcessor

No internal endpoint, tenant, host, or production service names are embedded here.
"""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup_otel(service_name: str, endpoint: str = "http://jaeger:4317") -> None:
    """Configure OpenTelemetry tracing for a service.

    Args:
        service_name: Logical service name shown in Jaeger.
        endpoint: OTLP gRPC endpoint. Defaults to the Jaeger service in docker-compose.
    """
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
