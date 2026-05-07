# FactoryFlow Design Decisions

## Purpose of this document

This document captures the architectural decisions, constraints, and trade-offs used in the FactoryFlow demo.

The goal of this repository is not to simulate a full MES platform.
Instead, it demonstrates reusable architecture patterns commonly needed in event-driven manufacturing and industrial workflow systems.

---

# Decision 1 - Use event-driven messaging instead of direct synchronous service calls

## Context

Traditional manufacturing applications often evolve into tightly coupled systems where:

- PLC communication
- validation logic
- workflow coordination
- database writes
- downstream integrations

all become connected through direct API calls or shared database logic.

This approach can work for small systems, but scaling and maintainability become difficult over time.

## Decision

FactoryFlow uses Kafka-compatible event streaming as the primary communication backbone.

Services communicate through published events instead of direct service-to-service dependencies.

## Rationale

Benefits include:

- Service decoupling
- Independent scaling
- Better failure isolation
- Easier observability
- Replayability of workflow events
- Simpler extensibility for future services

## Trade-offs

Event-driven systems introduce:

- Eventual consistency
- Additional operational complexity
- More infrastructure components
- More complex debugging without proper tracing

This design intentionally accepts those trade-offs to demonstrate scalable architecture patterns.

---

# Decision 2 - Separate Scan Gateway from Workflow Orchestrator

## Context

A common anti-pattern in industrial systems is combining:

- HTTP ingestion
- business workflow logic
- validation
- orchestration
- persistence

inside a single application.

## Decision

FactoryFlow separates scan ingestion from workflow orchestration.

The Scan Gateway is responsible only for:

- request ingestion
- payload normalization
- trace initialization
- event publishing

The Workflow Orchestrator is responsible for:

- workflow coordination
- workflow selection
- routing decisions
- workflow progression

## Rationale

This separation improves:

- maintainability
- operational isolation
- deployment flexibility
- architectural clarity

It also allows future replacement of the orchestration mechanism without impacting ingestion.

## Trade-offs

This introduces:

- additional topics
- additional message hops
- slightly higher end-to-end latency

The design intentionally favors clarity and extensibility over minimal latency.

---

# Decision 3 - Use correlation IDs and distributed tracing everywhere

## Context

Debugging distributed manufacturing workflows becomes extremely difficult without end-to-end observability.

Failures often span multiple services and asynchronous boundaries.

## Decision

Every workflow event should carry:

- correlation_id
- trace_id
- timestamp
- event metadata

OpenTelemetry is used for distributed tracing.

## Rationale

This enables:

- end-to-end workflow visibility
- distributed trace reconstruction
- easier root-cause analysis
- cross-service debugging
- future metrics and monitoring integration

## Trade-offs

Observability introduces:

- telemetry overhead
- additional infrastructure
- more metadata propagation requirements

However, observability is considered essential for distributed systems.

---

# Decision 4 - Use Redis for lightweight state and idempotency

## Context

Event-driven systems frequently encounter duplicate delivery scenarios.

Manufacturing workflows also often require temporary short-lived workflow state.

## Decision

Redis is used for:

- lightweight workflow state
- temporary aggregation
- idempotency protection
- rate limiting experiments

## Rationale

Redis provides:

- low operational overhead
- fast key-based lookups
- TTL support
- lightweight deployment characteristics

## Trade-offs

Redis is intentionally not treated as a system of record.

Persistent business history should ultimately reside in durable storage systems.

---

# Decision 5 - Keep the repository intentionally sanitized

## Context

Industrial and enterprise systems frequently contain:

- proprietary workflows
- customer information
- internal naming conventions
- confidential operational logic

## Decision

This repository intentionally avoids:

- real production payloads
- customer-specific logic
- internal identifiers
- real plant names
- internal topic naming standards
- proprietary workflow rules

## Rationale

The objective is to demonstrate reusable architecture concepts without exposing internal implementation details.

## Trade-offs

This makes some examples less realistic than actual production systems.

However, preserving clean-room boundaries is more important than creating a highly realistic simulation.

---

# Decision 6 - Prefer clarity over premature optimization

## Context

Many technical demos become difficult to understand because they attempt to optimize too early.

## Decision

FactoryFlow intentionally prioritizes:

- readability
- architecture clarity
- traceability
- learning value

over:

- maximum throughput
- aggressive optimization
- infrastructure minimization

## Rationale

This repository is intended to function as:

- a learning resource
- a reference architecture
- an experimentation platform
- a discussion starter for event-driven industrial systems

## Trade-offs

The current implementation should not be interpreted as production-ready infrastructure.

Production deployments would require:

- HA planning
- security hardening
- topic governance
- schema governance
- backup strategy
- operational monitoring
- disaster recovery planning
- deployment automation

---

# Future exploration areas

Potential future enhancements include:

- workflow definition engines
- schema registry integration
- dead-letter queue handling
- replay tooling
- workflow visualization
- advanced retry strategies
- persistent workflow state
- metrics dashboards
- Kubernetes deployment patterns
- authentication and authorization layers

These areas are intentionally deferred to keep the repository focused and understandable.
