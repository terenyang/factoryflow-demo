# FactoryFlow Architecture

## Goals

FactoryFlow demonstrates modern event-driven architecture patterns for industrial systems.

The objective is not to replicate a full MES or production traceability platform.
The objective is to demonstrate:

- service decoupling
- asynchronous workflows
- distributed tracing
- event contracts
- idempotent processing
- workflow orchestration

---

# Core Principles

## Event-Driven

Services communicate through events instead of direct synchronous dependencies.

Benefits:

- loose coupling
- scalability
- resiliency
- replayability
- observability

---

## Workflow-Oriented

Workflows are explicit.

Services should avoid embedding rigid next-step assumptions.

The orchestrator determines workflow progression.

---

## Traceable

Every request propagates:

- correlation_id
- trace_id
- workflow_id

This allows complete distributed tracing through Jaeger/OpenTelemetry.

---

## Idempotent

Industrial systems frequently encounter:

- repeated scans
- retry storms
- intermittent connectivity

Services should safely handle duplicate events.

---

# Service Overview

## Scan Gateway

Responsibilities:

- receive scan requests
- create correlation metadata
- create initial trace span
- publish workflow start event

---

## Workflow Orchestrator

Responsibilities:

- determine workflow steps
- route events
- track workflow progression
- emit workflow lifecycle events

---

## Validation Service

Responsibilities:

- validate synthetic serials
- simulate manufacturing checks
- publish validation result events

---

## Part Counter

Responsibilities:

- simulate production counting
- maintain lightweight state
- emit completion events

---

# Infrastructure

## Redpanda

Used as the event backbone.

Primary topics:

- factory.scan.received
- factory.validation.requested
- factory.validation.completed
- factory.partcounter.requested
- factory.workflow.completed

---

## Redis

Used for:

- idempotency
- temporary workflow state
- lightweight counters

---

## Jaeger

Used for:

- distributed tracing
- service dependency visualization
- end-to-end workflow analysis

---

# Future Extensions

Potential future areas:

- AI-assisted diagnostics
- PLC/L5X semantic ingestion
- graph-based workflow reasoning
- industrial knowledge graph integration
- OTEL metrics dashboards
- workflow replay engine
