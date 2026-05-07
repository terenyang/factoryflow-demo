# Run FactoryFlow Locally

## Start infrastructure and services

```bash
docker compose up --build
```

---

## Open service UIs

| Service | URL |
|---|---|
| Scan Gateway | http://localhost:8000/docs |
| Jaeger | http://localhost:16686 |
| Redpanda Console | http://localhost:8080 |

---

## Submit a synthetic scan

```bash
curl -X POST http://localhost:8000/scans \
  -H "Content-Type: application/json" \
  -d '{
    "serial":"DEMO-001",
    "station":"ASSEMBLY-01",
    "operator":"demo-user"
  }'
```

---

## Expected flow

```text
scan-gateway
  -> factory.scan.received
  -> workflow-orchestration-service
  -> factory.validation.requested
  -> validator-service
  -> factory.errorproof.completed
```

---

## Trace validation

Open Jaeger and search for:

```text
Service: scan-gateway
```

You should see a distributed trace spanning:

- scan-gateway
- workflow-orchestration-service
- validator-service

---

## Design notes

The demo intentionally prioritizes:

- event choreography
- workflow orchestration
- OpenTelemetry propagation
- industrial traceability patterns

The demo intentionally avoids:

- MES business complexity
- authentication
- production schemas
- customer-specific logic
