# SMX NATS Monitor

Monitoring-Service für NATS JetStream — Streams, Consumer, Lag (num_pending), Advisories. Eigenständig, ohne Prometheus/Grafana.

## Tech

- Python 3.11 / FastAPI / nats-py / SQLite
- Dashboard: statisches HTML + Canvas-Charts (kein CDN, kein Node-Build)

## NATS-User

`smx-nats-monitor` im Account `SMX_GATEWAY` — **read-only**:

- pub `$JS.API.>` (Stream-/Consumer-Info abfragen)
- sub `$JS.EVENT.ADVISORY.>` (Advisories live)
- sub `_INBOX.>` (JS-API-Antworten)

Kein Zugriff auf Daten-Subjects (`smx.gateway.>`/`smx.dis.>`/`smx.clearing.>`).

## Deployment (.60)

```
cd /opt/smx-nats-monitor && docker compose up -d --build
```

- Port 8090
- Creds: `/opt/smx-hub/creds/smx-nats-monitor.creds` (mount, ro)
- CA: `/opt/smx-hub/certs/ca.crt` (mount, ro)
- SQLite: Volume `monitor-data` → `/data/monitor.db`

## API

| Endpoint | Zweck |
|---|---|
| `GET /api/state` | Streams + Consumer (aktuell) |
| `GET /api/snapshots?minutes=60` | Zeitreihe (messages, bytes, num_pending, ...) |
| `GET /api/events` | Advisories (Lebenszyklus) |
| `GET /api/events/stream` | SSE Live-Feed |

## Hinweise

- `$JS.EVENT.ADVISORY.API` (API-Audit) wird gefiltert — er auditiert auch die eigenen Polling-Reads (INFO/LIST) und wäre Rauschen. Lebenszyklus-Events (STREAM.CREATED, CONSUMER.*, MAX_DELIVERIES, MSG_TERMINATED, LEADER_ELECTED) kommen über eigene Subjects.
- „Nak" ist kein Advisory → wird über die Consumer-Metrik `num_redelivered` abgebildet.
- CA-Zertifikat (SMX-NATS-CA) hat kein `keyUsage` — Python ≥ 3.13/3.14 (OpenSSL-neu) lehnt es ab. Produktion nutzt `python:3.11-slim` (OpenSSL 3.0), das akzeptiert.
