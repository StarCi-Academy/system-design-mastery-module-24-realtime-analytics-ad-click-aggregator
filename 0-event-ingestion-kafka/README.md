# Event Ingestion (Kafka)

High-throughput ad-click ingestion: an HTTP **producer** appends click events to
a Kafka topic; a **counting consumer** reads the log, counts unique clicks per
ad, and rejects duplicates by idempotency key (`clickId`). The consumer commits
offsets per group, so on restart it resumes from where it left off instead of
replaying the whole log.

## How to run

```bash
cd 0-event-ingestion-kafka/.docker
docker compose up -d --build
```

- Producer: `POST http://localhost:3000/clicks`
- Consumer counts: `GET http://localhost:3001/counts`
- Kafka UI: `http://localhost:8080`

Cleanup: `docker compose down -v`.

## Architecture / Stack

| Service | Port | Role |
| --- | --- | --- |
| `kafka` (KRaft) | 9092 | Append-only log for topic `ad-clicks`, 3 partitions, no Zookeeper. |
| `ingest-api` | 3000 | NestJS producer: `POST /clicks` → emit with `key = adId`, header `clickId`. |
| `count-consumer` | 3001 | NestJS consumer: dedupes by `clickId`, counts per ad, `GET /counts`. |
| `kafka-ui` | 8080 | Inspect topics, partitions, offsets, consumer lag. |

`ingest-api/0-typescript` and `count-consumer/0-typescript` are the TypeScript
track. Other language tracks (`1-java`, `2-csharp`, `3-go`) are TODO.

## Smoke Test

```text
POST /clicks {"clickId":"c-1","adId":"ad-1"} → {"status":"accepted","topic":"ad-clicks","clickId":"c-1","adId":"ad-1"}
POST /clicks {"clickId":"c-1","adId":"ad-1"} (dup) → still 202, consumer logs DUPLICATE dropped
GET /counts → {"counts":{"ad-1":2,"ad-2":1},"totalUnique":3,"duplicatesRejected":2}
```

## How it works (log / dedup / offset)

- `key = adId` hashes the click to a fixed partition, so all clicks of one ad
  keep relative order.
- The consumer keeps a `Set<clickId>` of seen ids; the first sighting is counted,
  later sightings are dropped — idempotent counting under at-least-once delivery.
- Offsets are committed per consumer group; restarting the consumer resumes from
  the last committed offset (no replay of already-processed events).

## Design decisions

- KRaft single-broker keeps the lesson lightweight (no Zookeeper, no ClickHouse).
- Dedup store is in-memory for teaching; production swaps it for Redis/RocksDB so
  dedup state survives a crash.

> Repo remote URL is set after pushing (TODO).
