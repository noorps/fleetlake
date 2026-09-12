# fleetlake

[![verify](https://github.com/noorps/fleetlake/actions/workflows/verify.yml/badge.svg)](https://github.com/noorps/fleetlake/actions/workflows/verify.yml)

fleetlake is the analytics side of a device telemetry platform. it takes an unbounded kafka stream and turns it into three things different teams need: partitioned historical files, one-minute operational aggregates, and a searchable fault index.

i built it to work through the part of data platforms that starts after an event is accepted. the hard questions here are how late data changes an aggregate, how a restarted job avoids counting an event twice, how tenants stay separable, and how the same stream can support both interactive sql and incident search.

it pairs with [pulsegrid](https://github.com/noorps/pulsegrid), which handles api ingestion and reliable delivery into kafka. fleetlake starts where pulsegrid stops.

## measured run

[github actions run #34717694507](https://github.com/noorps/fleetlake/actions/runs/34717694507) processed 200,000 simulated events at a median 73,019 events/sec with a 3,050 ms p95 batch runtime across 5 measured runs after 2 warmups.

the run produced 190,000 unique events after deduplication, 18,000 one-minute aggregate rows, and 7,600 fault events. it covers spark parsing, watermarking, deduplication, aggregation, and fault filtering on a 4-core linux runner. it does not include kafka ingestion or external sink i/o.

the [benchmark method](benchmarks/README.md) and [raw result](benchmarks/latest.json) are committed so the numbers can be checked and rerun.

## architecture

```text
telemetry.events.v1
        |
        v
scala + spark structured streaming
        |
        +--> parquet history, partitioned by tenant and metric
        +--> one-minute device aggregates
        +--> telemetry.faults.v1 --> logstash --> elasticsearch
        |
        +--> checkpoints + two-minute event-time watermark

trino --> sql over the raw kafka topic
```

## design choices

- **event-time processing:** spark uses the device timestamp, a two-minute watermark, and one-minute windows instead of grouping by arrival time
- **replay safety:** tenant and event ids form the deduplication boundary, while each sink keeps an independent checkpoint
- **tenant isolation:** historical files are physically partitioned by tenant and every example query keeps tenant id as a first-class column
- **two access paths:** trino supports ad hoc sql over raw events while elasticsearch handles fast fault and severity searches
- **bounded state:** watermarking lets spark discard old aggregation and deduplication state instead of growing forever

## stack

- scala 2.12 and spark structured streaming 3.5.6
- kafka 3.9 for replayable telemetry streams
- trino 483 for federated sql access
- elasticsearch and logstash 9.1.4 for fault search
- parquet for partitioned historical storage
- docker compose for a reproducible local stack

## run it

you need java 17, sbt, and docker compose.

```bash
sbt test package
docker compose up
```

send JSON events to `telemetry.events.v1`. each event follows this shape:

```json
{
  "tenantId": "fleet-a",
  "deviceId": "vehicle-7",
  "eventId": "01J8M2Y0Q9S8V7T6R5P4N3K2H1",
  "recordedAt": "2026-09-12T14:30:00Z",
  "metric": "battery_pct",
  "value": 81.4,
  "severity": "normal"
}
```

query the raw stream through trino:

```bash
docker compose exec trino trino --file /dev/stdin < queries/fleet_health.sql
```

search indexed faults:

```bash
curl "http://localhost:9200/fleet-faults-*/_search?q=severity:critical"
```

## what this demonstrates

this is a portfolio-scale system, not a claim of production traffic. the repository is meant to make the implementation choices inspectable: scala transformations, spark checkpoints and watermarks, kafka schemas, trino table mappings, elasticsearch routing, tests, and pinned infrastructure versions are all in one place.
