# benchmark

the benchmark runs fleetlake's spark parsing, watermarking, deduplication, aggregation, and fault-filter paths against a deterministic event set. it does not include kafka, parquet, trino, elasticsearch, or network i/o, so the result should be described as local spark transformation throughput rather than end-to-end production throughput.

## reproduce it

```bash
sbt "Test / runMain dev.noorps.fleetlake.FleetLakeBenchmark 200000 2 5"
```

the arguments are event volume, warmup runs, and measured runs. the command writes machine-readable results to `benchmarks/latest.json`.

## latest measured result

github actions run `34717694507` processed 200,000 simulated events at a median 73,019 events per second with a 3,050 ms p95 batch runtime. the run used spark 3.5.6, java 17, 2 warmups, and 5 measured runs on a 4-core linux runner.

the benchmark produced 190,000 unique events after deduplication, 18,000 one-minute aggregate rows, and 7,600 fault events. these numbers cover fleetlake's spark transformation path only, not kafka ingestion or external sink i/o.
