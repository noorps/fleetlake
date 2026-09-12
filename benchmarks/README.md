# benchmark

the benchmark runs fleetlake's spark parsing, watermarking, deduplication, aggregation, and fault-filter paths against a deterministic event set. it does not include kafka, parquet, trino, elasticsearch, or network i/o, so the result should be described as local spark transformation throughput rather than end-to-end production throughput.

## reproduce it

```bash
sbt "Test / runMain dev.noorps.fleetlake.FleetLakeBenchmark 200000 2 5"
```

the arguments are event volume, warmup runs, and measured runs. the command writes machine-readable results to `benchmarks/latest.json`.
