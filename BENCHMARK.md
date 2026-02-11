# Decode Throughput Benchmark Protocol

Use the same encoded dataset and output volume for all runs. Clear the output folder between runs.

## 1) Baseline (sequential engine)

```bash
cargo run -- decode "<encoded_dir>" "<out_seq>" \
  --decode-engine sequential \
  --progress plain
```

Capture the `Decode summary:` line (`throughput=` token).

## 2) Parallel defaults

```bash
cargo run -- decode "<encoded_dir>" "<out_parallel_default>" \
  --decode-engine parallel \
  --progress plain
```

Capture `throughput=` and compare to baseline.

## 3) Tuned high-performance run

```bash
cargo run -- decode "<encoded_dir>" "<out_parallel_tuned>" \
  --decode-engine parallel \
  --decode-workers 8 \
  --writer-workers 8 \
  --queue-mib 2048 \
  --batch-bytes 8388608 \
  --batch-ms 20 \
  --read-ahead-frames 16 \
  --progress plain
```

Capture `throughput=`, `queue_peak=`, `batches=`, and `avg_batch=`.

## Acceptance targets

- Windows: `parallel/default` >= 2.0x sequential throughput on multi-MKV workloads.
- macOS: no regressions, target >= 1.5x on multi-MKV workloads.
- Linux: no functional regressions; throughput tracked as secondary for this phase.
