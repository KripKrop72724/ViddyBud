# Decode Throughput Benchmark Protocol

Use the same encoded dataset and output volume for all runs. Clear the output folder between runs.

## 1) Baseline (sequential engine)

```bash
cargo run -- decode "<encoded_dir>" "<out_seq>" \
  --decode-engine sequential \
  --progress plain
```

Capture the `Decode summary:` line (`throughput=` token).

## 2) Parallel defaults (non-stitched)

```bash
cargo run -- decode "<encoded_dir>" "<out_parallel_default>" \
  --decode-engine parallel \
  --progress plain
```

Capture `throughput=` and compare to baseline.

## 3) Parallel tuned (non-stitched)

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

## 4) Encode baseline vs stitch overhead

```bash
# non-stitched
cargo run -- encode "<input_dir>" "<enc_normal>" --mode ffv1 --progress plain

# stitched single-file output
cargo run -- encode "<input_dir>" "<enc_stitched>" --mode ffv1 --stitch --progress plain
```

Capture `Encode summary:` lines and compare `duration=`.

## 5) Stitched decode throughput check

```bash
cargo run -- decode "<enc_stitched>/<dataset>_..._stitched.mkv" "<out_stitched_decode>" \
  --decode-engine parallel \
  --progress plain
```

Capture `Decode summary:` (`throughput=`, `source_count=`, `input_kind=`).

## Acceptance targets

- Windows: `parallel/default` >= 2.0x sequential throughput on multi-MKV workloads.
- macOS: no regressions, target >= 1.5x on multi-MKV workloads.
- Linux: no functional regressions; throughput tracked as secondary for this phase.
- Stitch guardrail: stitched encode wall-time overhead target `<=5%` versus non-stitched encode on the same dataset/hardware.
