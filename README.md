# f2v

Byte-perfect folder `<->` video codec (`bytes -> RGB frames -> lossless MKV`).

`f2v` can:
- encode any folder into one or more `.mkv` segments,
- decode those segments back to the original folder structure,
- run end-to-end roundtrip verification,
- show rich runtime telemetry (progress, ETA, bottleneck state, idle warnings).

## Platform Support

- Windows (priority platform for decode performance tuning)
- macOS
- Linux

## Prerequisites

`f2v` requires:
- Rust toolchain (`cargo`, `rustc`)
- `ffmpeg` available in `PATH`

Install `ffmpeg`:

- Windows (PowerShell + winget):
  - `winget install Gyan.FFmpeg`
- Windows (Chocolatey):
  - `choco install ffmpeg`
- macOS (Homebrew):
  - `brew install ffmpeg`
- Ubuntu/Debian:
  - `sudo apt-get update && sudo apt-get install -y ffmpeg`

## Build

```bash
cargo build --release
```

Binary locations:
- Windows: `target\release\f2v.exe`
- macOS/Linux: `target/release/f2v`

## Windows Downloadable Artifact (GitHub Actions)

Every push to `main` runs the workflow:
- `.github/workflows/windows-artifact.yml`

It:
- runs `cargo check` + `cargo test`,
- builds `f2v.exe` on `windows-latest`,
- smoke-tests binary execution (`--version`, `--help`),
- uploads `f2v-windows-x64-<commit_sha>.zip` artifact.

Download:
1. Open repository Actions tab.
2. Open latest `Windows Artifact` run on `main`.
3. Download `f2v-windows-x64-<commit_sha>`.

## Quick Start

### Encode

```bash
cargo run -- encode "<input_folder>" "<encoded_output_dir>" --mode ffv1
```

### Decode

```bash
cargo run -- decode "<encoded_output_dir>" "<decoded_output_folder>" --decode-engine parallel
```

### Roundtrip (encode + decode + verify)

```bash
cargo run -- roundtrip "<input_folder>" "<temp_work_dir>" --mode ffv1
```

## Command Reference

## Global

```text
f2v <command>
```

Commands:
- `encode`
- `decode`
- `roundtrip`

## `encode`

```text
f2v encode <INPUT_FOLDER> <OUTPUT_DIR> [options]
```

Options:
- `--mode <ffv1|raw>`: encoding mode (default `ffv1`)
- `--workers <usize>`: override segment encoder worker count
- `--chunk-mib <u64>`: override chunk size in MiB
- `--segment-gib <u64>`: override target segment payload size in GiB
- `--frame <4k|sq4k>`: frame preset (`3840x2160` or `4096x4096`)
- `--mmap-threshold-mib <u64>`: mmap files at/above threshold (default `64`)
- `--no-mmap`: disable mmap path explicitly
- `--progress <auto|rich|plain|quiet>`: progress mode (default `auto`)

## `decode`

```text
f2v decode <INPUT_DIR> <OUTPUT_FOLDER> [options]
```

Options:
- `--decode-engine <parallel|sequential>`: decode engine (default `parallel`)
- `--decode-workers <usize>`: decoder worker count override
- `--writer-workers <usize>`: writer worker count override
- `--writers <usize>`: backward-compatible alias for `--writer-workers`
- `--queue-mib <u64>`: decode-to-writer queue budget in MiB
- `--batch-bytes <u64>`: writer flush target size in bytes
- `--batch-ms <u64>`: max writer flush delay in milliseconds
- `--read-ahead-frames <usize>`: per-decoder read-ahead frame queue depth
- `--unsafe-skip-crc`: disable CRC validation (unsafe, faster)
- `--progress <auto|rich|plain|quiet>`: progress mode (default `auto`)

## `roundtrip`

```text
f2v roundtrip <INPUT_FOLDER> <TEMP_DIR> [options]
```

Options:
- `--mode <ffv1|raw>`: encode mode for phase 1 (default `ffv1`)
- `--decode-engine <parallel|sequential>`
- `--decode-workers <usize>`
- `--writer-workers <usize>`
- `--writers <usize>`: alias for `--writer-workers`
- `--queue-mib <u64>`
- `--batch-bytes <u64>`
- `--batch-ms <u64>`
- `--read-ahead-frames <usize>`
- `--unsafe-skip-crc`
- `--mmap-threshold-mib <u64>`: encode-side mmap threshold (default `64`)
- `--no-mmap`
- `--progress <auto|rich|plain|quiet>`: progress mode (default `auto`)

## Progress Modes

- `auto`: rich live UI on TTY, plain periodic logs otherwise.
- `rich`: `indicatif` multi-line progress display.
- `plain`: periodic readable status lines (suitable for logs/CI).
- `quiet`: suppress progress output.

Telemetry includes:
- stage/state,
- throughput,
- ETA,
- queue usage and bottleneck classification (decode parallel engine),
- idle warnings with component context,
- end-of-run summaries.

## Performance Tuning

High-throughput decode example:

```bash
cargo run -- decode "<encoded>" "<decoded>" \
  --decode-engine parallel \
  --decode-workers 8 \
  --writer-workers 8 \
  --queue-mib 2048 \
  --batch-bytes 8388608 \
  --batch-ms 20 \
  --read-ahead-frames 16
```

Benchmark protocol:
- `BENCHMARK.md`

## Troubleshooting

- Verify ffmpeg is visible:
  - `ffmpeg -version`
- Verify CLI:
  - `f2v --help`
- If debugging decode issues, try:
  - `--decode-engine sequential`
  - `--progress plain`
- Keep CRC on unless explicitly testing unsafe path:
  - do not use `--unsafe-skip-crc` for integrity-sensitive workflows.
