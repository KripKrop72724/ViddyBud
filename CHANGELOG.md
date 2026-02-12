# Changelog

All notable changes to ViddyBud will be documented in this file.

The format is based on Keep a Changelog, and this project follows SemVer.

## [0.1.3] - 2026-02-12

### Fixed
- Release workflow now builds `macos-x64` on `macos-14` (GitHub no longer schedules `macos-13` runners reliably).

## [0.1.2] - 2026-02-12

### Fixed
- `Cargo.lock` updated so CI/release builds using `--locked` work correctly.

## [0.1.1] - 2026-02-12

### Fixed
- Release workflow tag/version parsing (no functional changes to the ViddyBud binary).

## [0.1.0] - 2026-02-12

### Added
- Encode folders to lossless MKV segments (`ffv1`) or raw video (`raw`).
- Decode MKV segments back to a byte-identical folder tree (CRC on by default).
- Parallel decode engine with sharded writer pool, batching, and read-ahead.
- Encode-side mmap read path for large files.
- Rich CLI runtime telemetry (`--progress auto|rich|plain|quiet`) with ETA, throughput, bottleneck/queue stats, and idle warnings.
- Stitch mode (`--stitch`) to produce exactly one multi-track MKV without re-encoding.
