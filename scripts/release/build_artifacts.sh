#!/usr/bin/env bash
set -euo pipefail

target=""
asset=""
version=""
out_dir="dist"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      target="$2"
      shift 2
      ;;
    --asset)
      asset="$2"
      shift 2
      ;;
    --version)
      version="$2"
      shift 2
      ;;
    --out-dir)
      out_dir="$2"
      shift 2
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$target" || -z "$asset" || -z "$version" ]]; then
  echo "usage: build_artifacts.sh --target TRIPLE --asset linux-x64|macos-arm64|... --version X.Y.Z [--out-dir dist]" >&2
  exit 2
fi

mkdir -p "$out_dir"

cargo build --release --locked --target "$target"

bin="target/$target/release/viddybud"
if [[ ! -f "$bin" ]]; then
  echo "missing built binary: $bin" >&2
  exit 2
fi
chmod +x "$bin"

tar -C "target/$target/release" -czf "$out_dir/viddybud-v${version}-${asset}.tar.gz" viddybud

