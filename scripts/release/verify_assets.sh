#!/usr/bin/env bash
set -euo pipefail

bin="${1:-}"
if [[ -z "$bin" || ! -f "$bin" ]]; then
  echo "usage: verify_assets.sh /path/to/viddybud[.exe]" >&2
  exit 2
fi

"$bin" --version >/dev/null
"$bin" --help >/dev/null

