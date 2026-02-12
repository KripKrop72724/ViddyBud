#!/usr/bin/env bash
set -euo pipefail

dist_dir="${1:-dist}"
if [[ ! -d "$dist_dir" ]]; then
  echo "missing dist dir: $dist_dir" >&2
  exit 2
fi

out_file="$dist_dir/SHA256SUMS.txt"
rm -f "$out_file"

hash_file() {
  local f="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$f" | awk '{print $1}'
  else
    shasum -a 256 "$f" | awk '{print $1}'
  fi
}

pushd "$dist_dir" >/dev/null
for f in $(ls -1 | sort); do
  if [[ "$f" == "SHA256SUMS.txt" ]]; then
    continue
  fi
  if [[ -f "$f" ]]; then
    h="$(hash_file "$f")"
    printf "%s  %s\n" "$h" "$f" >>"SHA256SUMS.txt"
  fi
done
popd >/dev/null

