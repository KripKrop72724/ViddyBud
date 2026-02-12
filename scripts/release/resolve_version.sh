#!/usr/bin/env bash
set -euo pipefail

cargo_toml="${1:-Cargo.toml}"

if [[ ! -f "$cargo_toml" ]]; then
  echo "missing $cargo_toml" >&2
  exit 2
fi

cargo_version="$(
  # Use BRE-compatible capture groups so this works on both GNU and BSD sed.
  sed -n 's/^version = "\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\)"$/\1/p' "$cargo_toml" | head -n 1
)"
if [[ -z "$cargo_version" ]]; then
  echo "could not parse version from $cargo_toml" >&2
  exit 2
fi

event="${GITHUB_EVENT_NAME:-}"
ref_name="${GITHUB_REF_NAME:-}"

tag=""
if [[ "$event" == "workflow_dispatch" ]]; then
  tag="v$cargo_version"
else
  tag="$ref_name"
fi

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid tag format: '$tag' (expected vMAJOR.MINOR.PATCH)" >&2
  exit 2
fi

version="${tag#v}"

if [[ "$event" != "workflow_dispatch" && "$version" != "$cargo_version" ]]; then
  echo "tag version ($version) does not match Cargo.toml version ($cargo_version)" >&2
  exit 2
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "tag=$tag"
    echo "version=$version"
  } >>"$GITHUB_OUTPUT"
else
  echo "tag=$tag"
  echo "version=$version"
fi
