#!/usr/bin/env bash
set -euo pipefail

repo=""
src=""
dst=""
message=""
branch="main"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
      shift 2
      ;;
    --src)
      src="$2"
      shift 2
      ;;
    --dst)
      dst="$2"
      shift 2
      ;;
    --message)
      message="$2"
      shift 2
      ;;
    --branch)
      branch="$2"
      shift 2
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$repo" || -z "$src" || -z "$dst" || -z "$message" ]]; then
  echo "usage: mirror_to_repo.sh --repo OWNER/REPO --src FILE --dst PATH --message MSG [--branch main]" >&2
  exit 2
fi

if [[ ! -f "$src" ]]; then
  echo "missing src file: $src" >&2
  exit 2
fi

token="${PACKAGING_REPO_TOKEN:-}"
if [[ -z "$token" ]]; then
  echo "missing PACKAGING_REPO_TOKEN env var" >&2
  exit 2
fi

tmp="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

remote="https://x-access-token:${token}@github.com/${repo}.git"
git clone --depth 1 "$remote" "$tmp/repo"
cd "$tmp/repo"

git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

mkdir -p "$(dirname "$dst")"
cp "$src" "$dst"

git add "$dst"
if git diff --cached --quiet; then
  echo "no changes to commit for $repo ($dst)"
  exit 0
fi

git commit -m "$message"
git push origin "HEAD:$branch"

