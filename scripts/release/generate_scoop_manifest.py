#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True)
    ap.add_argument("--version", required=True)
    ap.add_argument("--zip-sha256", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    tpl = Path(args.template).read_text(encoding="utf-8")
    rendered = tpl.replace("{{VERSION}}", args.version).replace(
        "{{ZIP_SHA256}}", args.zip_sha256
    )
    if "{{VERSION}}" in rendered or "{{ZIP_SHA256}}" in rendered:
        raise SystemExit("template render incomplete (placeholders remain)")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

