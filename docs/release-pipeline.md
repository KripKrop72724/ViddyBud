# ViddyBud Release Pipeline

This document describes how ViddyBud releases are built and published.

## Release Model

- Stable releases are cut from Git tags of the form `vMAJOR.MINOR.PATCH`.
- The tag version must match `Cargo.toml` `version`.

## What The Release Workflow Does

On a stable tag push, `.github/workflows/release.yml` will:

1. Validate the tag and Cargo version match.
2. Run the test gate (`cargo test --locked`).
3. Build release artifacts:
   - Windows x64
   - macOS arm64 + x64
   - Linux x64
4. Generate `SHA256SUMS.txt`.
5. Create a **draft** GitHub Release and upload assets.
6. Publish package definitions:
   - Homebrew tap: `KripKrop72724/homebrew-viddybud` (formula at `Formula/viddybud.rb`)
   - Scoop bucket: `KripKrop72724/scoop-viddybud` (manifest at `bucket/viddybud.json`)
7. Verify Homebrew install from tap (macOS runner).
8. Publish the GitHub release (remove draft) only after Homebrew + Scoop succeed.
9. Attempt WinGet submission (non-blocking) after release publish.

## One-Time Setup Checklist

1. Create repos:
   - `KripKrop72724/homebrew-viddybud`
   - `KripKrop72724/scoop-viddybud`
2. Create a GitHub PAT with least privileges needed to push to those repos.
3. Set repo secrets in `KripKrop72724/ViddyBud`:
   - `PACKAGING_REPO_TOKEN` (write to tap + bucket repos)
   - `WINGET_TOKEN` (used by wingetcreate for PR submission)
4. Set repo variable:
   - `WINGET_BOOTSTRAPPED=true` after first WinGet package is merged upstream.

## Cutting A Release

1. Update `Cargo.toml` `version` and `CHANGELOG.md`.
2. Commit and push to `main`.
3. Create and push the tag:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

4. Monitor the `Release` workflow run in the Actions tab.

## Dry Runs

The release workflow supports a manual `workflow_dispatch` with `dry_run=true`.
This builds and generates artifacts/manifests but does not publish releases or packages.

