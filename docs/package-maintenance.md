# Package Maintenance (Homebrew, Scoop, WinGet)

This document covers how ViddyBud's package publishing is maintained and recovered.

## Homebrew (Tap)

Target repo: `KripKrop72724/homebrew-viddybud`  
Target file: `Formula/viddybud.rb`

### Common failure modes

- Token lacks write access to tap repo.
- Formula syntax error.
- Build-from-source breakage on Homebrew's environment (Rust/cargo changes).

### Recovery

1. Fix formula template in `packaging/homebrew/viddybud.rb.tpl`.
2. Re-run release as a new tag (preferred), or:
   - Manually run the mirror step from a local machine and push to tap repo.

## Scoop (Bucket)

Target repo: `KripKrop72724/scoop-viddybud`  
Target file: `bucket/viddybud.json`

### Common failure modes

- Wrong SHA256 for the zip.
- Release asset name mismatch.
- Missing dependency resolution (`ffmpeg`) if user doesn't have `main` bucket.

### Recovery

1. Fix template in `packaging/scoop/viddybud.json.tpl`.
2. Re-run release tag or manually update `bucket/viddybud.json`.

## WinGet

Package ID: `ViddyBud.ViddyBud`

### Bootstrap requirement (one-time)

The first submission is usually done manually via `wingetcreate new`.
After the package exists in `microsoft/winget-pkgs`, CI can run `wingetcreate update --submit`.

Set `WINGET_BOOTSTRAPPED=true` in repo variables once bootstrapped.

### Common failure modes

- Upstream validation rejects manifest changes.
- Installer URL not publicly reachable (release must be published).
- Token scope insufficient for forking/PR submission.

### Recovery

1. Re-run the `publish_winget` job (or re-run workflow).
2. If upstream rejects:
   - Fix metadata and resubmit via wingetcreate locally.

## Token Rotation

1. Create a new PAT with the least scopes required.
2. Update repo secrets:
   - `PACKAGING_REPO_TOKEN`
   - `WINGET_TOKEN`
3. Re-run a dry-run release to validate.

