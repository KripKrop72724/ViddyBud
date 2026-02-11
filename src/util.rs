use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;
use walkdir::WalkDir;

use crate::progress::VerifySummary;

pub fn ensure_ffmpeg_available() -> Result<()> {
    let out = Command::new("ffmpeg")
        .arg("-version")
        .output()
        .context("failed to run ffmpeg -version")?;
    if !out.status.success() {
        bail!("ffmpeg exists but returned non-zero on -version");
    }
    Ok(())
}

pub fn dataset_id_hex(dataset_id: [u8; 16]) -> String {
    dataset_id.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn folder_basename(p: &Path) -> String {
    p.file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "root".to_string())
}

pub fn list_mkvs(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut v = vec![];
    for entry in std::fs::read_dir(dir)? {
        let e = entry?;
        let p = e.path();
        if p.is_file() {
            if let Some(ext) = p.extension() {
                if ext.to_string_lossy().to_lowercase() == "mkv" {
                    v.push(p);
                }
            }
        }
    }
    v.sort();
    Ok(v)
}

/// Simple “did we reconstruct the same file tree sizes?”
pub fn basic_verify_structure(src_root: &Path, dst_root: &Path) -> Result<VerifySummary> {
    let started = Instant::now();
    let mut checked_dirs = 0usize;
    let mut checked_files = 0usize;
    let mut checked_bytes = 0u64;

    for entry in WalkDir::new(src_root) {
        let e = entry?;
        let src = e.path();
        let rel = src.strip_prefix(src_root)?;
        let dst = dst_root.join(rel);

        if src.is_dir() {
            if !dst.is_dir() {
                bail!("Missing dir: {:?}", dst);
            }
            checked_dirs += 1;
        } else if src.is_file() {
            let s1 = std::fs::metadata(src)?.len();
            let s2 = std::fs::metadata(&dst)
                .with_context(|| format!("Missing file: {:?}", dst))?
                .len();
            if s1 != s2 {
                bail!("Size mismatch for {:?}: {} vs {}", rel, s1, s2);
            }
            checked_files += 1;
            checked_bytes = checked_bytes.saturating_add(s1);
        }
    }
    let elapsed = started.elapsed();
    let avg_bytes_per_sec = if elapsed.as_secs_f64() <= f64::EPSILON {
        checked_bytes as f64
    } else {
        checked_bytes as f64 / elapsed.as_secs_f64()
    };

    Ok(VerifySummary {
        checked_dirs,
        checked_files,
        checked_bytes,
        elapsed,
        avg_bytes_per_sec,
    })
}
