use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u16,
    pub dataset_id: [u8; 16],
    pub created_utc_unix: i64,

    pub root_name: String,

    pub frame_w: u32,
    pub frame_h: u32,
    pub fps: u32,

    pub chunk_size_bytes: u64,
    pub approx_segment_payload_bytes: u64,
    pub mode: String, // "ffv1" or "raw"

    pub planned_segments: u32,

    pub dirs: Vec<String>,
    pub files: Vec<FileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub id: u32,
    pub rel_path: String,
    pub size: u64,
    pub mtime_unix: i64,
}

#[derive(Debug, Clone)]
pub struct FileScan {
    pub entry: FileEntry,
    pub abs_path: PathBuf,
}
