use crate::autotune;
use crate::frame::{self, FrameHeader, FrameType, HEADER_LEN};
use crate::manifest::{FileEntry, FileScan, Manifest};
use crate::record;
use crate::util;

use anyhow::{bail, Context, Result};
use filetime::FileTime;
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngCore;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
struct ChunkPlan {
    file_id: u32,
    abs_path: PathBuf,
    offset: u64,
    len: u32,
}

pub fn encode_folder(
    input_folder: &Path,
    output_dir: &Path,
    mode: &str,
    workers_override: Option<usize>,
    chunk_mib_override: Option<u64>,
    segment_gib_override: Option<u64>,
    frame_override: Option<&str>,
) -> Result<()> {
    if !input_folder.is_dir() {
        bail!("input_folder must be a directory");
    }
    std::fs::create_dir_all(output_dir)?;

    let mut tune = autotune::auto_tune_for_encode(input_folder)?;

    if let Some(w) = workers_override {
        tune.workers = w.max(1);
    }
    if let Some(mib) = chunk_mib_override {
        tune.chunk_size_bytes = mib.max(1) * 1024 * 1024;
    }
    if let Some(gib) = segment_gib_override {
        tune.segment_payload_bytes = gib.max(1) * 1024 * 1024 * 1024;
    }
    if let Some(fr) = frame_override {
        match fr.to_lowercase().as_str() {
            "4k" => { tune.frame_w = 3840; tune.frame_h = 2160; }
            "sq4k" => { tune.frame_w = 4096; tune.frame_h = 4096; }
            _ => bail!("frame must be '4k' or 'sq4k'"),
        }
    }

    let dataset_id = random_16();
    let dataset_hex = util::dataset_id_hex(dataset_id);
    let root_name = util::folder_basename(input_folder);

    // Scan filesystem
    let (dirs, files) = scan_folder(input_folder)?;
    let total_bytes: u64 = files.iter().map(|f| f.entry.size).sum();

    // Plan chunk assignments into segments (keeps file order -> more sequential IO)
    let segments = plan_segments(
        &files,
        tune.chunk_size_bytes,
        tune.segment_payload_bytes,
    )?;
    let planned_segments = segments.len() as u32;

    let created = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

    let manifest = Manifest {
        version: 1,
        dataset_id,
        created_utc_unix: created,
        root_name,
        frame_w: tune.frame_w,
        frame_h: tune.frame_h,
        fps: tune.fps,
        chunk_size_bytes: tune.chunk_size_bytes,
        approx_segment_payload_bytes: tune.segment_payload_bytes,
        mode: mode.to_string(),
        planned_segments,
        dirs,
        files: files.iter().map(|f| f.entry.clone()).collect(),
    };

    let manifest_json = serde_json::to_vec(&manifest)?;
    let mut manifest_blob = Vec::with_capacity(8 + manifest_json.len());
    manifest_blob.extend_from_slice(&(manifest_json.len() as u64).to_le_bytes());
    manifest_blob.extend_from_slice(&manifest_json);

    eprintln!(
        "Encode plan: total={}GiB files={} segments={} workers={}",
        total_bytes / (1024 * 1024 * 1024),
        files.len(),
        planned_segments,
        tune.workers
    );

    // Progress
    let pb = ProgressBar::new(total_bytes);
    pb.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {wide_bar} {bytes}/{total_bytes} ({bytes_per_sec})")
            .unwrap(),
    );

    // Run segment encoders concurrently (N workers)
    // If segments > workers, we schedule in waves
    let mut seg_index = 0usize;
    while seg_index < segments.len() {
        let wave_end = (seg_index + tune.workers).min(segments.len());
        let wave = &segments[seg_index..wave_end];

        let mut handles = vec![];
        for (i, seg) in wave.iter().enumerate() {
            let seg_id = (seg_index + i) as u32;
            let out_path = output_dir.join(format!(
                "{}_{}x{}_part{:04}.mkv",
                dataset_hex, tune.frame_w, tune.frame_h, seg_id
            ));

            let seg_chunks = seg.clone();
            let manifest_blob = manifest_blob.clone();
            let pb = pb.clone();
            let dataset_id = dataset_id;
            let mode = mode.to_string();
            let w = tune.frame_w;
            let h = tune.frame_h;
            let fps = tune.fps;

            handles.push(std::thread::spawn(move || -> Result<()> {
                encode_one_segment(
                    &out_path,
                    &mode,
                    w,
                    h,
                    fps,
                    dataset_id,
                    seg_id,
                    &manifest_blob,
                    &seg_chunks,
                    pb,
                    tune.workers,
                )
            }));
        }

        for h in handles {
            h.join().expect("thread panicked")?;
        }

        seg_index = wave_end;
    }

    pb.finish_with_message("done");
    println!("Encoded dataset {} into {}", dataset_hex, output_dir.display());
    Ok(())
}

fn encode_one_segment(
    out_path: &Path,
    mode: &str,
    w: u32,
    h: u32,
    fps: u32,
    dataset_id: [u8; 16],
    video_id: u32,
    manifest_blob: &[u8],
    chunks: &[ChunkPlan],
    pb: ProgressBar,
    total_workers: usize,
) -> Result<()> {
    let frame_bytes = (w as usize) * (h as usize) * 3;
    if frame_bytes <= HEADER_LEN + 1024 {
        bail!("frame too small");
    }
    let payload_cap = frame_bytes - HEADER_LEN;

    // Limit ffmpeg threads per process so multiple segments don't each steal all cores.
    let cores = num_cpus::get().max(1);
    let per_proc_threads = (cores / total_workers.max(1)).max(1);

    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-hide_banner")
        .arg("-loglevel").arg("error")
        .arg("-y")
        .arg("-f").arg("rawvideo")
        .arg("-pix_fmt").arg("rgb24")
        .arg("-s").arg(format!("{}x{}", w, h))
        .arg("-r").arg(fps.to_string())
        .arg("-i").arg("pipe:0");

    if mode == "raw" {
        cmd.arg("-c:v").arg("rawvideo")
            .arg("-f").arg("matroska");
    } else {
        // FFV1 fast lossless defaults
        cmd.arg("-c:v").arg("ffv1")
            .arg("-level").arg("3")
            .arg("-coder").arg("1")
            .arg("-context").arg("1")
            .arg("-slicecrc").arg("1")
            .arg("-threads").arg(per_proc_threads.to_string())
            .arg("-slices").arg(std::cmp::min(32, per_proc_threads * 2).to_string())
            .arg("-f").arg("matroska");
    }

    cmd.arg(out_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().context("failed to spawn ffmpeg")?;
    let stdin = child.stdin.take().unwrap();
    let mut wtr = BufWriter::with_capacity(8 * 1024 * 1024, stdin);

    // ---- 1) Write MANIFEST frames (type=Manifest) ----
    let mut frame_index = 0u64;
    let mut stream_offset = 0u64;

    let mut pos = 0usize;
    while pos < manifest_blob.len() {
        let take = (manifest_blob.len() - pos).min(payload_cap);
        let payload = &manifest_blob[pos..pos + take];

        let hdr = FrameHeader {
            version: 1,
            frame_type: FrameType::Manifest,
            flags: 0,
            dataset_id,
            video_id,
            frame_index,
            stream_offset,
            payload_len: take as u32,
            payload_crc32: frame::crc32(payload),
        };

        let frame = frame::build_frame_bytes(frame_bytes, &hdr, payload);
        wtr.write_all(&frame)?;

        pos += take;
        frame_index += 1;
        stream_offset += take as u64;
    }

    // ---- 2) Write DATA frames (records packed into payload stream) ----
    let mut packer = FramePacker::new(frame_bytes, payload_cap);

    // reuse open files for speed
    let mut open_files: HashMap<PathBuf, File> = HashMap::new();

    for ch in chunks {
        let data = read_chunk(&mut open_files, &ch.abs_path, ch.offset, ch.len as usize)?;
        pb.inc(data.len() as u64);

        let rec = record::build_data_record(ch.file_id, ch.offset, &data);
        packer.push_bytes(&rec);

        while let Some(payload) = packer.take_full_payload() {
            let hdr = FrameHeader {
                version: 1,
                frame_type: FrameType::Data,
                flags: 0,
                dataset_id,
                video_id,
                frame_index,
                stream_offset,
                payload_len: payload.len() as u32,
                payload_crc32: frame::crc32(&payload),
            };
            let frame = frame::build_frame_bytes(frame_bytes, &hdr, &payload);
            wtr.write_all(&frame)?;
            frame_index += 1;
            stream_offset += payload.len() as u64;
        }
    }

    // End record (optional marker)
    packer.push_bytes(&record::build_end_record());

    // flush remaining partial payload
    if let Some(payload) = packer.take_any_payload() {
        let hdr = FrameHeader {
            version: 1,
            frame_type: FrameType::Data,
            flags: 0,
            dataset_id,
            video_id,
            frame_index,
            stream_offset,
            payload_len: payload.len() as u32,
            payload_crc32: frame::crc32(&payload),
        };
        let frame = frame::build_frame_bytes(frame_bytes, &hdr, &payload);
        wtr.write_all(&frame)?;
        frame_index += 1;
        stream_offset += payload.len() as u64;
    }

    // ---- 3) Write END frame ----
    let hdr = FrameHeader {
        version: 1,
        frame_type: FrameType::End,
        flags: 0,
        dataset_id,
        video_id,
        frame_index,
        stream_offset,
        payload_len: 0,
        payload_crc32: 0,
    };
    let end_frame = frame::build_frame_bytes(frame_bytes, &hdr, &[]);
    wtr.write_all(&end_frame)?;
    wtr.flush()?;

    drop(wtr);

    let status = child.wait()?;
    if !status.success() {
        bail!("ffmpeg failed for {:?}", out_path);
    }
    Ok(())
}

struct FramePacker {
    payload_cap: usize,
    cur: Vec<u8>,
}

impl FramePacker {
    fn new(_frame_bytes: usize, payload_cap: usize) -> Self {
        Self { payload_cap, cur: Vec::with_capacity(payload_cap) }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        self.cur.extend_from_slice(bytes);
    }

    fn take_full_payload(&mut self) -> Option<Vec<u8>> {
        if self.cur.len() >= self.payload_cap {
            let out = self.cur.drain(0..self.payload_cap).collect::<Vec<u8>>();
            Some(out)
        } else {
            None
        }
    }

    fn take_any_payload(&mut self) -> Option<Vec<u8>> {
        if self.cur.is_empty() { None } else { Some(std::mem::take(&mut self.cur)) }
    }
}

fn read_chunk(open_files: &mut HashMap<PathBuf, File>, path: &Path, offset: u64, len: usize) -> Result<Vec<u8>> {
    let f = open_files.entry(path.to_path_buf()).or_insert_with(|| File::open(path).unwrap());
    f.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

fn scan_folder(root: &Path) -> Result<(Vec<String>, Vec<FileScan>)> {
    let mut dirs = vec![];
    let mut files = vec![];
    let mut id: u32 = 0;

    for entry in WalkDir::new(root) {
        let e = entry?;
        let p = e.path();

        let rel = p.strip_prefix(root)?.to_string_lossy().replace("\\", "/");
        if rel.is_empty() {
            continue;
        }

        if p.is_dir() {
            dirs.push(rel);
        } else if p.is_file() {
            let meta = std::fs::metadata(p)?;
            let mtime = meta.modified().ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            let fe = FileEntry {
                id,
                rel_path: rel,
                size: meta.len(),
                mtime_unix: mtime,
            };
            files.push(FileScan { entry: fe, abs_path: p.to_path_buf() });
            id += 1;
        }
    }

    // Stable order helps sequentiality
    files.sort_by(|a, b| a.entry.rel_path.cmp(&b.entry.rel_path));
    dirs.sort();
    Ok((dirs, files))
}

fn plan_segments(files: &[FileScan], chunk_size: u64, target_payload: u64) -> Result<Vec<Vec<ChunkPlan>>> {
    // Estimate record overhead per chunk
    const REC_OVERHEAD: u64 = 1 + 4 + 8 + 4 + 4; // kind + file_id + offset + len + crc

    let mut segments: Vec<Vec<ChunkPlan>> = vec![];
    let mut current: Vec<ChunkPlan> = vec![];
    let mut cur_bytes: u64 = 0;

    for f in files {
        let mut offset = 0u64;
        while offset < f.entry.size {
            let len = std::cmp::min(chunk_size, f.entry.size - offset) as u32;

            let approx = (len as u64) + REC_OVERHEAD;
            if !current.is_empty() && cur_bytes + approx > target_payload {
                segments.push(current);
                current = vec![];
                cur_bytes = 0;
            }

            current.push(ChunkPlan {
                file_id: f.entry.id,
                abs_path: f.abs_path.clone(),
                offset,
                len,
            });

            cur_bytes += approx;
            offset += len as u64;
        }
    }

    if !current.is_empty() {
        segments.push(current);
    }
    Ok(segments)
}

fn random_16() -> [u8; 16] {
    let mut id = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut id);
    id
}
