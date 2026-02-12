use crate::autotune;
use crate::frame::{self, FrameHeader, FrameType, HEADER_LEN};
use crate::manifest::{FileEntry, FileScan, Manifest};
use crate::progress::{EncodeSummary, ProgressConfig, ProgressHandle, ProgressReporter};
use crate::record;
use crate::util;

use anyhow::{bail, Context, Result};
use memmap2::MmapOptions;
use rand::RngCore;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
struct ChunkPlan {
    file_id: u32,
    abs_path: PathBuf,
    offset: u64,
    len: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct EncodeIoConfig {
    pub mmap_enabled: bool,
    pub mmap_threshold_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct StitchConfig {
    pub enabled: bool,
}

impl Default for EncodeIoConfig {
    fn default() -> Self {
        Self {
            mmap_enabled: true,
            mmap_threshold_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
struct EncodeIoStats {
    mapped_files: AtomicU64,
    mapped_bytes: AtomicU64,
    mmap_fallbacks: AtomicU64,
    mapped_seen: Mutex<HashSet<PathBuf>>,
    fallback_seen: Mutex<HashSet<PathBuf>>,
}

pub fn encode_folder(
    input_folder: &Path,
    output_dir: &Path,
    mode: &str,
    workers_override: Option<usize>,
    chunk_mib_override: Option<u64>,
    segment_gib_override: Option<u64>,
    frame_override: Option<&str>,
    progress: ProgressConfig,
    io_config: EncodeIoConfig,
    stitch_config: StitchConfig,
) -> Result<EncodeSummary> {
    if !input_folder.is_dir() {
        bail!("input_folder must be a directory");
    }
    std::fs::create_dir_all(output_dir)?;

    let reporter = ProgressReporter::new("encode", 0, progress);
    let progress = reporter.handle();
    progress.set_stage("scan");
    let io_stats = Arc::new(EncodeIoStats {
        mapped_files: AtomicU64::new(0),
        mapped_bytes: AtomicU64::new(0),
        mmap_fallbacks: AtomicU64::new(0),
        mapped_seen: Mutex::new(HashSet::new()),
        fallback_seen: Mutex::new(HashSet::new()),
    });

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
            "4k" => {
                tune.frame_w = 3840;
                tune.frame_h = 2160;
            }
            "sq4k" => {
                tune.frame_w = 4096;
                tune.frame_h = 4096;
            }
            _ => bail!("frame must be '4k' or 'sq4k'"),
        }
    }

    let dataset_id = random_16();
    let dataset_hex = util::dataset_id_hex(dataset_id);
    let root_name = util::folder_basename(input_folder);
    let segment_output_dir = if stitch_config.enabled {
        output_dir.join(format!(".viddybud_stitch_tmp_{}", dataset_hex))
    } else {
        output_dir.to_path_buf()
    };
    if stitch_config.enabled {
        std::fs::create_dir_all(&segment_output_dir)?;
    }

    // Scan filesystem
    let (dirs, files) = scan_folder(input_folder)?;
    let total_bytes: u64 = files.iter().map(|f| f.entry.size).sum();
    progress.set_total_bytes(total_bytes);

    progress.set_stage("plan");

    // Plan chunk assignments into segments (keeps file order -> more sequential IO)
    let segments = plan_segments(&files, tune.chunk_size_bytes, tune.segment_payload_bytes)?;
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

    progress.log(format!(
        "Encode plan: total={}GiB files={} segments={} workers={} mode={} frame={}x{} stitch={} strategy={}",
        total_bytes / (1024 * 1024 * 1024),
        files.len(),
        planned_segments,
        tune.workers,
        mode,
        tune.frame_w,
        tune.frame_h,
        stitch_config.enabled,
        if stitch_config.enabled {
            "multitrack-copy"
        } else {
            "none"
        },
    ));

    // Run segment encoders concurrently (N workers)
    // If segments > workers, schedule in waves.
    let mut segment_paths = vec![PathBuf::new(); segments.len()];
    let mut seg_index = 0usize;
    while seg_index < segments.len() {
        let wave_end = (seg_index + tune.workers).min(segments.len());
        let wave = &segments[seg_index..wave_end];

        progress.set_stage(format!(
            "encode wave {}/{}",
            (seg_index / tune.workers) + 1,
            segments.len().div_ceil(tune.workers)
        ));

        let mut handles = vec![];
        for (i, seg) in wave.iter().enumerate() {
            let seg_id = (seg_index + i) as u32;
            let out_path = segment_output_dir.join(format!(
                "{}_{}x{}_part{:04}.mkv",
                dataset_hex, tune.frame_w, tune.frame_h, seg_id
            ));
            segment_paths[seg_id as usize] = out_path.clone();

            let seg_chunks = seg.clone();
            let manifest_blob = manifest_blob.clone();
            let progress = progress.clone();
            let dataset_id = dataset_id;
            let mode = mode.to_string();
            let w = tune.frame_w;
            let h = tune.frame_h;
            let fps = tune.fps;
            let op_id = format!("seg{:04}", seg_id);
            let io_stats = Arc::clone(&io_stats);

            progress.set_operation_status(
                op_id.clone(),
                format!(
                    "queued chunks={} output={}",
                    seg_chunks.len(),
                    out_path.display()
                ),
            );

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
                    progress,
                    &op_id,
                    tune.workers,
                    io_config,
                    io_stats,
                )
            }));
        }

        for h in handles {
            h.join().expect("thread panicked")?;
        }

        seg_index = wave_end;
    }

    let mut stitched_output = None;
    let mut stitch_tracks = 0usize;
    let mut stitch_elapsed = Duration::ZERO;
    if stitch_config.enabled {
        let stitched_path = output_dir.join(format!(
            "{}_{}x{}_stitched.mkv",
            dataset_hex, tune.frame_w, tune.frame_h
        ));
        progress.set_stage("stitch remux");
        stitch_elapsed = stitch_segments_to_multitrack_mkv(
            &segment_paths,
            &stitched_path,
            &dataset_hex,
            mode == "raw",
            &progress,
        )
        .with_context(|| {
            format!(
                "stitch remux failed: temp_dir={} output={}",
                segment_output_dir.display(),
                stitched_path.display()
            )
        })?;
        stitch_tracks = segment_paths.len();
        stitched_output = Some(stitched_path);

        progress.set_stage("stitch cleanup");
        std::fs::remove_dir_all(&segment_output_dir).with_context(|| {
            format!(
                "stitch cleanup failed (strict single-file guarantee): temp_dir={}",
                segment_output_dir.display()
            )
        })?;
    }

    progress.set_stage("finalize");
    let outcome = reporter.finish(format!(
        "encoded dataset {} into {}",
        dataset_hex,
        output_dir.display()
    ));

    Ok(EncodeSummary {
        dataset_hex,
        output_dir: output_dir.to_path_buf(),
        total_bytes,
        processed_bytes: outcome.processed_bytes,
        file_count: files.len(),
        segment_count: planned_segments as usize,
        workers: tune.workers,
        elapsed: outcome.elapsed,
        avg_bytes_per_sec: outcome.avg_bytes_per_sec,
        warning_count: outcome.warning_count,
        warnings: outcome.warnings,
        mmap_enabled: io_config.mmap_enabled,
        mmap_threshold_bytes: io_config.mmap_threshold_bytes,
        mapped_files: io_stats.mapped_files.load(Ordering::Relaxed) as usize,
        mapped_bytes: io_stats.mapped_bytes.load(Ordering::Relaxed),
        mmap_fallbacks: io_stats.mmap_fallbacks.load(Ordering::Relaxed) as usize,
        stitched: stitch_config.enabled,
        stitched_output,
        stitch_tracks,
        stitch_elapsed,
    })
}

#[allow(clippy::too_many_arguments)]
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
    progress: ProgressHandle,
    operation_id: &str,
    total_workers: usize,
    io_config: EncodeIoConfig,
    io_stats: Arc<EncodeIoStats>,
) -> Result<()> {
    let operation_id = operation_id.to_string();
    let result: Result<()> = (|| {
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
            .arg("-loglevel")
            .arg("error")
            .arg("-y")
            .arg("-f")
            .arg("rawvideo")
            .arg("-pix_fmt")
            .arg("rgb24")
            .arg("-s")
            .arg(format!("{}x{}", w, h))
            .arg("-r")
            .arg(fps.to_string())
            .arg("-i")
            .arg("pipe:0");

        if mode == "raw" {
            cmd.arg("-c:v")
                .arg("rawvideo")
                .arg("-allow_raw_vfw")
                .arg("1")
                .arg("-f")
                .arg("matroska");
        } else {
            // FFV1 fast lossless defaults
            cmd.arg("-c:v")
                .arg("ffv1")
                .arg("-level")
                .arg("3")
                .arg("-coder")
                .arg("1")
                .arg("-context")
                .arg("1")
                .arg("-slicecrc")
                .arg("1")
                .arg("-threads")
                .arg(per_proc_threads.to_string())
                .arg("-slices")
                .arg(std::cmp::min(32, per_proc_threads * 2).to_string())
                .arg("-f")
                .arg("matroska");
        }

        cmd.arg(out_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        progress.set_operation_status(operation_id.clone(), "spawning ffmpeg".to_string());

        let mut child = cmd.spawn().context("failed to spawn ffmpeg")?;
        let stdin = child.stdin.take().context("ffmpeg stdin missing")?;
        let mut wtr = BufWriter::with_capacity(8 * 1024 * 1024, stdin);

        let stderr_handle = child.stderr.take().map(spawn_stderr_collector);

        // ---- 1) Write MANIFEST frames (type=Manifest) ----
        let mut frame_index = 0u64;
        let mut stream_offset = 0u64;

        progress.set_operation_status(operation_id.clone(), "writing manifest".to_string());

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
        let mut packer = FramePacker::new(payload_cap);

        // reuse open files for speed
        let mut open_sources: HashMap<PathBuf, SourceReader> = HashMap::new();
        let total_chunk_bytes: u64 = chunks.iter().map(|c| c.len as u64).sum();
        let mut written_chunk_bytes = 0u64;
        let mut last_status_update = Instant::now();

        for (chunk_idx, ch) in chunks.iter().enumerate() {
            let data = read_chunk(
                &mut open_sources,
                &ch.abs_path,
                ch.offset,
                ch.len as usize,
                io_config,
                &io_stats,
                &progress,
            )?;
            progress.inc_bytes(data.len() as u64);
            written_chunk_bytes = written_chunk_bytes.saturating_add(data.len() as u64);

            if chunk_idx == 0 || last_status_update.elapsed().as_millis() > 900 {
                let pct = if total_chunk_bytes == 0 {
                    100.0
                } else {
                    (written_chunk_bytes as f64 / total_chunk_bytes as f64) * 100.0
                };
                progress.set_operation_status(
                    operation_id.clone(),
                    format!(
                        "encoding {:.1}% chunks={}/{}",
                        pct,
                        chunk_idx + 1,
                        chunks.len()
                    ),
                );
                last_status_update = Instant::now();
            }

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

        progress.set_operation_status(operation_id.clone(), "waiting for ffmpeg".to_string());
        let status = child.wait()?;
        let stderr_lines = stderr_handle
            .map(|h| h.join().unwrap_or_default())
            .unwrap_or_default();
        if !status.success() {
            let tail = if stderr_lines.is_empty() {
                "<no ffmpeg stderr>".to_string()
            } else {
                stderr_lines.join(" | ")
            };
            bail!(
                "ffmpeg failed for segment={} out={} status={} stderr_tail={}",
                video_id,
                out_path.display(),
                status,
                tail
            );
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            progress.clear_operation(&operation_id, Some("done"));
            Ok(())
        }
        Err(err) => {
            progress.clear_operation(&operation_id, Some("failed"));
            Err(err)
        }
    }
}

struct FramePacker {
    payload_cap: usize,
    cur: Vec<u8>,
}

impl FramePacker {
    fn new(payload_cap: usize) -> Self {
        Self {
            payload_cap,
            cur: Vec::with_capacity(payload_cap),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        self.cur.extend_from_slice(bytes);
    }

    fn take_full_payload(&mut self) -> Option<Vec<u8>> {
        if self.cur.len() >= self.payload_cap {
            Some(self.cur.drain(0..self.payload_cap).collect::<Vec<u8>>())
        } else {
            None
        }
    }

    fn take_any_payload(&mut self) -> Option<Vec<u8>> {
        if self.cur.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.cur))
        }
    }
}

enum SourceReader {
    File(File),
    Mmap(memmap2::Mmap),
}

fn read_chunk(
    open_sources: &mut HashMap<PathBuf, SourceReader>,
    path: &Path,
    offset: u64,
    len: usize,
    io_config: EncodeIoConfig,
    io_stats: &Arc<EncodeIoStats>,
    progress: &ProgressHandle,
) -> Result<Vec<u8>> {
    if !open_sources.contains_key(path) {
        let mut inserted = false;
        if io_config.mmap_enabled {
            let meta = std::fs::metadata(path).with_context(|| format!("metadata {:?}", path))?;
            if meta.len() >= io_config.mmap_threshold_bytes {
                match File::open(path)
                    .with_context(|| format!("open {:?} for mmap", path))
                    .and_then(|f| unsafe {
                        MmapOptions::new()
                            .map(&f)
                            .with_context(|| format!("mmap {:?}", path))
                    }) {
                    Ok(map) => {
                        open_sources.insert(path.to_path_buf(), SourceReader::Mmap(map));
                        inserted = true;
                        let mut seen = io_stats.mapped_seen.lock().unwrap();
                        if seen.insert(path.to_path_buf()) {
                            io_stats.mapped_files.fetch_add(1, Ordering::Relaxed);
                            io_stats
                                .mapped_bytes
                                .fetch_add(meta.len(), Ordering::Relaxed);
                        }
                    }
                    Err(err) => {
                        let mut seen = io_stats.fallback_seen.lock().unwrap();
                        if seen.insert(path.to_path_buf()) {
                            io_stats.mmap_fallbacks.fetch_add(1, Ordering::Relaxed);
                            progress.warning(format!(
                                "mmap fallback for {} ({})",
                                path.display(),
                                err
                            ));
                        }
                    }
                }
            }
        }

        if !inserted {
            let file = File::open(path).with_context(|| format!("open {:?}", path))?;
            open_sources.insert(path.to_path_buf(), SourceReader::File(file));
        }
    }

    let src = open_sources
        .get_mut(path)
        .context("chunk source missing from cache")?;
    match src {
        SourceReader::Mmap(map) => {
            let start = offset as usize;
            let end = start.saturating_add(len);
            if end > map.len() {
                bail!(
                    "mmap out-of-bounds read path={} start={} len={} map_len={}",
                    path.display(),
                    start,
                    len,
                    map.len()
                );
            }
            Ok(map[start..end].to_vec())
        }
        SourceReader::File(f) => {
            f.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; len];
            f.read_exact(&mut buf)?;
            Ok(buf)
        }
    }
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
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            let fe = FileEntry {
                id,
                rel_path: rel,
                size: meta.len(),
                mtime_unix: mtime,
            };
            files.push(FileScan {
                entry: fe,
                abs_path: p.to_path_buf(),
            });
            id += 1;
        }
    }

    // Stable order helps sequentiality
    files.sort_by(|a, b| a.entry.rel_path.cmp(&b.entry.rel_path));
    dirs.sort();
    Ok((dirs, files))
}

fn plan_segments(
    files: &[FileScan],
    chunk_size: u64,
    target_payload: u64,
) -> Result<Vec<Vec<ChunkPlan>>> {
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

fn stitch_segments_to_multitrack_mkv(
    segment_paths: &[PathBuf],
    stitched_output: &Path,
    dataset_hex: &str,
    allow_raw_vfw: bool,
    progress: &ProgressHandle,
) -> Result<Duration> {
    let started = Instant::now();
    let op_id = "stitch";
    progress.set_operation_status(
        op_id,
        format!(
            "remuxing tracks={} output={}",
            segment_paths.len(),
            stitched_output.display()
        ),
    );

    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-y");
    for p in segment_paths {
        cmd.arg("-i").arg(p);
    }
    for idx in 0..segment_paths.len() {
        cmd.arg("-map").arg(format!("{}:v:0", idx));
    }
    cmd.arg("-c").arg("copy");
    if allow_raw_vfw {
        cmd.arg("-allow_raw_vfw").arg("1");
    }
    cmd.arg("-f")
        .arg("matroska")
        .arg("-metadata")
        .arg("viddybud_stitched=1")
        .arg("-metadata")
        .arg(format!("viddybud_segment_count={}", segment_paths.len()))
        .arg("-metadata")
        .arg(format!("viddybud_dataset={}", dataset_hex))
        .arg(stitched_output)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().context("spawn ffmpeg stitch remux")?;
    let stderr_handle = child.stderr.take().map(spawn_stderr_collector);
    let status = child.wait().context("wait for ffmpeg stitch remux")?;
    let stderr_lines = stderr_handle
        .map(|h| h.join().unwrap_or_default())
        .unwrap_or_default();
    if !status.success() {
        let tail = if stderr_lines.is_empty() {
            "<no ffmpeg stderr>".to_string()
        } else {
            stderr_lines.join(" | ")
        };
        progress.clear_operation(op_id, Some("failed"));
        bail!(
            "ffmpeg stitch remux failed output={} tracks={} status={} stderr_tail={}",
            stitched_output.display(),
            segment_paths.len(),
            status,
            tail
        );
    }

    let elapsed = started.elapsed();
    progress.clear_operation(op_id, Some("done"));
    Ok(elapsed)
}

fn spawn_stderr_collector(stderr: impl Read + Send + 'static) -> JoinHandle<Vec<String>> {
    std::thread::spawn(move || {
        let mut lines = VecDeque::new();
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();

        loop {
            line.clear();
            let read = reader.read_line(&mut line).unwrap_or(0);
            if read == 0 {
                break;
            }
            let cleaned = line.trim().to_string();
            if cleaned.is_empty() {
                continue;
            }
            lines.push_back(cleaned);
            if lines.len() > 20 {
                lines.pop_front();
            }
        }

        lines.into_iter().collect::<Vec<_>>()
    })
}
