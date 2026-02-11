use crate::autotune::{self, DecodePipelineTune};
use crate::frame::{self, FrameType, HEADER_LEN};
use crate::manifest::Manifest;
use crate::progress::{DecodeSummary, ProgressConfig, ProgressHandle, ProgressReporter};
use crate::record::{self, RecordParser};
use crate::util;

use anyhow::{anyhow, bail, Context, Result};
use clap::ValueEnum;
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use filetime::FileTime;
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum DecodeEngine {
    Parallel,
    Sequential,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodePerfConfig {
    pub engine: DecodeEngine,
    pub decode_workers: Option<usize>,
    pub writer_workers: Option<usize>,
    pub queue_mib: Option<u64>,
    pub batch_bytes: Option<u64>,
    pub batch_ms: Option<u64>,
    pub read_ahead_frames: Option<usize>,
    pub unsafe_skip_crc: bool,
}

impl Default for DecodePerfConfig {
    fn default() -> Self {
        Self {
            engine: DecodeEngine::Parallel,
            decode_workers: None,
            writer_workers: None,
            queue_mib: None,
            batch_bytes: None,
            batch_ms: None,
            read_ahead_frames: None,
            unsafe_skip_crc: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DecodeRuntimeConfig {
    engine: DecodeEngine,
    decode_workers: usize,
    writer_workers: usize,
    queue_bytes: u64,
    batch_bytes: usize,
    batch_ms: u64,
    read_ahead_frames: usize,
    crc_enabled: bool,
}

#[derive(Debug)]
struct SharedDecodeStats {
    queue_bytes: AtomicU64,
    queue_peak_bytes: AtomicU64,
    batches_flushed: AtomicU64,
    batch_bytes_flushed: AtomicU64,
    decoder_waits: AtomicU64,
    writer_waits: AtomicU64,
}

impl Default for SharedDecodeStats {
    fn default() -> Self {
        Self {
            queue_bytes: AtomicU64::new(0),
            queue_peak_bytes: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            batch_bytes_flushed: AtomicU64::new(0),
            decoder_waits: AtomicU64::new(0),
            writer_waits: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
struct WriteJob {
    file_id: usize,
    offset: u64,
    data: Vec<u8>,
    mkv_idx: usize,
    frame_index: u64,
}

#[derive(Debug)]
struct PendingBatch {
    start_offset: u64,
    bytes: Vec<u8>,
    last_update: Instant,
    mkv_idx: usize,
    frame_index: u64,
}

pub fn decode_folder(
    input_dir: &Path,
    output_folder: &Path,
    writers_override: Option<usize>,
    progress_cfg: ProgressConfig,
    perf_cfg: DecodePerfConfig,
) -> Result<DecodeSummary> {
    std::fs::create_dir_all(output_folder)?;

    let reporter = ProgressReporter::new("decode", 0, progress_cfg);
    let progress = reporter.handle();
    progress.set_stage("discover segments");

    let mkvs = util::list_mkvs(input_dir)?;
    if mkvs.is_empty() {
        bail!("No .mkv files found in {:?}", input_dir);
    }

    progress.set_stage("detect frame size");
    let (w, h) = detect_frame_size(&mkvs[0])?;
    progress.log(format!("Detected frame size: {}x{}", w, h));

    progress.set_stage("read manifest");
    let manifest = read_manifest(&mkvs[0], w, h)?;
    let total_bytes: u64 = manifest.files.iter().map(|f| f.size).sum();
    progress.set_total_bytes(total_bytes);
    progress.log(format!(
        "Manifest: dataset={} segments={} mode={} chunk={}MiB",
        util::dataset_id_hex(manifest.dataset_id),
        manifest.planned_segments,
        manifest.mode,
        manifest.chunk_size_bytes / (1024 * 1024),
    ));

    progress.set_stage("prepare output");
    for d in &manifest.dirs {
        let p = output_folder.join(d.replace("/", &std::path::MAIN_SEPARATOR.to_string()));
        std::fs::create_dir_all(&p)?;
    }

    let mut output_paths = vec![
        None;
        manifest
            .files
            .iter()
            .map(|f| f.id as usize)
            .max()
            .map(|m| m + 1)
            .unwrap_or(0)
    ];
    for f in &manifest.files {
        let out_path = output_folder.join(
            f.rel_path
                .replace("/", &std::path::MAIN_SEPARATOR.to_string()),
        );
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&out_path)
            .with_context(|| format!("open {:?}", out_path))?;
        file.set_len(f.size)?;
        let file_id = f.id as usize;
        if let Some(slot) = output_paths.get_mut(file_id) {
            if slot.is_some() {
                bail!("duplicate file_id {} in manifest", file_id);
            }
            *slot = Some(out_path);
        } else {
            bail!("manifest file_id {} out of range", file_id);
        }
    }

    let tuned = autotune::auto_tune_decode_pipeline(output_folder, mkvs.len())?;
    let runtime = resolve_runtime_config(perf_cfg, writers_override, tuned, mkvs.len());

    progress.log(format!(
        "Decision snapshot: engine={:?} decode_workers={} writer_workers={} queue={}MiB batch={}MiB/{}ms read_ahead={} crc_enabled={}",
        runtime.engine,
        runtime.decode_workers,
        runtime.writer_workers,
        runtime.queue_bytes / (1024 * 1024),
        runtime.batch_bytes / (1024 * 1024),
        runtime.batch_ms,
        runtime.read_ahead_frames,
        runtime.crc_enabled,
    ));

    let stats = Arc::new(SharedDecodeStats::default());

    match runtime.engine {
        DecodeEngine::Sequential => decode_sequential(
            &mkvs,
            w,
            h,
            &output_paths,
            runtime.crc_enabled,
            progress.clone(),
        )?,
        DecodeEngine::Parallel => decode_parallel(
            &mkvs,
            w,
            h,
            &output_paths,
            runtime,
            Arc::clone(&stats),
            progress.clone(),
        )?,
    }

    progress.set_stage("restore mtimes");
    for f in &manifest.files {
        let out_path = output_folder.join(
            f.rel_path
                .replace("/", &std::path::MAIN_SEPARATOR.to_string()),
        );
        let ft = FileTime::from_unix_time(f.mtime_unix, 0);
        let _ = filetime::set_file_mtime(&out_path, ft);
    }

    let outcome = reporter.finish(format!("decoded dataset into {}", output_folder.display()));
    let batches = stats.batches_flushed.load(Ordering::Relaxed);
    let batch_bytes = stats.batch_bytes_flushed.load(Ordering::Relaxed);

    Ok(DecodeSummary {
        input_dir: input_dir.to_path_buf(),
        output_dir: output_folder.to_path_buf(),
        total_bytes,
        processed_bytes: outcome.processed_bytes,
        file_count: manifest.files.len(),
        segment_count: mkvs.len(),
        decode_workers: runtime.decode_workers,
        writer_workers: runtime.writer_workers,
        queue_peak_bytes: stats.queue_peak_bytes.load(Ordering::Relaxed),
        queue_budget_bytes: runtime.queue_bytes,
        batches_flushed: batches,
        avg_batch_bytes: if batches == 0 {
            0
        } else {
            batch_bytes / batches
        },
        engine: format!("{:?}", runtime.engine).to_lowercase(),
        crc_enabled: runtime.crc_enabled,
        elapsed: outcome.elapsed,
        avg_bytes_per_sec: outcome.avg_bytes_per_sec,
        warning_count: outcome.warning_count,
        warnings: outcome.warnings,
    })
}

fn resolve_runtime_config(
    perf_cfg: DecodePerfConfig,
    writers_override: Option<usize>,
    tuned: DecodePipelineTune,
    mkv_count: usize,
) -> DecodeRuntimeConfig {
    let decode_workers = perf_cfg
        .decode_workers
        .unwrap_or(tuned.decode_workers)
        .max(1)
        .min(mkv_count.max(1));

    let writer_workers = perf_cfg
        .writer_workers
        .or(writers_override)
        .unwrap_or(tuned.writer_workers)
        .max(1);

    let queue_bytes = perf_cfg
        .queue_mib
        .map(|m| m.max(1) * 1024 * 1024)
        .unwrap_or(tuned.queue_bytes)
        .max(64 * 1024 * 1024);

    let batch_bytes = perf_cfg
        .batch_bytes
        .map(|v| v.max((256 * 1024) as u64) as usize)
        .unwrap_or(tuned.batch_bytes)
        .max(256 * 1024);

    let batch_ms = perf_cfg.batch_ms.unwrap_or(tuned.batch_ms).max(1);
    let read_ahead_frames = perf_cfg
        .read_ahead_frames
        .unwrap_or(tuned.read_ahead_frames)
        .max(1);

    DecodeRuntimeConfig {
        engine: perf_cfg.engine,
        decode_workers,
        writer_workers,
        queue_bytes,
        batch_bytes,
        batch_ms,
        read_ahead_frames,
        crc_enabled: !perf_cfg.unsafe_skip_crc,
    }
}

fn decode_parallel(
    mkvs: &[PathBuf],
    w: u32,
    h: u32,
    output_paths: &[Option<PathBuf>],
    runtime: DecodeRuntimeConfig,
    stats: Arc<SharedDecodeStats>,
    progress: ProgressHandle,
) -> Result<()> {
    let cancel = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let first_error = Arc::new(Mutex::new(None::<String>));

    let per_writer_budget = (runtime.queue_bytes / runtime.writer_workers as u64).max(1);
    let per_writer_slots = ((per_writer_budget / runtime.batch_bytes as u64)
        .max(32)
        .min(4096)) as usize;

    let mut writer_txs = Vec::with_capacity(runtime.writer_workers);
    let mut writer_rxs = Vec::with_capacity(runtime.writer_workers);
    for _ in 0..runtime.writer_workers {
        let (tx, rx) = bounded::<WriteJob>(per_writer_slots);
        writer_txs.push(tx);
        writer_rxs.push(rx);
    }

    let output_paths = Arc::new(output_paths.to_vec());
    let writer_handles = writer_rxs
        .into_iter()
        .enumerate()
        .map(|(writer_id, rx)| {
            let output_paths = Arc::clone(&output_paths);
            let stats = Arc::clone(&stats);
            let progress = progress.clone();
            let cancel = Arc::clone(&cancel);
            let first_error = Arc::clone(&first_error);
            std::thread::spawn(move || {
                if let Err(err) = writer_worker(
                    writer_id,
                    rx,
                    output_paths,
                    runtime,
                    stats,
                    progress,
                    Arc::clone(&cancel),
                ) {
                    record_error(&first_error, &cancel, err);
                }
            })
        })
        .collect::<Vec<_>>();

    let mkvs_arc = Arc::new(mkvs.to_vec());
    let tx_arc = Arc::new(writer_txs);
    let next_index = Arc::new(AtomicUsize::new(0));

    let decode_handles = (0..runtime.decode_workers)
        .map(|worker_id| {
            let mkvs_arc = Arc::clone(&mkvs_arc);
            let tx_arc = Arc::clone(&tx_arc);
            let next_index = Arc::clone(&next_index);
            let stats = Arc::clone(&stats);
            let progress = progress.clone();
            let cancel = Arc::clone(&cancel);
            let first_error = Arc::clone(&first_error);

            std::thread::spawn(move || {
                let op_id = format!("dec{:02}", worker_id);
                loop {
                    if cancel.load(Ordering::Relaxed) {
                        break;
                    }
                    let idx = next_index.fetch_add(1, Ordering::Relaxed);
                    if idx >= mkvs_arc.len() {
                        break;
                    }
                    let mkv = &mkvs_arc[idx];
                    progress.set_operation_status(
                        op_id.clone(),
                        format!(
                            "mkv={}/{} {}",
                            idx + 1,
                            mkvs_arc.len(),
                            mkv.file_name().unwrap_or_default().to_string_lossy()
                        ),
                    );

                    if let Err(err) = decode_one_video_parallel(
                        mkv,
                        idx,
                        w,
                        h,
                        runtime,
                        &tx_arc,
                        Arc::clone(&stats),
                        progress.clone(),
                        op_id.clone(),
                        Arc::clone(&cancel),
                    ) {
                        record_error(&first_error, &cancel, err);
                        progress.set_operation_status(op_id.clone(), "failed".to_string());
                        break;
                    }
                }
                progress.clear_operation(&op_id, Some("done"));
            })
        })
        .collect::<Vec<_>>();

    let monitor_progress = progress.clone();
    let monitor_cancel = Arc::clone(&cancel);
    let monitor_done = Arc::clone(&done);
    let monitor_stats = Arc::clone(&stats);
    let monitor_handle = std::thread::spawn(move || {
        let mut last_writer_warn = Instant::now() - Duration::from_secs(10);
        let mut last_decoder_warn = Instant::now() - Duration::from_secs(10);
        let mut prev_decoder_waits = 0u64;
        let mut prev_writer_waits = 0u64;
        while !monitor_done.load(Ordering::Relaxed) {
            let queued = monitor_stats.queue_bytes.load(Ordering::Relaxed);
            let queue_pct = if runtime.queue_bytes == 0 {
                0.0
            } else {
                (queued as f64 / runtime.queue_bytes as f64) * 100.0
            };
            let decoder_waits = monitor_stats.decoder_waits.load(Ordering::Relaxed);
            let writer_waits = monitor_stats.writer_waits.load(Ordering::Relaxed);
            let decoder_wait_delta = decoder_waits.saturating_sub(prev_decoder_waits);
            let writer_wait_delta = writer_waits.saturating_sub(prev_writer_waits);
            prev_decoder_waits = decoder_waits;
            prev_writer_waits = writer_waits;

            let bottleneck = if queue_pct > 70.0 {
                "writer-bound"
            } else if queue_pct < 20.0 {
                "decoder-bound"
            } else {
                "balanced"
            };

            monitor_progress.set_stage(format!(
                "parallel decode queue={:.0}% ({:.0}MiB/{:.0}MiB) bottleneck={}",
                queue_pct,
                queued as f64 / (1024.0 * 1024.0),
                runtime.queue_bytes as f64 / (1024.0 * 1024.0),
                bottleneck
            ));

            if queue_pct > 90.0 && last_writer_warn.elapsed() > Duration::from_secs(5) {
                monitor_progress.warning(format!(
                    "IDLE WARNING: component=writer queue saturated {:.0}%",
                    queue_pct
                ));
                last_writer_warn = Instant::now();
            } else if queue_pct < 10.0
                && writer_wait_delta > (runtime.writer_workers as u64).saturating_mul(8)
                && last_decoder_warn.elapsed() > Duration::from_secs(5)
            {
                monitor_progress.warning(format!(
                    "IDLE WARNING: component=decoder writers waiting (queue={:.0}% writer_wait_delta={})",
                    queue_pct, writer_wait_delta
                ));
                last_decoder_warn = Instant::now();
            } else if queue_pct > 80.0
                && decoder_wait_delta > (runtime.decode_workers as u64).saturating_mul(128)
                && last_writer_warn.elapsed() > Duration::from_secs(5)
            {
                monitor_progress.warning(format!(
                    "IDLE WARNING: component=writer decoders blocked by queue (queue={:.0}% decoder_wait_delta={})",
                    queue_pct, decoder_wait_delta
                ));
                last_writer_warn = Instant::now();
            }

            if monitor_cancel.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    });

    for h in decode_handles {
        if h.join().is_err() {
            record_error(
                &first_error,
                &cancel,
                anyhow!("decode worker thread panicked"),
            );
        }
    }
    drop(tx_arc);

    for h in writer_handles {
        if h.join().is_err() {
            record_error(
                &first_error,
                &cancel,
                anyhow!("writer worker thread panicked"),
            );
        }
    }

    done.store(true, Ordering::Relaxed);
    let _ = monitor_handle.join();

    if let Some(err) = first_error.lock().unwrap().take() {
        bail!(err);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn decode_one_video_parallel(
    mkv: &Path,
    mkv_idx: usize,
    w: u32,
    h: u32,
    runtime: DecodeRuntimeConfig,
    writer_txs: &[Sender<WriteJob>],
    stats: Arc<SharedDecodeStats>,
    progress: ProgressHandle,
    operation_id: String,
    cancel: Arc<AtomicBool>,
) -> Result<()> {
    let frame_bytes = (w as usize) * (h as usize) * 3;
    let payload_cap = frame_bytes - HEADER_LEN;

    let mut child = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(mkv)
        .arg("-f")
        .arg("rawvideo")
        .arg("-pix_fmt")
        .arg("rgb24")
        .arg("-s")
        .arg(format!("{}x{}", w, h))
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn ffmpeg decode {:?}", mkv))?;

    let stderr_handle = child.stderr.take().map(spawn_stderr_collector);

    let stdout = child.stdout.take().context("ffmpeg stdout missing")?;
    let (frame_tx, frame_rx) = bounded::<Vec<u8>>(runtime.read_ahead_frames);
    let reader_cancel = Arc::clone(&cancel);
    let reader_handle = std::thread::spawn(move || -> Result<()> {
        let mut stdout = stdout;
        loop {
            if reader_cancel.load(Ordering::Relaxed) {
                break;
            }
            let mut frame_buf = vec![0u8; frame_bytes];
            let n = read_exact_or_eof(&mut stdout, &mut frame_buf)?;
            if n == 0 {
                break;
            }
            if n < HEADER_LEN {
                continue;
            }
            if frame_tx.send(frame_buf).is_err() {
                break;
            }
        }
        Ok(())
    });

    let mut parser = RecordParser::new();
    let mut tasks: Vec<record::ChunkTask> = vec![];
    let mut frame_counter = 0u64;
    let mut dispatched_bytes = 0u64;
    let started = Instant::now();
    let mut last_status = Instant::now();

    loop {
        if cancel.load(Ordering::Relaxed) {
            break;
        }

        let frame_buf = match frame_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(v) => v,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };

        let hdr = match frame::parse_header(&frame_buf[..HEADER_LEN]) {
            Some(h) => h,
            None => continue,
        };
        frame_counter += 1;

        match hdr.frame_type {
            FrameType::Manifest => {}
            FrameType::Data => {
                let payload_len = hdr.payload_len as usize;
                let take = payload_len.min(payload_cap);
                let payload = &frame_buf[HEADER_LEN..HEADER_LEN + take];

                if runtime.crc_enabled && frame::crc32(payload) != hdr.payload_crc32 {
                    bail!(
                        "DATA payload CRC mismatch mkv={} frame_index={} stage=payload_crc",
                        mkv.display(),
                        hdr.frame_index
                    );
                }

                tasks.clear();
                parser.push(payload, &mut tasks);

                for t in tasks.drain(..) {
                    if runtime.crc_enabled {
                        let got = record::crc32(&t.data);
                        if got != t.expected_crc32 {
                            bail!(
                                "Chunk CRC mismatch file_id={} offset={} mkv={} frame_index={} stage=chunk_crc",
                                t.file_id,
                                t.offset,
                                mkv.display(),
                                hdr.frame_index
                            );
                        }
                    }

                    let len = t.data.len() as u64;
                    reserve_queue_budget(&stats, runtime.queue_bytes, len, &cancel)?;
                    let shard = (t.file_id as usize) % runtime.writer_workers;
                    let job = WriteJob {
                        file_id: t.file_id as usize,
                        offset: t.offset,
                        data: t.data,
                        mkv_idx,
                        frame_index: hdr.frame_index,
                    };

                    if writer_txs[shard].send(job).is_err() {
                        stats.queue_bytes.fetch_sub(len, Ordering::Relaxed);
                        bail!(
                            "writer channel closed while dispatching mkv={}",
                            mkv.display()
                        );
                    }
                    dispatched_bytes = dispatched_bytes.saturating_add(len);
                }
            }
            FrameType::End => break,
        }

        if last_status.elapsed().as_millis() > 900 {
            let elapsed = started.elapsed().as_secs_f64().max(1e-6);
            let bps = dispatched_bytes as f64 / elapsed;
            progress.set_operation_status(
                operation_id.clone(),
                format!(
                    "frames={} queued={}MiB rate={:.1}MiB/s",
                    frame_counter,
                    dispatched_bytes as f64 / (1024.0 * 1024.0),
                    bps / (1024.0 * 1024.0)
                ),
            );
            last_status = Instant::now();
        }
    }

    match reader_handle.join() {
        Ok(reader_res) => reader_res?,
        Err(_) => {
            bail!("frame reader thread panicked mkv={}", mkv.display());
        }
    }

    if cancel.load(Ordering::Relaxed) {
        let _ = child.kill();
    }

    let status = child.wait()?;
    let stderr_lines = stderr_handle
        .and_then(|h| h.join().ok())
        .unwrap_or_default();
    if !status.success() {
        let tail = if stderr_lines.is_empty() {
            "<no ffmpeg stderr>".to_string()
        } else {
            stderr_lines.join(" | ")
        };
        bail!(
            "ffmpeg decode failed mkv={} status={} frames={} stderr_tail={}",
            mkv.display(),
            status,
            frame_counter,
            tail
        );
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn writer_worker(
    writer_id: usize,
    rx: Receiver<WriteJob>,
    output_paths: Arc<Vec<Option<PathBuf>>>,
    runtime: DecodeRuntimeConfig,
    stats: Arc<SharedDecodeStats>,
    progress: ProgressHandle,
    cancel: Arc<AtomicBool>,
) -> Result<()> {
    let op_id = format!("wrt{:02}", writer_id);
    let mut open_files: HashMap<usize, File> = HashMap::new();
    let mut pending: HashMap<usize, PendingBatch> = HashMap::new();
    let mut last_status = Instant::now();

    progress.set_operation_status(op_id.clone(), "ready".to_string());

    loop {
        if cancel.load(Ordering::Relaxed) && rx.is_empty() {
            break;
        }

        match rx.recv_timeout(Duration::from_millis((runtime.batch_ms.max(2) / 2).max(1))) {
            Ok(job) => {
                stats
                    .queue_bytes
                    .fetch_sub(job.data.len() as u64, Ordering::Relaxed);
                ingest_write_job(
                    job,
                    &mut pending,
                    &mut open_files,
                    &output_paths,
                    runtime.batch_bytes,
                    &stats,
                    &progress,
                )?;
            }
            Err(RecvTimeoutError::Timeout) => {
                stats.writer_waits.fetch_add(1, Ordering::Relaxed);
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }

        flush_expired_batches(
            &mut pending,
            &mut open_files,
            &output_paths,
            runtime.batch_ms,
            &stats,
            &progress,
        )?;

        if last_status.elapsed().as_millis() > 700 {
            let queued = stats.queue_bytes.load(Ordering::Relaxed) / (1024 * 1024);
            let flushed = stats.batches_flushed.load(Ordering::Relaxed);
            let avg_batch = if flushed == 0 {
                0
            } else {
                stats.batch_bytes_flushed.load(Ordering::Relaxed) / flushed
            };
            progress.set_operation_status(
                op_id.clone(),
                format!(
                    "queue={}MiB pending_files={} flushed={} avg_batch={}KiB",
                    queued,
                    pending.len(),
                    flushed,
                    avg_batch / 1024
                ),
            );
            last_status = Instant::now();
        }
    }

    flush_all_batches(
        &mut pending,
        &mut open_files,
        &output_paths,
        &stats,
        &progress,
    )?;
    progress.clear_operation(&op_id, Some("done"));
    Ok(())
}

fn ingest_write_job(
    job: WriteJob,
    pending: &mut HashMap<usize, PendingBatch>,
    open_files: &mut HashMap<usize, File>,
    output_paths: &[Option<PathBuf>],
    batch_bytes: usize,
    stats: &SharedDecodeStats,
    progress: &ProgressHandle,
) -> Result<()> {
    match pending.get_mut(&job.file_id) {
        Some(batch) => {
            let expected = batch.start_offset + batch.bytes.len() as u64;
            if job.offset == expected && batch.bytes.len() + job.data.len() <= batch_bytes {
                batch.bytes.extend_from_slice(&job.data);
                batch.last_update = Instant::now();
                batch.mkv_idx = job.mkv_idx;
                batch.frame_index = job.frame_index;
            } else {
                flush_one_batch(
                    job.file_id,
                    pending,
                    open_files,
                    output_paths,
                    stats,
                    progress,
                )?;
                pending.insert(
                    job.file_id,
                    PendingBatch {
                        start_offset: job.offset,
                        bytes: job.data,
                        last_update: Instant::now(),
                        mkv_idx: job.mkv_idx,
                        frame_index: job.frame_index,
                    },
                );
            }
        }
        None => {
            pending.insert(
                job.file_id,
                PendingBatch {
                    start_offset: job.offset,
                    bytes: job.data,
                    last_update: Instant::now(),
                    mkv_idx: job.mkv_idx,
                    frame_index: job.frame_index,
                },
            );
        }
    }

    if let Some(batch) = pending.get(&job.file_id) {
        if batch.bytes.len() >= batch_bytes {
            flush_one_batch(
                job.file_id,
                pending,
                open_files,
                output_paths,
                stats,
                progress,
            )?;
        }
    }

    Ok(())
}

fn flush_expired_batches(
    pending: &mut HashMap<usize, PendingBatch>,
    open_files: &mut HashMap<usize, File>,
    output_paths: &[Option<PathBuf>],
    batch_ms: u64,
    stats: &SharedDecodeStats,
    progress: &ProgressHandle,
) -> Result<()> {
    let now = Instant::now();
    let due = pending
        .iter()
        .filter(|(_, b)| now.duration_since(b.last_update).as_millis() >= batch_ms as u128)
        .map(|(file_id, _)| *file_id)
        .collect::<Vec<_>>();

    for file_id in due {
        flush_one_batch(file_id, pending, open_files, output_paths, stats, progress)?;
    }

    Ok(())
}

fn flush_all_batches(
    pending: &mut HashMap<usize, PendingBatch>,
    open_files: &mut HashMap<usize, File>,
    output_paths: &[Option<PathBuf>],
    stats: &SharedDecodeStats,
    progress: &ProgressHandle,
) -> Result<()> {
    let ids = pending.keys().cloned().collect::<Vec<_>>();
    for file_id in ids {
        flush_one_batch(file_id, pending, open_files, output_paths, stats, progress)?;
    }
    Ok(())
}

fn flush_one_batch(
    file_id: usize,
    pending: &mut HashMap<usize, PendingBatch>,
    open_files: &mut HashMap<usize, File>,
    output_paths: &[Option<PathBuf>],
    stats: &SharedDecodeStats,
    progress: &ProgressHandle,
) -> Result<()> {
    let Some(batch) = pending.remove(&file_id) else {
        return Ok(());
    };

    let path = output_paths
        .get(file_id)
        .and_then(|p| p.as_ref())
        .context("invalid file_id in writer")?;
    let f = get_or_open_rw_file(open_files, file_id, path)?;

    write_all_at(f, batch.start_offset, &batch.bytes).with_context(|| {
        format!(
            "write failure file={} file_id={} offset={} mkv_idx={} frame_index={} shard_write",
            path.display(),
            file_id,
            batch.start_offset,
            batch.mkv_idx,
            batch.frame_index
        )
    })?;

    stats.batches_flushed.fetch_add(1, Ordering::Relaxed);
    stats
        .batch_bytes_flushed
        .fetch_add(batch.bytes.len() as u64, Ordering::Relaxed);
    progress.inc_bytes(batch.bytes.len() as u64);
    Ok(())
}

fn decode_sequential(
    mkvs: &[PathBuf],
    w: u32,
    h: u32,
    output_paths: &[Option<PathBuf>],
    crc_enabled: bool,
    progress: ProgressHandle,
) -> Result<()> {
    for (idx, mkv) in mkvs.iter().enumerate() {
        let op_id = format!("seq{:04}", idx);
        progress.set_stage(format!("sequential decode {}/{}", idx + 1, mkvs.len()));
        progress.set_operation_status(
            op_id.clone(),
            format!(
                "reading {}",
                mkv.file_name().unwrap_or_default().to_string_lossy()
            ),
        );
        decode_one_video_sequential(mkv, w, h, output_paths, crc_enabled, progress.clone())?;
        progress.clear_operation(&op_id, Some("done"));
    }
    Ok(())
}

fn decode_one_video_sequential(
    mkv: &Path,
    w: u32,
    h: u32,
    output_paths: &[Option<PathBuf>],
    crc_enabled: bool,
    progress: ProgressHandle,
) -> Result<()> {
    let frame_bytes = (w as usize) * (h as usize) * 3;
    let payload_cap = frame_bytes - HEADER_LEN;

    let mut child = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(mkv)
        .arg("-f")
        .arg("rawvideo")
        .arg("-pix_fmt")
        .arg("rgb24")
        .arg("-s")
        .arg(format!("{}x{}", w, h))
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn ffmpeg decode {:?}", mkv))?;

    let stderr_handle = child.stderr.take().map(spawn_stderr_collector);
    let mut stdout = child.stdout.take().context("ffmpeg stdout missing")?;
    let mut parser = RecordParser::new();
    let mut tasks: Vec<record::ChunkTask> = vec![];
    let mut open_files: HashMap<usize, File> = HashMap::new();

    loop {
        let mut frame_buf = vec![0u8; frame_bytes];
        let n = read_exact_or_eof(&mut stdout, &mut frame_buf)?;
        if n == 0 {
            break;
        }
        if n < HEADER_LEN {
            continue;
        }

        let hdr = match frame::parse_header(&frame_buf[..HEADER_LEN]) {
            Some(h) => h,
            None => continue,
        };

        match hdr.frame_type {
            FrameType::Manifest => {}
            FrameType::Data => {
                let payload_len = hdr.payload_len as usize;
                let take = payload_len.min(payload_cap);
                let payload = &frame_buf[HEADER_LEN..HEADER_LEN + take];

                if crc_enabled && frame::crc32(payload) != hdr.payload_crc32 {
                    bail!(
                        "DATA payload CRC mismatch mkv={} frame_index={} stage=payload_crc",
                        mkv.display(),
                        hdr.frame_index
                    );
                }

                tasks.clear();
                parser.push(payload, &mut tasks);

                for t in tasks.drain(..) {
                    if crc_enabled {
                        let got = record::crc32(&t.data);
                        if got != t.expected_crc32 {
                            bail!(
                                "Chunk CRC mismatch file_id={} offset={} mkv={} frame_index={} stage=chunk_crc",
                                t.file_id,
                                t.offset,
                                mkv.display(),
                                hdr.frame_index
                            );
                        }
                    }
                    write_chunk(
                        output_paths,
                        &mut open_files,
                        t.file_id as usize,
                        t.offset,
                        &t.data,
                    )
                    .with_context(|| {
                        format!(
                            "write failure mkv={} file_id={} offset={} stage=write_chunk",
                            mkv.display(),
                            t.file_id,
                            t.offset
                        )
                    })?;
                    progress.inc_bytes(t.data.len() as u64);
                }
            }
            FrameType::End => break,
        }
    }

    let status = child.wait()?;
    let stderr_lines = stderr_handle
        .and_then(|h| h.join().ok())
        .unwrap_or_default();
    if !status.success() {
        let tail = if stderr_lines.is_empty() {
            "<no ffmpeg stderr>".to_string()
        } else {
            stderr_lines.join(" | ")
        };
        bail!(
            "ffmpeg decode failed mkv={} status={} stderr_tail={}",
            mkv.display(),
            status,
            tail
        );
    }
    Ok(())
}

fn write_chunk(
    output_paths: &[Option<PathBuf>],
    open_files: &mut HashMap<usize, File>,
    file_id: usize,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    let path = output_paths
        .get(file_id)
        .and_then(|p| p.as_ref())
        .context("invalid file_id")?;
    let f = get_or_open_rw_file(open_files, file_id, path)?;
    write_all_at(f, offset, data)
}

fn get_or_open_rw_file<'a>(
    open_files: &'a mut HashMap<usize, File>,
    file_id: usize,
    path: &Path,
) -> Result<&'a mut File> {
    if !open_files.contains_key(&file_id) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;
        open_files.insert(file_id, file);
    }
    open_files
        .get_mut(&file_id)
        .context("open file cache insert/read failed")
}

fn write_all_at(f: &mut File, offset: u64, data: &[u8]) -> Result<()> {
    #[cfg(windows)]
    {
        let mut written = 0usize;
        while written < data.len() {
            let n = f.seek_write(&data[written..], offset + written as u64)?;
            if n == 0 {
                break;
            }
            written += n;
        }
        if written != data.len() {
            bail!("short write");
        }
        return Ok(());
    }

    #[cfg(not(windows))]
    {
        use std::io::{Seek, SeekFrom, Write};
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(data)?;
        Ok(())
    }
}

fn reserve_queue_budget(
    stats: &SharedDecodeStats,
    budget: u64,
    bytes: u64,
    cancel: &AtomicBool,
) -> Result<()> {
    loop {
        if cancel.load(Ordering::Relaxed) {
            bail!("cancelled while waiting for queue budget");
        }

        let cur = stats.queue_bytes.load(Ordering::Relaxed);
        if cur.saturating_add(bytes) <= budget {
            if stats
                .queue_bytes
                .compare_exchange(cur, cur + bytes, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                update_peak(stats, cur + bytes);
                return Ok(());
            }
            continue;
        }

        stats.decoder_waits.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn update_peak(stats: &SharedDecodeStats, new_value: u64) {
    loop {
        let cur = stats.queue_peak_bytes.load(Ordering::Relaxed);
        if new_value <= cur {
            break;
        }
        if stats
            .queue_peak_bytes
            .compare_exchange(cur, new_value, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            break;
        }
    }
}

fn record_error(
    first_error: &Arc<Mutex<Option<String>>>,
    cancel: &Arc<AtomicBool>,
    err: anyhow::Error,
) {
    let mut g = first_error.lock().unwrap();
    if g.is_none() {
        *g = Some(err.to_string());
    }
    cancel.store(true, Ordering::Relaxed);
}

fn detect_frame_size(mkv: &Path) -> Result<(u32, u32)> {
    let candidates = [(4096, 4096), (3840, 2160), (1920, 1080), (1280, 720)];

    for (w, h) in candidates {
        if header_magic_matches(mkv, w, h)? {
            return Ok((w, h));
        }
    }
    bail!("Could not detect frame size for {:?}", mkv);
}

fn header_magic_matches(mkv: &Path, w: u32, h: u32) -> Result<bool> {
    let frame_bytes = (w as usize) * (h as usize) * 3;

    let mut child = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(mkv)
        .arg("-f")
        .arg("rawvideo")
        .arg("-pix_fmt")
        .arg("rgb24")
        .arg("-s")
        .arg(format!("{}x{}", w, h))
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let mut stdout = child.stdout.take().context("ffmpeg stdout missing")?;
    let mut buf = vec![0u8; frame_bytes];
    let n = stdout.read(&mut buf)?;
    let _ = child.kill();
    if n < HEADER_LEN {
        return Ok(false);
    }

    Ok(&buf[0..4] == b"F2V1")
}

fn read_manifest(mkv: &Path, w: u32, h: u32) -> Result<Manifest> {
    let frame_bytes = (w as usize) * (h as usize) * 3;

    let mut child = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(mkv)
        .arg("-f")
        .arg("rawvideo")
        .arg("-pix_fmt")
        .arg("rgb24")
        .arg("-s")
        .arg(format!("{}x{}", w, h))
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn ffmpeg for manifest")?;

    let mut stdout = child.stdout.take().context("ffmpeg stdout missing")?;

    let mut manifest_blob: Vec<u8> = vec![];
    let mut needed_len: Option<usize> = None;

    loop {
        let mut frame_buf = vec![0u8; frame_bytes];
        let n = read_exact_or_eof(&mut stdout, &mut frame_buf)?;
        if n == 0 {
            break;
        }
        if n < HEADER_LEN {
            continue;
        }

        let hdr = match frame::parse_header(&frame_buf[..HEADER_LEN]) {
            Some(h) => h,
            None => continue,
        };

        if hdr.frame_type != FrameType::Manifest {
            if !manifest_blob.is_empty() {
                break;
            }
            continue;
        }

        let payload_len = hdr.payload_len as usize;
        let payload_cap = frame_bytes - HEADER_LEN;
        let take = payload_len.min(payload_cap);

        let payload = &frame_buf[HEADER_LEN..HEADER_LEN + take];
        if frame::crc32(payload) != hdr.payload_crc32 {
            bail!("Manifest payload CRC mismatch in {:?}", mkv);
        }
        manifest_blob.extend_from_slice(payload);

        if needed_len.is_none() && manifest_blob.len() >= 8 {
            let len = u64::from_le_bytes(manifest_blob[0..8].try_into().unwrap()) as usize;
            needed_len = Some(8 + len);
        }

        if let Some(need) = needed_len {
            if manifest_blob.len() >= need {
                let json = &manifest_blob[8..need];
                let m: Manifest = serde_json::from_slice(json)?;
                let _ = child.kill();
                return Ok(m);
            }
        }
    }

    bail!("Failed to read manifest from {:?}", mkv);
}

fn read_exact_or_eof(r: &mut dyn Read, buf: &mut [u8]) -> Result<usize> {
    let mut got = 0usize;
    while got < buf.len() {
        let n = r.read(&mut buf[got..])?;
        if n == 0 {
            return Ok(got);
        }
        got += n;
    }
    Ok(got)
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
