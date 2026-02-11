use crate::autotune;
use crate::frame::{self, FrameType, HEADER_LEN};
use crate::manifest::Manifest;
use crate::progress::{DecodeSummary, ProgressConfig, ProgressHandle, ProgressReporter};
use crate::record::{self, RecordParser};
use crate::util;

use anyhow::{bail, Context, Result};
use filetime::FileTime;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::Instant;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub fn decode_folder(
    input_dir: &Path,
    output_folder: &Path,
    writers_override: Option<usize>,
    progress: ProgressConfig,
) -> Result<DecodeSummary> {
    std::fs::create_dir_all(output_folder)?;

    let reporter = ProgressReporter::new("decode", 0, progress);
    let progress = reporter.handle();
    progress.set_stage("discover segments");

    let mkvs = util::list_mkvs(input_dir)?;
    if mkvs.is_empty() {
        bail!("No .mkv files found in {:?}", input_dir);
    }

    progress.set_stage("detect frame size");
    // Auto-detect frame size from first MKV
    let (w, h) = detect_frame_size(&mkvs[0])?;
    progress.log(format!("Detected frame size: {}x{}", w, h));

    progress.set_stage("read manifest");
    // Read manifest from first MKV
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
    // Create dirs
    for d in &manifest.dirs {
        let p = output_folder.join(d.replace("/", &std::path::MAIN_SEPARATOR.to_string()));
        std::fs::create_dir_all(&p)?;
    }

    // Create/open files (pre-allocate)
    let mut files = Vec::with_capacity(manifest.files.len());
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
        files.push((out_path, file));
    }

    let writers = if let Some(w) = writers_override {
        w.max(1)
    } else {
        autotune::auto_tune_for_decode(output_folder)?
    };
    progress.log(format!(
        "Decode writers hint={} (current decode path is sequential MKV processing)",
        writers
    ));

    for (idx, mkv) in mkvs.iter().enumerate() {
        let op_id = format!("mkv{:04}", idx);
        progress.set_stage(format!("decode segment {}/{}", idx + 1, mkvs.len()));
        progress.set_operation_status(
            op_id.clone(),
            format!(
                "reading {}",
                mkv.file_name().unwrap_or_default().to_string_lossy()
            ),
        );
        decode_one_video(mkv, w, h, &mut files, progress.clone(), &op_id)?;
        progress.clear_operation(&op_id, Some("done"));
    }

    progress.set_stage("restore mtimes");
    // Restore mtimes (best-effort)
    for f in &manifest.files {
        let out_path = output_folder.join(
            f.rel_path
                .replace("/", &std::path::MAIN_SEPARATOR.to_string()),
        );
        let ft = FileTime::from_unix_time(f.mtime_unix, 0);
        let _ = filetime::set_file_mtime(&out_path, ft);
    }

    let outcome = reporter.finish(format!("decoded dataset into {}", output_folder.display()));

    Ok(DecodeSummary {
        input_dir: input_dir.to_path_buf(),
        output_dir: output_folder.to_path_buf(),
        total_bytes,
        processed_bytes: outcome.processed_bytes,
        file_count: manifest.files.len(),
        segment_count: mkvs.len(),
        writers,
        elapsed: outcome.elapsed,
        avg_bytes_per_sec: outcome.avg_bytes_per_sec,
        warning_count: outcome.warning_count,
        warnings: outcome.warnings,
    })
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
            // once manifest ends, stop (manifest always first in our encoder)
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

fn decode_one_video(
    mkv: &Path,
    w: u32,
    h: u32,
    files: &mut Vec<(PathBuf, std::fs::File)>,
    progress: ProgressHandle,
    operation_id: &str,
) -> Result<()> {
    let operation_id = operation_id.to_string();
    let result: Result<()> = (|| {
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

        let mut frame_counter = 0u64;
        let mut video_bytes = 0u64;
        let started = Instant::now();
        let mut last_status = Instant::now();

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
            frame_counter += 1;

            match hdr.frame_type {
                FrameType::Manifest => {
                    // ignore; we already read manifest
                }
                FrameType::Data => {
                    let payload_len = hdr.payload_len as usize;
                    let take = payload_len.min(payload_cap);
                    let payload = &frame_buf[HEADER_LEN..HEADER_LEN + take];

                    if frame::crc32(payload) != hdr.payload_crc32 {
                        bail!(
                            "DATA payload CRC mismatch mkv={} frame_index={} stage=payload_crc",
                            mkv.display(),
                            hdr.frame_index
                        );
                    }

                    tasks.clear();
                    parser.push(payload, &mut tasks);

                    for t in tasks.drain(..) {
                        // verify chunk CRC
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
                        // write
                        write_chunk(files, t.file_id as usize, t.offset, &t.data).with_context(
                            || {
                                format!(
                                    "write failure mkv={} file_id={} offset={} stage=write_chunk",
                                    mkv.display(),
                                    t.file_id,
                                    t.offset
                                )
                            },
                        )?;
                        video_bytes = video_bytes.saturating_add(t.data.len() as u64);
                        progress.inc_bytes(t.data.len() as u64);
                    }
                }
                FrameType::End => break,
            }

            if last_status.elapsed().as_millis() > 900 {
                let elapsed = started.elapsed().as_secs_f64().max(1e-6);
                let bps = video_bytes as f64 / elapsed;
                progress.set_operation_status(
                    operation_id.clone(),
                    format!(
                        "frames={} bytes={} rate={:.1}MiB/s",
                        frame_counter,
                        video_bytes,
                        bps / (1024.0 * 1024.0)
                    ),
                );
                last_status = Instant::now();
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
                "ffmpeg decode failed mkv={} status={} frames={} stderr_tail={}",
                mkv.display(),
                status,
                frame_counter,
                tail
            );
        }

        Ok(())
    })();

    match result {
        Ok(()) => Ok(()),
        Err(err) => {
            progress.set_operation_status(operation_id, "failed".to_string());
            Err(err)
        }
    }
}

fn write_chunk(
    files: &mut Vec<(PathBuf, std::fs::File)>,
    file_id: usize,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    let (_path, f) = files.get(file_id).context("invalid file_id")?;

    #[cfg(windows)]
    {
        // positional write, thread-safe style
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
        let mut f = f;
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(data)?;
        Ok(())
    }
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
