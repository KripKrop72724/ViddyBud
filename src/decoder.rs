use crate::autotune;
use crate::frame::{self, FrameType, HEADER_LEN};
use crate::manifest::Manifest;
use crate::record::{self, RecordParser};
use crate::util;

use anyhow::{bail, Context, Result};
use filetime::FileTime;
use std::fs::{OpenOptions};
use std::io::{Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub fn decode_folder(input_dir: &Path, output_folder: &Path, writers_override: Option<usize>) -> Result<()> {
    std::fs::create_dir_all(output_folder)?;
    let mkvs = util::list_mkvs(input_dir)?;
    if mkvs.is_empty() {
        bail!("No .mkv files found in {:?}", input_dir);
    }

    // Auto-detect frame size from first MKV
    let (w, h) = detect_frame_size(&mkvs[0])?;
    eprintln!("Detected frame size: {}x{}", w, h);

    // Read manifest from first MKV
    let manifest = read_manifest(&mkvs[0], w, h)?;
    eprintln!(
        "Manifest: dataset={} segments={} mode={} chunk={}MiB",
        util::dataset_id_hex(manifest.dataset_id),
        manifest.planned_segments,
        manifest.mode,
        manifest.chunk_size_bytes / (1024 * 1024),
    );

    // Create dirs
    for d in &manifest.dirs {
        let p = output_folder.join(d.replace("/", &std::path::MAIN_SEPARATOR.to_string()));
        std::fs::create_dir_all(&p)?;
    }

    // Create/open files (pre-allocate)
    let mut files = Vec::with_capacity(manifest.files.len());
    for f in &manifest.files {
        let out_path = output_folder.join(f.rel_path.replace("/", &std::path::MAIN_SEPARATOR.to_string()));
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

    // NOTE: For simplicity + safety this version decodes MKVs sequentially.
    // You can parallelize across MKVs later (easy) by distributing into a worker pool.
    for mkv in mkvs {
        decode_one_video(&mkv, w, h, &manifest, &mut files)?;
    }

    // Restore mtimes (best-effort)
    for f in &manifest.files {
        let out_path = output_folder.join(f.rel_path.replace("/", &std::path::MAIN_SEPARATOR.to_string()));
        let ft = FileTime::from_unix_time(f.mtime_unix, 0);
        let _ = filetime::set_file_mtime(&out_path, ft);
    }

    println!("Decoded dataset into {}", output_folder.display());
    Ok(())
}

fn detect_frame_size(mkv: &Path) -> Result<(u32, u32)> {
    let candidates = [
        (4096, 4096),
        (3840, 2160),
        (1920, 1080),
        (1280, 720),
    ];

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
        .arg("-loglevel").arg("error")
        .arg("-i").arg(mkv)
        .arg("-f").arg("rawvideo")
        .arg("-pix_fmt").arg("rgb24")
        .arg("-s").arg(format!("{}x{}", w, h))
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let mut stdout = child.stdout.take().unwrap();
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
        .arg("-loglevel").arg("error")
        .arg("-i").arg(mkv)
        .arg("-f").arg("rawvideo")
        .arg("-pix_fmt").arg("rgb24")
        .arg("-s").arg(format!("{}x{}", w, h))
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn ffmpeg for manifest")?;

    let mut stdout = child.stdout.take().unwrap();

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
    manifest: &Manifest,
    files: &mut Vec<(PathBuf, std::fs::File)>,
) -> Result<()> {
    let frame_bytes = (w as usize) * (h as usize) * 3;
    let payload_cap = frame_bytes - HEADER_LEN;

    let mut child = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel").arg("error")
        .arg("-i").arg(mkv)
        .arg("-f").arg("rawvideo")
        .arg("-pix_fmt").arg("rgb24")
        .arg("-s").arg(format!("{}x{}", w, h))
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawn ffmpeg decode {:?}", mkv))?;

    let mut stdout = child.stdout.take().unwrap();
    let mut parser = RecordParser::new();
    let mut tasks: Vec<record::ChunkTask> = vec![];

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
            FrameType::Manifest => {
                // ignore; we already read manifest
            }
            FrameType::Data => {
                let payload_len = hdr.payload_len as usize;
                let take = payload_len.min(payload_cap);
                let payload = &frame_buf[HEADER_LEN..HEADER_LEN + take];

                if frame::crc32(payload) != hdr.payload_crc32 {
                    bail!("DATA payload CRC mismatch in {:?}", mkv);
                }

                tasks.clear();
                parser.push(payload, &mut tasks);

                for t in tasks.drain(..) {
                    // verify chunk CRC
                    let got = record::crc32(&t.data);
                    if got != t.expected_crc32 {
                        bail!("Chunk CRC mismatch file_id={} offset={} in {:?}", t.file_id, t.offset, mkv);
                    }
                    // write
                    write_chunk(files, t.file_id as usize, t.offset, &t.data)?;
                }
            }
            FrameType::End => break,
        }
    }

    let _ = child.kill();
    Ok(())
}

fn write_chunk(files: &mut Vec<(PathBuf, std::fs::File)>, file_id: usize, offset: u64, data: &[u8]) -> Result<()> {
    let (_path, f) = files.get(file_id).context("invalid file_id")?;

    #[cfg(windows)]
    {
        // positional write, thread-safe style
        let mut written = 0usize;
        while written < data.len() {
            let n = f.seek_write(&data[written..], offset + written as u64)?;
            if n == 0 { break; }
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
