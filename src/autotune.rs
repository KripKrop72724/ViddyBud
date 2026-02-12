use anyhow::Result;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Instant;
use sysinfo::System;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct Tune {
    pub workers: usize,
    pub chunk_size_bytes: u64,
    pub segment_payload_bytes: u64,
    pub frame_w: u32,
    pub frame_h: u32,
    pub fps: u32,
}

#[derive(Debug, Clone)]
pub struct DecodePipelineTune {
    pub decode_workers: usize,
    pub writer_workers: usize,
    pub queue_bytes: u64,
    pub read_ahead_frames: usize,
    pub batch_bytes: usize,
    pub batch_ms: u64,
}

pub fn auto_tune_for_encode(input_folder: &Path) -> Result<Tune> {
    let cores = num_cpus::get().max(1);

    let (ram_total, ram_avail) = system_ram_bytes();
    let read_mb_s = measure_sequential_read_mb_s(input_folder, 512)?;

    // Disk “profile” by measured MB/s (practical, works everywhere)
    // >1200: NVMe-ish, 400-1200: SSD-ish, 100-400: mixed/slow SSD/HDD, <100: very slow/HDD/USB
    let disk_class = if read_mb_s > 1200.0 {
        3
    } else if read_mb_s > 400.0 {
        2
    } else if read_mb_s > 100.0 {
        1
    } else {
        0
    };

    // Chunk sizes: larger chunks reduce overhead on fast disks
    let chunk_size_bytes = match disk_class {
        3 => 128 * 1024 * 1024,
        2 => 64 * 1024 * 1024,
        1 => 32 * 1024 * 1024,
        _ => 16 * 1024 * 1024,
    };

    // Segment size: keep reasonable (avoid giant single mkv)
    let segment_payload_bytes = match disk_class {
        3 => 8_u64 * 1024 * 1024 * 1024,
        2 => 6_u64 * 1024 * 1024 * 1024,
        1 => 4_u64 * 1024 * 1024 * 1024,
        _ => 2_u64 * 1024 * 1024 * 1024,
    };

    // Workers: depend on disk + cores
    let max_workers_by_disk = match disk_class {
        3 => 8,
        2 => 5,
        1 => 3,
        _ => 2,
    };

    let workers = (cores / 2).clamp(1, max_workers_by_disk).max(1);

    // Frame size: use square 4k only if RAM allows (big buffers)
    // sq4k frame_bytes = 4096*4096*3 ≈ 48MiB per frame
    // 4k frame_bytes = 3840*2160*3 ≈ 24MiB per frame
    let (frame_w, frame_h) = if ram_avail > 24_u64 * 1024 * 1024 * 1024 {
        (4096, 4096)
    } else {
        (3840, 2160)
    };

    // Higher FPS makes videos shorter for same data (purely cosmetic)
    let fps = 60;

    eprintln!(
        "Auto-tune (encode): cores={} ram_total={}GiB avail={}GiB read≈{:.0}MB/s disk_class={} workers={} chunk={}MiB segment≈{}GiB frame={}x{} fps= {}",
        cores,
        ram_total / (1024 * 1024 * 1024),
        ram_avail / (1024 * 1024 * 1024),
        read_mb_s,
        disk_class,
        workers,
        chunk_size_bytes / (1024 * 1024),
        segment_payload_bytes / (1024 * 1024 * 1024),
        frame_w,
        frame_h,
        fps
    );

    Ok(Tune {
        workers,
        chunk_size_bytes,
        segment_payload_bytes,
        frame_w,
        frame_h,
        fps,
    })
}

pub fn auto_tune_decode_pipeline(
    output_folder: &Path,
    mkv_count: usize,
) -> Result<DecodePipelineTune> {
    let cores = num_cpus::get().max(1);
    let (_total, avail) = system_ram_bytes();
    let mkv_count = mkv_count.max(1);
    let write_mb_s = measure_write_mb_s(output_folder, 256)?;

    let mut decode_workers = cores.min(mkv_count).max(1);
    if avail < 8_u64 * 1024 * 1024 * 1024 {
        decode_workers = decode_workers.min((cores / 2).max(1));
    }
    if write_mb_s < 150.0 {
        decode_workers = decode_workers.min(2);
    }

    let mut writer_workers = (cores / 2).clamp(2, cores.max(2));
    // Windows-first bias: increase writer parallelism slightly on NTFS-heavy workloads.
    if cfg!(windows) {
        writer_workers = (writer_workers + 1).min(cores.max(2));
    }
    if write_mb_s < 150.0 {
        writer_workers = writer_workers.min(3);
    }
    if avail < 4_u64 * 1024 * 1024 * 1024 {
        writer_workers = writer_workers.min(3);
    }

    let queue_floor = 256_u64 * 1024 * 1024;
    let queue_cap = 4_u64 * 1024 * 1024 * 1024;
    let mut queue_bytes = ((avail as u128 * 35) / 100) as u64;
    queue_bytes = queue_bytes.clamp(queue_floor, queue_cap);
    if write_mb_s < 150.0 {
        queue_bytes = queue_bytes.min(512_u64 * 1024 * 1024);
    }

    let read_ahead_frames = if avail > 32_u64 * 1024 * 1024 * 1024 {
        16
    } else if avail > 16_u64 * 1024 * 1024 * 1024 {
        12
    } else if avail > 8_u64 * 1024 * 1024 * 1024 {
        8
    } else {
        4
    };

    let batch_bytes = if write_mb_s < 150.0 {
        4 * 1024 * 1024
    } else {
        8 * 1024 * 1024
    };
    let batch_ms = 20;

    eprintln!(
        "Auto-tune (decode pipeline): cores={} avail={}GiB mkvs={} write≈{:.0}MB/s decode_workers={} writer_workers={} queue={}MiB read_ahead={} batch={}MiB/{}ms",
        cores,
        avail / (1024 * 1024 * 1024),
        mkv_count,
        write_mb_s,
        decode_workers,
        writer_workers,
        queue_bytes / (1024 * 1024),
        read_ahead_frames,
        batch_bytes / (1024 * 1024),
        batch_ms
    );

    Ok(DecodePipelineTune {
        decode_workers,
        writer_workers,
        queue_bytes,
        read_ahead_frames,
        batch_bytes,
        batch_ms,
    })
}

fn system_ram_bytes() -> (u64, u64) {
    let mut sys = System::new_all();
    sys.refresh_memory();
    // sysinfo reports KiB
    let total = sys.total_memory() * 1024;
    let avail = sys.available_memory() * 1024;
    (total, avail)
}

fn measure_sequential_read_mb_s(root: &Path, max_mib: usize) -> Result<f64> {
    // Read up to max_mib across largest files (sequentially)
    let mut files: Vec<(u64, std::path::PathBuf)> = vec![];
    for e in WalkDir::new(root).into_iter().filter_map(|x| x.ok()) {
        let p = e.path();
        if p.is_file() {
            let m = std::fs::metadata(p)?;
            files.push((m.len(), p.to_path_buf()));
        }
    }
    files.sort_by_key(|(sz, _)| std::cmp::Reverse(*sz));

    let target = (max_mib as u64) * 1024 * 1024;
    let mut read_total = 0_u64;
    let mut buf = vec![0u8; 8 * 1024 * 1024];

    let t0 = Instant::now();
    for (_sz, p) in files.iter().take(8) {
        let mut f = File::open(p)?;
        loop {
            if read_total >= target {
                break;
            }
            let want = (target - read_total).min(buf.len() as u64) as usize;
            let n = f.read(&mut buf[..want])?;
            if n == 0 {
                break;
            }
            read_total += n as u64;
        }
        if read_total >= target {
            break;
        }
    }
    let dt = t0.elapsed().as_secs_f64().max(1e-6);
    Ok((read_total as f64 / (1024.0 * 1024.0)) / dt)
}

fn measure_write_mb_s(dir: &Path, mib: usize) -> Result<f64> {
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(".viddybud_write_test.bin");
    let total = (mib as u64) * 1024 * 1024;
    let buf = vec![0u8; 8 * 1024 * 1024];

    let t0 = Instant::now();
    {
        let mut f = File::create(&tmp)?;
        let mut written = 0_u64;
        while written < total {
            let want = (total - written).min(buf.len() as u64) as usize;
            std::io::Write::write_all(&mut f, &buf[..want])?;
            written += want as u64;
        }
        std::io::Write::flush(&mut f)?;
    }
    let dt = t0.elapsed().as_secs_f64().max(1e-6);
    let _ = std::fs::remove_file(&tmp);
    Ok((total as f64 / (1024.0 * 1024.0)) / dt)
}
