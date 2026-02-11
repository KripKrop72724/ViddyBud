mod autotune;
mod decoder;
mod encoder;
mod frame;
mod manifest;
mod progress;
mod record;
mod util;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use indicatif::HumanBytes;
use progress::{
    DecodeSummary, EncodeSummary, ProgressConfig, ProgressMode, ProgressReporter, VerifySummary,
};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "f2v",
    version,
    about = "Byte-perfect folder<->video codec (bytes->RGB frames->lossless video)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Encode an input folder into one or more MKV video segments
    Encode {
        input_folder: PathBuf,
        output_dir: PathBuf,

        /// Mode: ffv1 (default) or raw (turbo, huge)
        #[arg(long, default_value = "ffv1")]
        mode: String,

        /// Optional override workers (segments encoded concurrently). Default: auto.
        #[arg(long)]
        workers: Option<usize>,

        /// Optional override chunk size in MiB. Default: auto.
        #[arg(long)]
        chunk_mib: Option<u64>,

        /// Optional override target segment size in GiB (payload approx). Default: auto.
        #[arg(long)]
        segment_gib: Option<u64>,

        /// Optional override frame preset: "4k" (3840x2160) or "sq4k" (4096x4096)
        #[arg(long)]
        frame: Option<String>,

        /// Use mmap for files >= this threshold in MiB.
        #[arg(long, default_value_t = 64)]
        mmap_threshold_mib: u64,

        /// Disable mmap reads in encode path.
        #[arg(long, default_value_t = false)]
        no_mmap: bool,

        /// Stitch all encoded segments into a single multi-track MKV output.
        #[arg(long, default_value_t = false)]
        stitch: bool,

        /// Progress display mode: auto (TTY-aware), rich, plain, quiet.
        #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
        progress: ProgressMode,
    },

    /// Decode MKV input back into the original folder structure (input can be a directory or stitched MKV)
    Decode {
        input_path: PathBuf,
        output_folder: PathBuf,

        /// Writer worker count override (alias: --writers)
        #[arg(long, alias = "writers")]
        writer_workers: Option<usize>,

        /// Decode engine: parallel (default) or sequential fallback.
        #[arg(long, value_enum, default_value_t = decoder::DecodeEngine::Parallel)]
        decode_engine: decoder::DecodeEngine,

        /// Decoder worker count override.
        #[arg(long)]
        decode_workers: Option<usize>,

        /// Queue budget in MiB for decode->writer pipeline.
        #[arg(long)]
        queue_mib: Option<u64>,

        /// Flush batch size in bytes.
        #[arg(long)]
        batch_bytes: Option<u64>,

        /// Flush timeout in milliseconds.
        #[arg(long)]
        batch_ms: Option<u64>,

        /// Per-decoder read-ahead frame queue depth.
        #[arg(long)]
        read_ahead_frames: Option<usize>,

        /// Disable payload/chunk CRC validation (unsafe, faster).
        #[arg(long, default_value_t = false)]
        unsafe_skip_crc: bool,

        /// Progress display mode: auto (TTY-aware), rich, plain, quiet.
        #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
        progress: ProgressMode,
    },

    /// Quick roundtrip test on a folder (encode then decode then verify sizes)
    Roundtrip {
        input_folder: PathBuf,
        temp_dir: PathBuf,

        /// Mode: ffv1 (default) or raw (turbo, huge)
        #[arg(long, default_value = "ffv1")]
        mode: String,

        /// Decode engine: parallel (default) or sequential fallback.
        #[arg(long, value_enum, default_value_t = decoder::DecodeEngine::Parallel)]
        decode_engine: decoder::DecodeEngine,

        /// Decoder worker count override.
        #[arg(long)]
        decode_workers: Option<usize>,

        /// Writer worker count override (alias: --writers)
        #[arg(long, alias = "writers")]
        writer_workers: Option<usize>,

        /// Queue budget in MiB for decode->writer pipeline.
        #[arg(long)]
        queue_mib: Option<u64>,

        /// Flush batch size in bytes.
        #[arg(long)]
        batch_bytes: Option<u64>,

        /// Flush timeout in milliseconds.
        #[arg(long)]
        batch_ms: Option<u64>,

        /// Per-decoder read-ahead frame queue depth.
        #[arg(long)]
        read_ahead_frames: Option<usize>,

        /// Disable payload/chunk CRC validation (unsafe, faster).
        #[arg(long, default_value_t = false)]
        unsafe_skip_crc: bool,

        /// Use mmap for files >= this threshold in MiB.
        #[arg(long, default_value_t = 64)]
        mmap_threshold_mib: u64,

        /// Disable mmap reads in encode path.
        #[arg(long, default_value_t = false)]
        no_mmap: bool,

        /// Stitch all encoded segments into a single multi-track MKV output.
        #[arg(long, default_value_t = false)]
        stitch: bool,

        /// Progress display mode: auto (TTY-aware), rich, plain, quiet.
        #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
        progress: ProgressMode,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    util::ensure_ffmpeg_available().context("ffmpeg not found in PATH")?;

    match cli.cmd {
        Commands::Encode {
            input_folder,
            output_dir,
            mode,
            workers,
            chunk_mib,
            segment_gib,
            frame,
            mmap_threshold_mib,
            no_mmap,
            stitch,
            progress,
        } => {
            let mode = validate_mode(mode)?;
            let summary = encoder::encode_folder(
                &input_folder,
                &output_dir,
                &mode,
                workers,
                chunk_mib,
                segment_gib,
                frame.as_deref(),
                ProgressConfig::new(progress),
                encoder::EncodeIoConfig {
                    mmap_enabled: !no_mmap,
                    mmap_threshold_bytes: mmap_threshold_mib.max(1) * 1024 * 1024,
                },
                encoder::StitchConfig { enabled: stitch },
            )?;
            print_encode_summary(&summary);
        }

        Commands::Decode {
            input_path,
            output_folder,
            writer_workers,
            decode_engine,
            decode_workers,
            queue_mib,
            batch_bytes,
            batch_ms,
            read_ahead_frames,
            unsafe_skip_crc,
            progress,
        } => {
            let summary = decoder::decode_folder(
                &input_path,
                &output_folder,
                writer_workers,
                ProgressConfig::new(progress),
                decoder::DecodePerfConfig {
                    engine: decode_engine,
                    decode_workers,
                    writer_workers,
                    queue_mib,
                    batch_bytes,
                    batch_ms,
                    read_ahead_frames,
                    unsafe_skip_crc,
                },
            )?;
            print_decode_summary(&summary);
        }

        Commands::Roundtrip {
            input_folder,
            temp_dir,
            mode,
            decode_engine,
            decode_workers,
            writer_workers,
            queue_mib,
            batch_bytes,
            batch_ms,
            read_ahead_frames,
            unsafe_skip_crc,
            mmap_threshold_mib,
            no_mmap,
            stitch,
            progress,
        } => {
            let mode = validate_mode(mode)?;
            let progress_cfg = ProgressConfig::new(progress);
            let phases = ProgressReporter::new("roundtrip", 3, progress_cfg);
            let phase_handle = phases.handle();

            let enc_dir = temp_dir.join("encoded");
            let dec_dir = temp_dir.join("decoded");

            std::fs::create_dir_all(&enc_dir)?;
            std::fs::create_dir_all(&dec_dir)?;

            phase_handle.set_stage("phase 1/3: encode");
            phase_handle.set_operation_status("encode", "running");
            let enc_summary = encoder::encode_folder(
                &input_folder,
                &enc_dir,
                &mode,
                None,
                None,
                None,
                None,
                progress_cfg,
                encoder::EncodeIoConfig {
                    mmap_enabled: !no_mmap,
                    mmap_threshold_bytes: mmap_threshold_mib.max(1) * 1024 * 1024,
                },
                encoder::StitchConfig { enabled: stitch },
            )?;
            phase_handle.clear_operation("encode", Some("encode complete"));
            phase_handle.inc_bytes(1);

            phase_handle.set_stage("phase 2/3: decode");
            phase_handle.set_operation_status("decode", "running");
            let decode_input = if stitch {
                enc_summary
                    .stitched_output
                    .clone()
                    .context("stitch enabled but encode summary did not include stitched output")?
            } else {
                enc_dir.clone()
            };
            let dec_summary = decoder::decode_folder(
                &decode_input,
                &dec_dir,
                writer_workers,
                progress_cfg,
                decoder::DecodePerfConfig {
                    engine: decode_engine,
                    decode_workers,
                    writer_workers,
                    queue_mib,
                    batch_bytes,
                    batch_ms,
                    read_ahead_frames,
                    unsafe_skip_crc,
                },
            )?;
            phase_handle.clear_operation("decode", Some("decode complete"));
            phase_handle.inc_bytes(1);

            phase_handle.set_stage("phase 3/3: verify");
            phase_handle.set_operation_status("verify", "comparing file tree");
            let verify_summary = util::basic_verify_structure(&input_folder, &dec_dir)?;
            phase_handle.clear_operation("verify", Some("verify complete"));
            phase_handle.inc_bytes(1);

            let roundtrip_outcome = phases.finish("roundtrip complete");

            println!("Roundtrip: OK");
            print_encode_summary(&enc_summary);
            print_decode_summary(&dec_summary);
            print_verify_summary(&verify_summary);
            println!(
                "Roundtrip summary: duration={} phases={}/{} warnings={} rate={}",
                fmt_duration(roundtrip_outcome.elapsed),
                roundtrip_outcome.processed_bytes,
                roundtrip_outcome.total_bytes,
                roundtrip_outcome.warning_count,
                fmt_rate(roundtrip_outcome.avg_bytes_per_sec)
            );
            for warning in roundtrip_outcome.warnings {
                println!("  warning: {}", warning);
            }
        }
    }

    Ok(())
}

fn validate_mode(mode: String) -> Result<String> {
    let mode = mode.to_lowercase();
    if mode != "ffv1" && mode != "raw" {
        bail!("mode must be 'ffv1' or 'raw'");
    }
    Ok(mode)
}

fn print_encode_summary(summary: &EncodeSummary) {
    println!(
        "Encode summary: dataset={} output={} duration={} throughput={} bytes={} / {} files={} segments={} workers={} warnings={} mmap={} threshold={}MiB mapped_files={} mapped_bytes={} mmap_fallbacks={} stitched={} stitch_tracks={} stitch_elapsed={} stitched_output={}",
        summary.dataset_hex,
        summary.output_dir.display(),
        fmt_duration(summary.elapsed),
        fmt_rate(summary.avg_bytes_per_sec),
        HumanBytes(summary.processed_bytes),
        HumanBytes(summary.total_bytes),
        summary.file_count,
        summary.segment_count,
        summary.workers,
        summary.warning_count,
        summary.mmap_enabled,
        summary.mmap_threshold_bytes / (1024 * 1024),
        summary.mapped_files,
        HumanBytes(summary.mapped_bytes),
        summary.mmap_fallbacks,
        summary.stitched,
        summary.stitch_tracks,
        fmt_duration(summary.stitch_elapsed),
        summary
            .stitched_output
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "-".to_string()),
    );
    for warning in &summary.warnings {
        println!("  warning: {}", warning);
    }
}

fn print_decode_summary(summary: &DecodeSummary) {
    println!(
        "Decode summary: input={} output={} input_kind={} source_count={} duration={} throughput={} bytes={} / {} files={} segments={} engine={} crc_enabled={} dec_workers={} wrt_workers={} queue_peak={} queue_budget={} batches={} avg_batch={} warnings={}",
        summary.input_dir.display(),
        summary.output_dir.display(),
        summary.input_kind,
        summary.source_count,
        fmt_duration(summary.elapsed),
        fmt_rate(summary.avg_bytes_per_sec),
        HumanBytes(summary.processed_bytes),
        HumanBytes(summary.total_bytes),
        summary.file_count,
        summary.segment_count,
        summary.engine,
        summary.crc_enabled,
        summary.decode_workers,
        summary.writer_workers,
        HumanBytes(summary.queue_peak_bytes),
        HumanBytes(summary.queue_budget_bytes),
        summary.batches_flushed,
        HumanBytes(summary.avg_batch_bytes),
        summary.warning_count,
    );
    for warning in &summary.warnings {
        println!("  warning: {}", warning);
    }
}

fn print_verify_summary(summary: &VerifySummary) {
    println!(
        "Verify summary: duration={} throughput={} checked_dirs={} checked_files={} checked_bytes={}",
        fmt_duration(summary.elapsed),
        fmt_rate(summary.avg_bytes_per_sec),
        summary.checked_dirs,
        summary.checked_files,
        HumanBytes(summary.checked_bytes),
    );
}

fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}", m, s)
    }
}

fn fmt_rate(bps: f64) -> String {
    if bps <= 0.1 {
        "0 B/s".to_string()
    } else {
        format!("{}/s", HumanBytes(bps as u64))
    }
}
