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

        /// Progress display mode: auto (TTY-aware), rich, plain, quiet.
        #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
        progress: ProgressMode,
    },

    /// Decode a folder containing MKV segments back into the original folder structure
    Decode {
        input_dir: PathBuf,
        output_folder: PathBuf,

        /// Optional override writer threads. Default: auto.
        #[arg(long)]
        writers: Option<usize>,

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

        /// Progress display mode: auto (TTY-aware), rich, plain, quiet.
        #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
        progress: ProgressMode,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Basic ffmpeg availability check (encode/decode both need it)
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
            )?;
            print_encode_summary(&summary);
        }

        Commands::Decode {
            input_dir,
            output_folder,
            writers,
            progress,
        } => {
            let summary = decoder::decode_folder(
                &input_dir,
                &output_folder,
                writers,
                ProgressConfig::new(progress),
            )?;
            print_decode_summary(&summary);
        }

        Commands::Roundtrip {
            input_folder,
            temp_dir,
            mode,
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
            )?;
            phase_handle.clear_operation("encode", Some("encode complete"));
            phase_handle.inc_bytes(1);

            phase_handle.set_stage("phase 2/3: decode");
            phase_handle.set_operation_status("decode", "running");
            let dec_summary = decoder::decode_folder(&enc_dir, &dec_dir, None, progress_cfg)?;
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
        "Encode summary: dataset={} output={} duration={} throughput={} bytes={} / {} files={} segments={} workers={} warnings={}",
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
    );
    for warning in &summary.warnings {
        println!("  warning: {}", warning);
    }
}

fn print_decode_summary(summary: &DecodeSummary) {
    println!(
        "Decode summary: input={} output={} duration={} throughput={} bytes={} / {} files={} segments={} writers={} warnings={}",
        summary.input_dir.display(),
        summary.output_dir.display(),
        fmt_duration(summary.elapsed),
        fmt_rate(summary.avg_bytes_per_sec),
        HumanBytes(summary.processed_bytes),
        HumanBytes(summary.total_bytes),
        summary.file_count,
        summary.segment_count,
        summary.writers,
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
