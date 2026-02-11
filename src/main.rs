mod autotune;
mod decoder;
mod encoder;
mod frame;
mod manifest;
mod record;
mod util;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "f2v", version, about = "Byte-perfect folder<->video codec (bytes->RGB frames->lossless video)")]
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
    },

    /// Decode a folder containing MKV segments back into the original folder structure
    Decode {
        input_dir: PathBuf,
        output_folder: PathBuf,

        /// Optional override writer threads. Default: auto.
        #[arg(long)]
        writers: Option<usize>,
    },

    /// Quick roundtrip test on a folder (encode then decode then verify sizes)
    Roundtrip {
        input_folder: PathBuf,
        temp_dir: PathBuf,
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
        } => {
            let mode = mode.to_lowercase();
            if mode != "ffv1" && mode != "raw" {
                bail!("mode must be 'ffv1' or 'raw'");
            }
            encoder::encode_folder(
                &input_folder,
                &output_dir,
                &mode,
                workers,
                chunk_mib,
                segment_gib,
                frame.as_deref(),
            )?;
        }

        Commands::Decode {
            input_dir,
            output_folder,
            writers,
        } => {
            decoder::decode_folder(&input_dir, &output_folder, writers)?;
        }

        Commands::Roundtrip { input_folder, temp_dir } => {
            let enc_dir = temp_dir.join("encoded");
            let dec_dir = temp_dir.join("decoded");

            std::fs::create_dir_all(&enc_dir)?;
            std::fs::create_dir_all(&dec_dir)?;

            encoder::encode_folder(&input_folder, &enc_dir, "ffv1", None, None, None, None)?;
            decoder::decode_folder(&enc_dir, &dec_dir, None)?;

            // simple structural check (sizes match)
            util::basic_verify_structure(&input_folder, &dec_dir)?;
            println!("Roundtrip: OK (basic size verification)");
        }
    }

    Ok(())
}
