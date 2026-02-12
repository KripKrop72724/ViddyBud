use assert_cmd::Command;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn ffmpeg_available() -> bool {
    std::process::Command::new("ffmpeg")
        .arg("-version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn combined_output(output: &std::process::Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn write_test_file(path: &Path, size: usize, seed: u8) {
    let mut data = vec![0u8; size];
    for (idx, b) in data.iter_mut().enumerate() {
        *b = seed.wrapping_add((idx % 251) as u8);
    }
    fs::write(path, data).expect("write test file");
}

#[test]
fn roundtrip_help_includes_mode_and_progress_flags() {
    let output = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("roundtrip")
        .arg("--help")
        .output()
        .expect("roundtrip --help runs");

    assert!(output.status.success());
    let text = combined_output(&output);
    assert!(text.contains("--mode"), "help text missing --mode: {text}");
    assert!(
        text.contains("--progress"),
        "help text missing --progress: {text}"
    );
    assert!(
        text.contains("--decode-engine"),
        "help text missing --decode-engine: {text}"
    );
    assert!(
        text.contains("--writer-workers"),
        "help text missing --writer-workers: {text}"
    );
    assert!(
        text.contains("--mmap-threshold-mib"),
        "help text missing --mmap-threshold-mib: {text}"
    );
    assert!(
        text.contains("--stitch"),
        "help text missing --stitch: {text}"
    );
}

#[test]
fn encode_help_includes_mmap_flags() {
    let output = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg("--help")
        .output()
        .expect("encode --help runs");
    assert!(output.status.success());
    let text = combined_output(&output);
    assert!(
        text.contains("--mmap-threshold-mib"),
        "help text missing --mmap-threshold-mib: {text}"
    );
    assert!(
        text.contains("--no-mmap"),
        "help text missing --no-mmap: {text}"
    );
    assert!(
        text.contains("--stitch"),
        "help text missing --stitch: {text}"
    );
}

#[test]
fn encode_plain_progress_includes_eta_and_summary() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let output_dir = tmp.path().join("encoded");
    fs::create_dir_all(&input).expect("create input dir");

    write_test_file(&input.join("a.bin"), 3 * 1024 * 1024, 11);
    write_test_file(&input.join("b.bin"), 2 * 1024 * 1024, 77);

    let output = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&output_dir)
        .arg("--mode")
        .arg("ffv1")
        .arg("--progress")
        .arg("plain")
        .output()
        .expect("encode runs");

    assert!(output.status.success(), "{}", combined_output(&output));

    let text = combined_output(&output);
    assert!(
        text.contains("[PROGRESS] encode"),
        "missing plain progress: {text}"
    );
    assert!(text.contains("ETA="), "missing ETA token: {text}");
    assert!(text.contains("Encode summary:"), "missing summary: {text}");
}

#[test]
fn decode_plain_progress_includes_eta_and_summary() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let encoded = tmp.path().join("encoded");
    let decoded = tmp.path().join("decoded");
    fs::create_dir_all(&input).expect("create input dir");

    write_test_file(&input.join("c.bin"), 2 * 1024 * 1024, 31);

    let enc_out = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&encoded)
        .arg("--mode")
        .arg("ffv1")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("encode runs");
    assert!(enc_out.status.success(), "{}", combined_output(&enc_out));

    let dec_out = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&encoded)
        .arg(&decoded)
        .arg("--decode-engine")
        .arg("parallel")
        .arg("--queue-mib")
        .arg("64")
        .arg("--batch-bytes")
        .arg("1048576")
        .arg("--batch-ms")
        .arg("10")
        .arg("--read-ahead-frames")
        .arg("2")
        .arg("--progress")
        .arg("plain")
        .output()
        .expect("decode runs");

    assert!(dec_out.status.success(), "{}", combined_output(&dec_out));

    let text = combined_output(&dec_out);
    assert!(
        text.contains("[PROGRESS] decode"),
        "missing plain progress: {text}"
    );
    assert!(text.contains("ETA="), "missing ETA token: {text}");
    assert!(text.contains("Decode summary:"), "missing summary: {text}");
    assert!(
        text.contains("Decision snapshot:"),
        "missing decision snapshot: {text}"
    );
    assert!(
        text.contains("bottleneck="),
        "missing bottleneck telemetry: {text}"
    );
}

#[test]
fn decode_failure_reports_contextual_path_details() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let bad_input = tmp.path().join("bad");
    let out = tmp.path().join("out");
    fs::create_dir_all(&bad_input).expect("create bad dir");
    fs::write(bad_input.join("broken.mkv"), b"not-a-real-mkv").expect("write bad mkv");

    let output = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&bad_input)
        .arg(&out)
        .arg("--progress")
        .arg("plain")
        .output()
        .expect("decode executes");

    assert!(!output.status.success(), "decode unexpectedly succeeded");

    let text = combined_output(&output);
    assert!(
        text.contains("Could not detect frame size"),
        "missing decode failure context: {text}"
    );
    assert!(
        text.contains("broken.mkv"),
        "missing file path context: {text}"
    );
}

#[test]
fn encode_no_mmap_reports_disabled() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let output_dir = tmp.path().join("encoded");
    fs::create_dir_all(&input).expect("create input dir");
    write_test_file(&input.join("a.bin"), 2 * 1024 * 1024, 99);

    let output = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&output_dir)
        .arg("--mode")
        .arg("ffv1")
        .arg("--no-mmap")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("encode runs");
    assert!(output.status.success(), "{}", combined_output(&output));

    let text = combined_output(&output);
    assert!(
        text.contains("mmap=false"),
        "summary missing mmap=false when --no-mmap is set: {text}"
    );
}

#[test]
fn encode_stitch_produces_single_mkv_and_reports_summary() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let output_dir = tmp.path().join("encoded");
    fs::create_dir_all(&input).expect("create input dir");
    write_test_file(&input.join("a.bin"), 3 * 1024 * 1024, 19);
    write_test_file(&input.join("b.bin"), 2 * 1024 * 1024, 73);

    let out = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&output_dir)
        .arg("--mode")
        .arg("ffv1")
        .arg("--stitch")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("encode runs");
    assert!(out.status.success(), "{}", combined_output(&out));

    let mut mkvs = vec![];
    for entry in fs::read_dir(&output_dir).expect("read output dir") {
        let path = entry.expect("read entry").path();
        if path
            .extension()
            .map(|e| e.to_string_lossy().to_ascii_lowercase())
            .as_deref()
            == Some("mkv")
        {
            mkvs.push(path);
        }
    }
    assert_eq!(mkvs.len(), 1, "expected one stitched mkv in output dir");
    assert!(
        mkvs[0]
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .contains("_stitched"),
        "stitched filename should include _stitched: {}",
        mkvs[0].display()
    );

    let text = combined_output(&out);
    assert!(
        text.contains("stitched=true"),
        "encode summary missing stitched=true: {text}"
    );
    assert!(
        text.contains("stitch_tracks="),
        "encode summary missing stitch_tracks: {text}"
    );
}

#[test]
fn decode_accepts_stitched_file_and_reports_input_kind() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let output_dir = tmp.path().join("encoded");
    let decoded = tmp.path().join("decoded");
    fs::create_dir_all(&input).expect("create input dir");
    write_test_file(&input.join("a.bin"), 2 * 1024 * 1024, 41);
    write_test_file(&input.join("b.bin"), 2 * 1024 * 1024, 87);

    let enc_out = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&output_dir)
        .arg("--mode")
        .arg("ffv1")
        .arg("--stitch")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("encode runs");
    assert!(enc_out.status.success(), "{}", combined_output(&enc_out));

    let stitched = fs::read_dir(&output_dir)
        .expect("read output dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| {
            p.extension()
                .map(|e| e.to_string_lossy().to_ascii_lowercase())
                .as_deref()
                == Some("mkv")
        })
        .expect("stitched mkv exists");

    let dec_out = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&stitched)
        .arg(&decoded)
        .arg("--decode-engine")
        .arg("parallel")
        .arg("--progress")
        .arg("plain")
        .output()
        .expect("decode runs");
    assert!(dec_out.status.success(), "{}", combined_output(&dec_out));
    let text = combined_output(&dec_out);
    assert!(
        text.contains("Decision snapshot:"),
        "missing decision snapshot: {text}"
    );
    assert!(
        text.contains("stitch=true strategy=multitrack"),
        "missing stitched decision marker: {text}"
    );
    assert!(
        text.contains("input_kind=stitched_file"),
        "decode summary missing input_kind=stitched_file: {text}"
    );
    assert!(
        text.contains("source_count="),
        "decode summary missing source_count: {text}"
    );
}

#[test]
fn roundtrip_stitch_ffv1_succeeds() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let work = tmp.path().join("work");
    fs::create_dir_all(&input).expect("create input dir");
    write_test_file(&input.join("a.bin"), 1024 * 1024, 12);

    let out = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("roundtrip")
        .arg(&input)
        .arg(&work)
        .arg("--mode")
        .arg("ffv1")
        .arg("--stitch")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("roundtrip ffv1 runs");
    assert!(out.status.success(), "{}", combined_output(&out));
    let text = combined_output(&out);
    assert!(
        text.contains("Roundtrip: OK"),
        "roundtrip missing success: {text}"
    );
    assert!(
        text.contains("stitched=true"),
        "roundtrip encode summary missing stitched=true: {text}"
    );
}

#[test]
fn roundtrip_stitch_raw_succeeds() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let work = tmp.path().join("work");
    fs::create_dir_all(&input).expect("create input dir");
    write_test_file(&input.join("tiny.bin"), 256 * 1024, 55);

    let out = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("roundtrip")
        .arg(&input)
        .arg(&work)
        .arg("--mode")
        .arg("raw")
        .arg("--stitch")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("roundtrip raw runs");
    assert!(out.status.success(), "{}", combined_output(&out));
    let text = combined_output(&out);
    assert!(
        text.contains("Roundtrip: OK"),
        "roundtrip missing success: {text}"
    );
    assert!(
        text.contains("stitched=true"),
        "roundtrip encode summary missing stitched=true: {text}"
    );
}
