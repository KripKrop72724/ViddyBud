use assert_cmd::Command;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use walkdir::WalkDir;

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

fn write_pattern(path: &Path, size: usize, seed: u8) {
    let mut data = vec![0u8; size];
    for (idx, b) in data.iter_mut().enumerate() {
        *b = seed.wrapping_add((idx % 251) as u8);
    }
    fs::write(path, data).expect("write test payload");
}

fn write_test_tree(root: &Path) {
    fs::create_dir_all(root.join("nested/deeper")).expect("mkdir tree");
    write_pattern(&root.join("a.bin"), 900 * 1024, 7);
    write_pattern(&root.join("nested/b.bin"), 700 * 1024, 53);
    write_pattern(&root.join("nested/deeper/c.bin"), 1100 * 1024, 129);
}

fn read_tree_bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        let path = entry.path();
        if path.is_file() {
            let rel = path.strip_prefix(root).expect("strip prefix").to_path_buf();
            out.insert(rel, fs::read(path).expect("read file bytes"));
        }
    }
    out
}

fn find_single_mkv(dir: &Path) -> PathBuf {
    let mut mkvs = fs::read_dir(dir)
        .expect("read dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .map(|e| e.to_string_lossy().to_ascii_lowercase())
                .as_deref()
                == Some("mkv")
        })
        .collect::<Vec<_>>();
    mkvs.sort();
    assert_eq!(
        mkvs.len(),
        1,
        "expected exactly one mkv in {}",
        dir.display()
    );
    mkvs.remove(0)
}

fn assert_tree_equal(
    expected: &BTreeMap<PathBuf, Vec<u8>>,
    actual: &BTreeMap<PathBuf, Vec<u8>>,
    label: &str,
) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "{label}: file-count mismatch expected={} actual={}",
        expected.len(),
        actual.len()
    );

    for (path, exp) in expected {
        let got = actual
            .get(path)
            .unwrap_or_else(|| panic!("{label}: missing file {}", path.display()));
        if exp != got {
            let first_diff = exp
                .iter()
                .zip(got.iter())
                .position(|(a, b)| a != b)
                .unwrap_or(exp.len().min(got.len()));
            panic!(
                "{label}: byte mismatch file={} expected_len={} actual_len={} first_diff_offset={}",
                path.display(),
                exp.len(),
                got.len(),
                first_diff
            );
        }
    }
}

#[test]
fn parallel_and_sequential_engines_are_byte_identical() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let encoded = tmp.path().join("encoded");
    let decoded_parallel = tmp.path().join("decoded_parallel");
    let decoded_sequential = tmp.path().join("decoded_sequential");
    write_test_tree(&input);

    let enc = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&encoded)
        .arg("--mode")
        .arg("ffv1")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("encode run");
    assert!(enc.status.success(), "{}", combined_output(&enc));

    let dec_parallel = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&encoded)
        .arg(&decoded_parallel)
        .arg("--decode-engine")
        .arg("parallel")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("decode parallel run");
    assert!(
        dec_parallel.status.success(),
        "{}",
        combined_output(&dec_parallel)
    );

    let dec_sequential = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&encoded)
        .arg(&decoded_sequential)
        .arg("--decode-engine")
        .arg("sequential")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("decode sequential run");
    assert!(
        dec_sequential.status.success(),
        "{}",
        combined_output(&dec_sequential)
    );

    let expected = read_tree_bytes(&input);
    let par_bytes = read_tree_bytes(&decoded_parallel);
    let seq_bytes = read_tree_bytes(&decoded_sequential);
    assert_tree_equal(&expected, &par_bytes, "parallel decode changed bytes");
    assert_tree_equal(&expected, &seq_bytes, "sequential decode changed bytes");
}

#[test]
fn decode_engine_and_crc_mode_are_reflected_in_summary() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let encoded = tmp.path().join("encoded");
    let decoded_seq = tmp.path().join("decoded_seq");
    let decoded_no_crc = tmp.path().join("decoded_no_crc");
    write_test_tree(&input);

    let enc = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&encoded)
        .arg("--mode")
        .arg("ffv1")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("encode run");
    assert!(enc.status.success(), "{}", combined_output(&enc));

    let seq = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&encoded)
        .arg(&decoded_seq)
        .arg("--decode-engine")
        .arg("sequential")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("decode sequential run");
    assert!(seq.status.success(), "{}", combined_output(&seq));
    let seq_text = combined_output(&seq);
    assert!(
        seq_text.contains("engine=sequential"),
        "summary missing sequential engine marker: {seq_text}"
    );
    assert!(
        seq_text.contains("crc_enabled=true"),
        "summary missing crc_enabled=true marker: {seq_text}"
    );

    let no_crc = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&encoded)
        .arg(&decoded_no_crc)
        .arg("--decode-engine")
        .arg("parallel")
        .arg("--unsafe-skip-crc")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("decode no-crc run");
    assert!(no_crc.status.success(), "{}", combined_output(&no_crc));
    let no_crc_text = combined_output(&no_crc);
    assert!(
        no_crc_text.contains("engine=parallel"),
        "summary missing parallel engine marker: {no_crc_text}"
    );
    assert!(
        no_crc_text.contains("crc_enabled=false"),
        "summary missing crc_enabled=false marker: {no_crc_text}"
    );
}

#[test]
fn parallel_decode_with_small_queue_completes() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let encoded = tmp.path().join("encoded");
    let decoded = tmp.path().join("decoded");
    fs::create_dir_all(&input).expect("mkdir input");
    for idx in 0..8u8 {
        write_pattern(
            &input.join(format!("f{idx:02}.bin")),
            650 * 1024,
            idx.wrapping_mul(17),
        );
    }

    let enc = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&encoded)
        .arg("--mode")
        .arg("ffv1")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("encode run");
    assert!(enc.status.success(), "{}", combined_output(&enc));

    let dec = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&encoded)
        .arg(&decoded)
        .arg("--decode-engine")
        .arg("parallel")
        .arg("--decode-workers")
        .arg("2")
        .arg("--writer-workers")
        .arg("2")
        .arg("--queue-mib")
        .arg("1")
        .arg("--batch-bytes")
        .arg("262144")
        .arg("--batch-ms")
        .arg("5")
        .arg("--read-ahead-frames")
        .arg("1")
        .arg("--progress")
        .arg("plain")
        .output()
        .expect("decode run");
    assert!(dec.status.success(), "{}", combined_output(&dec));
    let text = combined_output(&dec);
    assert!(text.contains("Decode summary:"), "missing summary: {text}");
    assert!(
        text.contains("queue_peak="),
        "missing queue metrics in summary: {text}"
    );
}

#[test]
fn stitched_file_decode_preserves_bytes_and_reports_source_metrics() {
    if !ffmpeg_available() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let input = tmp.path().join("input");
    let encoded = tmp.path().join("encoded");
    let decoded = tmp.path().join("decoded");
    write_test_tree(&input);

    let enc = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("encode")
        .arg(&input)
        .arg(&encoded)
        .arg("--mode")
        .arg("ffv1")
        .arg("--stitch")
        .arg("--progress")
        .arg("quiet")
        .output()
        .expect("encode run");
    assert!(enc.status.success(), "{}", combined_output(&enc));

    let stitched = find_single_mkv(&encoded);
    let dec = Command::new(assert_cmd::cargo::cargo_bin!("viddybud"))
        .arg("decode")
        .arg(&stitched)
        .arg(&decoded)
        .arg("--decode-engine")
        .arg("parallel")
        .arg("--progress")
        .arg("plain")
        .output()
        .expect("decode stitched run");
    assert!(dec.status.success(), "{}", combined_output(&dec));
    let text = combined_output(&dec);
    assert!(
        text.contains("input_kind=stitched_file"),
        "summary missing stitched input kind: {text}"
    );
    assert!(
        text.contains("source_count="),
        "summary missing source count: {text}"
    );

    let expected = read_tree_bytes(&input);
    let actual = read_tree_bytes(&decoded);
    assert_tree_equal(&expected, &actual, "stitched decode changed bytes");
}
