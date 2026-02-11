use clap::ValueEnum;
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MAX_STORED_WARNINGS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum ProgressMode {
    Auto,
    Rich,
    Plain,
    Quiet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedProgressMode {
    Rich,
    Plain,
    Quiet,
}

#[derive(Debug, Clone, Copy)]
pub struct ProgressConfig {
    pub mode: ProgressMode,
    pub idle_threshold: Duration,
    pub plain_interval: Duration,
    tty_override: Option<bool>,
}

impl Default for ProgressConfig {
    fn default() -> Self {
        Self {
            mode: ProgressMode::Auto,
            idle_threshold: Duration::from_secs(30),
            plain_interval: Duration::from_secs(2),
            tty_override: None,
        }
    }
}

impl ProgressConfig {
    pub fn new(mode: ProgressMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub fn with_tty_override(mut self, is_tty: bool) -> Self {
        self.tty_override = Some(is_tty);
        self
    }

    pub fn resolve_mode(self) -> ResolvedProgressMode {
        self.mode.resolve(
            self.tty_override
                .unwrap_or_else(|| std::io::stderr().is_terminal()),
        )
    }
}

impl ProgressMode {
    fn resolve(self, stderr_is_tty: bool) -> ResolvedProgressMode {
        match self {
            ProgressMode::Auto => {
                if stderr_is_tty {
                    ResolvedProgressMode::Rich
                } else {
                    ResolvedProgressMode::Plain
                }
            }
            ProgressMode::Rich => ResolvedProgressMode::Rich,
            ProgressMode::Plain => ResolvedProgressMode::Plain,
            ProgressMode::Quiet => ResolvedProgressMode::Quiet,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncodeSummary {
    pub dataset_hex: String,
    pub output_dir: PathBuf,
    pub total_bytes: u64,
    pub processed_bytes: u64,
    pub file_count: usize,
    pub segment_count: usize,
    pub workers: usize,
    pub elapsed: Duration,
    pub avg_bytes_per_sec: f64,
    pub warning_count: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DecodeSummary {
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub total_bytes: u64,
    pub processed_bytes: u64,
    pub file_count: usize,
    pub segment_count: usize,
    pub writers: usize,
    pub elapsed: Duration,
    pub avg_bytes_per_sec: f64,
    pub warning_count: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct VerifySummary {
    pub checked_dirs: usize,
    pub checked_files: usize,
    pub checked_bytes: u64,
    pub elapsed: Duration,
    pub avg_bytes_per_sec: f64,
}

#[derive(Debug, Clone)]
pub struct ProgressOutcome {
    pub elapsed: Duration,
    pub total_bytes: u64,
    pub processed_bytes: u64,
    pub avg_bytes_per_sec: f64,
    pub warning_count: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    pub label: String,
    pub stage: String,
    pub processed_bytes: u64,
    pub total_bytes: u64,
    pub elapsed: Duration,
    pub throughput_bps: f64,
    pub eta: Option<Duration>,
    pub active_ops: Vec<String>,
}

#[derive(Clone)]
pub struct ProgressHandle {
    inner: Arc<ProgressInner>,
}

pub struct ProgressReporter {
    handle: ProgressHandle,
    ticker: Option<JoinHandle<()>>,
}

struct ProgressInner {
    label: String,
    mode: ResolvedProgressMode,
    idle_threshold: Duration,
    plain_interval: Duration,
    state: Mutex<ProgressState>,
    rich: Option<RichUi>,
    stop: AtomicBool,
    finalized: AtomicBool,
}

struct RichUi {
    _multi: MultiProgress,
    overall: ProgressBar,
    stage: ProgressBar,
    ops: Mutex<HashMap<String, ProgressBar>>,
}

#[derive(Debug)]
struct ProgressState {
    started: Instant,
    stage: String,
    total_bytes: u64,
    processed_bytes: u64,
    last_progress: Instant,
    last_plain_emit: Instant,
    last_idle_warn: Option<Instant>,
    rate_sample_at: Instant,
    rate_sample_bytes: u64,
    smoothed_bps: f64,
    operations: HashMap<String, String>,
    warnings: Vec<String>,
}

impl ProgressReporter {
    pub fn new(label: impl Into<String>, total_bytes: u64, config: ProgressConfig) -> Self {
        let label = label.into();
        let mode = config.resolve_mode();
        let now = Instant::now();

        let rich = if mode == ResolvedProgressMode::Rich {
            Some(RichUi::new(&label, total_bytes))
        } else {
            None
        };

        let inner = Arc::new(ProgressInner {
            label: label.clone(),
            mode,
            idle_threshold: config.idle_threshold,
            plain_interval: config.plain_interval,
            state: Mutex::new(ProgressState {
                started: now,
                stage: "initializing".to_string(),
                total_bytes,
                processed_bytes: 0,
                last_progress: now,
                last_plain_emit: now.checked_sub(config.plain_interval).unwrap_or(now),
                last_idle_warn: None,
                rate_sample_at: now,
                rate_sample_bytes: 0,
                smoothed_bps: 0.0,
                operations: HashMap::new(),
                warnings: Vec::new(),
            }),
            rich,
            stop: AtomicBool::new(false),
            finalized: AtomicBool::new(false),
        });

        let ticker_inner = Arc::clone(&inner);
        let ticker = thread::spawn(move || {
            while !ticker_inner.stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
                ticker_inner.tick_once();
            }
            ticker_inner.tick_once();
        });

        let handle = ProgressHandle { inner };
        handle.set_stage("starting");
        Self {
            handle,
            ticker: Some(ticker),
        }
    }

    pub fn handle(&self) -> ProgressHandle {
        self.handle.clone()
    }

    pub fn finish(mut self, final_message: impl Into<String>) -> ProgressOutcome {
        self.shutdown_ticker();
        self.handle.inner.finalize(Some(final_message.into()))
    }

    fn shutdown_ticker(&mut self) {
        self.handle.inner.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.ticker.take() {
            let _ = join.join();
        }
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        self.shutdown_ticker();
        let _ = self.handle.inner.finalize(None);
    }
}

impl ProgressHandle {
    pub fn set_total_bytes(&self, total_bytes: u64) {
        let snapshot = {
            let mut state = self.inner.state.lock().unwrap();
            state.total_bytes = total_bytes;
            refresh_rate_locked(&mut state, Instant::now());
            snapshot_locked(&self.inner.label, &state)
        };
        self.inner.render_snapshot(&snapshot, true);
    }

    pub fn set_stage(&self, stage: impl Into<String>) {
        let stage = stage.into();
        let snapshot = {
            let mut state = self.inner.state.lock().unwrap();
            state.stage = stage;
            refresh_rate_locked(&mut state, Instant::now());
            snapshot_locked(&self.inner.label, &state)
        };
        self.inner.render_snapshot(&snapshot, true);
    }

    pub fn inc_bytes(&self, delta: u64) {
        if delta == 0 {
            return;
        }
        let snapshot = {
            let mut state = self.inner.state.lock().unwrap();
            state.processed_bytes = state.processed_bytes.saturating_add(delta);
            if state.total_bytes > 0 {
                state.processed_bytes = state.processed_bytes.min(state.total_bytes);
            }
            let now = Instant::now();
            state.last_progress = now;
            refresh_rate_locked(&mut state, now);
            snapshot_locked(&self.inner.label, &state)
        };
        self.inner.render_snapshot(&snapshot, false);
    }

    pub fn set_operation_status(&self, operation_id: impl Into<String>, status: impl Into<String>) {
        let op_id = operation_id.into();
        let status = status.into();

        let snapshot = {
            let mut state = self.inner.state.lock().unwrap();
            state.operations.insert(op_id.clone(), status.clone());
            refresh_rate_locked(&mut state, Instant::now());
            snapshot_locked(&self.inner.label, &state)
        };

        if let Some(rich) = &self.inner.rich {
            rich.set_operation(&op_id, &status);
        }
        self.inner.render_snapshot(&snapshot, false);
    }

    pub fn clear_operation(&self, operation_id: &str, final_status: Option<&str>) {
        let snapshot = {
            let mut state = self.inner.state.lock().unwrap();
            state.operations.remove(operation_id);
            refresh_rate_locked(&mut state, Instant::now());
            snapshot_locked(&self.inner.label, &state)
        };

        if let Some(rich) = &self.inner.rich {
            rich.clear_operation(operation_id, final_status);
        }
        self.inner.render_snapshot(&snapshot, false);
    }

    pub fn log(&self, message: impl Into<String>) {
        self.inner.emit_message("INFO", &message.into());
    }
}

impl ProgressInner {
    fn tick_once(&self) {
        if self.mode == ResolvedProgressMode::Quiet {
            return;
        }

        let (snapshot, plain_due, idle_warning) = {
            let mut state = self.state.lock().unwrap();
            let now = Instant::now();
            refresh_rate_locked(&mut state, now);

            let plain_due = now.duration_since(state.last_plain_emit) >= self.plain_interval;
            if plain_due {
                state.last_plain_emit = now;
            }

            let idle_due = idle_warning_due(
                now,
                state.last_progress,
                state.last_idle_warn,
                self.idle_threshold,
                state.processed_bytes,
                state.total_bytes,
            );
            let idle_warning = if idle_due {
                state.last_idle_warn = Some(now);
                let active = active_op_display(&state.operations);
                let msg = format!(
                    "IDLE WARNING: no byte progress for {}s (stage={}, active={})",
                    now.duration_since(state.last_progress).as_secs(),
                    state.stage,
                    active
                );
                push_warning_locked(&mut state, &msg);
                Some(msg)
            } else {
                None
            };

            let snapshot = snapshot_locked(&self.label, &state);
            (snapshot, plain_due, idle_warning)
        };

        if self.mode == ResolvedProgressMode::Rich {
            self.render_snapshot_rich(&snapshot);
        } else if plain_due {
            self.render_snapshot_plain(&snapshot);
        }

        if let Some(msg) = idle_warning {
            self.emit_message("WARN", &msg);
        }
    }

    fn render_snapshot(&self, snapshot: &ProgressSnapshot, force_plain: bool) {
        match self.mode {
            ResolvedProgressMode::Rich => self.render_snapshot_rich(snapshot),
            ResolvedProgressMode::Plain => {
                if force_plain {
                    self.render_snapshot_plain(snapshot);
                }
            }
            ResolvedProgressMode::Quiet => {}
        }
    }

    fn render_snapshot_rich(&self, snapshot: &ProgressSnapshot) {
        let Some(rich) = &self.rich else {
            return;
        };

        rich.overall.set_length(snapshot.total_bytes.max(1));
        rich.overall
            .set_position(snapshot.processed_bytes.min(snapshot.total_bytes.max(1)));
        rich.overall.set_message(format!(
            "stage={} rate={} ETA={} active={}",
            snapshot.stage,
            format_rate(snapshot.throughput_bps),
            format_eta(snapshot.eta),
            if snapshot.active_ops.is_empty() {
                "-".to_string()
            } else {
                snapshot.active_ops.join(",")
            }
        ));

        rich.stage.set_message(format!(
            "{} | elapsed {}",
            snapshot.stage,
            format_duration(snapshot.elapsed)
        ));
        rich.stage.tick();
    }

    fn render_snapshot_plain(&self, snapshot: &ProgressSnapshot) {
        let pct = if snapshot.total_bytes == 0 {
            0.0
        } else {
            (snapshot.processed_bytes as f64 / snapshot.total_bytes as f64) * 100.0
        };
        eprintln!(
            "[PROGRESS] {} elapsed={} stage={} done={} / {} ({:.1}%) rate={} ETA={} active={}",
            snapshot.label,
            format_duration(snapshot.elapsed),
            snapshot.stage,
            HumanBytes(snapshot.processed_bytes),
            HumanBytes(snapshot.total_bytes),
            pct,
            format_rate(snapshot.throughput_bps),
            format_eta(snapshot.eta),
            if snapshot.active_ops.is_empty() {
                "-".to_string()
            } else {
                snapshot.active_ops.join(",")
            }
        );
    }

    fn emit_message(&self, level: &str, message: &str) {
        match self.mode {
            ResolvedProgressMode::Quiet => {}
            ResolvedProgressMode::Plain => {
                eprintln!("[{}] {}: {}", level, self.label, message);
            }
            ResolvedProgressMode::Rich => {
                if let Some(rich) = &self.rich {
                    rich.stage
                        .println(format!("[{}] {}: {}", level, self.label, message));
                } else {
                    eprintln!("[{}] {}: {}", level, self.label, message);
                }
            }
        }
    }

    fn finalize(&self, final_message: Option<String>) -> ProgressOutcome {
        if self.finalized.swap(true, Ordering::Relaxed) {
            return self.current_outcome();
        }

        let (snapshot, warnings) = {
            let mut state = self.state.lock().unwrap();
            refresh_rate_locked(&mut state, Instant::now());
            let snapshot = snapshot_locked(&self.label, &state);
            let warnings = state.warnings.clone();
            (snapshot, warnings)
        };

        match self.mode {
            ResolvedProgressMode::Quiet => {}
            ResolvedProgressMode::Plain => {
                self.render_snapshot_plain(&snapshot);
                if let Some(msg) = final_message.as_deref() {
                    eprintln!("[DONE] {}: {}", self.label, msg);
                }
            }
            ResolvedProgressMode::Rich => {
                if let Some(rich) = &self.rich {
                    rich.overall.finish_with_message(format!(
                        "done={} / {} elapsed={} rate={}",
                        HumanBytes(snapshot.processed_bytes),
                        HumanBytes(snapshot.total_bytes),
                        format_duration(snapshot.elapsed),
                        format_rate(snapshot.throughput_bps)
                    ));
                    if let Some(msg) = final_message {
                        rich.stage.finish_with_message(msg);
                    } else {
                        rich.stage.finish_and_clear();
                    }
                    rich.clear_all_ops();
                }
            }
        }

        ProgressOutcome {
            elapsed: snapshot.elapsed,
            total_bytes: snapshot.total_bytes,
            processed_bytes: snapshot.processed_bytes,
            avg_bytes_per_sec: average_rate(snapshot.processed_bytes, snapshot.elapsed),
            warning_count: warnings.len(),
            warnings,
        }
    }

    fn current_outcome(&self) -> ProgressOutcome {
        let (snapshot, warnings) = {
            let state = self.state.lock().unwrap();
            (snapshot_locked(&self.label, &state), state.warnings.clone())
        };
        ProgressOutcome {
            elapsed: snapshot.elapsed,
            total_bytes: snapshot.total_bytes,
            processed_bytes: snapshot.processed_bytes,
            avg_bytes_per_sec: average_rate(snapshot.processed_bytes, snapshot.elapsed),
            warning_count: warnings.len(),
            warnings,
        }
    }
}

impl RichUi {
    fn new(label: &str, total_bytes: u64) -> Self {
        let multi = MultiProgress::new();
        let overall = multi.add(ProgressBar::new(total_bytes.max(1)));
        let stage = multi.add(ProgressBar::new_spinner());

        overall
            .set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] {wide_bar:.cyan/blue} {bytes}/{total_bytes} {bytes_per_sec} ETA {eta_precise} | {msg}",
                )
                .expect("valid progress template"),
            );
        overall.set_message(format!("{} starting", label));

        stage.set_style(
            ProgressStyle::with_template("{spinner:.yellow} {msg}")
                .expect("valid stage template")
                .tick_chars("|/-\\ "),
        );
        stage.enable_steady_tick(Duration::from_millis(120));
        stage.set_message("starting");

        Self {
            _multi: multi,
            overall,
            stage,
            ops: Mutex::new(HashMap::new()),
        }
    }

    fn set_operation(&self, op_id: &str, status: &str) {
        let mut ops = self.ops.lock().unwrap();
        let bar = ops.entry(op_id.to_string()).or_insert_with(|| {
            let pb = self._multi.add(ProgressBar::new_spinner());
            pb.set_style(
                ProgressStyle::with_template("{spinner:.magenta} {prefix:.bold} {msg}")
                    .expect("valid operation template")
                    .tick_chars("|/-\\ "),
            );
            pb.enable_steady_tick(Duration::from_millis(120));
            pb.set_prefix(op_id.to_string());
            pb
        });
        bar.set_message(status.to_string());
    }

    fn clear_operation(&self, op_id: &str, final_status: Option<&str>) {
        let mut ops = self.ops.lock().unwrap();
        if let Some(bar) = ops.remove(op_id) {
            if let Some(status) = final_status {
                bar.finish_with_message(status.to_string());
            } else {
                bar.finish_and_clear();
            }
        }
    }

    fn clear_all_ops(&self) {
        let mut ops = self.ops.lock().unwrap();
        for (_id, bar) in ops.drain() {
            bar.finish_and_clear();
        }
    }
}

fn push_warning_locked(state: &mut ProgressState, message: &str) {
    if state.warnings.len() >= MAX_STORED_WARNINGS {
        state.warnings.remove(0);
    }
    state.warnings.push(message.to_string());
}

fn snapshot_locked(label: &str, state: &ProgressState) -> ProgressSnapshot {
    let throughput_bps = if state.smoothed_bps > 0.1 {
        state.smoothed_bps
    } else {
        average_rate(state.processed_bytes, state.started.elapsed())
    };
    let eta = compute_eta(state.total_bytes, state.processed_bytes, throughput_bps);

    let mut active_ops = state.operations.keys().cloned().collect::<Vec<_>>();
    active_ops.sort();
    active_ops.truncate(4);

    ProgressSnapshot {
        label: label.to_string(),
        stage: state.stage.clone(),
        processed_bytes: state.processed_bytes,
        total_bytes: state.total_bytes,
        elapsed: state.started.elapsed(),
        throughput_bps,
        eta,
        active_ops,
    }
}

fn refresh_rate_locked(state: &mut ProgressState, now: Instant) {
    let dt = now
        .duration_since(state.rate_sample_at)
        .as_secs_f64()
        .max(1e-6);
    if dt < 0.5 {
        return;
    }
    let delta_bytes = state
        .processed_bytes
        .saturating_sub(state.rate_sample_bytes);
    let instant_bps = delta_bytes as f64 / dt;
    state.smoothed_bps = if state.smoothed_bps <= f64::EPSILON {
        instant_bps
    } else {
        // EWMA to avoid erratic ETA changes.
        (state.smoothed_bps * 0.7) + (instant_bps * 0.3)
    };
    state.rate_sample_at = now;
    state.rate_sample_bytes = state.processed_bytes;
}

fn active_op_display(ops: &HashMap<String, String>) -> String {
    if ops.is_empty() {
        return "-".to_string();
    }
    let mut keys = ops.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter().take(3).collect::<Vec<_>>().join(",")
}

fn average_rate(bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64().max(1e-6);
    bytes as f64 / secs
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}", m, s)
    }
}

fn format_rate(bps: f64) -> String {
    if bps <= 0.1 {
        "0 B/s".to_string()
    } else {
        format!("{}/s", HumanBytes(bps as u64))
    }
}

fn format_eta(eta: Option<Duration>) -> String {
    eta.map(format_duration)
        .unwrap_or_else(|| "--:--".to_string())
}

fn compute_eta(total_bytes: u64, processed_bytes: u64, throughput_bps: f64) -> Option<Duration> {
    if throughput_bps <= 0.1 || processed_bytes >= total_bytes {
        return None;
    }
    let remaining = total_bytes.saturating_sub(processed_bytes) as f64;
    let secs = (remaining / throughput_bps).max(0.0);
    Some(Duration::from_secs_f64(secs))
}

fn idle_warning_due(
    now: Instant,
    last_progress: Instant,
    last_idle_warn: Option<Instant>,
    idle_threshold: Duration,
    processed_bytes: u64,
    total_bytes: u64,
) -> bool {
    if processed_bytes >= total_bytes && total_bytes > 0 {
        return false;
    }
    if now.duration_since(last_progress) < idle_threshold {
        return false;
    }
    match last_idle_warn {
        Some(last_warn) => now.duration_since(last_warn) >= idle_threshold,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_resolution_respects_tty_override() {
        let cfg_tty = ProgressConfig::new(ProgressMode::Auto).with_tty_override(true);
        assert_eq!(cfg_tty.resolve_mode(), ResolvedProgressMode::Rich);

        let cfg_not_tty = ProgressConfig::new(ProgressMode::Auto).with_tty_override(false);
        assert_eq!(cfg_not_tty.resolve_mode(), ResolvedProgressMode::Plain);

        let cfg_quiet = ProgressConfig::new(ProgressMode::Quiet).with_tty_override(true);
        assert_eq!(cfg_quiet.resolve_mode(), ResolvedProgressMode::Quiet);
    }

    #[test]
    fn eta_computation_uses_remaining_bytes() {
        let eta = compute_eta(1_000, 250, 50.0).expect("eta should exist");
        // Remaining: 750 bytes at 50 bytes/s = 15s
        assert_eq!(eta.as_secs(), 15);

        assert!(compute_eta(100, 100, 1_000.0).is_none());
        assert!(compute_eta(100, 10, 0.0).is_none());
    }

    #[test]
    fn idle_warning_triggers_at_boundary() {
        let now = Instant::now();
        let threshold = Duration::from_secs(30);
        assert!(idle_warning_due(
            now + threshold,
            now,
            None,
            threshold,
            10,
            100
        ));

        // If warning already emitted recently, no duplicate yet.
        assert!(!idle_warning_due(
            now + threshold,
            now,
            Some(now + Duration::from_secs(10)),
            threshold,
            10,
            100
        ));
    }
}
