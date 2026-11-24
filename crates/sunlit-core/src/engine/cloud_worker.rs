//! The thread that fetches and decodes the cloud texture, and the handle the
//! engine holds it by.
//!
//! It never touches the GPU. Everything here is network and JPEG work parked in
//! the mailbox for the engine loop to upload on its own schedule, which is what
//! keeps a slow fetch off the render path.
//!
//! One attempt runs at a time, on its own thread. A historical fetch can hold
//! an HTTP request for the better part of a minute, and the only way a newer
//! request answers inside that window is to stop waiting for the old attempt:
//! the coordinator raises the attempt's own stop, drops the channel its result
//! would arrive on, and starts the new attempt at once. The abandoned thread
//! unwinds at its source's next check point, or ends when its request does,
//! and commits nothing — its own stop is raised before any of that matters.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded, select, unbounded};
use tracing::{debug, info, warn};

use super::schedule::{RoundSchedule, Schedule};
use crate::assets::cloud_archive::CloudArchive;
use crate::assets::cloud_fetcher::{CloudUpdater, EnsureOutcome, PollOutcome, RetryBackoff};
use crate::assets::cloud_source::CloudSource;
use crate::assets::mailbox::TextureMailbox;

/// What the engine asks the worker to do.
#[derive(Clone, Copy, Debug)]
pub(super) enum CloudRequest {
    /// One poll of the source in force, on schedule or on demand.
    Poll,
    /// Put one historical round on screen: the archive first, the upstream
    /// where the archive does not answer. The ensure's serve-half answers
    /// covered rounds from the archive alone, so no separate request is
    /// needed.
    Fetch(time::OffsetDateTime),
    /// Warm the GPU cache with one archived round, without the network and
    /// without touching the timeline. Best effort: dropped while the worker
    /// is busy with a real request.
    Prefetch(time::OffsetDateTime),
}

/// What one historical attempt came back with, for the engine's timeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FetchResult {
    pub round: time::OffsetDateTime,
    pub outcome: EnsureOutcome,
    /// The user stop stood when the attempt ended, so the engine stands its
    /// own state down rather than scheduling a retry.
    pub stopped: bool,
}

/// What one attempt ended with. The coordinator turns this into shared
/// answers; the engine reads them on its ticks.
#[derive(Clone, Copy)]
enum AttemptOutcome {
    Poll { outcome: PollOutcome, posted: bool },
    Fetch(FetchResult),
    Prefetch,
}

/// The cloud fetch coordinator and the handle the engine holds it by.
pub(super) struct CloudWorker {
    tx: Sender<CloudRequest>,
    busy: Arc<AtomicBool>,
    /// True while the attempt in flight is a cache warm rather than a fetch
    /// the user asked for, so the Fetch button stays a Fetch.
    prefetching: Arc<AtomicBool>,
    /// The texture resolution the worker should be fetching the variant of.
    ///
    /// Read by the worker before every attempt rather than sent as a message,
    /// so a switch that lands while an attempt is in flight takes effect on
    /// the next one instead of queueing behind one that may be minutes from
    /// finishing.
    target: Arc<AtomicU32>,
    /// The source a `SetCloudSource` left for the worker to apply, overwritten
    /// by every later one, so rapid applies leave only the last behind.
    pending: Arc<Mutex<Option<Arc<dyn CloudSource>>>>,
    /// The cloud generation in force, shared with the updater and the
    /// renderer, which is what makes the first-answer and ready states
    /// per-target rather than per-engine.
    generation: Arc<AtomicU64>,
    /// The source in force, so the engine can ask it what it is doing.
    current: Arc<Mutex<Option<Arc<dyn CloudSource>>>>,
    /// The worker's own coarse state, for the UI's source status.
    status: Arc<Mutex<String>>,
    /// The resolved source's first-answer budget, in milliseconds, which the
    /// wallpaper's cloud wait and the headless waits follow.
    wait_ms: Arc<AtomicU64>,
    pub(super) schedule: Schedule,
    rounds: RoundSchedule,
    /// A poll came due but the worker was busy, so it still owes one. Kept
    /// separate from the schedule so a busy worker does not drag the deadline
    /// backwards and forwards on every tick.
    pub(super) owed: bool,
    /// What the worker's first attempt came to, a [`FirstAnswer`] as its
    /// discriminant. Written by the coordinator, before a failed attempt's
    /// backoff, and read by the engine's tick.
    answer: Arc<AtomicU8>,
    /// Whether the cloud layer is switched on, which the engine writes on
    /// every tick and the worker reads before every attempt and while it
    /// backs off, so a retry stops when the layer is switched off.
    enabled: Arc<AtomicBool>,
    /// The worker ended a request without finishing it because the layer was
    /// switched off. The engine takes it and owes the poll again.
    abandoned: Arc<AtomicBool>,
    /// The fetch result the last historical attempt landed on, taken by the
    /// engine's tick.
    fetch_result: Arc<Mutex<Option<FetchResult>>>,
    /// The attempt in flight's own stop flag, which [`CloudWorker::abort`]
    /// raises. `None` between attempts, so a click between them lands
    /// nowhere.
    abort_flag: Arc<Mutex<Option<Arc<AtomicBool>>>>,
    _thread: JoinHandle<()>,
}

/// What the worker's first attempt came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum FirstAnswer {
    /// No attempt has finished yet.
    Pending,
    /// A map is on its way to the mailbox, from the cache or the network.
    Posted,
    /// There was no cached map and the poll failed, so nothing is coming until
    /// a retry succeeds.
    Unavailable,
}

impl FirstAnswer {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Posted,
            2 => Self::Unavailable,
            _ => Self::Pending,
        }
    }
}

impl CloudWorker {
    /// What the worker's first attempt came to, so far.
    pub(super) fn first_answer(&self) -> FirstAnswer {
        FirstAnswer::from_u8(self.answer.load(Ordering::SeqCst))
    }

    /// Point the worker at a different texture resolution's cloud variant.
    ///
    /// Returns whether that changed anything, so the caller can decide whether
    /// a poll is worth asking for.
    pub(super) fn retarget(&self, resolution: u32) -> bool {
        self.target.swap(resolution, Ordering::SeqCst) != resolution
    }

    /// Hand the worker a new source, replacing any switch not yet applied:
    /// rapid applies leave only the last one behind.
    pub(super) fn set_source(&mut self, source: Arc<dyn CloudSource>) {
        *self.pending.lock().unwrap_or_else(PoisonError::into_inner) = Some(source);
        self.rounds = RoundSchedule::default();
    }

    pub(super) fn scheduled_due(
        &mut self,
        elapsed: Duration,
        utc: time::OffsetDateTime,
        enabled: bool,
    ) -> bool {
        let fetching = self.fetching();
        let publication = self
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|source| source.publication());
        if let Some(publication) = publication {
            let due = self.rounds.due(utc, fetching, publication);
            enabled && due
        } else {
            enabled && self.schedule.due(elapsed)
        }
    }

    /// How long one round of the source in force covers, when it publishes on
    /// a schedule: the engine floors instants onto it.
    pub(super) fn publication_interval(&self) -> Option<Duration> {
        self.publication().map(|publication| publication.interval)
    }

    /// The newest round the source in force has published, as its last
    /// check saw. A historical round newer than this is not missing — the
    /// upstream has not caught up to it yet.
    pub(super) fn publication_latest(&self) -> Option<time::OffsetDateTime> {
        self.publication()
            .and_then(|publication| publication.latest)
    }

    fn publication(&self) -> Option<crate::assets::cloud_source::CloudPublication> {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|source| source.publication())
    }

    /// The cloud generation in force: raised whenever the effective target
    /// changes, which is what makes the first-answer and ready states
    /// per-target rather than per-engine.
    pub(super) fn cloud_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// What the cloud layer is doing right now: the source's own phase when
    /// it has one (a download's progress, say), the worker's coarse state
    /// otherwise. Which round the window wears is the round line's answer,
    /// so nothing here repeats it.
    pub(super) fn status(&self) -> String {
        compose_status(&self.coarse(), self.current_status())
    }

    /// The one line worth showing beside the round line, when there is one:
    /// the source's own progress phase, and the coarse lines that name a
    /// failure or a stop. The ready and checking chatter says nothing the
    /// round line does not, and the UI folds this into the round line, so
    /// the panel carries ONE cloud line.
    pub(super) fn note(&self) -> Option<String> {
        let phase = self.current_status();
        if let Some(phase) = phase {
            return Some(phase);
        }
        let coarse = self.coarse();
        (coarse.contains("failed") || coarse.contains("stopped")).then_some(coarse)
    }

    fn current_status(&self) -> Option<String> {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|source| source.status())
            .filter(|phase| !phase.is_empty())
    }

    fn coarse(&self) -> String {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// How long a client waits for the source in force to answer once.
    pub(super) fn wait(&self) -> Duration {
        Duration::from_millis(self.wait_ms.load(Ordering::SeqCst))
    }

    /// Whether a fetch run is in flight, a superseded attempt's unwind and
    /// the retry backoff included: the UI's stop shows against this.
    pub(super) fn fetching(&self) -> bool {
        self.busy.load(Ordering::SeqCst) && !self.prefetching.load(Ordering::SeqCst)
    }

    /// Tell the worker whether the cloud layer is switched on.
    pub(super) fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    /// Whether the worker gave up a request because the layer was switched
    /// off, clearing the flag.
    pub(super) fn take_abandoned(&self) -> bool {
        self.abandoned.swap(false, Ordering::SeqCst)
    }

    /// Raise the user stop against the attempt in flight, if there is one:
    /// its source unwinds at its next check point and the run ends without a
    /// retry. A click between attempts raises nothing, which is the point.
    pub(super) fn abort(&self) {
        if let Some(flag) = self
            .abort_flag
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            flag.store(true, Ordering::SeqCst);
        }
    }

    /// Ask the worker for one poll. Returns `false` when the worker is still
    /// busy with the previous one, so the caller can retry soon. A poll never
    /// preempts what is in flight; the engine owes it instead.
    pub(super) fn request(&mut self) -> bool {
        if self.busy.swap(true, Ordering::SeqCst) {
            return false;
        }
        if self.tx.send(CloudRequest::Poll).is_err() {
            self.busy.store(false, Ordering::SeqCst);
            return false;
        }
        self.rounds.started();
        true
    }

    /// Fetch one historical round, the upstream included. Sent unguarded for
    /// the same reason: a newer drag preempts an older fetch instead of
    /// queueing behind the minute it may still hold.
    pub(super) fn fetch(&self, round: time::OffsetDateTime) {
        let _ = self.tx.send(CloudRequest::Fetch(round));
    }

    /// Warm the GPU cache with one archived round. Best effort: the
    /// coordinator drops it if a real request is in flight.
    pub(super) fn prefetch(&self, round: time::OffsetDateTime) {
        let _ = self.tx.send(CloudRequest::Prefetch(round));
    }

    /// Take the last historical attempt's result, if it has landed.
    pub(super) fn take_fetch_result(&self) -> Option<FetchResult> {
        self.fetch_result
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// The status line the window shows: the source's own phase wins while it
/// has one, since that is the finer account of the same run, and the
/// worker's coarse state stands otherwise. The round line names what the
/// window wears; nothing here repeats it.
fn compose_status(coarse: &str, phase: Option<String>) -> String {
    phase.unwrap_or_else(|| coarse.to_owned())
}

/// The shared state one attempt runs against. Cloned per attempt; nothing
/// here outlives the attempt that holds it. The `active_entry` and `first`
/// are shared across the clones, so an entry switch is noticed exactly once.
struct AttemptState {
    mailbox: TextureMailbox,
    /// The cache entry the last attempt adopted. An updater is built per
    /// attempt at the resolution in force, so a switch of the entry is
    /// invisible to `set_resolution`; the attempt that finds a different key
    /// here replays what a long-lived updater's switch did: seed the archive
    /// for the new entry and show whatever it has on disk.
    active_entry: Arc<Mutex<Option<String>>>,
    notify: crate::assets::cloud_fetcher::NotifyFn,
    archive: CloudArchive,
    cache_dir: Option<PathBuf>,
    target: Arc<AtomicU32>,
    generation: Arc<AtomicU64>,
    answer: Arc<AtomicU8>,
    enabled: Arc<AtomicBool>,
    abandoned: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    first: Arc<Mutex<bool>>,
}

impl AttemptState {
    /// Whether this attempt is the first on a different cache entry, and if
    /// so make that entry the archive's active one and seed it — what a
    /// long-lived updater's resolution switch did.
    fn adopt(&self, updater: &mut CloudUpdater) -> bool {
        let key = updater.cache_key().to_owned();
        let changed = {
            let mut active = self
                .active_entry
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let changed = active.as_deref() != Some(key.as_str());
            if changed {
                *active = Some(key.clone());
            }
            changed
        };
        if changed {
            updater.activate_entry();
        }
        changed
    }

    /// An updater for the source in force, at the resolution in force,
    /// behind the layer's gate: a download that arrives with the layer off
    /// is dropped undecoded, as a long-lived updater's was.
    fn updater(&self, source: &Arc<dyn CloudSource>) -> CloudUpdater {
        let mut updater = CloudUpdater::new(
            Arc::clone(source),
            self.mailbox.clone(),
            Arc::clone(&self.notify),
            Arc::clone(&self.generation),
            self.archive.clone(),
            self.cache_dir.clone(),
            self.target.load(Ordering::SeqCst),
        );
        updater.set_gate(Arc::clone(&self.enabled));
        updater
    }
}

/// Spawn the coordinator thread that does cloud network I/O and JPEG decoding
/// by way of one attempt thread at a time.
///
/// It never touches the GPU: it parks decoded frames in the mailbox and pokes
/// the engine, which uploads them on its own schedule.
#[expect(
    clippy::too_many_arguments,
    reason = "the worker's whole configuration, handed over once at spawn"
)]
pub(super) fn spawn_cloud_worker(
    source: &Arc<dyn CloudSource>,
    mailbox: &TextureMailbox,
    notify: &crate::assets::cloud_fetcher::NotifyFn,
    generation: &Arc<AtomicU64>,
    archive: &CloudArchive,
    cache_dir: Option<&std::path::Path>,
    interval: Duration,
    now: Duration,
    texture_resolution: u32,
    enabled: bool,
) -> CloudWorker {
    info!(source = %source.describe(), poll_secs = interval.as_secs(), "cloud source configured");

    let (tx, rx) = unbounded::<CloudRequest>();
    let busy = Arc::new(AtomicBool::new(false));
    let target = Arc::new(AtomicU32::new(texture_resolution));
    let pending = Arc::new(Mutex::new(None));
    let generation = Arc::clone(generation);
    let current = Arc::new(Mutex::new(None));
    let status = Arc::new(Mutex::new(String::from("waiting for the first check")));
    let wait_ms = Arc::new(AtomicU64::new(
        u64::try_from(source.wait().as_millis()).unwrap_or(u64::MAX),
    ));
    let answer = Arc::new(AtomicU8::new(FirstAnswer::Pending as u8));
    let enabled = Arc::new(AtomicBool::new(enabled));
    let abandoned = Arc::new(AtomicBool::new(false));
    let fetch_result = Arc::new(Mutex::new(None));
    let abort_flag = Arc::new(Mutex::new(None::<Arc<AtomicBool>>));

    let state = AttemptState {
        mailbox: mailbox.clone(),
        active_entry: Arc::new(Mutex::new(None)),
        notify: Arc::clone(notify),
        archive: archive.clone(),
        cache_dir: cache_dir.map(std::path::Path::to_path_buf),
        target: Arc::clone(&target),
        generation: Arc::clone(&generation),
        answer: Arc::clone(&answer),
        enabled: Arc::clone(&enabled),
        abandoned: Arc::clone(&abandoned),
        status: Arc::clone(&status),
        first: Arc::new(Mutex::new(true)),
    };

    let worker_source = Arc::clone(source);
    let worker_busy = Arc::clone(&busy);
    let prefetching = Arc::new(AtomicBool::new(false));
    let worker_prefetching = Arc::clone(&prefetching);
    let worker_pending = Arc::clone(&pending);
    let worker_current = Arc::clone(&current);
    let worker_wait_ms = Arc::clone(&wait_ms);
    let worker_abort_flag = Arc::clone(&abort_flag);
    let worker_fetch_result = Arc::clone(&fetch_result);
    *current.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&worker_source));

    // The first showing is the coordinator's own: whatever the entry has on
    // disk goes up before any attempt, and the archive seeds from the same
    // cache while it is here.
    {
        let mut updater = CloudUpdater::new(
            Arc::clone(&worker_source),
            mailbox.clone(),
            Arc::clone(notify),
            Arc::clone(&generation),
            archive.clone(),
            cache_dir.map(std::path::Path::to_path_buf),
            target.load(Ordering::SeqCst),
        );
        updater.set_gate(Arc::clone(&enabled));
        updater.activate_entry();
        updater.post_cached();
    }

    let thread = std::thread::Builder::new()
        .name("sunlit-cloud".into())
        .spawn(move || {
            coordinator(
                &rx,
                &worker_source,
                &state,
                &worker_pending,
                &worker_current,
                &worker_busy,
                &worker_prefetching,
                &worker_wait_ms,
                &worker_abort_flag,
                &worker_fetch_result,
            );
        })
        .expect("failed to spawn cloud worker thread");

    CloudWorker {
        tx,
        busy,
        prefetching,
        target,
        pending,
        generation,
        current,
        status,
        wait_ms,
        // The startup check rides `owed` below, so the schedule itself starts
        // one interval out: rolling it from the first tick instead would owe
        // a second check right after the startup one.
        schedule: Schedule {
            interval,
            next: now + interval,
        },
        // The startup check is owed whatever auto-refresh says: it feeds the
        // first wallpaper, which is a wallpaper update even with the timer off.
        owed: true,
        rounds: RoundSchedule::default(),
        answer,
        enabled,
        abandoned,
        fetch_result,
        abort_flag,
        _thread: thread,
    }
}

/// The coordinator: apply switches, start one attempt at a time, and let a
/// newer user request supersede the attempt in flight.
///
/// The loop's shape: a request arrives; an attempt thread runs it; the
/// coordinator selects between that attempt's result and the request channel.
/// A poll arriving during an attempt waits — polls never preempt. A serve or
/// a fetch arriving during one raises the attempt's own stop, drops the
/// result channel, and takes the new request up at once; the abandoned
/// thread unwinds at its source's next check point and commits nothing.
#[expect(
    clippy::too_many_arguments,
    reason = "the coordinator's shared handles are handed over once from the spawn"
)]
fn coordinator(
    rx: &Receiver<CloudRequest>,
    source: &Arc<dyn CloudSource>,
    state: &AttemptState,
    pending: &Arc<Mutex<Option<Arc<dyn CloudSource>>>>,
    current: &Arc<Mutex<Option<Arc<dyn CloudSource>>>>,
    busy: &AtomicBool,
    prefetching: &AtomicBool,
    wait_ms: &AtomicU64,
    abort_flag: &Mutex<Option<Arc<AtomicBool>>>,
    fetch_result: &Mutex<Option<FetchResult>>,
) {
    // The backoff ramps within one poll attempt, which is the run an outage
    // lives inside; a fresh attempt starts the ramp over.
    let mut queued: Option<CloudRequest> = None;
    loop {
        let request = queued.take().unwrap_or_else(|| match rx.recv() {
            Ok(request) => request,
            Err(_) => CloudRequest::Poll,
        });
        apply_switch(state, pending, current, wait_ms);
        prefetching.store(false, Ordering::SeqCst);
        if !state.enabled.load(Ordering::SeqCst) {
            give_up(&state.abandoned);
            set_coarse(&state.status, "The cloud layer is switched off.");
            // A historical request answered only by the layer switch must
            // still clear the engine's in-flight mark.
            if let CloudRequest::Fetch(round) = request {
                *fetch_result.lock().unwrap_or_else(PoisonError::into_inner) = Some(FetchResult {
                    round,
                    outcome: EnsureOutcome::Failed,
                    stopped: false,
                });
            }
            busy.store(false, Ordering::SeqCst);
            continue;
        }
        busy.store(true, Ordering::SeqCst);
        prefetching.store(
            matches!(request, CloudRequest::Prefetch(_)),
            Ordering::SeqCst,
        );
        let cancel = Arc::new(AtomicBool::new(false));
        let attempt_cancel = Arc::clone(&cancel);
        *abort_flag.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&cancel));
        let (result_tx, result_rx) = bounded::<AttemptOutcome>(1);
        let attempt = std::thread::Builder::new()
            .name("sunlit-cloud-attempt".into())
            .spawn({
                let mut attempt_state = AttemptState {
                    mailbox: state.mailbox.clone(),
                    active_entry: Arc::clone(&state.active_entry),
                    notify: Arc::clone(&state.notify),
                    archive: state.archive.clone(),
                    cache_dir: state.cache_dir.clone(),
                    target: Arc::clone(&state.target),
                    generation: Arc::clone(&state.generation),
                    answer: Arc::clone(&state.answer),
                    enabled: Arc::clone(&state.enabled),
                    abandoned: Arc::clone(&state.abandoned),
                    status: Arc::clone(&state.status),
                    first: Arc::clone(&state.first),
                };
                let source = current_source(current).unwrap_or_else(|| Arc::clone(source));
                move || {
                    run_attempt(
                        request,
                        &source,
                        &attempt_cancel,
                        &result_tx,
                        &mut attempt_state,
                    );
                }
            });
        let Ok(attempt) = attempt else {
            warn!("the cloud attempt thread could not be started");
            *abort_flag.lock().unwrap_or_else(PoisonError::into_inner) = None;
            busy.store(false, Ordering::SeqCst);
            continue;
        };
        // Wait for the attempt to end, or for a request that outranks it.
        loop {
            select! {
                recv(result_rx) -> outcome => {
                    prefetching.store(false, Ordering::SeqCst);
                    *abort_flag.lock().unwrap_or_else(PoisonError::into_inner) = None;
                    if let Ok(outcome) = outcome {
                        handle_outcome(state, outcome, fetch_result);
                    } else {
                        warn!("the cloud attempt thread ended without an answer");
                    }
                    let _ = attempt.join();
                    // Idle again, unless a request the select already took
                    // is waiting to be attempted.
                    if queued.is_none() {
                        busy.store(false, Ordering::SeqCst);
                    }
                    break;
                }
                recv(rx) -> next => {
                    match next {
                        Ok(CloudRequest::Poll | CloudRequest::Prefetch(_)) => {
                            // Polls never preempt: the engine owes another
                            // one when this attempt is over. A cache warm is
                            // dropped outright, and the engine asks again on
                            // a later tick. Waiting resumes with the same
                            // select.
                        }
                        Ok(request) => {
                            // A newer user request outranks the attempt in
                            // flight: raise its stop, drop its answer, and
                            // take the new one up at once. The abandoned
                            // thread is detached, never joined — joining it
                            // would wait out the very request the supersede
                            // exists to skip; it ends when its own request
                            // does, and commits nothing.
                            cancel.store(true, Ordering::SeqCst);
                            prefetching.store(false, Ordering::SeqCst);
                            drop(result_rx);
                            drop(attempt);
                            *abort_flag.lock().unwrap_or_else(PoisonError::into_inner) = None;
                            queued = Some(request);
                            break;
                        }
                        Err(_) => return,
                    }
                }
            }
        }
    }
}

/// The source in force, as the coordinator sees it.
fn current_source(
    current: &Arc<Mutex<Option<Arc<dyn CloudSource>>>>,
) -> Option<Arc<dyn CloudSource>> {
    current
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}

/// Apply the newest pending source switch, if there is one: raise the
/// generation so the previous target's in-flight results are dropped, reset
/// the first answer to the new target's, re-point the archive, and show
/// whatever the new target already has on disk.
fn apply_switch(
    state: &AttemptState,
    pending: &Arc<Mutex<Option<Arc<dyn CloudSource>>>>,
    current: &Arc<Mutex<Option<Arc<dyn CloudSource>>>>,
    wait_ms: &AtomicU64,
) {
    let Some(source) = pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
    else {
        return;
    };
    info!(
        source = %source.describe(),
        "the cloud source changed, rebuilding the fetcher"
    );
    *current.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&source));
    set_coarse(&state.status, "The cloud source changed.");
    state.generation.fetch_add(1, Ordering::SeqCst);
    state
        .answer
        .store(FirstAnswer::Pending as u8, Ordering::SeqCst);
    wait_ms.store(
        u64::try_from(source.wait().as_millis()).unwrap_or(u64::MAX),
        Ordering::SeqCst,
    );
    let mut updater = CloudUpdater::new(
        source,
        state.mailbox.clone(),
        Arc::clone(&state.notify),
        Arc::clone(&state.generation),
        state.archive.clone(),
        state.cache_dir.clone(),
        state.target.load(Ordering::SeqCst),
    );
    updater.set_gate(Arc::clone(&state.enabled));
    updater.activate_entry();
    updater.post_cached();
}

/// Run one attempt to its end. The thread owns the source's stop flag and
/// the result channel; whatever it ends with goes out through the channel,
/// and a superseded attempt finds the channel gone and exits quietly.
fn run_attempt(
    request: CloudRequest,
    source: &Arc<dyn CloudSource>,
    cancel: &Arc<AtomicBool>,
    result_tx: &Sender<AttemptOutcome>,
    state: &mut AttemptState,
) {
    source.set_cancel(Arc::clone(cancel));
    let outcome = match request {
        CloudRequest::Poll => {
            let mut backoff = RetryBackoff::new();
            poll_attempt(source, cancel, state, &mut backoff)
        }
        CloudRequest::Prefetch(round) => {
            let mut updater = state.updater(source);
            updater.prefetch_round(round);
            AttemptOutcome::Prefetch
        }
        CloudRequest::Fetch(round) => {
            set_coarse(&state.status, "Fetching the historical round.");
            let mut updater = state.updater(source);
            updater.set_resolution(state.target.load(Ordering::SeqCst));
            state.adopt(&mut updater);
            let outcome = updater.ensure_round(round);
            let stopped = outcome != EnsureOutcome::Served && cancel.load(Ordering::SeqCst);
            match (&outcome, stopped) {
                (EnsureOutcome::Served, _) => {
                    set_coarse(&state.status, "The historical round is ready.");
                }
                (EnsureOutcome::Missing | EnsureOutcome::Failed, false) => {
                    set_coarse(&state.status, "");
                }
                (_, true) => {
                    set_coarse(&state.status, "The fetch was stopped.");
                    info!("the historical fetch was stopped by the user");
                }
            }
            AttemptOutcome::Fetch(FetchResult {
                round,
                outcome,
                stopped,
            })
        }
    };
    // A superseded attempt's receiver is gone; the send failing is how the
    // thread ends quietly.
    let _ = result_tx.send(outcome);
}

/// One poll attempt: the live pipeline's loop, ending when the source
/// settles, the layer goes off, or a stop lands.
fn poll_attempt(
    source: &Arc<dyn CloudSource>,
    cancel: &Arc<AtomicBool>,
    state: &mut AttemptState,
    backoff: &mut RetryBackoff,
) -> AttemptOutcome {
    // The first request shows whatever the target already has on disk before
    // touching the network, on the first attempt rather than at spawn — the
    // coordinator's own showing covers a clean start, and this covers the
    // first attempt after a switch.
    if std::mem::take(&mut *state.first.lock().unwrap_or_else(PoisonError::into_inner)) {
        let mut updater = state.updater(source);
        if !updater.set_resolution(state.target.load(Ordering::SeqCst)) {
            updater.post_cached();
        }
    }
    loop {
        // Before every attempt, so a layer switched off during an outage
        // makes no further one. A download already under way is not
        // interrupted; the updater drops what it brings back undecoded.
        if !state.enabled.load(Ordering::SeqCst) {
            give_up(&state.abandoned);
            set_coarse(&state.status, "The cloud layer is switched off.");
            return AttemptOutcome::Poll {
                outcome: PollOutcome::Abandoned,
                posted: false,
            };
        }
        set_coarse(&state.status, "Checking the cloud source.");
        let mut updater = state.updater(source);
        updater.set_resolution(state.target.load(Ordering::SeqCst));
        // The updater was built at the resolution in force, so a switch the
        // engine made since the last attempt is invisible to
        // `set_resolution`; the attempt that adopts a new entry replays what
        // a long-lived updater's switch did: seed the archive and show
        // whatever that entry has on disk.
        let adopted = state.adopt(&mut updater);
        if adopted {
            updater.post_cached();
        }
        let outcome = updater.poll_once();
        // Consumed whatever the flag now reads: a stop that lands while the
        // poll is finishing belongs to the attempt after this one, and a
        // stop is only ever taken against a failed attempt — a round that
        // produced a map delivers it.
        let stopped = cancel.swap(false, Ordering::SeqCst);
        let posted = updater.has_posted();
        record_first_answer(&state.answer, outcome, posted);
        match outcome {
            PollOutcome::Updated | PollOutcome::Unchanged => {
                debug!("cloud poll settled");
                set_coarse(&state.status, "The cloud map is ready.");
                backoff.reset();
                return AttemptOutcome::Poll { outcome, posted };
            }
            PollOutcome::Abandoned => {
                give_up(&state.abandoned);
                return AttemptOutcome::Poll { outcome, posted };
            }
            PollOutcome::Failed => {
                if stopped {
                    set_coarse(
                        &state.status,
                        "The download was stopped; the next check follows the regular schedule.",
                    );
                    info!("the cloud poll was stopped by the user");
                    return AttemptOutcome::Poll { outcome, posted };
                }
                let delay = backoff.fail();
                set_coarse(
                    &state.status,
                    &format!("The last poll failed; retrying in {} s.", delay.as_secs()),
                );
                warn!(
                    retry_delay_secs = delay.as_secs(),
                    "cloud poll failed, retrying"
                );
                rest_while_enabled(delay, &state.enabled, cancel);
            }
        }
    }
}

/// Sleep out `delay` in slices, so a stop or a layer switch lands within
/// about [`REST_SLICE`] rather than at the delay's end.
fn rest_while_enabled(delay: Duration, enabled: &AtomicBool, cancel: &AtomicBool) {
    let until = std::time::Instant::now() + delay;
    loop {
        if !enabled.load(Ordering::SeqCst) || cancel.load(Ordering::SeqCst) {
            return;
        }
        let left = until.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return;
        }
        std::thread::sleep(left.min(REST_SLICE));
    }
}

/// How often a backoff looks at whether the layer is still switched on.
const REST_SLICE: Duration = Duration::from_millis(100);

/// Turn an attempt's end into the shared answers: the first answer, the
/// busy flag on the paths that need it, and the engine's fetch result.
fn handle_outcome(
    state: &AttemptState,
    outcome: AttemptOutcome,
    fetch_result: &Mutex<Option<FetchResult>>,
) {
    match outcome {
        AttemptOutcome::Poll {
            outcome: poll,
            posted,
        } => {
            record_first_answer(&state.answer, poll, posted);
        }
        AttemptOutcome::Fetch(result) => {
            *fetch_result.lock().unwrap_or_else(PoisonError::into_inner) = Some(result);
        }
        AttemptOutcome::Prefetch => {}
    }
}

/// Write the worker's coarse state; the engine forwards it when it changes.
fn set_coarse(status: &Mutex<String>, text: &str) {
    text.clone_into(&mut status.lock().unwrap_or_else(PoisonError::into_inner));
}

/// The layer is switched off: nothing more runs until it is on again, and the
/// engine learns a request ended unfinished so it can owe the poll.
fn give_up(abandoned: &AtomicBool) {
    info!("the cloud layer is switched off, the poll waits until it is on again");
    abandoned.store(true, Ordering::SeqCst);
}

/// Record what the worker's first finished attempt came to, once: a map on its
/// way if one has been posted, and nothing to show otherwise. A download
/// dropped because the layer went off is no answer.
fn record_first_answer(answer: &AtomicU8, outcome: PollOutcome, posted: bool) {
    if outcome == PollOutcome::Abandoned
        || answer.load(Ordering::SeqCst) != FirstAnswer::Pending as u8
    {
        return;
    }
    let first = if posted {
        FirstAnswer::Posted
    } else {
        FirstAnswer::Unavailable
    };
    answer.store(first as u8, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::cloud_fetcher::no_notify;
    use crate::assets::cloud_source::{CloudImage, Download};
    use image::ImageEncoder;

    /// Enough slots for the grid, the file-backed textures and the pair.
    const SLOT_COUNT: usize = 3;
    const TIMEOUT: Duration = Duration::from_secs(5);

    fn fixture_jpeg() -> Vec<u8> {
        let mut jpeg = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 88)
            .write_image(&[128; 4], 2, 2, image::ExtendedColorType::L8)
            .expect("the fixture encodes");
        jpeg
    }

    /// A source that answers every poll with one map, and counts.
    struct ScriptedSource {
        fetches: std::sync::atomic::AtomicU64,
    }

    impl ScriptedSource {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                fetches: std::sync::atomic::AtomicU64::new(0),
            })
        }
    }

    impl CloudSource for ScriptedSource {
        fn fetch_if_changed(
            &self,
            _known_etag: Option<&str>,
        ) -> Result<Option<CloudImage>, String> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(Some(CloudImage {
                bytes: Download::new(fixture_jpeg()),
                etag: None,
                last_modified: None,
                round: None,
                raws: Vec::new(),
            }))
        }

        fn describe(&self) -> String {
            "scripted worker source".to_owned()
        }
    }

    /// A source whose historical fetches park until the test releases them,
    /// honouring the stop the worker hands over. This is the slow compose
    /// request the supersede exists for.
    struct ParkedRoundSource {
        cancel: Mutex<Option<Arc<AtomicBool>>>,
        park: AtomicBool,
        started: std::sync::atomic::AtomicU64,
    }

    impl ParkedRoundSource {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                cancel: Mutex::new(None),
                park: AtomicBool::new(false),
                started: std::sync::atomic::AtomicU64::new(0),
            })
        }

        fn started(&self) -> u64 {
            self.started.load(Ordering::SeqCst)
        }
    }

    impl CloudSource for ParkedRoundSource {
        fn set_cancel(&self, flag: Arc<AtomicBool>) {
            *self.cancel.lock().expect("the stop slot") = Some(flag);
        }

        fn fetch_if_changed(
            &self,
            _known_etag: Option<&str>,
        ) -> Result<Option<CloudImage>, String> {
            self.started.fetch_add(1, Ordering::SeqCst);
            self.wait_parked()?;
            Ok(Some(CloudImage {
                bytes: Download::new(fixture_jpeg()),
                etag: None,
                last_modified: None,
                round: None,
                raws: Vec::new(),
            }))
        }

        fn fetch_round_at(
            &self,
            round: time::OffsetDateTime,
            _resolution: u32,
        ) -> Result<Option<CloudImage>, String> {
            self.started.fetch_add(1, Ordering::SeqCst);
            self.wait_parked()?;
            Ok(Some(CloudImage {
                bytes: Download::new(fixture_jpeg()),
                etag: None,
                last_modified: None,
                round: Some(round),
                raws: Vec::new(),
            }))
        }

        fn describe(&self) -> String {
            "parked round source".to_owned()
        }
    }

    impl ParkedRoundSource {
        /// The slow download the tests steer: parked, the request answers
        /// when released, or unwinds when the user stop lands.
        fn wait_parked(&self) -> Result<(), String> {
            while self.park.load(Ordering::SeqCst) {
                let stopped = self
                    .cancel
                    .lock()
                    .expect("the stop slot")
                    .as_ref()
                    .is_some_and(|flag| flag.load(Ordering::SeqCst));
                if stopped {
                    return Err("the download was stopped".to_owned());
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Ok(())
        }
    }

    /// Spawn a worker at one poll a day, so only the test's own requests move
    /// it, and hand back the worker, the mailbox it posts to and the source.
    fn worker(source: &Arc<dyn CloudSource>) -> (CloudWorker, TextureMailbox, Arc<AtomicU64>) {
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let generation = Arc::new(AtomicU64::new(0));
        let worker = spawn_cloud_worker(
            source,
            &mailbox,
            &no_notify(),
            &generation,
            &CloudArchive::new(None),
            None,
            Duration::from_secs(86_400),
            Duration::ZERO,
            2048,
            true,
        );
        (worker, mailbox, generation)
    }

    /// Wait until a pair is parked, or panic.
    fn wait_for_pair(mailbox: &TextureMailbox, what: &str) {
        let deadline = std::time::Instant::now() + TIMEOUT;
        loop {
            if mailbox.take_cloud_frame().is_some() {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{what}: no cloud pair within {TIMEOUT:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Ask for a poll, retrying while the worker is still finishing the
    /// previous one.
    fn request_when_free(worker: &mut CloudWorker) {
        let deadline = std::time::Instant::now() + TIMEOUT;
        while !worker.request() {
            assert!(
                std::time::Instant::now() < deadline,
                "the worker never became free"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn a_switch_applies_between_attempts_and_bumps_the_generation() {
        let (mut worker, mailbox, generation) =
            worker(&(ScriptedSource::new() as Arc<dyn CloudSource>));
        request_when_free(&mut worker);
        wait_for_pair(&mailbox, "the first source's map");
        assert_eq!(generation.load(Ordering::SeqCst), 0);

        worker.set_source(ScriptedSource::new());
        assert_eq!(
            generation.load(Ordering::SeqCst),
            0,
            "the switch bumps when the worker applies it, not when it is handed over"
        );
        request_when_free(&mut worker);
        wait_for_pair(&mailbox, "the new source's map");
        assert_eq!(generation.load(Ordering::SeqCst), 1, "the switch bumps");
        assert_eq!(worker.first_answer(), FirstAnswer::Posted);
    }

    /// A stop lands on a run parked mid-fetch: the run unwinds without
    /// posting or retrying, the status names the stop, and the worker takes
    /// the next run clean — a stale stop must not eat it.
    #[test]
    fn an_abort_stops_a_poll_without_a_retry_and_the_next_run_starts_clean() {
        let source = ParkedRoundSource::new();
        let (mut worker, mailbox, _generation) =
            worker(&(Arc::clone(&source) as Arc<dyn CloudSource>));
        request_when_free(&mut worker);
        wait_for_pair(&mailbox, "the first map");

        // The next run parks mid-fetch, which is the slow download; the stop
        // answers it there.
        source.park.store(true, Ordering::SeqCst);
        request_when_free(&mut worker);
        let deadline = std::time::Instant::now() + TIMEOUT;
        while source.started() < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "the second fetch never started: fetching={} status={:?}",
                worker.fetching(),
                worker.status()
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        worker.abort();
        let deadline = std::time::Instant::now() + TIMEOUT;
        while worker.fetching() {
            assert!(
                std::time::Instant::now() < deadline,
                "the stopped run never ended"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(
            worker.status().contains("stopped"),
            "the status names the stop: {:?}",
            worker.status()
        );

        // And the worker is clean afterwards: the next run starts fresh and
        // fetches normally, which is what a stale stop would break.
        source.park.store(false, Ordering::SeqCst);
        request_when_free(&mut worker);
        wait_for_pair(&mailbox, "the map after the stop");
    }

    /// The whole point of the coordinator: a newer historical fetch takes
    /// over inside the window the older one is parked, instead of queueing
    /// behind the minute the older one may still hold.
    #[test]
    fn a_newer_fetch_supersedes_the_one_in_flight() {
        let source = ParkedRoundSource::new();
        let (worker, mailbox, _generation) = worker(&(Arc::clone(&source) as Arc<dyn CloudSource>));
        let early = time::OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
        let late = time::OffsetDateTime::UNIX_EPOCH + time::Duration::hours(2);

        source.park.store(true, Ordering::SeqCst);
        worker.fetch(early);
        let deadline = std::time::Instant::now() + TIMEOUT;
        while source.started() < 1 {
            assert!(
                std::time::Instant::now() < deadline,
                "the first fetch never started"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        // The second fetch goes out while the first is parked; the
        // coordinator supersedes, and the first's stop answers its park.
        worker.fetch(late);
        source.park.store(false, Ordering::SeqCst);
        let deadline = std::time::Instant::now() + TIMEOUT;
        while source.started() < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "the superseding fetch never started"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        wait_for_pair(&mailbox, "the superseding fetch's map");
        let deadline = std::time::Instant::now() + TIMEOUT;
        let result = loop {
            if let Some(result) = worker.take_fetch_result() {
                break result;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the superseded run's result never landed"
            );
            std::thread::sleep(Duration::from_millis(2));
        };
        assert_eq!(
            result.round, late,
            "the result the engine reads is the one it asked for last"
        );
    }
}
