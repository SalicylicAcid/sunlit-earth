//! The thread that fetches and decodes the cloud texture, and the handle the
//! engine holds it by.
//!
//! It never touches the GPU. Everything here is network and JPEG work parked in
//! the mailbox for the engine loop to upload on its own schedule, which is what
//! keeps a slow fetch off the render path.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Sender, TryRecvError, bounded};
use tracing::{debug, info, warn};

use super::schedule::Schedule;
use crate::assets::cloud_fetcher::{CloudUpdater, PollOutcome, RetryBackoff};
use crate::assets::cloud_source::CloudSource;
use crate::assets::mailbox::TextureMailbox;

/// The cloud fetch thread and the flag that keeps requests from piling up.
pub(super) struct CloudWorker {
    tx: Sender<()>,
    busy: Arc<AtomicBool>,
    /// The texture resolution the worker should be fetching the variant of.
    ///
    /// Read by the worker before every poll rather than sent as a message, so a
    /// switch that lands while a fetch is in flight (or while a failed poll is
    /// backing off) takes effect on the next attempt instead of queueing behind
    /// one that may be minutes from finishing.
    target: Arc<AtomicU32>,
    /// The source a `SetCloudSource` left for the worker to apply, overwritten
    /// by every later one, so rapid applies leave only the last behind. The
    /// worker takes it between attempts; a fetch already under way is never
    /// interrupted, and its results are dropped by the generation the apply
    /// raises.
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
    /// A poll came due but the worker was busy, so it still owes one. Kept
    /// separate from the schedule so a busy worker does not drag the deadline
    /// backwards and forwards on every tick.
    pub(super) owed: bool,
    /// What the worker's first attempt came to, a [`FirstAnswer`] as its
    /// discriminant. Written once by the worker, before a failed attempt's
    /// backoff, and read by the engine's tick.
    answer: Arc<AtomicU8>,
    /// Whether the cloud layer is switched on, which the engine writes on
    /// every tick and the worker reads before every attempt and while it
    /// backs off, so a retry stops when the layer is switched off.
    enabled: Arc<AtomicBool>,
    /// The worker ended a request without finishing it because the layer was
    /// switched off. The engine takes it and owes the poll again.
    abandoned: Arc<AtomicBool>,
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
    pub(super) fn set_source(&self, source: Arc<dyn CloudSource>) {
        *self.pending.lock().unwrap_or_else(PoisonError::into_inner) = Some(source);
    }

    /// The cloud generation in force: raised whenever the effective target
    /// changes, which is what makes the first-answer and ready states
    /// per-target rather than per-engine.
    pub(super) fn cloud_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// What the cloud layer is doing right now: the source's own phase when
    /// it has one, the worker's coarse state otherwise.
    pub(super) fn status(&self) -> String {
        let coarse = self
            .status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let current = self
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        current.and_then(|source| source.status()).unwrap_or(coarse)
    }

    /// How long a client waits for the source in force to answer once.
    pub(super) fn wait(&self) -> Duration {
        Duration::from_millis(self.wait_ms.load(Ordering::SeqCst))
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

    /// Ask the worker for one poll. Returns `false` when the worker is still
    /// busy with the previous one, so the caller can retry soon.
    pub(super) fn request(&self) -> bool {
        if self.busy.swap(true, Ordering::SeqCst) {
            return false;
        }
        if self.tx.send(()).is_err() {
            self.busy.store(false, Ordering::SeqCst);
            return false;
        }
        true
    }
}

/// Spawn the thread that does cloud network I/O and JPEG decoding.
///
/// It never touches the GPU: it parks decoded frames in the mailbox and pokes
/// the engine, which uploads them on its own schedule.
#[expect(
    clippy::too_many_arguments,
    reason = "the worker's whole configuration, handed over once at spawn"
)]
pub(super) fn spawn_cloud_worker(
    source: Arc<dyn CloudSource>,
    mailbox: TextureMailbox,
    notify: crate::assets::cloud_fetcher::NotifyFn,
    generation: &Arc<AtomicU64>,
    cache_dir: Option<PathBuf>,
    interval: Duration,
    now: Duration,
    texture_resolution: u32,
    enabled: bool,
) -> CloudWorker {
    info!(source = %source.describe(), poll_secs = interval.as_secs(), "cloud source configured");

    let (tx, rx) = bounded::<()>(1);
    let busy = Arc::new(AtomicBool::new(false));
    let worker_busy = Arc::clone(&busy);
    let target = Arc::new(AtomicU32::new(texture_resolution));
    let worker_target = Arc::clone(&target);
    let pending = Arc::new(Mutex::new(None));
    let worker_pending = Arc::clone(&pending);
    let generation = Arc::clone(generation);
    let worker_generation = Arc::clone(&generation);
    let current = Arc::new(Mutex::new(None));
    let worker_current = Arc::clone(&current);
    let status = Arc::new(Mutex::new(String::from("waiting for the first check")));
    let worker_status = Arc::clone(&status);
    let wait_ms = Arc::new(AtomicU64::new(
        u64::try_from(source.wait().as_millis()).unwrap_or(u64::MAX),
    ));
    let worker_wait_ms = Arc::clone(&wait_ms);
    let answer = Arc::new(AtomicU8::new(FirstAnswer::Pending as u8));
    let worker_answer = Arc::clone(&answer);
    let enabled = Arc::new(AtomicBool::new(enabled));
    let worker_enabled = Arc::clone(&enabled);
    let abandoned = Arc::new(AtomicBool::new(false));
    let worker_abandoned = Arc::clone(&abandoned);

    let thread = std::thread::Builder::new()
        .name("sunlit-cloud".into())
        .spawn(move || {
            // Kept beside the updater so a source switch can rebuild it: the
            // originals are moved into the first construction.
            let updater_source = Arc::clone(&source);
            *worker_current
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(updater_source);
            let worker_mailbox = mailbox.clone();
            let worker_notify = Arc::clone(&notify);
            let worker_cache_dir = cache_dir.clone();
            let mut updater = CloudUpdater::new(
                source,
                mailbox,
                notify,
                Arc::clone(&worker_generation),
                cache_dir,
                texture_resolution,
            );
            updater.set_gate(Arc::clone(&worker_enabled));
            // The backoff outlives one fetch run: a failed run's next run
            // continues the ramp, which is what makes an outage's retries
            // grow instead of starting over.
            let mut backoff = RetryBackoff::new();
            let mut first = true;
            while rx.recv().is_ok() {
                if !worker_enabled.load(Ordering::SeqCst) {
                    give_up(&worker_abandoned);
                    worker_busy.store(false, Ordering::SeqCst);
                    continue;
                }
                let mut fetch = FetchLoop {
                    updater: &mut updater,
                    backoff: &mut backoff,
                    pending: &worker_pending,
                    current: &worker_current,
                    status: &worker_status,
                    wait_ms: &worker_wait_ms,
                    mailbox: &worker_mailbox,
                    notify: &worker_notify,
                    cache_dir: worker_cache_dir.as_ref(),
                    target: &worker_target,
                    generation: &worker_generation,
                    answer: &worker_answer,
                    enabled: &worker_enabled,
                    abandoned: &worker_abandoned,
                };
                // A switch that arrived while the worker was busy applies
                // before anything else, including the first cached showing.
                fetch.apply_switch();
                // Show whatever is on disk before touching the network, on
                // the first request rather than at spawn.
                fetch.first_showing(&mut first);
                match fetch.run(&rx) {
                    FetchEnds::Settled | FetchEnds::LayerOff => {}
                    FetchEnds::EngineGone => return,
                }
                worker_busy.store(false, Ordering::SeqCst);
            }
        })
        .expect("failed to spawn cloud worker thread");

    CloudWorker {
        tx,
        busy,
        target,
        pending,
        generation,
        current,
        status,
        wait_ms,
        // Poll immediately on the first tick, then on the configured interval.
        schedule: Schedule {
            interval,
            next: now,
        },
        owed: false,
        answer,
        enabled,
        abandoned,
        _thread: thread,
    }
}

/// What one fetch run ended with. The worker's busy flag drops either way;
/// `EngineGone` is the one that ends the thread.
enum FetchEnds {
    /// The target is current, or the fetch was dropped with the layer off.
    Settled,
    LayerOff,
    EngineGone,
}

/// The fetch loop: apply a switch, follow the resolution, poll, and back a
/// failure off until the target is current, the layer goes off, or the engine
/// is gone. Its inputs are the worker's shared state, gathered here so the
/// loop takes one argument instead of eight.
struct FetchLoop<'a> {
    updater: &'a mut CloudUpdater,
    backoff: &'a mut RetryBackoff,
    pending: &'a Mutex<Option<Arc<dyn CloudSource>>>,
    current: &'a Mutex<Option<Arc<dyn CloudSource>>>,
    status: &'a Mutex<String>,
    wait_ms: &'a AtomicU64,
    mailbox: &'a TextureMailbox,
    notify: &'a crate::assets::cloud_fetcher::NotifyFn,
    cache_dir: Option<&'a PathBuf>,
    target: &'a AtomicU32,
    generation: &'a Arc<AtomicU64>,
    answer: &'a AtomicU8,
    enabled: &'a AtomicBool,
    abandoned: &'a AtomicBool,
}

impl FetchLoop<'_> {
    /// Run the loop to one of its ends.
    fn run(&mut self, rx: &crossbeam_channel::Receiver<()>) -> FetchEnds {
        loop {
            // Before every attempt, so a layer switched off during an outage
            // makes no further one. A download already under way is not
            // interrupted; the updater drops what it brings back undecoded.
            if !self.enabled.load(Ordering::SeqCst) {
                give_up(self.abandoned);
                set_coarse(self.status, "the cloud layer is switched off");
                return FetchEnds::LayerOff;
            }
            set_coarse(self.status, "checking the cloud source");
            // A switch that arrived while an attempt was running applies
            // here, before the next one asks for anything.
            self.apply_switch();
            // Follow the resolution inside the loop, not outside it: an
            // outage can hold this thread for minutes at a time, and a
            // resolution switch made during one should change what the next
            // attempt asks for rather than wait its turn. The updater bumps
            // the shared generation itself when the target it lands on
            // differs, so the pair of the previous target is dropped and its
            // cached image, if any, lands stamped with the generation now in
            // force.
            self.updater
                .set_resolution(self.target.load(Ordering::SeqCst));
            let outcome = self.updater.poll_once();
            record_first_answer(self.answer, outcome, self.updater.has_posted());
            match outcome {
                PollOutcome::Updated | PollOutcome::Unchanged => {
                    debug!("cloud poll settled");
                    set_coarse(self.status, "the cloud map is ready");
                    self.backoff.reset();
                    return FetchEnds::Settled;
                }
                PollOutcome::Abandoned => {
                    give_up(self.abandoned);
                    return FetchEnds::LayerOff;
                }
                PollOutcome::Failed => {
                    let delay = self.backoff.fail();
                    set_coarse(
                        self.status,
                        &format!("the last poll failed; retrying in {}s", delay.as_secs()),
                    );
                    warn!(
                        retry_delay_secs = delay.as_secs(),
                        "cloud poll failed, retrying"
                    );
                    rest_while_enabled_or_pending(delay, self.enabled, self.pending);
                    // Give up if the engine went away while we slept.
                    if matches!(rx.try_recv(), Err(TryRecvError::Disconnected)) {
                        return FetchEnds::EngineGone;
                    }
                }
            }
        }
    }

    /// The first request shows whatever the target already has on disk
    /// before touching the network, so a session that starts with the layer
    /// switched off decodes nothing until it is on. A resolution switched
    /// before then posts its own entry.
    fn first_showing(&mut self, first: &mut bool) {
        if std::mem::take(first)
            && !self
                .updater
                .set_resolution(self.target.load(Ordering::SeqCst))
        {
            self.updater.post_cached();
        }
    }

    /// Apply the newest pending source switch, if there is one: rebuild the
    /// updater for the new source, raise the generation so the previous
    /// target's in-flight results are dropped, reset the first answer to the
    /// new target's, and show whatever the new target already has on disk.
    /// Rapid switches leave only the last pending source behind, which is the
    /// one this applies.
    fn apply_switch(&mut self) {
        let Some(source) = self
            .pending
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
        *self.updater = CloudUpdater::new(
            Arc::clone(&source),
            self.mailbox.clone(),
            self.notify.clone(),
            Arc::clone(self.generation),
            self.cache_dir.cloned(),
            self.target.load(Ordering::SeqCst),
        );
        self.wait_ms.store(
            u64::try_from(source.wait().as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Some(source);
        set_coarse(self.status, "the cloud source changed");
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.answer
            .store(FirstAnswer::Pending as u8, Ordering::SeqCst);
        // A cache hit reports the new target as available without the network.
        self.updater.post_cached();
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

/// How often a backoff looks at whether the layer is still switched on.
const REST_SLICE: Duration = Duration::from_millis(100);

/// Sleep out `delay`, or less as soon as `enabled` reads false or a source
/// switch is waiting to be applied: a switch is worth an attempt as soon as
/// the worker can make one, not after the full backoff.
fn rest_while_enabled_or_pending(
    delay: Duration,
    enabled: &AtomicBool,
    pending: &Mutex<Option<Arc<dyn CloudSource>>>,
) {
    let until = std::time::Instant::now() + delay;
    loop {
        let switch_pending = pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some();
        if !enabled.load(Ordering::SeqCst) || switch_pending {
            return;
        }
        let left = until.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return;
        }
        std::thread::sleep(left.min(REST_SLICE));
    }
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
    use image::ImageEncoder;
    use std::sync::atomic::AtomicU64;

    use super::*;
    use crate::assets::cloud_fetcher::no_notify;
    use crate::assets::cloud_source::{CloudImage, Download};

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

        fn fetches(&self) -> u64 {
            self.fetches.load(Ordering::SeqCst)
        }
    }

    impl CloudSource for ScriptedSource {
        fn fetch_if_changed(
            &self,
            _known_etag: Option<&str>,
        ) -> Result<Option<CloudImage>, String> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            let mut jpeg = Vec::new();
            image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 88)
                .write_image(&[128; 4], 2, 2, image::ExtendedColorType::L8)
                .expect("the fixture encodes");
            Ok(Some(CloudImage {
                bytes: Download::new(jpeg),
                etag: None,
                last_modified: None,
            }))
        }

        fn describe(&self) -> String {
            "scripted worker source".to_owned()
        }
    }

    /// Spawn a worker at one poll a day, so only the test's own requests move
    /// it, and hand back the worker and the mailbox it posts to.
    fn worker() -> (CloudWorker, TextureMailbox, Arc<AtomicU64>) {
        let mailbox = TextureMailbox::new(3);
        let generation = Arc::new(AtomicU64::new(0));
        let worker = spawn_cloud_worker(
            ScriptedSource::new(),
            mailbox.clone(),
            no_notify(),
            &generation,
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
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    const TIMEOUT: Duration = Duration::from_secs(5);

    #[test]
    fn a_switch_applies_between_attempts_and_bumps_the_generation() {
        let (worker, mailbox, generation) = worker();
        worker.request();
        wait_for_pair(&mailbox, "the first source's map");
        assert_eq!(generation.load(Ordering::SeqCst), 0);

        worker.set_source(ScriptedSource::new());
        assert_eq!(
            generation.load(Ordering::SeqCst),
            0,
            "the switch bumps when the worker applies it, not when it is handed over"
        );
        worker.request();
        wait_for_pair(&mailbox, "the new source's map");
        assert_eq!(generation.load(Ordering::SeqCst), 1, "the switch bumps");
        // The first answer belongs to the target in force again.
        assert_eq!(worker.first_answer(), FirstAnswer::Posted);
    }

    #[test]
    fn rapid_switches_keep_only_the_last_pending_source() {
        let (worker, mailbox, _generation) = worker();
        let first = ScriptedSource::new();
        worker.set_source(ScriptedSource::new());
        worker.set_source(ScriptedSource::new());
        worker.set_source(Arc::clone(&first) as Arc<dyn CloudSource>);
        worker.set_source(ScriptedSource::new());
        worker.request();
        wait_for_pair(&mailbox, "the last source's map");
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            first.fetches(),
            0,
            "a superseded pending switch must never fetch"
        );
    }
}
