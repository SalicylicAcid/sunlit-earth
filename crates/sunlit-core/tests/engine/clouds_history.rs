//! The cloud timeline: what a fixed instant wears, when the covering round is
//! fetched, and what a stop, a press and a policy change about it.
//!
//! These cases run their own engine each, because the source and the archive
//! directory are startup configuration. The source is a behavioral twin of the
//! native sources' shape: a live round that publishes hourly, a round list the
//! historical fetch consults, and a historical fetch that can park the way a
//! real compose request does.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use sunlit_core::assets::cloud_source::{CloudImage, CloudSource};
use sunlit_core::engine::clock::MockClock;
use sunlit_core::engine::{EngineCommand, EngineEvent};
use sunlit_core::params::SceneParams;
use sunlit_core::scene::scene_time::SceneTime;

use crate::harness::TIMEOUT;
use crate::test_support::ScratchDir;

/// One encoded map, shared by every answer the fixture gives.
fn fixture_jpeg() -> Vec<u8> {
    use image::ImageEncoder;
    let mut jpeg = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 88)
        .write_image(&[180; 16], 4, 4, image::ExtendedColorType::L8)
        .expect("the fixture encodes");
    jpeg
}

/// A source that publishes one hourly round, holds a set of historical hours,
/// and parks its historical fetches the way a compose request can.
struct RoundsSource {
    cancel: Mutex<Option<Arc<AtomicBool>>>,
    park: AtomicBool,
    live: Mutex<time::OffsetDateTime>,
    held: Mutex<HashSet<time::OffsetDateTime>>,
    polls: AtomicU64,
    backfills: AtomicU64,
}

impl RoundsSource {
    fn new(live: time::OffsetDateTime) -> Arc<Self> {
        Arc::new(Self {
            cancel: Mutex::new(None),
            park: AtomicBool::new(false),
            live: Mutex::new(live),
            held: Mutex::new(HashSet::new()),
            polls: AtomicU64::new(0),
            backfills: AtomicU64::new(0),
        })
    }

    fn holds(&self, round: time::OffsetDateTime) {
        self.held.lock().expect("the held set").insert(round);
    }

    fn polls(&self) -> u64 {
        self.polls.load(Ordering::SeqCst)
    }

    fn backfills(&self) -> u64 {
        self.backfills.load(Ordering::SeqCst)
    }
}

impl CloudSource for RoundsSource {
    fn publication(&self) -> Option<sunlit_core::assets::cloud_source::CloudPublication> {
        Some(sunlit_core::assets::cloud_source::CloudPublication {
            interval: Duration::from_hours(1),
            latest: Some(*self.live.lock().expect("the live round")),
        })
    }

    fn fetch_if_changed(&self, known_etag: Option<&str>) -> Result<Option<CloudImage>, String> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        let live = *self.live.lock().expect("the live round");
        let stamp = live.format(stamp_format()).expect("the stamp formats");
        if known_etag == Some(stamp.as_str()) {
            return Ok(None);
        }
        Ok(Some(CloudImage {
            bytes: download_of(fixture_jpeg()),
            etag: Some(stamp),
            last_modified: None,
            round: Some(live),
            raws: Vec::new(),
        }))
    }

    fn set_cancel(&self, flag: Arc<AtomicBool>) {
        *self.cancel.lock().expect("the stop slot") = Some(flag);
    }

    fn fetch_round_at(
        &self,
        round: time::OffsetDateTime,
        _resolution: u32,
    ) -> Result<Option<CloudImage>, String> {
        self.backfills.fetch_add(1, Ordering::SeqCst);
        // The slow compose request the tests steer: parked, it answers when
        // released, or unwinds when the user stop lands.
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
        if !self.held.lock().expect("the held set").contains(&round) {
            return Ok(None);
        }
        Ok(Some(CloudImage {
            bytes: download_of(fixture_jpeg()),
            etag: None,
            last_modified: None,
            round: Some(round),
            raws: Vec::new(),
        }))
    }

    fn describe(&self) -> String {
        "rounds fixture".to_owned()
    }
}

fn stamp_format() -> &'static [time::format_description::BorrowedFormatItem<'static>] {
    time::macros::format_description!("[year][month][day][hour][minute]")
}

fn download_of(bytes: Vec<u8>) -> sunlit_core::assets::cloud_source::Download {
    sunlit_core::assets::cloud_source::Download::new(bytes)
}

fn hourly(at: time::OffsetDateTime, hours: i64) -> time::OffsetDateTime {
    at + time::Duration::hours(hours)
}

/// The instant `at`'s scene, fixed.
fn fixed_params(at: time::OffsetDateTime) -> SceneParams {
    let mut params = SceneParams {
        cloud_opacity: 0.85,
        ..SceneParams::default()
    };
    params.time = SceneTime::fixed(at);
    params
}

/// Wait for a round line matching `want`, discarding everything else.
fn wait_for_round_line(
    harness: &crate::harness::Harness,
    want: impl Fn(&str) -> bool,
    what: &str,
) -> String {
    let deadline = std::time::Instant::now() + TIMEOUT;
    while let Ok(event) = harness.events.recv_deadline(deadline) {
        if let EngineEvent::CloudRound { text } = event
            && want(&text)
        {
            return text;
        }
    }
    panic!("{what}: no round line within {TIMEOUT:?}");
}

/// Start an engine of the group's own: the rounds fixture, an archive on
/// disk, unlimited retention, the layer on.
fn history_harness(source: &Arc<RoundsSource>, scratch: &ScratchDir) -> crate::harness::Harness {
    crate::harness::Harness::start(|config| {
        config.cloud = Some(Arc::clone(source) as Arc<dyn CloudSource>);
        config.cloud_poll_interval = Duration::from_hours(1);
        config.cloud_history_hours = None;
        config.cache_dir = Some(scratch.as_ref().to_path_buf());
    })
}

const LIVE: time::OffsetDateTime = time::macros::datetime!(2026-11-22 03:00 UTC);

/// The scene lands on an archived hour: its round is worn at once, from the
/// archive, with no upstream request.
#[test]
fn a_landing_on_the_live_hour_wears_it_without_a_fetch() {
    let scratch = ScratchDir::new("cloud_history_serves");
    let source = RoundsSource::new(LIVE);
    source.holds(hourly(LIVE, -1));
    let harness = history_harness(&source, &scratch);
    harness.settle();
    let polls = source.polls();

    // The live poll lands 03:00; the scene lands inside its hour.
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, 0) + time::Duration::minutes(30),
        ))));
    let line = wait_for_round_line(
        &harness,
        |text| text.contains("Historical round"),
        "the covered landing",
    );
    assert!(line.contains("2026-11-22 03:00"), "{line}");
    assert_eq!(
        source.backfills(),
        0,
        "the archive answers; the upstream is never asked"
    );
    assert!(source.polls() >= polls, "the live schedule is untouched");
}

/// At live time a round an hour or more behind the clock says its age, so a
/// stalled upstream does not read as current imagery.
#[test]
fn a_live_round_states_how_far_behind_the_clock_it_is() {
    let scratch = ScratchDir::new("cloud_history_live_lag");
    let source = RoundsSource::new(LIVE);
    let clock = Arc::new(MockClock::new(LIVE + time::Duration::hours(13)));
    let harness = crate::harness::Harness::start(|config| {
        config.cloud = Some(Arc::clone(&source) as Arc<dyn CloudSource>);
        config.cloud_poll_interval = Duration::from_hours(1);
        config.cache_dir = Some(scratch.as_ref().to_path_buf());
        config.clock = Arc::clone(&clock) as Arc<dyn sunlit_core::engine::clock::Clock>;
        config.params.time = SceneTime::Live;
    });
    let line = wait_for_round_line(
        &harness,
        |text| text.contains("13 h old"),
        "the live round's age",
    );
    assert!(line.contains("2026-11-22 03:00"), "{line}");
}

/// The scene lands on an hour nobody holds: about a second of stillness, then
/// the covering round is fetched, pinned, and worn; the line says so.
#[test]
fn a_landing_on_an_unheld_hour_fetches_after_the_settle_and_pins_it() {
    let scratch = ScratchDir::new("cloud_history_fetches");
    let source = RoundsSource::new(LIVE);
    source.holds(hourly(LIVE, -1));
    let harness = history_harness(&source, &scratch);
    harness.settle();

    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -1) + time::Duration::minutes(45),
        ))));
    let line = wait_for_round_line(
        &harness,
        |text| text.contains("Fetching round 2026-11-22 02:00"),
        "the settle",
    );
    assert!(line.contains("Fetching round"), "{line}");
    wait_for_round_line(
        &harness,
        |text| text.contains("Historical round is 2026-11-22 02:00 UTC"),
        "the fetched round",
    );
    assert_eq!(source.backfills(), 1, "one fetch for one landing");

    // The fetched round is pinned: retention never sweeps a visited hour.
    let manifest = scratch
        .as_ref()
        .join("clouds_history")
        .join(entry_key(&scratch));
    let text = std::fs::read_to_string(manifest.join("manifest.toml")).expect("the manifest");
    assert!(text.contains("20261122T020000"), "{text}");
    assert!(text.contains("pinned = true"), "{text}");
}

/// The manifest entry the fixture's source prepares under, found by listing
/// the archive root: the tests do not hard-code the hash.
fn entry_key(scratch: &ScratchDir) -> String {
    let root = scratch.as_ref().join("clouds_history");
    let mut entries: Vec<String> = std::fs::read_dir(&root)
        .expect("the archive root")
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(entries.len(), 1, "one entry: {entries:?}");
    entries.sort();
    entries.remove(0)
}

/// A drag that lands elsewhere while a fetch is parked supersedes it: the new
/// round is the one fetched and worn, and the abandoned one commits nothing.
#[test]
fn a_new_landing_supersedes_the_fetch_in_flight() {
    let scratch = ScratchDir::new("cloud_history_supersedes");
    let source = RoundsSource::new(LIVE);
    source.holds(hourly(LIVE, -1));
    source.holds(hourly(LIVE, -2));
    let harness = history_harness(&source, &scratch);
    harness.settle();

    source.park.store(true, Ordering::SeqCst);
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -1) + time::Duration::minutes(45),
        ))));
    wait_for_round_line(
        &harness,
        |text| text.contains("Fetching round 2026-11-22 02:00"),
        "the first fetch",
    );
    let deadline = std::time::Instant::now() + TIMEOUT;
    while source.backfills() < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the first fetch never started"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    // The new landing takes over inside the window the first fetch holds.
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -2) + time::Duration::minutes(45),
        ))));
    source.park.store(false, Ordering::SeqCst);
    wait_for_round_line(
        &harness,
        |text| text.contains("Historical round is 2026-11-22 01:00 UTC"),
        "the superseding fetch",
    );
    assert!(
        source.backfills() >= 2,
        "the second round was fetched: {}",
        source.backfills()
    );
}

/// A stop stands the landing down: no retry, the line says so, and the next
/// landing fetches afresh.
#[test]
fn a_stop_stands_the_landing_down_until_the_next_one() {
    let scratch = ScratchDir::new("cloud_history_stops");
    let source = RoundsSource::new(LIVE);
    source.holds(hourly(LIVE, -1));
    source.holds(hourly(LIVE, -2));
    let harness = history_harness(&source, &scratch);
    harness.settle();

    source.park.store(true, Ordering::SeqCst);
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -1) + time::Duration::minutes(45),
        ))));
    let deadline = std::time::Instant::now() + TIMEOUT;
    while source.backfills() < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the fetch never started"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    harness.engine.send(EngineCommand::AbortClouds);
    let line = wait_for_round_line(&harness, |text| text.contains("stopped"), "the stop");
    assert!(line.contains("stopped"), "{line}");

    // No retry follows: the parked fetch stays parked and the backfill count
    // stays where the stop left it, for long enough that a retry would have
    // shown.
    source.park.store(false, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(source.backfills(), 1, "a stop is answered, not retried");

    // A new landing is a new ask.
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -2) + time::Duration::minutes(45),
        ))));
    wait_for_round_line(
        &harness,
        |text| text.contains("Historical round is 2026-11-22 01:00 UTC"),
        "the next landing",
    );
}

/// A round the upstream does not hold is learned once: the line says so and
/// no retry burns the upstream.
#[test]
fn an_upstream_without_the_round_says_so_and_does_not_retry() {
    let scratch = ScratchDir::new("cloud_history_unheld");
    let source = RoundsSource::new(LIVE);
    source.holds(hourly(LIVE, -1));
    let harness = history_harness(&source, &scratch);
    harness.settle();

    // The scene lands inside the held hour, which the archive serves.
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -1) + time::Duration::minutes(45),
        ))));
    wait_for_round_line(
        &harness,
        |text| text.contains("Historical round is 2026-11-22 02:00 UTC"),
        "the held round is served",
    );
    assert_eq!(
        source.backfills(),
        1,
        "the held hour was not archived yet, so the fetch fetched it"
    );

    // A later hour the upstream does not hold: one ask, then the line, then
    // quiet.
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -5) + time::Duration::minutes(45),
        ))));
    let line = wait_for_round_line(
        &harness,
        |text| text.contains("does not hold"),
        "the unheld round",
    );
    assert!(line.contains("2026-11-21 22:00"), "{line}");
    let asked = source.backfills();
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        source.backfills(),
        asked,
        "an unheld round is not asked again"
    );
}

/// The refresh-only policy fetches only when the press asks.
#[test]
fn the_refresh_only_policy_fetches_only_on_a_press() {
    let scratch = ScratchDir::new("cloud_history_refresh_only");
    let source = RoundsSource::new(LIVE);
    source.holds(hourly(LIVE, -1));
    let harness = crate::harness::Harness::start(|config| {
        config.cloud = Some(Arc::clone(&source) as Arc<dyn CloudSource>);
        config.cloud_poll_interval = Duration::from_hours(1);
        config.cloud_history_hours = None;
        config.cache_dir = Some(scratch.as_ref().to_path_buf());
        config.cloud_backfill = sunlit_core::config::CloudBackfillPolicy::RefreshOnly;
    });
    harness.settle();

    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -1) + time::Duration::minutes(45),
        ))));
    wait_for_round_line(
        &harness,
        |text| text.contains("Press Fetch for round"),
        "the policy holds",
    );
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        source.backfills(),
        0,
        "the policy fetches nothing on its own"
    );

    harness.engine.send(EngineCommand::RefreshClouds);
    wait_for_round_line(
        &harness,
        |text| text.contains("Historical round is 2026-11-22 02:00 UTC"),
        "the press fetches",
    );
}

/// The never policy never fetches, and says the fetching is off.
#[test]
fn the_never_policy_wears_nothing_and_never_asks() {
    let scratch = ScratchDir::new("cloud_history_never");
    let source = RoundsSource::new(LIVE);
    let harness = crate::harness::Harness::start(|config| {
        config.cloud = Some(Arc::clone(&source) as Arc<dyn CloudSource>);
        config.cloud_poll_interval = Duration::from_hours(1);
        config.cloud_history_hours = None;
        config.cache_dir = Some(scratch.as_ref().to_path_buf());
        config.cloud_backfill = sunlit_core::config::CloudBackfillPolicy::Never;
    });
    harness.settle();

    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, -3) + time::Duration::minutes(45),
        ))));
    let line = wait_for_round_line(
        &harness,
        |text| text.contains("Fetching is off"),
        "the policy says so",
    );
    assert!(line.contains("2026-11-22 00:00"), "{line}");
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(source.backfills(), 0, "the never policy never asks");
}

/// A round newer than the upstream's publication edge has not been
/// published YET: the line says so, the round is not stood down for good,
/// and once the edge moves past it the round is fetched and worn.
#[test]
fn a_round_ahead_of_the_publication_edge_is_not_yet_and_lands_when_it_publishes() {
    let scratch = ScratchDir::new("cloud_history_not_yet");
    let source = RoundsSource::new(LIVE);
    let clock = Arc::new(MockClock::new(
        LIVE + time::Duration::hours(1) + time::Duration::minutes(45),
    ));
    let harness = crate::harness::Harness::start(|config| {
        config.cloud = Some(Arc::clone(&source) as Arc<dyn CloudSource>);
        config.cloud_poll_interval = Duration::from_hours(1);
        config.cloud_history_hours = None;
        config.cache_dir = Some(scratch.as_ref().to_path_buf());
        config.clock = Arc::clone(&clock) as Arc<dyn sunlit_core::engine::clock::Clock>;
    });
    harness.settle();

    // The scene lands on 04:45 — the 04:00 round is an hour past the edge
    // (03:00), which is exactly where the upstream's lag puts a recent hour.
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, 1) + time::Duration::minutes(45),
        ))));
    // The drag's settle is measured on the clock, and the mock clock only
    // moves when the test moves it: keep it moving so the settle can expire
    // and the ask go out.
    let line = wait_for_round_line(
        &harness,
        |text| text.contains("is not published yet"),
        "the lagging hour",
    );
    assert!(line.contains("2026-11-22 04:00"), "{line}");
    let deadline = std::time::Instant::now() + TIMEOUT;
    while source.backfills() < 1 {
        harness.advance(&clock, Duration::from_millis(500));
        assert!(
            std::time::Instant::now() < deadline,
            "the lagging hour was never asked for"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        source.backfills(),
        1,
        "a not-yet round is not asked again until the retry is due"
    );

    // The edge moves past 04:00 — the upstream published it — and the next
    // landing on the same hour fetches and wears it, instead of the old
    // "not held" standing for the rest of the session.
    source.holds(hourly(LIVE, 1));
    *source.live.lock().expect("the live round") = hourly(LIVE, 4);
    harness.engine.send(EngineCommand::RefreshClouds);
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, 1) + time::Duration::minutes(45),
        ))));
    wait_for_round_line(
        &harness,
        |text| text.contains("Historical round is 2026-11-22 04:00 UTC"),
        "the published hour",
    );
}

/// A future hour is not asked for: the upstream has not published it yet.
#[test]
fn a_future_hour_is_not_asked_for() {
    let scratch = ScratchDir::new("cloud_history_future");
    let source = RoundsSource::new(LIVE);
    let harness = crate::harness::Harness::start(|config| {
        config.cloud = Some(Arc::clone(&source) as Arc<dyn CloudSource>);
        config.cloud_poll_interval = Duration::from_hours(1);
        config.cloud_history_hours = None;
        config.cache_dir = Some(scratch.as_ref().to_path_buf());
        config.clock = Arc::new(MockClock::new(LIVE + time::Duration::minutes(30)));
    });
    harness.settle();

    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            hourly(LIVE, 5) + time::Duration::minutes(45),
        ))));
    let line = wait_for_round_line(
        &harness,
        |text| text.contains("is not published yet"),
        "the future hour",
    );
    assert!(line.contains("2026-11-22 08:00"), "{line}");
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(source.backfills(), 0, "the future is never fetched");
}

/// The round display the line uses, e.g. `2026-11-22 03:00`.
fn round_line(round: time::OffsetDateTime) -> String {
    format!(
        "Historical round is {}-{:02}-{:02} {:02}:00 UTC.",
        round.year(),
        u8::from(round.month()),
        round.day(),
        round.hour()
    )
}

/// Wait until the GPU cache holds `round`.
fn wait_for_cached(harness: &crate::harness::Harness, round: time::OffsetDateTime) {
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let cached = harness
            .engine
            .cloud_cache_rounds()
            .expect("the engine answers");
        if cached.contains(&Some(round)) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the cache never held {round}: {cached:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The cache holds the covering round and its neighbours, and a neighbour
/// the drag pushed out is warmed from the archive before the time bar
/// returns to it — so the return costs no upload gap and no upstream ask.
#[test]
fn a_lost_neighbour_is_prefetched_from_the_archive() {
    let scratch = ScratchDir::new("cloud_history_prefetch");
    let source = RoundsSource::new(LIVE);
    for hours in [-3, -2, -1] {
        source.holds(hourly(LIVE, hours));
    }
    let harness = history_harness(&source, &scratch);
    harness.settle();
    wait_for_cached(&harness, LIVE);

    // Visit 00:00, 01:00 and 02:00. Three entries is the cache's capacity,
    // so the third landing pushes the live round (03:00) out.
    for hours in [-3, -2, -1] {
        let round = hourly(LIVE, hours);
        harness
            .engine
            .send(EngineCommand::UpdateParams(Box::new(fixed_params(
                round + time::Duration::minutes(45),
            ))));
        wait_for_round_line(
            &harness,
            move |text| text.contains(&round_line(round)),
            "the historical landing",
        );
    }

    // At 02:00 the archive still holds 03:00; the engine warms the cache
    // with it, and the warm never counts as an upstream ask.
    let asks = source.backfills();
    wait_for_cached(&harness, LIVE);
    assert_eq!(source.backfills(), asks, "a warm stays off the upstream");

    // The return to 03:00 is a cache hit: nothing is fetched for it.
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            LIVE + time::Duration::minutes(45),
        ))));
    wait_for_round_line(
        &harness,
        move |text| text.contains(&round_line(LIVE)),
        "the prefetched landing",
    );
    assert_eq!(
        source.backfills(),
        asks,
        "the cached round is not asked for again"
    );
}

/// A bar that has been quiet for longer than the browse window keeps only
/// the round it wears; the neighbours a move warmed are evicted.
#[test]
fn a_quiet_bar_trims_the_cache_to_the_worn_round() {
    let scratch = ScratchDir::new("cloud_history_idle_cache");
    let source = RoundsSource::new(LIVE);
    source.holds(hourly(LIVE, -1));
    let clock = Arc::new(MockClock::new(LIVE + time::Duration::hours(2)));
    let harness = crate::harness::Harness::start(|config| {
        config.cloud = Some(Arc::clone(&source) as Arc<dyn CloudSource>);
        config.cloud_poll_interval = Duration::from_hours(1);
        config.cache_dir = Some(scratch.as_ref().to_path_buf());
        config.clock = Arc::clone(&clock) as Arc<dyn sunlit_core::engine::clock::Clock>;
    });
    harness.settle();
    wait_for_cached(&harness, LIVE);

    let historical = hourly(LIVE, -1);
    harness
        .engine
        .send(EngineCommand::UpdateParams(Box::new(fixed_params(
            historical + time::Duration::minutes(45),
        ))));
    // The settle runs on the injected clock, so it only expires as the test
    // moves it.
    let deadline = std::time::Instant::now() + TIMEOUT;
    while source.backfills() < 1 {
        harness.advance(&clock, Duration::from_millis(500));
        assert!(
            std::time::Instant::now() < deadline,
            "the historical fetch never started"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    wait_for_round_line(
        &harness,
        move |text| text.contains(&round_line(historical)),
        "the historical landing",
    );

    // The worn round's frame and the archived live neighbour are both on
    // the GPU: browsing keeps the neighbour.
    wait_for_cached(&harness, historical);
    wait_for_cached(&harness, LIVE);
    let cached = harness
        .engine
        .cloud_cache_rounds()
        .expect("the engine answers");
    assert!(cached.len() >= 2, "browsing kept a neighbour: {cached:?}");

    // Past the browse window the next tick trims to the worn round.
    harness.advance(&clock, Duration::from_secs(21));
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let cached = harness
            .engine
            .cloud_cache_rounds()
            .expect("the engine answers");
        if cached == vec![Some(historical)] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the cache never trimmed to the worn round: {cached:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A date-track move warms the archived rounds one day either side, not the
/// rounds either side of the hour.
#[test]
fn a_date_move_warms_the_days_either_side() {
    let scratch = ScratchDir::new("cloud_history_date_warm");
    let source = RoundsSource::new(LIVE);
    let previous_day = hourly(LIVE, -24);
    let two_days_back = hourly(LIVE, -48);
    for round in [
        previous_day,
        two_days_back,
        previous_day + time::Duration::hours(3),
    ] {
        source.holds(round);
    }
    let harness = history_harness(&source, &scratch);
    harness.settle();
    wait_for_cached(&harness, LIVE);

    // Visit three fixed instants so the cache (three pairs) pushes the live
    // round out.
    for at in [
        two_days_back + time::Duration::minutes(45),
        previous_day + time::Duration::minutes(45),
        previous_day + time::Duration::hours(3) + time::Duration::minutes(45),
        previous_day + time::Duration::minutes(45),
    ] {
        harness
            .engine
            .send(EngineCommand::UpdateParams(Box::new(fixed_params(at))));
        let round = at.replace_minute(0).expect("the hour");
        wait_for_round_line(
            &harness,
            move |text| text.contains(&round_line(round)),
            "the date visit",
        );
    }

    // The date track's move warms the day on the other side, which the drag
    // had pushed out of the cache; the day before it is still cached.
    let asks = source.backfills();
    harness
        .engine
        .send(EngineCommand::CloudBrowse { date: true });
    wait_for_cached(&harness, LIVE);
    assert_eq!(
        source.backfills(),
        asks,
        "a day warm stays off the upstream"
    );
}

/// A fixed time at startup fetches its covering round with the cache at one
/// pair; that pair must be kept, not evicted before it can be worn and then
/// fetched again in a loop.
#[test]
fn a_startup_round_is_not_served_in_a_loop() {
    let scratch = ScratchDir::new("cloud_history_startup_cache");
    let source = RoundsSource::new(LIVE);
    let round = hourly(LIVE, -1);
    source.holds(round);
    let harness = crate::harness::Harness::start(|config| {
        config.cloud = Some(Arc::clone(&source) as Arc<dyn CloudSource>);
        config.cloud_poll_interval = Duration::from_hours(1);
        config.cache_dir = Some(scratch.as_ref().to_path_buf());
        config.params.time = SceneTime::fixed(round + time::Duration::minutes(45));
    });
    harness.settle();
    wait_for_round_line(
        &harness,
        move |text| text.contains(&round_line(round)),
        "the startup covering round",
    );
    wait_for_cached(&harness, round);

    // Nothing moves the bar, so the cache stays at one pair. A loop would
    // toggle the busy flag on every serve; give it two seconds to show.
    std::thread::sleep(Duration::from_millis(500));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut busy_ups = 0;
    while std::time::Instant::now() < deadline {
        if let Ok(event) = harness.events.recv_timeout(Duration::from_millis(50))
            && matches!(event, EngineEvent::CloudBusy(true))
        {
            busy_ups += 1;
        }
    }
    assert_eq!(busy_ups, 0, "the covering round is served in a loop");
    assert!(
        harness
            .engine
            .cloud_cache_rounds()
            .expect("the engine answers")
            .contains(&Some(round)),
        "the worn pair is still cached"
    );
}
