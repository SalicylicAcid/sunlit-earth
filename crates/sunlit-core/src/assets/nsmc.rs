//! The native NSMC cloud source: the `FengYun` satellites' geostationary
//! infrared mosaic over the National Satellite Meteorological Center's
//! WMS-style interface.
//!
//! The service ([www.nsmc.org.cn/nsmc/cn/image/wms.html], the documented
//! "FYWMS") serves `GEOS_IRX`, the global composite of the geostationary
//! fleet's 10.8 um infrared channels, one round an hour. A round is one
//! `GetMap` request for the full globe at 4096x2048, and the response is an
//! RGBA PNG whose **alpha channel carries the cloud intensity**: 0 is clear
//! sky or missing polar observations; the geostationary ring cannot see the
//! poles. A value of 255 marks the deepest convective
//! cloud. The RGB channels only echo that as a binary companion — white
//! wherever any coverage is present, black elsewhere — so the derivation
//! reads the alpha alone. Measured service quirks the constants below rest
//! on: the JPEG output channel returns a fixed blank image for full-globe
//! requests, so the request is always PNG, and full-globe renders beyond
//! about 4K come back blank too, so the fetch is fixed at 4096x2048 and the
//! larger texture variants are produced by resizing in the derivation.
//!
//! A round is gated by the service's round list (`data.json`'s sibling for
//! this product, documented on the same page): the list names every round
//! published in the last day, and its latest entry is what a poll compares
//! the stored raw mosaic against. That replaces the EUMETSAT source's
//! freshness window — the list is the authority, so nothing is age-gated —
//! and it means every poll costs one small JSON request even when the
//! imagery is current. Missing polar caps stay empty, with a five-degree
//! inward fade that never moves observed rows.
//!
//! The imagery requires attribution to the National Satellite Meteorological
//! Center (CMA); see `assets/ATTRIBUTION.md`.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use tracing::debug;
use tracing::info;
use tracing::warn;

use super::cloud_fetcher::cloud_variant;
use super::cloud_source::CloudImage;
use super::cloud_source::CloudSource;
use super::cloud_source::CloudTarget;
use super::cloud_source::CloudWait;
use super::cloud_source::Download;
use super::eumetsat::FetchError;
use super::eumetsat::GrayMap;
use super::eumetsat::encode_jpeg;
use super::eumetsat::gaussian_blur;
use super::eumetsat::mib;
use super::eumetsat::retry_after_header;
use super::eumetsat::sha256_hex;
use super::eumetsat::speed_text;
use super::eumetsat::write_committed;
use crate::config::DEFAULT_TEXTURE_RESOLUTION;
use crate::config::clouds::NsmcSettings;

/// The documented WMS-style `GetMap` endpoint, unauthenticated.
const COMPOSE_URL: &str = "https://data.nsmc.org.cn/NSMCAPI/v1/nsmc/image/wms/compose";
/// The round list endpoint: every `GEOS_IRX` round published in the last day,
/// as the service's own viewer requests it.
const TIMES_URL: &str = "https://data.nsmc.org.cn/nsmcapi/v1/nsmc/image/animation/datatime/mongodb";
/// The product code the round list is asked for: the global geostationary
/// 10.8 um infrared mosaic at its native 4 km grid.
const DATA_CODE: &str = "GEO_MULT_GBAL_L2_GGM_IRX_GLL_YYYYMMDD_HHmm_4000M.PNG";
/// The global infrared mosaic layer, the one layer of the three the service
/// declares that carries a continuous cloud intensity.
const LAYER: &str = "GEOS_IRX";
/// The interface version the service documents for the mosaic's `GetMap`.
const WMS_VERSION: &str = "1.1.0";
/// The fixed fetch size. The native product is its 4 km grid, full-globe
/// renders beyond this come back blank (measured), and the larger texture
/// variants are produced by resizing here, so the raw mosaic is one file
/// every variant derives from.
const RAW_WIDTH: u32 = 4096;
const RAW_HEIGHT: u32 = 2048;
/// One request's timeout, as the EUMETSAT source's: the service renders the
/// full globe on the fly, which measured about half a minute for the 4K PNG.
const REQUEST_TIMEOUT: Duration = Duration::from_mins(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// The whole round's budget: the round list, the mosaic, and their retries.
const ROUND_BUDGET: Duration = Duration::from_mins(10);
/// Attempts per request inside the budget.
const MAX_ATTEMPTS: u32 = 3;
/// Backoff between attempts; tests shorten these to zero.
const RETRY_DELAYS: [Duration; (MAX_ATTEMPTS - 1) as usize] =
    [Duration::from_secs(10), Duration::from_secs(20)];
/// The mosaic's body cap. A 4096x2048 RGBA PNG measured about 5 MB.
const MAX_BODY_BYTES: u64 = 32 * 1024 * 1024;
/// The round list's body cap; a day of hourly entries measured under 2 KB.
const LIST_BODY_BYTES: u64 = 1024 * 1024;
/// How often a download refreshes its progress line.
const DOWNLOAD_REPORT_EVERY: Duration = Duration::from_millis(500);
/// How often a retry nap looks at the user stop.
const REST_SLICE: Duration = Duration::from_millis(100);
/// The processing algorithm's version, stored in every derived map's
/// metadata. Bump when the math changes, so stale derivations are
/// recomputed rather than served.
const ALGORITHM_VERSION: u32 = 3;
/// The round-list range a historical fetch asks for: about 416 days of
/// hourly entries, the widest window the service answers.
const HISTORY_HOURS_RANGE: u32 = 10000;
/// How long a held wide list is trusted before a miss refreshes it. The
/// upstream publishes with a lag of hours, and a list taken at session
/// start misses every hour that publishes after it — exactly the recent
/// rounds a user drags to.
const LIST_STALE_REFRESH: Duration = Duration::from_secs(120);

/// The `GetMap` request for one round, at the fixed raw size. The service's
/// own parameter spelling is lowercase, and the bbox is longitude,latitude
/// order, both as the service's documentation shows.
fn compose_url(datetime: &str) -> String {
    format!(
        "{COMPOSE_URL}?layers={LAYER}&datetime={datetime}&request=GetMap\
         &bbox=-180,-90,180,90&width={RAW_WIDTH}&height={RAW_HEIGHT}\
         &version={WMS_VERSION}&format=png"
    )
}

/// The round list request, as the service's viewer builds it.
fn times_url() -> String {
    format!("{TIMES_URL}?dataCode={DATA_CODE}&hourRange=24")
}

/// One entry of the round list, as the parse pairs them: the round's date
/// and time-of-day, in the service's `YYYYMMDD` and `hhmmss` spellings.
type Round = (String, String);

/// The quoted value that follows `"field":"`, with the offset past its
/// closing quote.
fn quoted_value(rest: &str, field: &str) -> Option<(String, usize)> {
    let needle = format!("\"{field}\":\"");
    let start = rest.find(&needle)? + needle.len();
    let end = rest[start..].find('"')?;
    Some((rest[start..start + end].to_owned(), start + end))
}

/// Whether a scanned date and time spell a round: an eight-digit date and an
/// hour-minute prefix of digits, so a partially filled entry cannot win.
fn usable_round(date: &str, time: &str) -> bool {
    date.len() == 8
        && time.len() >= 4
        && date.bytes().all(|b| b.is_ascii_digit())
        && time.bytes().take(4).all(|b| b.is_ascii_digit())
}

/// The latest round's `datetime` value, `YYYYMMDDhhmm`, from a round-list
/// body.
///
/// The list is flat, machine-generated JSON whose entries carry `dataDate`
/// and `dataTime` as quoted digit strings, `dataDate` first. It is scanned
/// for those two field names rather than deserialized, and on purpose:
/// `serde_json`'s `impl PartialEq<Value> for i32` leaks into every crate
/// that links it, and turns the test suites' `assert_eq!(rates, [])`
/// comparisons into ambiguous inference. A date without its own time — the
/// next entry's date is what bounds the search — a non-digit value, or an
/// entry whose fields arrived in the other order, is skipped rather than
/// guessed at; values never carry escapes.
fn parse_latest(body: &[u8]) -> Result<String, String> {
    let body = String::from_utf8_lossy(body);
    let mut latest: Option<Round> = None;
    let mut rest = body.as_ref();
    while let Some((date, after)) = quoted_value(rest, "dataDate") {
        let tail = &rest[after..];
        let time = tail.find("\"dataTime\":\"").and_then(|at| {
            let (value, _) = quoted_value(&tail[at..], "dataTime")?;
            (tail.find("\"dataDate\":\"").is_none_or(|next| next > at)).then_some(value)
        });
        if let Some(time) = time
            && usable_round(&date, &time)
        {
            let round = format!("{date}{}", &time[..4]);
            if latest
                .as_ref()
                .is_none_or(|(best, _)| round.as_str() > best.as_str())
            {
                latest = Some((round, time));
            }
        }
        rest = tail;
    }
    latest
        .map(|(round, _)| round)
        .ok_or_else(|| "the round list holds no usable rounds".to_owned())
}

/// Every usable round a round-list body names, as instants, sorted.
///
/// The same flat scan [`parse_latest`] runs, collecting instead of reducing.
/// An entry with its fields in the other order, or a value that is not the
/// digits the product promises, is skipped rather than guessed at.
fn parse_all_rounds(body: &[u8]) -> Vec<time::OffsetDateTime> {
    let body = String::from_utf8_lossy(body);
    let mut rounds = Vec::new();
    let mut rest = body.as_ref();
    while let Some((date, after)) = quoted_value(rest, "dataDate") {
        let tail = &rest[after..];
        let time = tail.find("\"dataTime\":\"").and_then(|at| {
            let (value, _) = quoted_value(&tail[at..], "dataTime")?;
            (tail.find("\"dataDate\":\"").is_none_or(|next| next > at)).then_some(value)
        });
        if let Some(time) = time
            && usable_round(&date, &time)
            && let Some(round) = parse_round_instant(&format!("{date}{}", &time[..4]))
        {
            rounds.push(round);
        }
        rest = tail;
    }
    rounds.sort();
    rounds.dedup();
    rounds
}

/// One `YYYYMMDDhhmm` round stamp, as an instant.
fn parse_round_instant(stamp: &str) -> Option<time::OffsetDateTime> {
    if stamp.len() != 12 {
        return None;
    }
    let at = |range: std::ops::Range<usize>| stamp.get(range);
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute)) =
        (at(0..4), at(4..6), at(6..8), at(8..10), at(10..12))
    else {
        return None;
    };
    time::Date::from_calendar_date(
        year.parse().ok()?,
        time::Month::try_from(month.parse::<u8>().ok()?).ok()?,
        day.parse().ok()?,
    )
    .ok()?
    .with_hms(hour.parse().ok()?, minute.parse().ok()?, 0)
    .ok()
    .map(time::PrimitiveDateTime::assume_utc)
}

/// The `YYYYMMDDhhmm` stamp a compose request names a round by, in UTC —
/// the zone the upstream names its rounds in, whatever offset the caller's
/// scene instant carries.
fn round_stamp(round: time::OffsetDateTime) -> String {
    round
        .to_offset(time::OffsetDateTime::UNIX_EPOCH.offset())
        .format(time::macros::format_description!(
            "[year][month][day][hour][minute]"
        ))
        .expect("the round stamp formats")
}

/// The stored raw mosaic, its bytes and what its manifest named. The
/// manifest also records the fetch time, which nothing gates on — the round
/// comparison is the gate — so it is not carried here.
struct RawMosaic {
    bytes: Vec<u8>,
    sha256: String,
    /// The `YYYYMMDDhhmm` round the mosaic carries.
    round: String,
}

/// The raw mosaic's manifest, the commit point of a stored round: written
/// last, after the PNG is on disk and verified.
#[derive(serde::Serialize, serde::Deserialize)]
struct RawManifest {
    version: u32,
    algorithm: u32,
    sha256: String,
    round: String,
    fetched_epoch_secs: u64,
}

/// The derived map's metadata, the commit point of a derivation: only a
/// metadata row that matches the raw hash, the parameters and the algorithm
/// version lets the derived file be reused.
#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
struct DerivedMeta {
    version: u32,
    algorithm: u32,
    floor: u8,
    ceiling: u8,
    gamma: f64,
    blur: f64,
    cap: f32,
    sha256: String,
}

/// The native NSMC source: one round list request and, when a new round has
/// published, one full-globe mosaic request, then the derivation into the
/// grayscale map the cloud pipeline already decodes.
///
/// There are no conditional requests: the round list is the change detector,
/// and `known_etag` is ignored on purpose, as the EUMETSAT source's is.
pub struct NsmcCloudSource {
    settings: NsmcSettings,
    /// The prepared variant's size in pixels. `prepare` and the fetch both
    /// run on the engine's single cloud worker; the lock is for construction
    /// and for tests.
    size: Mutex<(u32, u32)>,
    /// Where the raw mosaic and the derived maps are cached, the fetcher's
    /// own cache directory. `None` caches nothing and every round hits the
    /// network.
    cache_dir: Option<std::path::PathBuf>,
    /// Backoff between transient attempts; the shipped values are
    /// [`RETRY_DELAYS`] and tests shorten them.
    retry_delays: [Duration; (MAX_ATTEMPTS - 1) as usize],
    /// What the round is doing right now, for the UI's source status: set at
    /// the pipeline's visible phases and cleared once the map is ready.
    phase: Mutex<String>,
    /// The download's progress line, written as the body moves and composed
    /// into the status when it is read.
    progress: Mutex<Option<String>>,
    /// The round the last settled fetch left on screen, `YYYYMMDDhhmm`, for
    /// the idle status line. Only a round whose map is in hand is written
    /// here, so a failed attempt leaves the previous one in place.
    round: Mutex<Option<String>>,
    /// The wide round list a historical fetch consults, held for the session.
    /// `None` is "unknown", which the next fetchable attempt fills; a held
    /// list is never retired by a miss.
    known: Mutex<Option<Vec<time::OffsetDateTime>>>,
    /// When the last wide-list attempt was made, so an unusable answer holds
    /// the next one off instead of hammering the service.
    list_fetched_at: Mutex<Option<Instant>>,
    /// The user stop the fetch worker hands over: set while a running round
    /// is wanted gone, checked between rounds, inside every chunk read and
    /// between the derivation's stages. Behind a lock so
    /// [`CloudSource::set_cancel`] can replace it, and never `None`: before
    /// a worker attaches one, a private flag nobody sets reads false.
    cancel: Mutex<Arc<AtomicBool>>,
    agent: ureq::Agent,
}

impl NsmcCloudSource {
    pub fn new(settings: NsmcSettings, cache_dir: Option<std::path::PathBuf>) -> Self {
        let user_agent = format!("sunlit.earth/{}", env!("CARGO_PKG_VERSION"));
        let agent = ureq::Agent::config_builder()
            .user_agent(&user_agent)
            // Errors are classified here, from the status and its headers.
            .http_status_as_error(false)
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_per_call(Some(REQUEST_TIMEOUT))
            // The service sits behind a national network; a user running a
            // local proxy is the standard way around that. The standard
            // variables carry it, and NO_PROXY is honored with it.
            .proxy(ureq::Proxy::try_from_env())
            .build()
            .new_agent();
        Self {
            settings,
            size: Mutex::new(cloud_variant(DEFAULT_TEXTURE_RESOLUTION)),
            cache_dir,
            retry_delays: RETRY_DELAYS,
            phase: Mutex::new(String::new()),
            progress: Mutex::new(None),
            round: Mutex::new(None),
            known: Mutex::new(None),
            list_fetched_at: Mutex::new(None),
            cancel: Mutex::new(Arc::new(AtomicBool::new(false))),
            agent,
        }
    }

    // The startup cache must not post imagery derived with a superseded coordinate mapping.
    fn identity(width: u32, height: u32) -> String {
        format!("{LAYER}|{width}x{height}|v{ALGORITHM_VERSION}")
    }

    /// Set the round's visible phase; an empty string clears it. The download
    /// progress line belongs to the download phase and goes with it.
    fn set_phase(&self, phase: &str) {
        phase.clone_into(&mut self.phase.lock().unwrap_or_else(PoisonError::into_inner));
        *self.progress.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    fn set_progress(&self, line: &str) {
        *self.progress.lock().unwrap_or_else(PoisonError::into_inner) = Some(line.to_owned());
    }

    /// Record the round whose map is now the one on screen.
    fn settle(&self, round: &str) {
        *self.round.lock().unwrap_or_else(PoisonError::into_inner) = Some(round.to_owned());
    }

    /// A settled round as the status line shows it: `2026-11-16 05:00 UTC`.
    ///
    /// The service publishes in UTC while the window's clock may be set to
    /// the local zone, so the zone is spelled out rather than left to be
    /// assumed. A round that is not the twelve digits the list promises is
    /// passed through as it stands.
    fn round_text(round: &str) -> String {
        if round.len() != 12 {
            return round.to_owned();
        }
        let at = |range: std::ops::Range<usize>| {
            round
                .get(range)
                .filter(|part| part.bytes().all(|b| b.is_ascii_digit()))
        };
        let (Some(year), Some(month), Some(day), Some(hour), Some(minute)) =
            (at(0..4), at(4..6), at(6..8), at(8..10), at(10..12))
        else {
            return round.to_owned();
        };
        format!("{year}-{month}-{day} {hour}:{minute} UTC")
    }

    fn size(&self) -> (u32, u32) {
        *self
            .size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether the user's stop is standing: the round is wanted gone, and
    /// every check point between the stages and inside each chunk read
    /// unwinds to the nearest one.
    fn cancelled(&self) -> bool {
        self.cancel
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .load(Ordering::SeqCst)
    }

    /// The error a stop produces, whatever check point raised it. The fetch
    /// worker tells this apart from a real failure by its own flag, so the
    /// wording only has to read well in the status line.
    fn stop_error() -> String {
        "the download was stopped".to_owned()
    }

    /// Run one request through the round's attempt budget: transient
    /// failures retried with the backoff and a Retry-After honored; a
    /// permanent refusal, a spent budget or a stop fails the round at once.
    fn with_attempts<T>(
        &self,
        deadline: Instant,
        name: &str,
        attempt: impl Fn() -> Result<T, FetchError>,
    ) -> Result<T, String> {
        let mut delay_index = 0usize;
        loop {
            if Instant::now() >= deadline {
                return Err(format!("NSMC {name}: the round budget is spent"));
            }
            match attempt() {
                Ok(value) => return Ok(value),
                Err(FetchError::Permanent(message)) => {
                    return Err(format!("NSMC {name}: {message}"));
                }
                Err(FetchError::Transient(message, retry_after)) => {
                    let Some(&delay) = self.retry_delays.get(delay_index) else {
                        return Err(format!(
                            "NSMC {name}: {message} after {MAX_ATTEMPTS} attempts"
                        ));
                    };
                    delay_index += 1;
                    self.rest(retry_after.unwrap_or(delay), deadline)?;
                }
            }
        }
    }

    /// Sleep a retry delay in slices, so a stop or a spent budget lands
    /// within about [`REST_SLICE`] rather than at the delay's end.
    fn rest(&self, delay: Duration, deadline: Instant) -> Result<(), String> {
        let mut left = delay;
        while !left.is_zero() {
            if self.cancelled() {
                return Err(Self::stop_error());
            }
            if Instant::now() >= deadline {
                return Err("NSMC: the round budget is spent".to_owned());
            }
            let slice = left.min(REST_SLICE);
            std::thread::sleep(slice);
            left = left.saturating_sub(slice);
        }
        Ok(())
    }

    /// The round list's body, read whole: a day of entries is kilobytes.
    fn request_round_list(&self) -> Result<Vec<u8>, FetchError> {
        use std::io::Read as _;
        let mut response = self
            .agent
            .get(times_url())
            .call()
            .map_err(|e| FetchError::Transient(format!("transport: {e}"), None))?;
        classify_status(response.status().as_u16(), response.headers())?;
        let mut body = Vec::new();
        response
            .body_mut()
            .with_config()
            .limit(LIST_BODY_BYTES)
            .reader()
            .read_to_end(&mut body)
            .map_err(|e| FetchError::Transient(format!("round list read: {e}"), None))?;
        Ok(body)
    }

    /// One round's mosaic: the full-globe PNG read incrementally with the
    /// progress line moving under it, then decoded and validated. The
    /// response must be an RGBA PNG of the fixed raw size whose alpha is
    /// neither uniformly clear nor uniformly saturated — a round the service
    /// has listed but not finished rendering comes back like that, and is
    /// worth a retry rather than a refusal.
    fn request_mosaic(&self, datetime: &str) -> Result<(Vec<u8>, Vec<u8>), FetchError> {
        let mut response = self
            .agent
            .get(compose_url(datetime))
            .call()
            .map_err(|e| FetchError::Transient(format!("transport: {e}"), None))?;
        classify_status(response.status().as_u16(), response.headers())?;
        let content_type = response
            .headers()
            .get("Content-Type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !content_type.starts_with("image/") {
            return Err(FetchError::Permanent(format!(
                "the service answered {content_type:?}, not an image"
            )));
        }
        let total = response
            .headers()
            .get("Content-Length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        let mut reader = response
            .body_mut()
            .with_config()
            .limit(MAX_BODY_BYTES)
            .reader();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the capacity is clamped to 64 MiB before the narrowing cast"
        )]
        let capacity = total.map_or(1 << 20, |n| (n as usize).min(64 << 20));
        let mut body = Vec::with_capacity(capacity);
        let request_started = Instant::now();
        let mut last_report = Instant::now();
        loop {
            use std::io::Read as _;
            // The stop is checked once per chunk, so a stop lands within
            // about one 64 KiB read rather than at the request timeout.
            if self.cancelled() {
                return Err(FetchError::Permanent(Self::stop_error()));
            }
            #[expect(
                clippy::large_stack_arrays,
                reason = "64 KiB is the chunk a download reads in, on a worker thread"
            )]
            let mut chunk = [0u8; 64 * 1024];
            let read = reader.read(&mut chunk).map_err(|e: std::io::Error| {
                let over_limit = e
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<ureq::Error>())
                    .is_some_and(|err| matches!(err, ureq::Error::BodyExceedsLimit(_)));
                if over_limit {
                    FetchError::Permanent(format!(
                        "the response exceeds the {MAX_BODY_BYTES} byte cap"
                    ))
                } else {
                    FetchError::Transient(format!("body read: {e}"), None)
                }
            })?;
            if read == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..read]);
            if last_report.elapsed() >= DOWNLOAD_REPORT_EVERY {
                last_report = Instant::now();
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "a byte count is far inside the f64 mantissa"
                )]
                let speed = body.len() as f64 / request_started.elapsed().as_secs_f64();
                let line = match total.filter(|&total| total > 0) {
                    Some(total) => {
                        #[expect(
                            clippy::cast_precision_loss,
                            reason = "a byte count is far inside the f64 mantissa"
                        )]
                        let percent = body.len() as f64 / total as f64 * 100.0;
                        #[expect(
                            clippy::cast_possible_truncation,
                            reason = "the length header was parsed from bytes the cap bounds"
                        )]
                        let total = total as usize;
                        format!(
                            "Mosaic: {} of {} ({}), {:.0}%.",
                            mib(body.len()),
                            mib(total),
                            speed_text(speed),
                            percent
                        )
                    }
                    None => format!("Mosaic: {} ({}).", mib(body.len()), speed_text(speed)),
                };
                self.set_progress(&line);
            }
        }
        let alpha = decode_alpha(&body).map_err(FetchError::Permanent)?;
        validate_alpha(&alpha)?;
        Ok((body, alpha))
    }

    /// One whole round: the round list names the latest hour, the stored raw
    /// mosaic is compared against it, and only a newer round downloads.
    fn fetch_round(&self) -> Result<Option<CloudImage>, String> {
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        let started = Instant::now();
        let deadline = started + ROUND_BUDGET;
        let (width, height) = self.size();
        self.set_phase("Checking the round list.");
        let latest = self.with_attempts(deadline, "round list", || {
            self.request_round_list()
                .and_then(|body| parse_latest(&body).map_err(FetchError::Permanent))
        })?;
        let cached = self.load_raw();
        if let Some(raw) = &cached
            && raw.round == latest
        {
            if self.derived_matches(width, height, &raw.sha256) {
                info!(
                    round = %latest,
                    "the stored raw mosaic and its derivation are still current"
                );
                self.settle(&latest);
                return Ok(None);
            }
            self.set_phase("Processing locally.");
            info!(
                round = %latest,
                "reprocessing the stored raw mosaic without downloading it again"
            );
            let alpha = decode_alpha(&raw.bytes)?;
            let bytes =
                self.derive_store_and_encode(&alpha, width, height, &raw.sha256, started)?;
            self.settle(&latest);
            return Ok(Some(CloudImage {
                bytes: Download::new(bytes),
                etag: None,
                last_modified: None,
                round: parse_round_instant(&latest),
                raws: vec![("raw.png", Download::new(raw.bytes.clone()))],
            }));
        }
        self.set_phase("Downloading the mosaic.");
        let (body, alpha) =
            self.with_attempts(deadline, "mosaic", || self.request_mosaic(&latest))?;
        info!(elapsed = ?started.elapsed(), round = %latest, "the mosaic arrived");
        // A stop that landed mid-round drops it here rather than committing a
        // round the user asked to be rid of: the next poll starts over.
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        self.set_phase("Processing the round.");
        let sha = sha256_hex(&body);
        self.store_raw(&body, &latest)?;
        // Upstream unchanged and the derivation current: nothing to report,
        // and nothing to process. The round list can move to a new entry
        // while the imagery it names is byte-identical to the stored one.
        if let Some(raw) = &cached
            && body == raw.bytes
            && self.derived_matches(width, height, &raw.sha256)
        {
            debug!(round = %latest, "the upstream round matches the stored one");
            self.settle(&latest);
            return Ok(None);
        }
        let bytes = self.derive_store_and_encode(&alpha, width, height, &sha, started)?;
        self.settle(&latest);
        Ok(Some(CloudImage {
            bytes: Download::new(bytes),
            etag: None,
            last_modified: None,
            round: parse_round_instant(&latest),
            raws: vec![("raw.png", Download::new(body))],
        }))
    }

    /// Store one round's raw mosaic: the PNG written to a temporary file and
    /// verified by decoding what landed, then renamed, and the manifest
    /// written last as the commit point. A failure anywhere leaves the
    /// previous mosaic and its manifest untouched.
    fn store_raw(&self, body: &[u8], round: &str) -> Result<(), String> {
        let Some(dir) = &self.cache_dir else {
            return Ok(());
        };
        let tmp = dir.join("nsmc_raw.png.tmp");
        std::fs::write(&tmp, body).map_err(|e| format!("the raw mosaic file failed: {e}"))?;
        // Verify the written bytes before anything is published under a real
        // name: a decode of what is on disk is the check the manifest's claim
        // rests on.
        decode_alpha(
            &std::fs::read(&tmp).map_err(|e| format!("the raw mosaic read back failed: {e}"))?,
        )?;
        let path = dir.join("nsmc_raw.png");
        std::fs::rename(&tmp, &path).map_err(|e| format!("the raw mosaic commit failed: {e}"))?;
        let manifest = RawManifest {
            version: 1,
            algorithm: ALGORITHM_VERSION,
            sha256: sha256_hex(body),
            round: round.to_owned(),
            fetched_epoch_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        };
        let manifest =
            toml::to_string(&manifest).map_err(|e| format!("the raw manifest failed: {e}"))?;
        write_committed(&dir.join("nsmc_raw_meta.toml"), manifest.as_bytes())?;
        info!(round = %round, "the raw mosaic is stored and committed");
        Ok(())
    }

    /// Load the stored raw mosaic, trusting it only when the manifest's hash
    /// matches what is on disk. `None` is "no usable mosaic", which the next
    /// network round replaces; the files are left alone either way.
    fn load_raw(&self) -> Option<RawMosaic> {
        let dir = self.cache_dir.as_ref()?;
        let manifest = std::fs::read(dir.join("nsmc_raw_meta.toml")).ok()?;
        let manifest: RawManifest = toml::from_str(&String::from_utf8_lossy(&manifest)).ok()?;
        let bytes = std::fs::read(dir.join("nsmc_raw.png")).ok()?;
        let sha256 = sha256_hex(&bytes);
        if manifest.sha256 != sha256 {
            warn!("the stored raw mosaic does not match its manifest, ignoring it");
            return None;
        }
        Some(RawMosaic {
            bytes,
            sha256,
            round: manifest.round,
        })
    }

    /// Whether the stored derived map is reusable: it exists, and its
    /// metadata matches the raw hash, the current parameters and the
    /// algorithm version, all together.
    #[expect(
        clippy::float_cmp,
        reason = "the metadata must equal the tuning bit for bit, not approximately"
    )]
    fn derived_matches(&self, width: u32, height: u32, sha256: &str) -> bool {
        let Some(dir) = &self.cache_dir else {
            return false;
        };
        if !dir.join(format!("nsmc_map_{width}x{height}.jpg")).exists() {
            return false;
        }
        let Ok(meta) = std::fs::read(dir.join(format!("nsmc_map_{width}x{height}_meta.toml")))
        else {
            return false;
        };
        let Ok(meta) = toml::from_str::<DerivedMeta>(&String::from_utf8_lossy(&meta)) else {
            return false;
        };
        let s = &self.settings;
        meta.version == 1
            && meta.algorithm == ALGORITHM_VERSION
            && meta.floor == s.floor
            && meta.ceiling == s.ceiling
            && meta.gamma == s.gamma
            && meta.blur == s.blur
            && meta.cap == s.cap
            && meta.sha256 == sha256
    }

    /// Derive the resized coverage, blur it, fade missing polar edges, and
    /// return the JPEG bytes, without touching the live cache. The
    /// historical fetches and the archive's re-derivations run through here;
    /// the live pipeline wraps it with the store.
    fn derive_encode(
        &self,
        alpha: &[u8],
        width: u32,
        height: u32,
        started: Instant,
    ) -> Result<Vec<u8>, String> {
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        let resized = resize_alpha(alpha.to_vec(), RAW_WIDTH, RAW_HEIGHT, width, height);
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        let map = GrayMap::new(
            width,
            height,
            resized.into_iter().map(|v| f32::from(v) / 255.0).collect(),
        )
        .expect("the resized rectangle");
        let map = to_coverage(&map, &self.settings);
        let map = if self.settings.blur > 0.0 {
            gaussian_blur(&map, self.settings.blur)
        } else {
            map
        };
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        let map = fade_poles(&map, alpha, RAW_WIDTH, RAW_HEIGHT);
        let bytes = encode_jpeg(&map);
        info!(
            bytes = bytes.len(),
            elapsed = ?started.elapsed(),
            "the NSMC round is generated"
        );
        Ok(bytes)
    }

    /// Derive one round's map and cache it as the live pipeline's map. Only
    /// the live poll runs this: a historical round must not overwrite the
    /// entry the live pipeline's freshness check reads.
    fn derive_store_and_encode(
        &self,
        alpha: &[u8],
        width: u32,
        height: u32,
        sha256: &str,
        started: Instant,
    ) -> Result<Vec<u8>, String> {
        let bytes = self.derive_encode(alpha, width, height, started)?;
        if let Some(dir) = &self.cache_dir {
            let meta = DerivedMeta {
                version: 1,
                algorithm: ALGORITHM_VERSION,
                floor: self.settings.floor,
                ceiling: self.settings.ceiling,
                gamma: self.settings.gamma,
                blur: self.settings.blur,
                cap: self.settings.cap,
                sha256: sha256.to_owned(),
            };
            let meta =
                toml::to_string(&meta).map_err(|e| format!("the derived metadata failed: {e}"))?;
            let committed = (|| -> Result<(), String> {
                write_committed(&dir.join(format!("nsmc_map_{width}x{height}.jpg")), &bytes)?;
                write_committed(
                    &dir.join(format!("nsmc_map_{width}x{height}_meta.toml")),
                    meta.as_bytes(),
                )
            })();
            if let Err(e) = committed {
                // The bytes are already in hand and about to be posted; a
                // failed cache write costs the next start its fast path, not
                // this round its image.
                warn!(error = %e, "the derived map could not be cached");
            }
        }
        Ok(bytes)
    }

    /// One historical round, as [`CloudSource::fetch_round_at`] runs it: the
    /// wide list says whether the hour is held, the compose request renders
    /// it, and the derivation never touches the live pipeline's cache entry.
    fn fetch_round_named(&self, round: time::OffsetDateTime) -> Result<Option<CloudImage>, String> {
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        let started = Instant::now();
        let deadline = started + ROUND_BUDGET;
        let (width, height) = self.size();
        let stamp = round_stamp(round);
        let (listed, _latest) = self.round_is_listed(round, deadline)?;
        if !listed {
            info!(round = %Self::round_text(&stamp), "the upstream does not hold this round");
            return Ok(None);
        }
        self.set_phase(&format!(
            "Downloading the map for {}.",
            Self::round_text(&stamp)
        ));
        let (body, alpha) =
            self.with_attempts(deadline, "mosaic", || self.request_mosaic(&stamp))?;
        info!(elapsed = ?started.elapsed(), round = %Self::round_text(&stamp), "the historical mosaic arrived");
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        self.set_phase("Processing the round.");
        let bytes = self.derive_encode(&alpha, width, height, started)?;
        Ok(Some(CloudImage {
            bytes: Download::new(bytes),
            etag: None,
            last_modified: None,
            round: Some(round),
            raws: vec![("raw.png", Download::new(body))],
        }))
    }

    /// Whether the wide round list names `round`, and the newest hour the
    /// list holds.
    ///
    /// The list is held for the session, but a held list goes stale while
    /// the session runs: an hour that publishes after it was taken misses
    /// forever, which reads as "the upstream does not hold it" for exactly
    /// the recent rounds a user drags to. So a miss against a list older
    /// than [`LIST_STALE_REFRESH`] refreshes it once — the cooldown still
    /// bounds the traffic — before the miss is believed. A held list is
    /// never retired by a miss: one unnamed hour must not turn the hours it
    /// does name unavailable for the session.
    fn round_is_listed(
        &self,
        round: time::OffsetDateTime,
        deadline: Instant,
    ) -> Result<(bool, Option<time::OffsetDateTime>), String> {
        let held_recently = self
            .list_fetched_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some_and(|at| at.elapsed() < LIST_STALE_REFRESH);
        if !held_recently {
            self.set_phase("Checking the round list.");
            let body =
                self.with_attempts(deadline, "round list", || self.request_round_list_wide())?;
            let rounds = parse_all_rounds(&body);
            if rounds.is_empty() {
                // An unusable body is not "holds nothing": stamp the attempt
                // and keep whatever the previous list said.
                *self
                    .list_fetched_at
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
            } else {
                info!(
                    list_len = rounds.len(),
                    "the wide round list is held for the session"
                );
                *self.known.lock().unwrap_or_else(PoisonError::into_inner) = Some(rounds);
                *self
                    .list_fetched_at
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
            }
        }
        let known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(known) = known.as_ref() else {
            return Err("NSMC: the round list is unavailable".to_owned());
        };
        Ok((known.binary_search(&round).is_ok(), known.last().copied()))
    }

    /// The wide round list request: about 416 days of hourly entries, a few
    /// hundred kilobytes, held for the session once it parses.
    fn request_round_list_wide(&self) -> Result<Vec<u8>, FetchError> {
        use std::io::Read as _;
        let url = format!("{TIMES_URL}?dataCode={DATA_CODE}&hourRange={HISTORY_HOURS_RANGE}");
        let mut response = self
            .agent
            .get(url)
            .call()
            .map_err(|e| FetchError::Transient(format!("transport: {e}"), None))?;
        classify_status(response.status().as_u16(), response.headers())?;
        let mut body = Vec::new();
        response
            .body_mut()
            .with_config()
            .limit(LIST_BODY_BYTES)
            .reader()
            .read_to_end(&mut body)
            .map_err(|e| FetchError::Transient(format!("round list read: {e}"), None))?;
        Ok(body)
    }

    /// Re-derive one archived round's map from its raw files, for the
    /// raw-only cache mode where no rendered map is kept. The derivation
    /// applies the settings in force now.
    fn serve_from_raw(&self, raws: &[std::path::PathBuf]) -> Result<Option<Vec<u8>>, String> {
        let (width, height) = self.size();
        let started = Instant::now();
        let mut bytes = None;
        for path in raws {
            let body =
                std::fs::read(path).map_err(|e| format!("the archived raw read failed: {e}"))?;
            let alpha = decode_alpha(&body)?;
            bytes = Some(self.derive_encode(&alpha, width, height, started)?);
        }
        Ok(bytes)
    }
}

/// Classify one response's status the way the EUMETSAT source does: a rate
/// limit or a server fault is worth another attempt, everything else the
/// same request would answer the same way again.
fn classify_status(status: u16, headers: &ureq::http::HeaderMap) -> Result<(), FetchError> {
    if status == 429 || status >= 500 {
        Err(FetchError::Transient(
            format!("HTTP {status}"),
            retry_after_header(headers),
        ))
    } else if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(FetchError::Permanent(format!("HTTP {status}")))
    }
}

/// Decode one mosaic response into its alpha channel.
///
/// The body must be a PNG of exactly the fixed raw size, 8-bit RGBA — the
/// service's grayscale or palette companions would decode into an opaque
/// alpha everywhere, which reads as a world of full cloud, so anything but
/// RGBA is a refusal. The decoder's allocation is bounded to the raw size on
/// each side.
fn decode_alpha(body: &[u8]) -> Result<Vec<u8>, String> {
    decode_alpha_sized(body, RAW_WIDTH, RAW_HEIGHT)
}

/// The decode [`decode_alpha`] runs, over an expected size the tests shrink.
fn decode_alpha_sized(body: &[u8], width: u32, height: u32) -> Result<Vec<u8>, String> {
    const PNG_SIGNATURE: [u8; 8] = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
    if !body.starts_with(&PNG_SIGNATURE) {
        return Err("the mosaic response is not a PNG".to_owned());
    }
    // The IHDR chunk's width, height, bit depth and color type sit at fixed
    // offsets, so the exact size and layout are validated before any decode
    // allocation happens.
    if body.len() < 26 {
        return Err("the mosaic PNG is too short to hold its dimensions".to_owned());
    }
    let png_width = u32::from_be_bytes([body[16], body[17], body[18], body[19]]);
    let png_height = u32::from_be_bytes([body[20], body[21], body[22], body[23]]);
    if png_width != width || png_height != height {
        return Err(format!(
            "the mosaic response is {png_width}x{png_height}, expected {width}x{height}"
        ));
    }
    if body[24] != 8 {
        return Err(format!("the mosaic PNG's bit depth is {}", body[24]));
    }
    if body[25] != 6 {
        return Err(format!("the mosaic PNG's color type is {}", body[25]));
    }
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(width);
    limits.max_image_height = Some(height);
    let mut reader = image::ImageReader::new(Cursor::new(body));
    reader.set_format(image::ImageFormat::Png);
    reader.limits(limits);
    let img = reader
        .decode()
        .map_err(|e| format!("the mosaic PNG did not decode: {e}"))?;
    let rgba = img.into_rgba8().into_raw();
    Ok(rgba
        .as_chunks::<4>()
        .0
        .iter()
        .map(|pixel| pixel[3])
        .collect())
}

/// A round whose alpha carries no information is not a map: the service
/// lists a round before it has finished rendering it, and such a round is
/// worth a retry rather than a refusal.
fn validate_alpha(alpha: &[u8]) -> Result<(), FetchError> {
    let clear = alpha.iter().all(|&a| a == 0);
    let saturated = alpha.iter().all(|&a| a == 255);
    if clear || saturated {
        Err(FetchError::Transient(
            "the mosaic came back without usable coverage".to_owned(),
            None,
        ))
    } else {
        Ok(())
    }
}

// Alpha also encodes clear sky, so only the global outer empty rows define polar support.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    reason = "normalized row coordinates of a bounded image"
)]
fn fade_poles(image: &GrayMap, alpha: &[u8], raw_width: u32, raw_height: u32) -> GrayMap {
    let mut rows = alpha.chunks_exact(raw_width as usize);
    let top = rows.clone().position(|row| row.iter().any(|&v| v > 0));
    let bottom = rows.rposition(|row| row.iter().any(|&v| v > 0));
    let mut pixels = image.pixels().to_vec();
    if let (Some(top), Some(bottom)) = (top, bottom) {
        let north = top as f64 / f64::from(raw_height);
        let south = (bottom + 1) as f64 / f64::from(raw_height);
        for (y, row) in pixels.chunks_exact_mut(image.width() as usize).enumerate() {
            let latitude_row = (y as f64 + 0.5) / f64::from(image.height());
            let from_north = if top == 0 { 1.0 } else { latitude_row - north };
            let from_south = if bottom + 1 == raw_height as usize {
                1.0
            } else {
                south - latitude_row
            };
            let t = (from_north.min(from_south) / (5.0 / 180.0)).clamp(0.0, 1.0);
            let weight = (t * t * (3.0 - 2.0 * t)) as f32;
            for value in row {
                *value *= weight;
            }
        }
    } else {
        pixels.fill(0.0);
    }
    GrayMap::new(image.width(), image.height(), pixels).expect("the same rectangle")
}

/// Resize the coverage input from the fixed raw size to the prepared
/// variant's size, a triangle-filter resample in the u8 domain the levels
/// curve reads.
fn resize_alpha(
    alpha: Vec<u8>,
    from_width: u32,
    from_height: u32,
    to_width: u32,
    to_height: u32,
) -> Vec<u8> {
    if from_width == to_width && from_height == to_height {
        return alpha;
    }
    let image =
        image::ImageBuffer::<image::Luma<u8>, Vec<u8>>::from_raw(from_width, from_height, alpha)
            .expect("the raw rectangle");
    image::imageops::resize(
        &image,
        to_width,
        to_height,
        image::imageops::FilterType::Triangle,
    )
    .into_raw()
}

/// Turn the mosaic's alpha coverage into the cloud coverage the renderer
/// reads, through the same levels curve the EUMETSAT pipeline applies.
/// Alpha zero is clear sky, so there is no no-data special case: the curve
/// clamps it to zero on its own.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the coverage curve runs in f32 like the rest of the pipeline"
)]
fn to_coverage(image: &GrayMap, settings: &NsmcSettings) -> GrayMap {
    // The bounds are derived in f64, as the EUMETSAT generator's Levels math
    // does, and the mapping runs in f32 like the rest of the pipeline.
    // Validation keeps floor below ceiling, so the span never divides by
    // zero.
    let floor = f64::from(settings.floor) / 255.0;
    let span = f64::from(settings.ceiling) / 255.0 - floor;
    let gamma = settings.gamma as f32;
    // The shoulder's shape: `v / (1 + (1 / cap - 1) v)` keeps its slope of one
    // at zero and approaches `cap` from below, so a thin deck keeps the value
    // the curve gave it and a saturated core comes down off the rail.
    let shoulder = (1.0 / settings.cap - 1.0).max(0.0);
    let pixels = image
        .pixels()
        .iter()
        .map(|&v| {
            let t = ((f64::from(v) - floor) / span).clamp(0.0, 1.0) as f32;
            let shaped = t.powf(gamma);
            if shoulder > 0.0 {
                shaped / (1.0 + shoulder * shaped)
            } else {
                shaped
            }
        })
        .collect();
    GrayMap::new(image.width(), image.height(), pixels).expect("same rectangle")
}

impl CloudSource for NsmcCloudSource {
    fn publication(&self) -> Option<super::cloud_source::CloudPublication> {
        let latest = self
            .round
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_deref()
            .and_then(|round| {
                time::PrimitiveDateTime::parse(
                    round,
                    time::macros::format_description!("[year][month][day][hour][minute]"),
                )
                .ok()
            })
            .map(time::PrimitiveDateTime::assume_utc);
        Some(super::cloud_source::CloudPublication {
            interval: Duration::from_hours(1),
            latest,
        })
    }

    fn wait(&self) -> Duration {
        // One mosaic request costs the service's render time — measured about
        // half a minute for the 4K PNG — plus the transfer and the
        // derivation, so a first answer is given minutes, not seconds, but
        // nothing like the EUMETSAT generation budget.
        CloudWait::SlowFetch.duration()
    }

    fn set_cancel(&self, flag: Arc<AtomicBool>) {
        *self.cancel.lock().unwrap_or_else(PoisonError::into_inner) = flag;
    }

    fn status(&self) -> Option<String> {
        let phase = self.phase.lock().unwrap_or_else(PoisonError::into_inner);
        let progress = self.progress.lock().unwrap_or_else(PoisonError::into_inner);
        match (phase.is_empty(), progress.as_deref()) {
            (true, None) => None,
            (true, Some(line)) => Some(line.to_owned()),
            (false, None) => Some(phase.clone()),
            (false, Some(line)) => Some(format!("{phase} {line}")),
        }
    }

    fn prepare(&self, resolution: u32) -> CloudTarget {
        let (width, height) = cloud_variant(resolution);
        *self
            .size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = (width, height);
        CloudTarget::nsmc(&Self::identity(width, height))
    }

    fn fetch_if_changed(&self, _known_etag: Option<&str>) -> Result<Option<CloudImage>, String> {
        let outcome = self.fetch_round();
        // Whatever the round came to, it is over: the phase and the download
        // line named what it was doing while it ran. Without this the last
        // phase outlives the round — "mosaic: 100%" hanging on the status
        // line for an hour — and hides the worker's own answer, which is
        // what ready, stopped and retrying are.
        self.set_phase("");
        outcome
    }

    fn settled_round(&self) -> Option<time::OffsetDateTime> {
        self.load_raw()
            .map(|raw| raw.round)
            .and_then(|round| parse_round_instant(&round))
    }

    /// One historical round: the wide round list names what the service
    /// holds, the compose request renders the named hour, and the derivation
    /// runs without touching the live pipeline's cache entry.
    fn fetch_round_at(
        &self,
        round: time::OffsetDateTime,
        _resolution: u32,
    ) -> Result<Option<CloudImage>, String> {
        let outcome = self.fetch_round_named(round);
        self.set_phase("");
        outcome
    }

    fn derive_from_raw(
        &self,
        raws: &[std::path::PathBuf],
        _resolution: u32,
    ) -> Result<Option<Vec<u8>>, String> {
        self.serve_from_raw(raws)
    }

    fn describe(&self) -> String {
        format!("nsmc {LAYER}")
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use approx::assert_relative_eq;
    use image::GenericImageView;
    use ureq::http::{Request, Response};
    use ureq::middleware::MiddlewareNext;
    use ureq::{Body, SendBody};

    use super::*;

    fn gray_png(width: u32, height: u32, value: u8) -> Vec<u8> {
        let img = image::GrayImage::from_pixel(width, height, image::Luma([value]));
        let mut out = Vec::new();
        image::DynamicImage::ImageLuma8(img)
            .write_to(&mut Cursor::new(&mut out), image::ImageFormat::Png)
            .unwrap();
        out
    }

    /// An RGBA PNG whose every pixel is opaque white with the given alpha,
    /// the shape the mosaic's companion RGB implies (white wherever any
    /// coverage is present).
    fn rgba_png(width: u32, height: u32, alpha: u8) -> Vec<u8> {
        rgba_png_where(width, height, |_, _| alpha)
    }

    fn rgba_png_where(width: u32, height: u32, alpha: impl Fn(u32, u32) -> u8) -> Vec<u8> {
        let mut img = image::RgbaImage::new(width, height);
        for (x, y, pixel) in img.enumerate_pixels_mut() {
            *pixel = image::Rgba([255, 255, 255, alpha(x, y)]);
        }
        let mut out = Vec::new();
        image::DynamicImage::ImageRgba8(img)
            .write_to(&mut Cursor::new(&mut out), image::ImageFormat::Png)
            .unwrap();
        out
    }

    /// An agent whose responses come from `make`, one per call, with the URI
    /// each call asked for so the list and the mosaic can be scripted apart,
    /// and the call count left in the counter for the test to assert on.
    fn stub_agent(
        make: impl Fn(usize, &str) -> Result<Response<Body>, ureq::Error> + Send + Sync + 'static,
    ) -> (ureq::Agent, std::sync::Arc<AtomicUsize>) {
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let seen = std::sync::Arc::clone(&calls);
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .middleware(move |req: Request<SendBody>, _: MiddlewareNext| {
                let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
                let uri = req.uri().to_string();
                make(n, &uri)
            })
            .build()
            .new_agent();
        (agent, calls)
    }

    fn list_response(body: &str) -> Response<Body> {
        Response::builder()
            .status(200)
            .header("Content-Type", "application/json")
            .body(Body::builder().data(body.as_bytes().to_vec()))
            .unwrap()
    }

    fn mosaic_response(bytes: Vec<u8>) -> Response<Body> {
        Response::builder()
            .status(200)
            .header("Content-Type", "image/png")
            .body(Body::builder().data(bytes))
            .unwrap()
    }

    fn round_list_body() -> String {
        r#"{"returnCode":0,"ds":[
            {"dataDate":"20261113","dataTime":"230000","endTime":""},
            {"dataDate":"20261114","dataTime":"070000","endTime":""},
            {"dataDate":"20261114","dataTime":"230000","endTime":""}
        ]}"#
        .to_owned()
    }

    fn test_source(
        settings: NsmcSettings,
        dir: Option<PathBuf>,
        agent: ureq::Agent,
    ) -> NsmcCloudSource {
        let mut source = NsmcCloudSource::new(settings, dir);
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        source.agent = agent;
        source
    }

    #[test]
    fn documented_urls_carry_the_service_parameters() {
        let url = compose_url("202611140700");
        assert!(url.starts_with("https://data.nsmc.org.cn/NSMCAPI/v1/nsmc/image/wms/compose?"));
        for part in [
            "layers=GEOS_IRX",
            "datetime=202611140700",
            "request=GetMap",
            "bbox=-180,-90,180,90",
            "width=4096&height=2048",
            "version=1.1.0",
            "format=png",
        ] {
            assert!(url.contains(part), "{part} missing from {url}");
        }
        let list = times_url();
        assert!(list.starts_with(
            "https://data.nsmc.org.cn/nsmcapi/v1/nsmc/image/animation/datatime/mongodb?"
        ));
        for part in [
            "dataCode=GEO_MULT_GBAL_L2_GGM_IRX_GLL_YYYYMMDD_HHmm_4000M.PNG",
            "hourRange=24",
        ] {
            assert!(list.contains(part), "{part} missing from {list}");
        }
        assert_eq!(REQUEST_TIMEOUT, Duration::from_mins(5));
        assert_eq!(CONNECT_TIMEOUT, Duration::from_secs(15));
        assert_eq!(ROUND_BUDGET, Duration::from_mins(10));
        assert_eq!(MAX_ATTEMPTS, 3);
        assert_eq!(
            RETRY_DELAYS,
            [Duration::from_secs(10), Duration::from_secs(20)]
        );
        assert_eq!(MAX_BODY_BYTES, 32 * 1024 * 1024);
        assert_eq!(RAW_WIDTH, 4096);
        assert_eq!(RAW_HEIGHT, 2048);
    }

    #[test]
    fn the_round_list_parses_its_latest_round_and_refuses_the_rest() {
        let latest = parse_latest(round_list_body().as_bytes()).unwrap();
        assert_eq!(latest, "202611142300");
        assert_eq!(
            parse_latest(br#"{"ds":[{"dataDate":"20261114","dataTime":"061200"}]}"#).unwrap(),
            "202611140612"
        );
        // A partially filled entry cannot win, whatever its position.
        let with_holes = br#"{"ds":[
            {"dataDate":"20261114","dataTime":"230000"},
            {"dataDate":"20261115","dataTime":"01:0000"},
            {"dataDate":"202611xx","dataTime":"010000"},
            {"dataDate":"20261115"},
            {"dataDate":"20261115","dataTime":"010"}
        ]}"#;
        assert_eq!(parse_latest(with_holes).unwrap(), "202611142300");
        assert_eq!(
            parse_latest(br#"{"ds":[]}"#).unwrap_err(),
            "the round list holds no usable rounds"
        );
        assert_eq!(
            parse_latest(b"not json").unwrap_err(),
            "the round list holds no usable rounds"
        );
        // A time that arrives without its own date is never paired with the
        // next entry's date.
        let time_first = br#"{"ds":[
            {"dataTime":"090000"},
            {"dataDate":"20261113","dataTime":"050000"}
        ]}"#;
        assert_eq!(parse_latest(time_first).unwrap(), "202611130500");
    }

    #[test]
    fn the_alpha_channel_of_an_rgba_png_becomes_the_coverage_input() {
        let body = rgba_png_where(8, 4, |x, _| u8::try_from(x * 30).unwrap());
        let alpha = decode_alpha_sized(&body, 8, 4).unwrap();
        assert_eq!(alpha.len(), 8 * 4);
        for (x, expected) in [0u8, 30, 60, 90, 120, 150, 180, 210].iter().enumerate() {
            assert_eq!(alpha[x], *expected);
        }
        validate_alpha(&alpha).unwrap();
    }

    #[test]
    fn a_png_that_is_not_rgba_or_the_wrong_shape_is_refused() {
        let error = decode_alpha_sized(&gray_png(8, 4, 128), 8, 4).unwrap_err();
        assert!(error.contains("color type is 0"), "{error}");
        let error = decode_alpha_sized(&rgba_png(8, 2, 128), 8, 4).unwrap_err();
        assert!(error.contains("8x2, expected 8x4"), "{error}");
        let error = decode_alpha(b"not a png at all").unwrap_err();
        assert!(error.contains("not a PNG"), "{error}");
        let truncated = [0x89u8, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0, 1];
        let error = decode_alpha(&truncated).unwrap_err();
        assert!(error.contains("too short"), "{error}");
    }

    #[test]
    fn a_degenerate_round_is_transient_and_a_real_one_is_not() {
        let clear = vec![0u8; RAW_WIDTH as usize * RAW_HEIGHT as usize];
        let saturated = vec![255u8; RAW_WIDTH as usize * RAW_HEIGHT as usize];
        for degenerate in [&clear, &saturated] {
            assert!(matches!(
                validate_alpha(degenerate),
                Err(FetchError::Transient(_, None))
            ));
        }
        let mut real = clear;
        real[0] = 1;
        validate_alpha(&real).unwrap();
    }

    fn polar_fixture(alpha: &[u8], width: u32, height: u32) -> GrayMap {
        let image = GrayMap::new(
            width,
            height,
            alpha.iter().map(|&v| f32::from(v) / 255.0).collect(),
        )
        .unwrap();
        fade_poles(&image, alpha, width, height)
    }

    #[test]
    fn polar_fade_preserves_interior_coordinates_and_clear_rows() {
        let mut alpha = vec![0u8; 8 * 180];
        for y in 15..165 {
            for x in 0..8 {
                alpha[y * 8 + x] = u8::try_from(y + x).unwrap();
            }
        }
        alpha[40 * 8..41 * 8].fill(0);
        let faded = polar_fixture(&alpha, 8, 180);
        let expected: Vec<_> = alpha.iter().map(|&v| f32::from(v) / 255.0).collect();
        assert_eq!(&faded.pixels()[20 * 8..160 * 8], &expected[20 * 8..160 * 8]);
        assert!(faded.pixels()[..15 * 8].iter().all(|&v| v == 0.0));
        assert!(faded.pixels()[165 * 8..].iter().all(|&v| v == 0.0));
    }

    #[test]
    fn polar_fade_keeps_sparse_clouds_without_copying_them_poleward() {
        let mut alpha = vec![0u8; 8 * 180];
        alpha[15 * 8 + 3] = 100;
        alpha[25 * 8..155 * 8].fill(100);
        alpha[164 * 8 + 6] = 100;
        let faded = polar_fixture(&alpha, 8, 180);
        for (&before, &after) in alpha.iter().zip(faded.pixels()) {
            if before == 0 {
                assert_eq!(after, 0.0);
            }
        }
        assert!(faded.pixels()[15 * 8 + 3] > 0.0);
        assert!(faded.pixels()[164 * 8 + 6] > 0.0);
        assert_eq!(faded.pixels()[30 * 8 + 3], 100.0 / 255.0);
    }

    #[test]
    fn polar_fade_is_smooth_and_symmetric_over_five_degrees() {
        let mut alpha = vec![0u8; 180];
        alpha[15..165].fill(255);
        let faded = polar_fixture(&alpha, 1, 180);
        for y in 15..20 {
            let t = (f32::from(u16::try_from(y).unwrap()) + 0.5 - 15.0) / 5.0;
            let expected = t * t * (3.0 - 2.0 * t);
            assert!((faded.pixels()[y] - expected).abs() < 1e-5);
            assert!((faded.pixels()[179 - y] - expected).abs() < 1e-5);
        }
        assert_eq!(faded.pixels()[20], 1.0);
        assert_eq!(faded.pixels()[159], 1.0);
    }

    #[test]
    fn polar_fade_uses_latitude_rather_than_output_pixel_count() {
        let mut alpha = vec![0u8; 180];
        alpha[15..165].fill(255);
        let low = polar_fixture(&alpha, 1, 180);
        let high_input = GrayMap::new(1, 540, vec![1.0; 540]).unwrap();
        let high = fade_poles(&high_input, &alpha, 1, 180);
        for y in 0..180 {
            assert!((low.pixels()[y] - high.pixels()[3 * y + 1]).abs() < 1e-6);
        }
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "observed support must remain exactly unchanged"
    )]
    fn polar_fade_leaves_an_observed_pole_alone() {
        let mut alpha = vec![255u8; 180];
        alpha[165..].fill(0);
        let north = polar_fixture(&alpha, 1, 180);
        assert!(north.pixels()[..160].iter().all(|&v| v == 1.0));
        alpha.reverse();
        let south = polar_fixture(&alpha, 1, 180);
        assert!(south.pixels()[20..].iter().all(|&v| v == 1.0));
    }

    #[test]
    fn polar_fade_never_extends_a_single_observed_row() {
        let mut alpha = vec![0u8; 8 * 180];
        alpha[30 * 8 + 2] = 75;
        let faded = polar_fixture(&alpha, 8, 180);
        assert!(faded.pixels()[30 * 8 + 2] > 0.0);
        assert!(faded.pixels()[30 * 8 + 2] < 75.0 / 255.0);
        for (i, &v) in faded.pixels().iter().enumerate() {
            if i != 30 * 8 + 2 {
                assert_eq!(v, 0.0);
            }
        }
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "full support must remain exactly unchanged"
    )]
    fn a_map_with_data_at_both_edges_is_left_alone() {
        let alpha = decode_alpha_sized(&rgba_png(8, 8, 100), 8, 8).unwrap();
        let faded = polar_fixture(&alpha, 8, 8);
        assert!(faded.pixels().iter().all(|&v| v == 100.0 / 255.0));
    }

    #[test]
    fn an_all_clear_map_is_left_alone_for_the_caller_to_reject() {
        let alpha = vec![0u8; 8 * 8];
        assert!(
            polar_fixture(&alpha, 8, 8)
                .pixels()
                .iter()
                .all(|&v| v == 0.0)
        );
    }

    #[test]
    fn derivation_fades_after_gamma_and_blur_without_resurrecting_the_caps() {
        let mut alpha = vec![0u8; (RAW_WIDTH * RAW_HEIGHT) as usize];
        alpha[170 * RAW_WIDTH as usize..1878 * RAW_WIDTH as usize].fill(64);
        let settings = NsmcSettings {
            gamma: 0.3,
            blur: 1.0,
            ..pass_through()
        };
        let source = NsmcCloudSource::new(settings.clone(), None);
        let bytes = source
            .derive_store_and_encode(&alpha, 360, 180, "fixture", Instant::now())
            .unwrap();
        let output = image::load_from_memory(&bytes).unwrap().into_luma8();
        let resized = resize_alpha(alpha, RAW_WIDTH, RAW_HEIGHT, 360, 180);
        let input = GrayMap::new(
            360,
            180,
            resized.into_iter().map(|v| f32::from(v) / 255.0).collect(),
        )
        .unwrap();
        let baseline = gaussian_blur(&to_coverage(&input, &settings), settings.blur);
        for y in [0u32, 5, 10, 169, 175, 179] {
            assert_eq!(output.get_pixel(180, y)[0], 0, "row {y}");
        }
        for y in [16u16, 17, 18, 90, 161, 162, 163] {
            let latitude_row = (f32::from(y) + 0.5) / 180.0;
            let distance = (latitude_row - 170.0 / 2048.0).min(1878.0 / 2048.0 - latitude_row);
            let t = (distance / (5.0 / 180.0)).clamp(0.0, 1.0);
            let weight = t * t * (3.0 - 2.0 * t);
            let expected = baseline.pixels()[usize::from(y) * 360 + 180] * weight * 255.0;
            let actual = f32::from(output.get_pixel(180, u32::from(y))[0]);
            assert!(
                (actual - expected).abs() < 3.0,
                "row {y}: {actual} != {expected}"
            );
        }
    }

    #[test]
    fn resizing_moves_between_the_variant_sizes() {
        let alpha = vec![200u8; 8 * 4];
        let up = resize_alpha(alpha.clone(), 8, 4, 16, 8);
        assert_eq!(up.len(), 16 * 8);
        assert!(up.iter().all(|&v| (150..=200).contains(&v)));
        let down = resize_alpha(alpha, 8, 4, 4, 2);
        assert_eq!(down, vec![200; 4 * 2]);
    }

    /// The curve that leaves the mosaic's alpha as it is, for the tests that
    /// assert on the pipeline rather than on the shipped tuning.
    fn pass_through() -> NsmcSettings {
        NsmcSettings {
            floor: 0,
            ceiling: 255,
            gamma: 1.0,
            blur: 0.0,
            cap: 1.0,
        }
    }

    /// The shipped curve is the one the renderer can actually draw: `fs_cloud`
    /// discards a land pixel under 0.25 of full scale, so the mosaic's own
    /// median alpha — 10 of 255 over a November round — has to come out above
    /// that line or the layer shows nothing. The pass-through this source
    /// shipped with failed exactly that.
    #[test]
    fn the_shipped_curve_carries_the_mosaics_thin_cloud_past_the_renderers_floor() {
        let map = |alpha: u8| {
            GrayMap::new(1, 1, vec![f32::from(alpha) / 255.0]).expect("a one pixel map")
        };
        let covered = |alpha: u8| to_coverage(&map(alpha), &NsmcSettings::default()).pixels()[0];

        assert_relative_eq!(covered(0), 0.0, epsilon = 1e-6);
        for alpha in [10, 32, 64, 97] {
            assert!(
                covered(alpha) > 0.25,
                "alpha {alpha} lands at {}",
                covered(alpha)
            );
        }
        // The shoulder holds the densest cloud under the cap rather than at
        // 1, and the thin end still clears the floor.
        assert_relative_eq!(covered(255), NsmcSettings::default().cap, epsilon = 1e-5);
        let mut last = 0.0;
        for alpha in (0..=255).step_by(3) {
            let value = covered(alpha);
            assert!(value >= last, "the curve is monotone at {alpha}");
            last = value;
        }
    }

    /// The shoulder pulls the deepest cloud off full white and leaves the thin
    /// end where the curve put it, which is what keeps a typhoon core from
    /// flattening into one white blob.
    #[test]
    fn the_shoulder_caps_the_densest_cloud_and_leaves_the_thin_alone() {
        let map = GrayMap::new(3, 1, vec![0.1, 0.5, 1.0]).unwrap();
        let plain = NsmcSettings {
            floor: 0,
            ceiling: 255,
            gamma: 1.0,
            blur: 0.0,
            cap: 1.0,
        };
        let capped = NsmcSettings {
            cap: 0.8,
            ..plain.clone()
        };
        let open = to_coverage(&map, &plain);
        let shaped = to_coverage(&map, &capped);
        assert_relative_eq!(open.pixels()[2], 1.0, epsilon = 1e-6);
        assert_relative_eq!(shaped.pixels()[2], 0.8, epsilon = 1e-6);
        // 0.1 / (1 + 0.25 * 0.1) and 0.5 / (1 + 0.25 * 0.5).
        assert_relative_eq!(shaped.pixels()[0], 0.097_561, epsilon = 1e-5);
        assert_relative_eq!(shaped.pixels()[1], 0.444_444, epsilon = 1e-5);
        assert!(shaped.pixels()[0] < open.pixels()[0]);
        assert!(shaped.pixels()[1] < open.pixels()[1]);
    }

    #[test]
    fn coverage_maps_alpha_through_the_levels_curve() {
        let map = GrayMap::new(4, 1, vec![0.0, 0.5, 0.8, 1.0]).unwrap();
        let settings = NsmcSettings {
            floor: 51,
            ceiling: 204,
            gamma: 2.0,
            blur: 0.0,
            cap: 1.0,
        };
        let covered = to_coverage(&map, &settings);
        // floor 51/255 = 0.2, ceiling 204/255 = 0.8: 0.0 and 0.2 clamp low,
        // 0.8 and 1.0 clamp high, 0.5 is the midpoint squared.
        assert_relative_eq!(covered.pixels()[0], 0.0);
        assert_relative_eq!(covered.pixels()[1], 0.25);
        assert_relative_eq!(covered.pixels()[2], 1.0);
        assert_relative_eq!(covered.pixels()[3], 1.0);
    }

    #[test]
    fn identical_imagery_still_commits_the_new_round() {
        let scratch = crate::test_support::ScratchDir::new("nsmc_identical_round");
        let mosaics = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&mosaics);
        let bytes = rgba_png(RAW_WIDTH, RAW_HEIGHT, 80);
        let cached = bytes.clone();
        let (agent, _) = stub_agent(move |_, uri| {
            if uri.contains("datatime") {
                Ok(list_response(
                    r#"{"ds":[{"dataDate":"20261118","dataTime":"060000"}]}"#,
                ))
            } else {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(mosaic_response(bytes.clone()))
            }
        });
        let source = test_source(pass_through(), Some(scratch.path().to_path_buf()), agent);
        *source.size.lock().unwrap() = (32, 16);
        source.store_raw(&cached, "202611180500").unwrap();
        let alpha = decode_alpha(&cached).unwrap();
        source
            .derive_store_and_encode(&alpha, 32, 16, &sha256_hex(&cached), Instant::now())
            .unwrap();
        assert!(source.fetch_if_changed(None).unwrap().is_none());
        assert_eq!(source.load_raw().unwrap().round, "202611180600");
        assert!(source.fetch_if_changed(None).unwrap().is_none());
        assert_eq!(mosaics.load(Ordering::SeqCst), 1);
        let publication = source.publication().unwrap();
        assert_eq!(publication.interval, Duration::from_hours(1));
        assert_eq!(
            publication.latest,
            Some(time::macros::datetime!(2026-11-18 06:00 UTC))
        );
    }

    #[test]
    fn the_settled_round_reads_as_a_utc_instant_and_is_unset_until_one_settles() {
        assert_eq!(
            NsmcCloudSource::round_text("202611160500"),
            "2026-11-16 05:00 UTC"
        );
        // Anything that is not the twelve digits the list promises is passed
        // through rather than guessed at.
        for odd in ["", "2026111", "2026-11-16", "20261116050x", "2026111605000"] {
            assert_eq!(NsmcCloudSource::round_text(odd), odd);
        }
    }

    #[test]
    fn prepare_names_the_variant_and_keeps_parameters_out_of_the_key() {
        let source = NsmcCloudSource::new(
            NsmcSettings {
                floor: 10,
                ..NsmcSettings::default()
            },
            None,
        );
        let wide = source.prepare(8192);
        assert_eq!(source.size(), (8192, 4096));
        let narrow = source.prepare(2048);
        assert_eq!(source.size(), (2048, 1024));
        assert_ne!(wide.cache_key, narrow.cache_key);
        assert!(wide.cache_key.starts_with("clouds_cache_nsmc_"));
        assert_eq!(source.prepare(8192), wide);
        assert_eq!(wide.wait, CloudWait::SlowFetch);
        assert_eq!(source.describe(), "nsmc GEOS_IRX");
    }

    #[test]
    fn prepare_does_not_reuse_stretched_or_repeated_polar_startup_caches() {
        let source = NsmcCloudSource::new(NsmcSettings::default(), None);
        for resolution in [2048, 4096, 8192] {
            let (width, height) = cloud_variant(resolution);
            for suffix in ["", "|v2"] {
                let old = CloudTarget::nsmc(&format!("{LAYER}|{width}x{height}{suffix}"));
                assert_ne!(source.prepare(resolution).cache_key, old.cache_key);
            }
        }
    }

    #[test]
    fn the_status_composes_the_phase_and_the_progress_line() {
        let source = NsmcCloudSource::new(NsmcSettings::default(), None);
        assert_eq!(source.status(), None);
        source.set_phase("Downloading the mosaic.");
        assert_eq!(source.status().as_deref(), Some("Downloading the mosaic."));
        source.set_progress("Mosaic: 1.2 of 5.0 MiB, 24%.");
        assert_eq!(
            source.status().as_deref(),
            Some("Downloading the mosaic. Mosaic: 1.2 of 5.0 MiB, 24%.")
        );
        // A new phase takes the progress line with it, and an empty phase
        // clears the whole status; a line set while idle stands alone.
        source.set_phase("Checking the round list.");
        assert_eq!(source.status().as_deref(), Some("Checking the round list."));
        source.set_phase("");
        assert_eq!(source.status(), None);
        source.set_progress("Mosaic: 5.0 of 5.0 MiB, 100%.");
        assert_eq!(
            source.status().as_deref(),
            Some("Mosaic: 5.0 of 5.0 MiB, 100%.")
        );
    }

    /// The full round over a scripted service: the list, the mosaic, the
    /// stored pair, the derived map, and a second poll served whole from
    /// disk.
    #[test]
    fn a_round_downloads_stores_and_derives_then_the_next_poll_serves_the_cache() {
        let scratch = crate::test_support::ScratchDir::new("nsmc_round");
        let (agent, calls) = stub_agent(|_, uri| {
            if uri.contains("datatime/mongodb") {
                Ok(list_response(&round_list_body()))
            } else if uri.contains("image/wms/compose") {
                Ok(mosaic_response(rgba_png(RAW_WIDTH, RAW_HEIGHT, 200)))
            } else {
                panic!("unexpected request: {uri}");
            }
        });
        let source = test_source(pass_through(), Some(scratch.path().to_path_buf()), agent);
        source.prepare(2048);
        let image = source
            .fetch_if_changed(None)
            .unwrap()
            .expect("a first round");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "list + mosaic");
        let decoded = image::load_from_memory(&image.bytes).unwrap();
        assert_eq!(decoded.dimensions(), (2048, 1024));
        let raw = decoded.into_luma8().into_raw();
        let sum: u32 = raw.iter().map(|&v| u32::from(v)).sum();
        #[expect(clippy::cast_precision_loss, reason = "a mean over a small fixture")]
        let mean = f64::from(sum) / raw.len() as f64;
        assert!((mean - 200.0).abs() < 2.0, "mean {mean}");
        for name in [
            "nsmc_raw.png",
            "nsmc_raw_meta.toml",
            "nsmc_map_2048x1024.jpg",
            "nsmc_map_2048x1024_meta.toml",
        ] {
            assert!(scratch.join(name).exists(), "{name} missing");
        }
        // The settled round is the hour drawn, and a poll that changes
        // nothing keeps it.
        assert_eq!(
            source.round.lock().expect("the settled round").as_deref(),
            Some("202611142300")
        );
        // The second poll names the same round: one list request, no
        // mosaic, no image.
        assert!(source.fetch_if_changed(None).unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 3, "list only");
        assert_eq!(
            source.round.lock().expect("the settled round").as_deref(),
            Some("202611142300")
        );
    }

    #[test]
    fn a_new_derivation_reprocesses_the_stored_mosaic_without_downloading_it() {
        let scratch = crate::test_support::ScratchDir::new("nsmc_reprocess");
        let (agent, calls) = stub_agent(|_, uri| {
            if uri.contains("datatime/mongodb") {
                Ok(list_response(&round_list_body()))
            } else {
                Ok(mosaic_response(rgba_png(RAW_WIDTH, RAW_HEIGHT, 120)))
            }
        });
        let source = test_source(
            NsmcSettings::default(),
            Some(scratch.path().to_path_buf()),
            agent,
        );
        source.prepare(2048);
        source
            .fetch_if_changed(None)
            .unwrap()
            .expect("a first round");
        // A fresh instance with different parameters: the round list is
        // still consulted (it is the authority on what is current), but the
        // mosaic itself is not downloaded again.
        let (agent, list_calls) = stub_agent(|_, uri| {
            if uri.contains("datatime/mongodb") {
                Ok(list_response(&round_list_body()))
            } else {
                panic!("the mosaic must not be downloaded again: {uri}");
            }
        });
        let retuned = test_source(
            NsmcSettings {
                gamma: 1.5,
                ..NsmcSettings::default()
            },
            Some(scratch.path().to_path_buf()),
            agent,
        );
        retuned.prepare(2048);
        let image = retuned
            .fetch_if_changed(None)
            .unwrap()
            .expect("a reprocessed round");
        assert_eq!(list_calls.load(Ordering::SeqCst), 1, "the list only");
        let decoded = image::load_from_memory(&image.bytes).unwrap();
        assert_eq!(decoded.dimensions(), (2048, 1024));
        // The reprocessed derivation rewrote the one derived file rather
        // than accumulating another.
        assert!(scratch.join("nsmc_map_2048x1024.jpg").exists());
        assert!(!scratch.join("nsmc_map_2048x1024.jpg.tmp").exists());
        let _ = calls.load(Ordering::SeqCst);
    }

    #[test]
    fn a_repeated_polar_derivation_is_rebuilt_from_the_unchanged_raw_mosaic() {
        let scratch = crate::test_support::ScratchDir::new("nsmc_latitude_cache");
        let (agent, calls) = stub_agent(|_, uri| {
            assert!(
                uri.contains("datatime/mongodb"),
                "unexpected download: {uri}"
            );
            Ok(list_response(&round_list_body()))
        });
        let settings = pass_through();
        let source = test_source(settings.clone(), Some(scratch.path().to_path_buf()), agent);
        source.prepare(2048);
        let raw = rgba_png(RAW_WIDTH, RAW_HEIGHT, 120);
        source.store_raw(&raw, "202611142300").unwrap();
        let raw_meta = std::fs::read(scratch.join("nsmc_raw_meta.toml")).unwrap();
        let sha = sha256_hex(&raw);
        let old_meta = DerivedMeta {
            version: 1,
            algorithm: 2,
            floor: settings.floor,
            ceiling: settings.ceiling,
            gamma: settings.gamma,
            blur: settings.blur,
            cap: settings.cap,
            sha256: sha.clone(),
        };
        std::fs::write(scratch.join("nsmc_map_2048x1024.jpg"), b"old derived map").unwrap();
        std::fs::write(
            scratch.join("nsmc_map_2048x1024_meta.toml"),
            toml::to_string(&old_meta).unwrap(),
        )
        .unwrap();
        assert!(!source.derived_matches(2048, 1024, &sha));
        let image = source
            .fetch_if_changed(None)
            .unwrap()
            .expect("the old derivation must be replaced");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "only the round list");
        let decoded = image::load_from_memory(&image.bytes).unwrap().into_luma8();
        assert_eq!(decoded.dimensions(), (2048, 1024));
        assert!(decoded.get_pixel(1024, 512)[0].abs_diff(120) < 2);
        assert!(source.derived_matches(2048, 1024, &sha));
        assert_eq!(std::fs::read(scratch.join("nsmc_raw.png")).unwrap(), raw);
        assert_eq!(
            std::fs::read(scratch.join("nsmc_raw_meta.toml")).unwrap(),
            raw_meta
        );
    }

    #[test]
    fn a_corrupted_raw_mosaic_is_redownloaded() {
        let scratch = crate::test_support::ScratchDir::new("nsmc_corrupt");
        let (agent, calls) = stub_agent(|_, uri| {
            if uri.contains("datatime/mongodb") {
                Ok(list_response(&round_list_body()))
            } else {
                Ok(mosaic_response(rgba_png(RAW_WIDTH, RAW_HEIGHT, 90)))
            }
        });
        let source = test_source(
            NsmcSettings::default(),
            Some(scratch.path().to_path_buf()),
            agent,
        );
        source.prepare(2048);
        source
            .fetch_if_changed(None)
            .unwrap()
            .expect("a first round");
        let mut raw = std::fs::read(scratch.join("nsmc_raw.png")).unwrap();
        raw[100] ^= 0xff;
        std::fs::write(scratch.join("nsmc_raw.png"), raw).unwrap();
        // The hash no longer matches the manifest, so the mosaic is
        // downloaded again and the round still succeeds.
        let image = source
            .fetch_if_changed(None)
            .unwrap()
            .expect("a refetched round");
        assert_eq!(
            image::load_from_memory(&image.bytes).unwrap().dimensions(),
            (2048, 1024)
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "list, mosaic, list, mosaic"
        );
    }

    #[test]
    fn an_unusable_service_answer_fails_the_round() {
        // An empty round list.
        let (agent, _) = stub_agent(|_, _| Ok(list_response(r#"{"ds":[]}"#)));
        let source = test_source(NsmcSettings::default(), None, agent);
        let Err(error) = source.fetch_if_changed(None) else {
            panic!("an empty round list must fail the round");
        };
        assert!(error.contains("no usable rounds"), "{error}");
        // A mosaic listed but not yet rendered: all-clear alpha, retried to
        // the end of the attempt budget and then given up.
        let (agent, mosaic_calls) = stub_agent(|_, uri| {
            if uri.contains("datatime/mongodb") {
                Ok(list_response(&round_list_body()))
            } else {
                Ok(mosaic_response(rgba_png(RAW_WIDTH, RAW_HEIGHT, 0)))
            }
        });
        let source = test_source(NsmcSettings::default(), None, agent);
        source.prepare(2048);
        let Err(error) = source.fetch_if_changed(None) else {
            panic!("a degenerate mosaic must fail the round");
        };
        assert!(error.contains("without usable coverage"), "{error}");
        assert_eq!(
            mosaic_calls.load(Ordering::SeqCst),
            4,
            "one list request, three mosaic attempts"
        );
        // A refusal the same request would answer the same way again.
        let (agent, _) = stub_agent(|_, uri| {
            if uri.contains("datatime/mongodb") {
                Ok(list_response(&round_list_body()))
            } else {
                Ok(Response::builder()
                    .status(403)
                    .body(Body::builder().data(Vec::new()))
                    .unwrap())
            }
        });
        let source = test_source(NsmcSettings::default(), None, agent);
        source.prepare(2048);
        let Err(error) = source.fetch_if_changed(None) else {
            panic!("a refusal must fail the round");
        };
        assert!(error.contains("HTTP 403"), "{error}");
    }

    #[test]
    fn the_user_stop_unwinds_the_round_at_its_first_check() {
        let (agent, calls) = stub_agent(|_, _| {
            panic!("a stopped round must not reach the network");
        });
        let source = test_source(NsmcSettings::default(), None, agent);
        source.set_cancel(Arc::new(AtomicBool::new(true)));
        let Err(error) = source.fetch_if_changed(None) else {
            panic!("a stopped round must fail");
        };
        assert_eq!(error, "the download was stopped");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_transport_failure_retries_within_the_attempt_budget() {
        let (agent, calls) = stub_agent(|n, uri| {
            if uri.contains("datatime/mongodb") {
                if n <= 2 {
                    Err(ureq::Error::Io(std::io::Error::other("connection died")))
                } else {
                    Ok(list_response(&round_list_body()))
                }
            } else {
                Ok(mosaic_response(rgba_png(RAW_WIDTH, RAW_HEIGHT, 60)))
            }
        });
        let source = test_source(NsmcSettings::default(), None, agent);
        source.prepare(2048);
        let image = source
            .fetch_if_changed(None)
            .unwrap()
            .expect("a round after retries");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "two failed lists, one good, one mosaic"
        );
        assert_eq!(
            image::load_from_memory(&image.bytes).unwrap().dimensions(),
            (2048, 1024)
        );
    }

    /// A miss against a stale held list refreshes it once before the miss is
    /// believed: an hour that publishes after the list was taken is exactly
    /// the recent round a user drags to, and it must not read as "the
    /// upstream does not hold it" for the rest of the session.
    #[test]
    fn a_miss_against_a_stale_list_refreshes_it_once() {
        let bodies = Arc::new(Mutex::new(vec![round_list_body()]));
        let bodies_for_agent = Arc::clone(&bodies);
        let agent = stub_agent(move |_, _| {
            let index = bodies_for_agent
                .lock()
                .expect("the bodies")
                .last()
                .expect("a body")
                .clone();
            Ok(list_response(&index))
        })
        .0;
        let source = test_source(NsmcSettings::default(), None, agent);
        let round = parse_round_instant("202611142300").expect("the fixture round");
        let deadline = Instant::now() + ROUND_BUDGET;

        // First ask: the wide list is fetched and names the round.
        let (listed, latest) = source.round_is_listed(round, deadline).expect("listed");
        assert!(listed, "the held list names the round");
        assert_eq!(latest, Some(round), "the list's latest is the round itself");

        // The list goes stale and the upstream publishes the next hour.
        *source.list_fetched_at.lock().expect("the stamp") = Some(
            Instant::now()
                .checked_sub(LIST_STALE_REFRESH + Duration::from_secs(1))
                .expect("the process has run longer than the staleness window"),
        );
        bodies
            .lock()
            .expect("the bodies")
            .push(round_list_body_with("202611150000"));

        let next = parse_round_instant("202611150000").expect("the next round");
        let (listed, latest) = source.round_is_listed(next, deadline).expect("refreshed");
        assert!(listed, "the refreshed list names the hour that published");
        assert_eq!(latest, Some(next), "the refreshed list's latest moved up");
    }

    /// A miss against a FRESH list is believed without a refresh: the
    /// staleness window's whole point is not hammering the service for
    /// hours it genuinely does not hold.
    #[test]
    fn a_miss_against_a_fresh_list_is_believed_without_a_refresh() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_agent = Arc::clone(&calls);
        let agent = stub_agent(move |_, _| {
            calls_for_agent.fetch_add(1, Ordering::SeqCst);
            Ok(list_response(&round_list_body()))
        })
        .0;
        let source = test_source(NsmcSettings::default(), None, agent);
        let deadline = Instant::now() + ROUND_BUDGET;

        let held = parse_round_instant("202611140700").expect("the held round");
        let (listed, _) = source.round_is_listed(held, deadline).expect("listed");
        assert!(listed);

        let missing = parse_round_instant("202611150000").expect("an unheld hour");
        let (listed, _) = source.round_is_listed(missing, deadline).expect("asked");
        assert!(!listed, "the fresh list's miss is the answer");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no refresh on a fresh miss"
        );
    }

    /// The wide-list body with one more entry appended, for the refresh test.
    fn round_list_body_with(extra: &str) -> String {
        let (date, time) = extra.split_at(8);
        format!(
            r#"{{"returnCode":0,"ds":[
            {{"dataDate":"20261113","dataTime":"230000","endTime":""}},
            {{"dataDate":"20261114","dataTime":"070000","endTime":""}},
            {{"dataDate":"20261114","dataTime":"230000","endTime":""}},
            {{"dataDate":"{date}","dataTime":"{time}","endTime":""}}
        ]}}"#
        )
    }
}
