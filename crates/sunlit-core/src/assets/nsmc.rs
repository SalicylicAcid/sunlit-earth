//! The native NSMC cloud source: the `FengYun` satellites' geostationary
//! infrared mosaic over the National Satellite Meteorological Center's
//! WMS-style interface.
//!
//! The service ([www.nsmc.org.cn/nsmc/cn/image/wms.html], the documented
//! "FYWMS") serves `GEOS_IRX`, the global composite of the geostationary
//! fleet's 10.8 um infrared channels, one round an hour. A round is one
//! `GetMap` request for the full globe at 4096x2048, and the response is an
//! RGBA PNG whose **alpha channel carries the cloud intensity**: 0 is clear
//! sky (and, above roughly 60 degrees of latitude, no data at all — the
//! geostationary ring cannot see the poles), 255 the deepest convective
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
//! imagery is current. The pole handling stretches the outermost
//! data-bearing rows across the blind bands, the same latitude-shift
//! approximation the EUMETSAT pole trim makes.
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
/// A row counts as data-bearing when at least this fraction of its pixels
/// carry coverage: the polar no-data bands are whole rows of alpha zero,
/// while a clear-sky row inside the data area still holds scattered thin
/// cloud.
const BAND_ROW_FRACTION: f64 = 0.5;
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
const ALGORITHM_VERSION: u32 = 1;

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
            cancel: Mutex::new(Arc::new(AtomicBool::new(false))),
            agent,
        }
    }

    /// What one round's cache entry is named after: the layer and the map
    /// size. The derivation parameters are deliberately absent — a parameter
    /// change must reuse the same derived file rather than accumulate one
    /// per tuning — so the key names the imagery, not the look.
    fn identity(width: u32, height: u32) -> String {
        format!("{LAYER}|{width}x{height}")
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
                            "mosaic: {} of {} ({}) — {:.0}%",
                            mib(body.len()),
                            mib(total),
                            speed_text(speed),
                            percent
                        )
                    }
                    None => format!("mosaic: {} ({})", mib(body.len()), speed_text(speed)),
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
        self.set_phase("checking the round list");
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
            self.set_phase("processing locally");
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
            }));
        }
        self.set_phase("downloading the mosaic");
        let (body, alpha) =
            self.with_attempts(deadline, "mosaic", || self.request_mosaic(&latest))?;
        info!(elapsed = ?started.elapsed(), round = %latest, "the mosaic arrived");
        // A stop that landed mid-round drops it here rather than committing a
        // round the user asked to be rid of: the next poll starts over.
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        self.set_phase("processing the round");
        let sha = sha256_hex(&body);
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
        self.store_raw(&body, &latest)?;
        let bytes = self.derive_store_and_encode(&alpha, width, height, &sha, started)?;
        self.settle(&latest);
        Ok(Some(CloudImage {
            bytes: Download::new(bytes),
            etag: None,
            last_modified: None,
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
                .map(|d| d.as_secs())
                .unwrap_or(0),
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

    /// Process one round into the derived map, and store it with its
    /// metadata: the polar bands filled, the map resized to the prepared
    /// variant, the levels curve applied, the optional blur, and the
    /// grayscale JPEG encoded. The stages run on the worker thread; the user
    /// stop is checked between them.
    fn derive_store_and_encode(
        &self,
        alpha: &[u8],
        width: u32,
        height: u32,
        sha256: &str,
        started: Instant,
    ) -> Result<Vec<u8>, String> {
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        let filled = fill_poles(alpha, RAW_WIDTH, RAW_HEIGHT);
        let resized = resize_alpha(filled, RAW_WIDTH, RAW_HEIGHT, width, height);
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
        let bytes = encode_jpeg(&map);
        info!(
            bytes = bytes.len(),
            elapsed = ?started.elapsed(),
            "the NSMC round is generated"
        );
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
    Ok(rgba.chunks_exact(4).map(|pixel| pixel[3]).collect())
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

/// Fill the polar no-data bands by stretching the outermost data-bearing
/// rows across them.
///
/// The geostationary ring cannot see above roughly 60 degrees of latitude,
/// so the mosaic arrives with whole bands of alpha zero at the top and the
/// bottom — which the coverage curve reads as a cloudless cap. The band
/// edges are found once, globally: the blind zone runs at nearly constant
/// latitude, and a per-column search would chase clear-sky rows inside the
/// data area. What the outermost data rows hold is then interpolated across
/// the full height, the same latitude-shift approximation the EUMETSAT pole
/// trim makes.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "row indices and fractions of a fixed-size image"
)]
fn fill_poles(alpha: &[u8], width: u32, height: u32) -> Vec<u8> {
    let w = width as usize;
    let h = height as usize;
    let threshold = (BAND_ROW_FRACTION * f64::from(width)) as usize;
    let row_has_data =
        |y: usize| alpha[y * w..(y + 1) * w].iter().filter(|&&a| a > 0).count() >= threshold;
    let Some(top) = (0..h).find(|&y| row_has_data(y)) else {
        // Nothing anywhere is data-bearing; the caller rejects such a round
        // before this runs, and leaving the map alone costs nothing.
        return alpha.to_vec();
    };
    let Some(bottom) = (top..h).rev().find(|&y| row_has_data(y)) else {
        return alpha.to_vec();
    };
    if top == 0 && bottom == h - 1 {
        return alpha.to_vec();
    }
    // np.interp of each column over the data band's rows, sampled across the
    // full height: position y/(h-1)*(band-1), mixed linearly between its
    // neighboring band rows.
    let band = bottom - top + 1;
    let mut out = vec![0u8; w * h];
    for y in 0..h {
        let pos = if h > 1 {
            y as f64 / (h as f64 - 1.0) * (band as f64 - 1.0)
        } else {
            0.0
        };
        let row_index = (pos as usize).min(band - 1);
        let frac = (pos - row_index as f64) as f32;
        let top_row = (top + row_index) * w;
        let bottom_row = if row_index + 1 < band {
            top_row + w
        } else {
            top_row
        };
        for x in 0..w {
            let a = f32::from(alpha[top_row + x]);
            let b = f32::from(alpha[bottom_row + x]);
            #[expect(
                clippy::cast_possible_truncation,
                reason = "an interpolated coverage byte, clamped before the cast"
            )]
            let value = (a + (b - a) * frac).round().clamp(0.0, 255.0) as u8;
            out[y * w + x] = value;
        }
    }
    out
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
            (false, Some(line)) => Some(format!("{phase} — {line}")),
        }
    }

    /// The round last settled on, so the idle line says which hour is drawn.
    /// Nothing here is cleared on a failure: what the window shows when an
    /// attempt fails is precisely that older round.
    fn settled_note(&self) -> Option<String> {
        self.round
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_deref()
            .map(|round| format!("round {}", Self::round_text(round)))
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

    #[test]
    fn the_polar_bands_are_stretched_across_the_poles() {
        // 8 rows: two no-data bands of two rows around a data band of four,
        // every pixel of the band carrying one coverage value.
        let mut alpha = vec![0u8; 8 * 8];
        for y in 2..6 {
            for x in 0..8 {
                alpha[y * 8 + x] = u8::try_from(100 + x * 10).unwrap();
            }
        }
        let filled = fill_poles(&alpha, 8, 8);
        // Every row now holds the band's own column values: the stretch is
        // vertical, so a column's value is its band value everywhere.
        for y in 0..8 {
            for x in 0..8 {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "the band value this row must land on"
                )]
                let expected = (100 + x * 10) as u8;
                assert_eq!(filled[y * 8 + x], expected, "row {y}, column {x}");
            }
        }
    }

    #[test]
    fn a_map_with_data_at_both_edges_is_left_alone() {
        let alpha = rgba_png(8, 8, 100);
        let filled = fill_poles(&alpha, 8, 8);
        assert_eq!(filled, alpha);
    }

    #[test]
    fn an_all_clear_map_is_left_alone_for_the_caller_to_reject() {
        let alpha = vec![0u8; 8 * 8];
        assert_eq!(fill_poles(&alpha, 8, 8), alpha);
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
        let source = NsmcCloudSource::new(NsmcSettings::default(), None);
        assert_eq!(source.settled_note(), None);
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
    fn the_status_composes_the_phase_and_the_progress_line() {
        let source = NsmcCloudSource::new(NsmcSettings::default(), None);
        assert_eq!(source.status(), None);
        source.set_phase("downloading the mosaic");
        assert_eq!(source.status().as_deref(), Some("downloading the mosaic"));
        source.set_progress("mosaic: 1.2 of 5.0 MiB — 24%");
        assert_eq!(
            source.status().as_deref(),
            Some("downloading the mosaic — mosaic: 1.2 of 5.0 MiB — 24%")
        );
        // A new phase takes the progress line with it, and an empty phase
        // clears the whole status; a line set while idle stands alone.
        source.set_phase("checking the round list");
        assert_eq!(source.status().as_deref(), Some("checking the round list"));
        source.set_phase("");
        assert_eq!(source.status(), None);
        source.set_progress("mosaic: 5.0 of 5.0 MiB — 100%");
        assert_eq!(
            source.status().as_deref(),
            Some("mosaic: 5.0 of 5.0 MiB — 100%")
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
        // The status line can name the hour it is drawing from here on, and a
        // poll that changes nothing keeps naming it.
        assert_eq!(
            source.settled_note().as_deref(),
            Some("round 2026-11-14 23:00 UTC")
        );
        // The second poll names the same round: one list request, no
        // mosaic, no image.
        assert!(source.fetch_if_changed(None).unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 3, "list only");
        assert_eq!(
            source.settled_note().as_deref(),
            Some("round 2026-11-14 23:00 UTC")
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
}
