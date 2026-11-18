//! The native EUMETSAT cloud source: fetch the IR layer over WMS and process
//! it into the grayscale map the cloud pipeline already decodes.
//!
//! This is the Rust port of the `eumetsat` command in `tools/cloud-fetch`
//! (`plans/2026-11-13-cloud-map-generator.md`), which was verified against the
//! app's renderer. One round fetches the two hemispheres of
//! `mumi:worldcloudmap_ir108` from `EUMETView`'s WMS 1.3.0 endpoint (EPSG:4326,
//! no auth), stitches them west-first into a 2:1 equirectangular map, masks
//! the polar blind zone without stretching or moving valid samples, maps IR
//! brightness to cloud coverage, and fades inside the valid boundary after
//! gamma and blur. It encodes the quality-88 grayscale JPEG the fetcher decodes.
//!
//! The imagery requires the "Contains modified EUMETSAT data" attribution
//! (EUMETSAT data licensing). The two hemispheres are "latest" requests taken
//! seconds apart, not a strictly simultaneous instant.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use image::ImageEncoder;
use tracing::{debug, info, warn};

use super::cloud_fetcher::cloud_variant;
use super::cloud_source::{CloudImage, CloudSource, CloudTarget, CloudWait, Download};
use crate::config::DEFAULT_TEXTURE_RESOLUTION;
use crate::config::clouds::EumetsatSettings;

/// `EUMETView`'s WMS 1.3.0 endpoint, unauthenticated.
pub(crate) const WMS_URL: &str = "https://view.eumetsat.int/geoserver/ows";
/// The global IR (10.8 um) cloud layer, the raw upstream data behind
/// clouds.matteason.co.uk.
pub(crate) const LAYER: &str = "mumi:worldcloudmap_ir108";

/// WMS 1.3.0 with CRS EPSG:4326 uses latitude,longitude axis order. Each
/// hemisphere is a square 180x180 degree bbox rendered at size x size, so the
/// stitched map is (2 * size) x size, equirectangular. West first: its left
/// edge is -180, which is where the app's map convention starts.
pub(crate) const HEMISPHERES: [(&str, &str); 2] =
    [("west", "-90,-180,90,0"), ("east", "-90,0,90,180")];

/// Pole-connected blind-zone white, including the antialiased near-white rim.
/// This cutoff never rejects bright clouds inside a column's valid support.
const TRIM_THRESHOLD: f32 = 236.0 / 255.0;
/// Latitude width of the inward coverage fade, independent of map resolution.
const POLAR_FADE_DEGREES: f64 = 5.0;
/// Quality of the derived grayscale JPEG, as the generator's default.
pub(crate) const JPEG_QUALITY: u8 = 88;
/// One WMS tile's side in pixels. A hemisphere is fetched as a grid of this
/// size: the service renders each request on the fly and a long-lived
/// connection for a 9 MB hemisphere dies mid-transfer on a flaky link, which
/// costs the whole round. A 512 px tile is ~230 KB and lands in seconds, so
/// completed tiles accumulate and only the failed ones retry. The service's
/// rate limiter delays excess requests by a second rather than refusing
/// them, so the grid's extra requests are tolerated.
pub(crate) const TILE_PX: u32 = 512;
/// How many tile requests one hemisphere keeps in flight. The two hemisphere
/// threads run side by side, so the round peaks at twice this many
/// concurrent requests — well inside the service's rate limit, which delays
/// excess requests by a second rather than refusing them.
pub(crate) const TILE_WORKERS: u32 = 4;
/// How often a download refreshes its progress line: often enough to look
/// alive, rare enough that the status event stays a rarity.
const DOWNLOAD_REPORT_EVERY: Duration = Duration::from_millis(500);
/// How often a tile's retry nap looks at the user stop.
const REST_SLICE: Duration = Duration::from_millis(100);

/// Bytes as a one-decimal MiB figure, the unit every cloud map is measured
/// in.
#[expect(
    clippy::cast_precision_loss,
    reason = "a byte count is far inside the f64 mantissa"
)]
pub(crate) fn mib(bytes: usize) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// A transfer speed in the unit that reads best for it.
pub(crate) fn speed_text(bytes_per_second: f64) -> String {
    if bytes_per_second >= 1024.0 * 1024.0 {
        format!("{:.1} MiB/s", bytes_per_second / (1024.0 * 1024.0))
    } else {
        format!("{:.0} KiB/s", bytes_per_second / 1024.0)
    }
}

/// One WMS request at most, however slow the service is today.
const REQUEST_TIMEOUT: Duration = Duration::from_mins(5);
/// TCP/TLS handshake cap, so a dead endpoint is known in seconds.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// One whole round (both hemispheres, their tile grids and the retries) at
/// most. The tiled fetch spends the same total render time as one large
/// request would, spread over many small requests, and a slow link adds
/// retries on top.
const ROUND_BUDGET: Duration = Duration::from_mins(25);
/// Attempts per hemisphere for transient failures, within the budget.
const MAX_ATTEMPTS: u32 = 3;
/// Backoff between attempts of one hemisphere: 10 s, then 20 s. A Retry-After
/// within the budget replaces them.
const RETRY_DELAYS: [Duration; (MAX_ATTEMPTS - 1) as usize] =
    [Duration::from_secs(10), Duration::from_secs(20)];
/// Compressed response cap, the same bound the remote source's body uses.
const MAX_BODY_BYTES: u64 = 50 * 1024 * 1024;
/// Decoded hemisphere cap. The largest real hemisphere is 4096 x 4096 (the
/// 8192 texture resolution halves into it); nothing larger is decoded.
const MAX_HEMISPHERE_SIDE: u32 = 4096;
const UPSTREAM_REFRESH: Duration = Duration::from_hours(3);

fn parse_latest_round(body: &str) -> Result<String, String> {
    use time::format_description::well_known::Rfc3339;
    let document =
        roxmltree::Document::parse(body).map_err(|e| format!("invalid WMS capabilities: {e}"))?;
    let layer = document
        .descendants()
        .find(|node| {
            node.has_tag_name("Layer")
                && node
                    .children()
                    .any(|child| child.has_tag_name("Name") && child.text() == Some(LAYER))
        })
        .ok_or_else(|| "the WMS capabilities do not name the cloud layer".to_owned())?;
    let dimension = layer
        .children()
        .find(|node| node.has_tag_name("Dimension") && node.attribute("name") == Some("time"))
        .ok_or_else(|| "the cloud layer has no time dimension".to_owned())?;
    dimension
        .text()
        .unwrap_or_default()
        .split([',', '/'])
        .filter_map(|value| time::OffsetDateTime::parse(value.trim(), &Rfc3339).ok())
        .max()
        .ok_or_else(|| "the cloud time dimension holds no usable rounds".to_owned())?
        .to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .map_err(|e| e.to_string())
}

/// A grayscale map as row-major f32 in [0, 1], north up, -180..180 west to
/// east: the same orientation the remote map and the generator use, which is
/// what [`texture_loader::orient`](super::texture_loader::orient) expects.
#[derive(Clone)]
pub(crate) struct GrayMap {
    width: u32,
    height: u32,
    pixels: Vec<f32>,
}

impl GrayMap {
    /// Build from row-major pixels; the length must fill the rectangle.
    pub(crate) fn new(width: u32, height: u32, pixels: Vec<f32>) -> Result<Self, String> {
        if pixels.len() != width as usize * height as usize {
            return Err(format!(
                "pixel count {} does not fill {width}x{height}",
                pixels.len()
            ));
        }
        Ok(Self {
            width,
            height,
            pixels,
        })
    }

    pub(crate) fn width(&self) -> u32 {
        self.width
    }

    pub(crate) fn height(&self) -> u32 {
        self.height
    }

    pub(crate) fn pixels(&self) -> &[f32] {
        &self.pixels
    }
}

/// The WMS 1.3.0 `GetMap` URL for one hemisphere.
///
/// The bbox is in latitude,longitude axis order, which is what EPSG:4326
/// means under WMS 1.3.0 and what the generator sends.
pub(crate) fn build_wms_url(bbox: &str, px: u32) -> String {
    format!(
        "{WMS_URL}?service=WMS&request=GetMap&version=1.3.0&layers={LAYER}\
         &styles=&format=image/png&crs=EPSG:4326&bbox={bbox}&width={px}&height={px}"
    )
}

/// Side-by-side stitch, west first.
///
/// Both inputs are square hemispheres of one round, so the result is the 2:1
/// equirectangular map: west's -180..0 left, east's 0..180 right.
pub(crate) fn stitch(west: &GrayMap, east: &GrayMap) -> GrayMap {
    assert_eq!(
        west.height(),
        east.height(),
        "hemispheres of one round share a height"
    );
    let height = west.height();
    let width = west.width() + east.width();
    let (west_width, east_width) = (west.width() as usize, east.width() as usize);
    let mut pixels = Vec::with_capacity(width as usize * height as usize);
    for y in 0..height as usize {
        pixels.extend_from_slice(&west.pixels()[y * west_width..(y + 1) * west_width]);
        pixels.extend_from_slice(&east.pixels()[y * east_width..(y + 1) * east_width]);
    }
    GrayMap::new(width, height, pixels).expect("stitched rectangle")
}

// Only pole-connected near-white runs are missing; bright interior clouds remain valid.
fn trim_poles(image: &GrayMap) -> (GrayMap, Vec<std::ops::Range<usize>>) {
    let width = image.width as usize;
    let height = image.height as usize;
    let mut masked = image.clone();
    let mut bounds = Vec::with_capacity(width);
    for x in 0..width {
        let top = (0..height)
            .find(|&y| image.pixels[y * width + x] < TRIM_THRESHOLD)
            .unwrap_or(height);
        let bottom = (top..height)
            .rfind(|&y| image.pixels[y * width + x] < TRIM_THRESHOLD)
            .map_or(top, |y| y + 1);
        for y in (0..top).chain(bottom..height) {
            masked.pixels[y * width + x] = 0.0;
        }
        bounds.push(top..bottom);
    }
    (masked, bounds)
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "image row distances are small; coverage is stored in f32"
)]
fn feather_poles(image: &mut GrayMap, bounds: &[std::ops::Range<usize>]) {
    let width = image.width as usize;
    let height = image.height as usize;
    let fade_rows = f64::from(image.height) * POLAR_FADE_DEGREES / 180.0;
    for y in 0..height {
        let center = y as f64 + 0.5;
        for (x, support) in bounds.iter().enumerate() {
            let pixel = &mut image.pixels[y * width + x];
            if !support.contains(&y) {
                *pixel = 0.0;
                continue;
            }
            let mut distance = fade_rows;
            if support.start > 0 {
                distance = distance.min(center - support.start as f64);
            }
            if support.end < height {
                distance = distance.min(support.end as f64 - center);
            }
            let t = (distance / fade_rows).clamp(0.0, 1.0);
            *pixel *= (t * t * (3.0 - 2.0 * t)) as f32;
        }
    }
}

fn process_map(image: &GrayMap, settings: &EumetsatSettings) -> GrayMap {
    let (masked, bounds) = trim_poles(image);
    let map = to_coverage(&masked, settings);
    drop(masked);
    // Mask before blur and fade after it, so blind-zone white cannot bleed into the clouds.
    let mut map = if settings.blur > 0.0 {
        gaussian_blur(&map, settings.blur)
    } else {
        map
    };
    feather_poles(&mut map, &bounds);
    map
}

/// Map masked IR brightness to coverage with gamma below one brightening midtones.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the coverage curve runs in f32 like the rest of the pipeline"
)]
pub(crate) fn to_coverage(image: &GrayMap, settings: &EumetsatSettings) -> GrayMap {
    // The bounds are derived in f64, as the generator's Levels math does, and
    // the mapping runs in f32 like the rest of the pipeline. Validation keeps
    // floor below ceiling, so the span never divides by zero.
    let floor = f64::from(settings.floor) / 255.0;
    let span = f64::from(settings.ceiling) / 255.0 - floor;
    let gamma = settings.gamma as f32;
    let pixels = image
        .pixels
        .iter()
        .map(|&v| {
            let t = ((f64::from(v) - floor) / span).clamp(0.0, 1.0) as f32;
            t.powf(gamma)
        })
        .collect();
    GrayMap::new(image.width, image.height, pixels).expect("same rectangle")
}

/// Gaussian blur with scipy's `gaussian_filter` semantics: `truncate` 4.0, a
/// symmetric boundary (the edge value repeats), and no-op at sigma <= 0.
pub(crate) fn gaussian_blur(image: &GrayMap, sigma: f64) -> GrayMap {
    let mut blurred = blur_f32(&image.pixels, image.width, image.height, sigma);
    GrayMap::new(image.width, image.height, std::mem::take(&mut blurred))
        .expect("the blurred rectangle")
}

/// Separable Gaussian blur over a row-major f32 buffer, with scipy's
/// `gaussian_filter` semantics (truncate 4.0, symmetric boundary). Shared by
/// the sources whose pipelines smooth their maps.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "kernel offsets around a fixed-size image, mirrored at the boundary"
)]
pub(crate) fn blur_f32(pixels: &[f32], width: u32, height: u32, sigma: f64) -> Vec<f32> {
    if sigma <= 0.0 {
        return pixels.to_vec();
    }
    // scipy.ndimage.gaussian_filter semantics: truncate 4.0, a symmetric
    // boundary (the edge value repeats), separable passes.
    let radius = (4.0 * sigma).round() as usize;
    let mut kernel = Vec::with_capacity(2 * radius + 1);
    for offset in -(radius as isize)..=(radius as isize) {
        let x = offset as f64;
        kernel.push((-(x * x) / (2.0 * sigma * sigma)).exp());
    }
    let total: f64 = kernel.iter().sum();
    let kernel: Vec<f32> = kernel
        .iter()
        .map(|&weight| (weight / total) as f32)
        .collect();

    let width = width as usize;
    let height = height as usize;
    // Horizontal pass, then vertical on the horizontal result.
    let mut horizontal = vec![0.0f32; width * height];
    for y in 0..height {
        for x in 0..width {
            let mut acc = 0.0f32;
            for (k, &weight) in kernel.iter().enumerate() {
                let dx = k as isize - radius as isize;
                let sx = mirror(x as isize + dx, width as isize) as usize;
                acc += pixels[y * width + sx] * weight;
            }
            horizontal[y * width + x] = acc;
        }
    }
    let mut out = vec![0.0f32; width * height];
    for y in 0..height {
        for x in 0..width {
            let mut acc = 0.0f32;
            for (k, &weight) in kernel.iter().enumerate() {
                let dy = k as isize - radius as isize;
                let sy = mirror(y as isize + dy, height as isize) as usize;
                acc += horizontal[sy * width + x] * weight;
            }
            out[y * width + x] = acc;
        }
    }
    out
}

/// scipy's 'reflect' boundary: the edge value repeats, so index -1 reads 0
/// and index n reads n-1.
fn mirror(index: isize, len: isize) -> isize {
    if index < 0 {
        -index - 1
    } else if index >= len {
        2 * len - index - 1
    } else {
        index
    }
}

/// Encode a coverage map as the grayscale JPEG the fetcher stores and decodes.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the generator's float_to_uint8 truncates toward zero"
)]
pub(crate) fn encode_jpeg(image: &GrayMap) -> Vec<u8> {
    // The generator's float_to_uint8: clip, scale, truncate.
    let bytes: Vec<u8> = image
        .pixels
        .iter()
        .map(|&v| (v.clamp(0.0, 1.0) * 255.0) as u8)
        .collect();
    let mut out = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, JPEG_QUALITY)
        .write_image(
            &bytes,
            image.width,
            image.height,
            image::ColorType::L8.into(),
        )
        .expect("a grayscale JPEG of an in-memory buffer");
    out
}

/// Decode one hemisphere's response into a [`GrayMap`].
///
/// The body must be a PNG of exactly `px` by `px`; anything else (a WMS XML
/// exception page, a disguised image, a size this round did not ask for) is a
/// permanent error. The decoder's allocation is bounded to
/// [`MAX_HEMISPHERE_SIDE`] on each side.
fn decode_png(body: &[u8], px: u32) -> Result<GrayMap, String> {
    const PNG_SIGNATURE: [u8; 8] = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
    if !body.starts_with(&PNG_SIGNATURE) {
        return Err("EUMETSAT response is not a PNG".to_owned());
    }
    // The IHDR chunk's width and height sit at fixed offsets, so the exact
    // square size is validated before any decode allocation happens.
    if body.len() < 24 {
        return Err("EUMETSAT PNG is too short to hold its dimensions".to_owned());
    }
    let width = u32::from_be_bytes([body[16], body[17], body[18], body[19]]);
    let height = u32::from_be_bytes([body[20], body[21], body[22], body[23]]);
    if width != px || height != px {
        return Err(format!(
            "EUMETSAT response is {width}x{height}, expected {px}x{px}"
        ));
    }
    if px > MAX_HEMISPHERE_SIDE {
        return Err(format!(
            "EUMETSAT hemisphere {px}x{px} exceeds the {MAX_HEMISPHERE_SIDE} side cap"
        ));
    }
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_HEMISPHERE_SIDE);
    limits.max_image_height = Some(MAX_HEMISPHERE_SIDE);
    let mut reader = image::ImageReader::new(Cursor::new(body));
    reader.set_format(image::ImageFormat::Png);
    reader.limits(limits);
    let img = reader
        .decode()
        .map_err(|e| format!("EUMETSAT PNG did not decode: {e}"))?;
    let gray = img.into_luma8();
    let pixels = gray
        .into_raw()
        .into_iter()
        .map(|v| f32::from(v) / 255.0)
        .collect();
    Ok(GrayMap::new(px, px, pixels).expect("square decode"))
}

/// Why a hemisphere fetch failed, and whether trying again can help.
#[derive(Debug)]
pub(crate) enum FetchError {
    /// A transport failure, a 5xx, a 429, or a body read that died: worth
    /// another attempt within the budget.
    Transient(String, Option<Duration>),
    /// A permanent refusal or an invalid response: no retry this round.
    Permanent(String),
}

/// The native cloud source: one worker thread fetches both hemispheres of a
/// round sequentially and only a complete pair produces a new image.
///
/// There are no conditional requests against this source: `known_etag` is
/// ignored on purpose, and it is the capabilities' latest round, not an
/// `ETag`, that decides what a poll fetches — the current round answers from
/// the cache, a newer round downloads. The two hemispheres are "latest"
/// requests taken seconds apart; the round's start and end times are logged,
/// not claimed to be one satellite instant.
///
/// The disk cache holds two layers. The raw pair (the two hemisphere PNGs and
/// a manifest naming their hashes, published with the manifest last) is what
/// a processing-parameter change reuses instead of the network. The derived
/// map carries the raw hashes, the parameters and the algorithm version in
/// its metadata, and is reused only when all three match, one file per
/// hemisphere size: sliding the parameters rewrites it rather than
/// accumulating files.
pub struct EumetsatCloudSource {
    settings: EumetsatSettings,
    /// Hemisphere size in pixels currently prepared. `prepare` and the fetch
    /// both run on the engine's single cloud worker; the lock is for
    /// construction and for tests, like `HttpCloudSource`'s URL.
    size: Mutex<u32>,
    /// Where the raw pair and the derived map are cached, the fetcher's own
    /// cache directory. `None` caches nothing and every round hits the
    /// network.
    cache_dir: Option<std::path::PathBuf>,
    request_time: Mutex<Option<String>>,
    round: Mutex<Option<time::OffsetDateTime>>,
    /// Backoff between transient attempts; the shipped values are
    /// [`RETRY_DELAYS`] and tests shorten them.
    retry_delays: [Duration; (MAX_ATTEMPTS - 1) as usize],
    /// What the round is doing right now, for the UI's source status: set at
    /// the pipeline's visible phases and cleared once the map is ready.
    phase: Mutex<String>,
    /// Each hemisphere's download line (bytes, speed, percent), written by the
    /// two fetch threads and composed into the status as they move. One slot
    /// per hemisphere, in [`HEMISPHERES`] order.
    progress: Mutex<[Option<String>; 2]>,
    /// The user stop the fetch worker hands over: set while a running round
    /// is wanted gone, checked between tiles and inside every chunk read so a
    /// stop lands within a second rather than at the round budget. Behind a
    /// lock so [`CloudSource::set_cancel`] can replace it, and never `None`:
    /// before a worker attaches one, a private flag nobody sets reads false.
    cancel: Mutex<Arc<AtomicBool>>,
    agent: ureq::Agent,
}

/// The processing algorithm's version, stored in every derived map's
/// metadata. Bump when the math changes, so stale derivations are
/// recomputed rather than served.
const ALGORITHM_VERSION: u32 = 2;

/// The raw pair's manifest, the commit point of a stored round: written last,
/// after both PNGs are on disk and verified.
#[derive(serde::Serialize, serde::Deserialize)]
struct RawManifest {
    version: u32,
    algorithm: u32,
    west_sha256: String,
    east_sha256: String,
    round: String,
}

/// The stored raw pair, its bytes and the hashes its manifest named.
struct RawPair {
    west: Vec<u8>,
    east: Vec<u8>,
    hashes: (String, String),
    round: String,
}

/// The derived map's metadata, the commit point of a derivation: only a
/// metadata row that matches the raw hashes, the parameters and the
/// algorithm version lets the derived file be reused.
#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
struct DerivedMeta {
    version: u32,
    algorithm: u32,
    floor: u8,
    ceiling: u8,
    gamma: f64,
    blur: f64,
    west_sha256: String,
    east_sha256: String,
}

/// The hemisphere size a texture resolution's map needs: the map's width
/// halves into two square hemispheres.
pub(crate) fn hemisphere_px(resolution: u32) -> u32 {
    cloud_variant(resolution).1
}

/// How many tiles one side of a hemisphere is split into: the largest grid
/// whose tiles stay at or under [`TILE_PX`], and one tile for a size below
/// it.
pub(crate) fn tiles_per_side(px: u32) -> u32 {
    (px / TILE_PX).max(1)
}

impl EumetsatCloudSource {
    /// One round, with its phase and per-hemisphere progress lines raised
    /// while it works. `fetch_if_changed` clears them on the way out.
    fn fetch_round(&self) -> Result<Option<CloudImage>, String> {
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        let px = self.px();
        let started = Instant::now();
        let deadline = started + ROUND_BUDGET;

        self.set_phase("checking the round list");
        let latest = self.request_latest_round()?;
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        *self
            .request_time
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(latest.clone());
        let cached_raw = self.load_raw_pair(px);
        if let Some(pair) = &cached_raw
            && pair.round == latest
        {
            if self.derived_matches(px, &pair.hashes.0, &pair.hashes.1) {
                info!(px, round = %latest, "the stored raw pair and its derivation are still current");
                self.settle(&latest);
                return Ok(None);
            }
            self.set_phase("processing locally");
            let west = decode_png(&pair.west, px)?;
            let east = decode_png(&pair.east, px)?;
            let bytes =
                self.process_and_store(px, &west, &east, &pair.hashes.0, &pair.hashes.1, started)?;
            self.settle(&latest);
            return Ok(Some(CloudImage {
                bytes: Download::new(bytes),
                etag: None,
                last_modified: None,
            }));
        }

        // Both hemispheres of one round, fetched in parallel over the one
        // deadline: the two requests share nothing but the budget, so a slow
        // west no longer holds the east's start. Only a complete pair goes on
        // to processing, so no half a map is ever stitched to history.
        self.set_phase("downloading the two hemispheres");
        let (west, east) = std::thread::scope(|scope| {
            let west = scope.spawn(|| {
                self.fetch_hemisphere(HEMISPHERES[0].0, 0, HEMISPHERES[0].1, px, deadline)
            });
            let east = scope.spawn(|| {
                self.fetch_hemisphere(HEMISPHERES[1].0, 1, HEMISPHERES[1].1, px, deadline)
            });
            (west.join(), east.join())
        });
        let west = west.map_err(|_| "the west hemisphere's fetch panicked".to_owned())?;
        let east = east.map_err(|_| "the east hemisphere's fetch panicked".to_owned())?;
        let (west_body, west) = west?;
        info!(elapsed = ?started.elapsed(), px, "the west hemisphere arrived");
        let (east_body, east) = east?;
        info!(elapsed = ?started.elapsed(), px, "both hemispheres arrived");
        // A stop that landed mid-round drops it here rather than committing a
        // round the user asked to be rid of: the next poll starts over.
        if self.cancelled() {
            return Err(Self::stop_error());
        }
        self.set_phase("processing the round");
        let west_sha = sha256_hex(&west_body);
        let east_sha = sha256_hex(&east_body);
        self.store_raw_pair(px, &west_body, &east_body, &latest)?;
        if let Some(pair) = &cached_raw
            && west_body == pair.west
            && east_body == pair.east
            && self.derived_matches(px, &pair.hashes.0, &pair.hashes.1)
        {
            debug!(px, "the upstream round matches the stored one");
            self.settle(&latest);
            return Ok(None);
        }
        let bytes = self.process_and_store(px, &west, &east, &west_sha, &east_sha, started)?;
        self.settle(&latest);
        Ok(Some(CloudImage {
            bytes: Download::new(bytes),
            etag: None,
            last_modified: None,
        }))
    }

    pub fn new(settings: EumetsatSettings, cache_dir: Option<std::path::PathBuf>) -> Self {
        let user_agent = format!("sunlit.earth/{}", env!("CARGO_PKG_VERSION"));
        let agent = ureq::Agent::config_builder()
            .user_agent(&user_agent)
            // Errors are classified here, from the status and its headers.
            .http_status_as_error(false)
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_per_call(Some(REQUEST_TIMEOUT))
            // The service is one slow server an ocean away; a user running a
            // local proxy is the standard way around that. The standard
            // variables carry it, and NO_PROXY is honored with it.
            .proxy(ureq::Proxy::try_from_env())
            .build()
            .new_agent();
        Self {
            settings,
            size: Mutex::new(hemisphere_px(DEFAULT_TEXTURE_RESOLUTION)),
            cache_dir,
            request_time: Mutex::new(None),
            round: Mutex::new(None),
            retry_delays: RETRY_DELAYS,
            phase: Mutex::new(String::new()),
            progress: Mutex::new([None, None]),
            cancel: Mutex::new(Arc::new(AtomicBool::new(false))),
            agent,
        }
    }

    /// The startup cache names the layer, hemisphere size and algorithm, so
    /// an old stretched map cannot load before reprocessing. Tuning remains
    /// absent: parameter edits reuse the same derived file rather than
    /// accumulating files. Raw-pair names are independent of this identity.
    fn identity(px: u32) -> String {
        format!("{LAYER}|{px}|v{ALGORITHM_VERSION}")
    }

    /// Set the round's visible phase; an empty string clears it. The download
    /// progress lines belong to the download phase and go with it.
    fn set_phase(&self, phase: &str) {
        phase.clone_into(&mut self.phase.lock().unwrap_or_else(PoisonError::into_inner));
        *self.progress.lock().unwrap_or_else(PoisonError::into_inner) = [None, None];
    }

    /// Set one hemisphere's download line. The slots are per hemisphere, in
    /// [`HEMISPHERES`] order, so the two parallel fetch threads never fight
    /// over one string; the status composes the phase and both lines when it
    /// is read.
    fn set_progress(&self, index: usize, line: &str) {
        let mut progress = self.progress.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = progress.get_mut(index) {
            slot.replace(line.to_owned());
        }
    }

    fn px(&self) -> u32 {
        *self
            .size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether the user's stop is standing: the round is wanted gone, and
    /// every check point between tiles and inside each chunk read unwinds to
    /// the nearest one.
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

    fn request_latest_round(&self) -> Result<String, String> {
        use std::io::Read as _;
        let url = format!("{WMS_URL}?service=WMS&version=1.3.0&request=GetCapabilities");
        let mut response = self
            .agent
            .get(&url)
            .call()
            .map_err(|e| format!("WMS round list: {e}"))?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(format!("WMS round list: HTTP {status}"));
        }
        let mut body = String::new();
        response
            .body_mut()
            .with_config()
            .limit(4 * 1024 * 1024)
            .reader()
            .read_to_string(&mut body)
            .map_err(|e| format!("WMS round list: {e}"))?;
        parse_latest_round(&body)
    }

    fn settle(&self, round: &str) {
        *self.round.lock().unwrap_or_else(PoisonError::into_inner) =
            time::OffsetDateTime::parse(round, &time::format_description::well_known::Rfc3339).ok();
    }

    /// Fetch one hemisphere as a grid of [`TILE_PX`] tiles.
    ///
    /// The service renders each request on the fly and does not support
    /// HTTP range requests (measured: a Range header is answered with the
    /// full 200 body), so a transfer that dies mid-body cannot be resumed
    /// against the server. Small tiles make that survivable: a completed
    /// tile is kept, only the failed ones are retried — first within their
    /// own attempt budget, then in a second pass over whatever is left.
    /// [`TILE_WORKERS`] fetch tiles of one hemisphere concurrently, and the
    /// two hemisphere threads run side by side. Tiles that no pass reaches
    /// fail the round; the worker's backoff starts a fresh one. The user
    /// stop unwinds at the nearest check point of all of them.
    ///
    /// Returns the stitched hemisphere re-encoded as PNG — what the raw pair
    /// stores — beside the decoded map. No tile starts after `deadline`.
    #[expect(
        clippy::too_many_lines,
        reason = "the pool's two passes and their abort checks read better as one body"
    )]
    fn fetch_hemisphere(
        &self,
        name: &str,
        index: usize,
        bbox: &str,
        px: u32,
        deadline: Instant,
    ) -> Result<(Vec<u8>, GrayMap), String> {
        let tiles = tiles_per_side(px);
        let tile_px = px / tiles;
        let total = (tiles * tiles) as usize;
        let canvas = Mutex::new(vec![0.0f32; (px * px) as usize]);
        let done = AtomicUsize::new(0);
        let mut queue: Vec<(u32, u32)> = {
            let coords = |i: u32| (0..i).flat_map(move |r| (0..i).map(move |c| (r, c)));
            coords(tiles).collect()
        };
        for pass in 1..=2 {
            let pending = Mutex::new(std::collections::VecDeque::from(queue.clone()));
            let failed = Mutex::new(Vec::new());
            // A permanent refusal applies to the layer, not the tile: every
            // other tile would refuse too, so the workers stop at once.
            let abort = AtomicBool::new(false);
            std::thread::scope(|scope| {
                for _ in 0..TILE_WORKERS {
                    scope.spawn(|| {
                        loop {
                            if abort.load(Ordering::SeqCst) {
                                return;
                            }
                            // A stop lands between tiles: no further request
                            // is started, and what this pass already holds
                            // is thrown away with the round.
                            if self.cancelled() {
                                abort.store(true, Ordering::SeqCst);
                                return;
                            }
                            let Some((row, col)) = pending
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .pop_front()
                            else {
                                return;
                            };
                            match self
                                .fetch_tile(name, index, bbox, row, col, tiles, tile_px, deadline)
                            {
                                Ok(tile) => {
                                    let mut grid =
                                        canvas.lock().unwrap_or_else(PoisonError::into_inner);
                                    paste_tile(&mut grid, px, &tile, row, col, tile_px, tiles);
                                    drop(grid);
                                    let done = done.fetch_add(1, Ordering::SeqCst) + 1;
                                    self.set_progress(
                                        index,
                                        &format!("{name}: {done}/{total} tiles"),
                                    );
                                }
                                Err(FetchError::Permanent(message)) => {
                                    abort.store(true, Ordering::SeqCst);
                                    failed
                                        .lock()
                                        .unwrap_or_else(PoisonError::into_inner)
                                        .push((row, col, message));
                                    return;
                                }
                                Err(FetchError::Transient(message, _)) => {
                                    failed
                                        .lock()
                                        .unwrap_or_else(PoisonError::into_inner)
                                        .push((row, col, message));
                                }
                            }
                        }
                    });
                }
            });
            // The stop is answered before the abort's own bookkeeping: a
            // cancel-abort may carry no recorded failure at all, the workers
            // having simply stopped between tiles.
            if self.cancelled() {
                return Err(Self::stop_error());
            }
            if abort.load(Ordering::SeqCst) {
                let failed = failed.into_inner().unwrap_or_default();
                let Some((_, _, message)) = failed.first() else {
                    unreachable!("an abort is only set with a recorded failure");
                };
                return Err(format!("EUMETSAT {name} hemisphere: {message}"));
            }
            let failed = failed.into_inner().unwrap_or_default();
            if failed.is_empty() {
                let pixels = canvas.into_inner().unwrap_or_default();
                let map = GrayMap::new(px, px, pixels).expect("the tiled rectangle");
                let png = encode_png(&map);
                return Ok((png, map));
            }
            let shortfall = total - done.load(Ordering::SeqCst) - failed.len();
            if shortfall > 0 {
                return Err(format!(
                    "EUMETSAT {name} hemisphere: the round budget is spent with                      {shortfall}/{total} tiles unfetched"
                ));
            }
            if pass == 2 {
                let names: Vec<String> = failed
                    .iter()
                    .map(|(r, c, m)| format!("({r},{c}): {m}"))
                    .collect();
                return Err(format!(
                    "EUMETSAT {name} hemisphere: {}/{} tiles failed after both passes: {}",
                    failed.len(),
                    total,
                    names.join("; ")
                ));
            }
            queue = failed.iter().map(|(r, c, _)| (*r, *c)).collect();
        }
        unreachable!("both passes return")
    }

    /// One tile with its own attempt budget: transient failures retried with
    /// the round's backoff and a Retry-After honored; a permanent refusal
    /// comes back as [`FetchError::Permanent`], which fails the hemisphere
    /// at once — the same request answered the same way, and every other
    /// tile would refuse too.
    #[expect(
        clippy::too_many_arguments,
        reason = "the tile's grid position, size and shared progress slot, handed over at once"
    )]
    fn fetch_tile(
        &self,
        name: &str,
        index: usize,
        bbox: &str,
        row: u32,
        col: u32,
        tiles: u32,
        tile_px: u32,
        deadline: Instant,
    ) -> Result<GrayMap, FetchError> {
        let tile_bbox = sub_bbox(bbox, row, col, tiles);
        let mut url = build_wms_url(&tile_bbox, tile_px);
        if let Some(round) = self
            .request_time
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
        {
            use std::fmt::Write as _;
            write!(url, "&time={round}").expect("writing a string");
        }
        for attempt in 1..=MAX_ATTEMPTS {
            if self.cancelled() {
                return Err(FetchError::Permanent(Self::stop_error()));
            }
            if Instant::now() >= deadline {
                return Err(FetchError::Permanent(
                    "the round budget is spent before attempt {attempt}".to_owned(),
                ));
            }
            match self.request_hemisphere(name, index, &url, tile_px, MAX_BODY_BYTES) {
                Ok((_, map)) => return Ok(map),
                Err(FetchError::Permanent(message)) => {
                    return Err(FetchError::Permanent(format!(
                        "tile {row},{col}: {message}"
                    )));
                }
                Err(FetchError::Transient(message, retry_after)) => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(FetchError::Transient(
                            format!(
                                "tile {row},{col} failed after {MAX_ATTEMPTS} attempts: {message}"
                            ),
                            retry_after,
                        ));
                    }
                    let delay = retry_after.unwrap_or(self.retry_delays[(attempt - 1) as usize]);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if delay >= remaining {
                        return Err(FetchError::Transient(
                            format!(
                                "tile {row},{col}: retrying after {delay:?} would pass the round budget"
                            ),
                            None,
                        ));
                    }
                    info!(attempt, hemisphere = name, tile = ?(row, col), delay = ?delay, "retrying a tile");
                    // The retry nap wakes on a stop: a stopped round must
                    // not sit out its backoff before noticing, and the next
                    // attempt's own check answers it.
                    let until = Instant::now() + delay;
                    while !self.cancelled() {
                        let left = until.saturating_duration_since(Instant::now());
                        if left.is_zero() {
                            break;
                        }
                        std::thread::sleep(left.min(REST_SLICE));
                    }
                }
            }
        }
        unreachable!("every loop path returns")
    }

    /// One attempt of one hemisphere: request, classify, read with live
    /// progress, validate.
    fn request_hemisphere(
        &self,
        name: &str,
        index: usize,
        url: &str,
        px: u32,
        max_body: u64,
    ) -> Result<(Vec<u8>, GrayMap), FetchError> {
        let mut response = self
            .agent
            .get(url)
            .call()
            .map_err(|e| FetchError::Transient(format!("transport: {e}"), None))?;
        let status = response.status().as_u16();
        if status == 429 || status >= 500 {
            let retry_after = retry_after_header(response.headers());
            return Err(FetchError::Transient(format!("HTTP {status}"), retry_after));
        }
        if !(200..300).contains(&status) {
            return Err(FetchError::Permanent(format!("HTTP {status}")));
        }
        let content_type = response
            .headers()
            .get("Content-Type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !content_type.starts_with("image/") {
            // A WMS exception page arrives as text/xml: the service refused
            // the request, and the same request answered the same way.
            return Err(FetchError::Permanent(format!(
                "the service answered {content_type:?}, not an image"
            )));
        }
        // Read incrementally and report the round's progress as it moves:
        // bytes and speed while the length is unknown, a percent beside them
        // once Content-Length says how big the map is.
        let total = response
            .headers()
            .get("Content-Length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        let mut reader = response.body_mut().with_config().limit(max_body).reader();
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
            // One read call's worth of the map: big enough that the 500 ms
            // progress tick sees movement, small enough for the stack.
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
                    FetchError::Permanent(format!("the response exceeds the {max_body} byte cap"))
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
                            "{name}: {} of {} ({}) — {:.0}%",
                            mib(body.len()),
                            mib(total),
                            speed_text(speed),
                            percent
                        )
                    }
                    None => format!("{name}: {} ({})", mib(body.len()), speed_text(speed)),
                };
                self.set_progress(index, &line);
            }
        }
        let map = decode_png(&body, px).map_err(FetchError::Permanent)?;
        Ok((body, map))
    }
}

impl CloudSource for EumetsatCloudSource {
    fn publication(&self) -> Option<super::cloud_source::CloudPublication> {
        Some(super::cloud_source::CloudPublication {
            interval: UPSTREAM_REFRESH,
            latest: *self.round.lock().unwrap_or_else(PoisonError::into_inner),
        })
    }

    fn settled_note(&self) -> Option<String> {
        self.round
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .map(|round| {
                format!(
                    "round {} UTC",
                    round
                        .format(time::macros::format_description!(
                            "[year]-[month]-[day] [hour]:[minute]"
                        ))
                        .expect("round format")
                )
            })
    }

    fn wait(&self) -> Duration {
        CloudWait::LongFetch.duration()
    }

    fn set_cancel(&self, flag: Arc<AtomicBool>) {
        *self.cancel.lock().unwrap_or_else(PoisonError::into_inner) = flag;
    }

    fn status(&self) -> Option<String> {
        let phase = self.phase.lock().unwrap_or_else(PoisonError::into_inner);
        let progress = self.progress.lock().unwrap_or_else(PoisonError::into_inner);
        let parts: Vec<&str> = progress.iter().flatten().map(String::as_str).collect();
        if parts.is_empty() {
            (!phase.is_empty()).then(|| phase.clone())
        } else if phase.is_empty() {
            Some(parts.join(" · "))
        } else {
            Some(format!("{phase} — {}", parts.join(" · ")))
        }
    }

    fn prepare(&self, resolution: u32) -> CloudTarget {
        let px = hemisphere_px(resolution);
        let mut size = self
            .size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *size = px;
        CloudTarget::eumetsat(&Self::identity(px))
    }

    fn fetch_if_changed(&self, _known_etag: Option<&str>) -> Result<Option<CloudImage>, String> {
        let outcome = self.fetch_round();
        // Whatever the round came to, it is over: the phase and the download
        // lines named what it was doing while it ran. Without this the last
        // phase outlives the round — "64/64 tiles" hanging on the status line
        // for an hour — and hides the worker's own answer, which is what
        // ready, stopped and retrying are.
        self.set_phase("");
        outcome
    }

    fn describe(&self) -> String {
        format!("eumetsat {LAYER}")
    }
}

impl EumetsatCloudSource {
    /// Store one round's raw pair: each PNG written to a temporary file and
    /// verified by decoding what landed, then renamed, and the manifest
    /// written last as the commit point. A failure anywhere leaves the
    /// previous pair and its manifest untouched.
    fn store_raw_pair(&self, px: u32, west: &[u8], east: &[u8], round: &str) -> Result<(), String> {
        let Some(dir) = &self.cache_dir else {
            return Ok(());
        };
        let west_tmp = dir.join(format!("eumetsat_raw_west_{px}.png.tmp"));
        let east_tmp = dir.join(format!("eumetsat_raw_east_{px}.png.tmp"));
        std::fs::write(&west_tmp, west).map_err(|e| format!("the raw west file failed: {e}"))?;
        std::fs::write(&east_tmp, east).map_err(|e| format!("the raw east file failed: {e}"))?;
        // Verify the written bytes before anything is published under a real
        // name: a decode of what is on disk is the check the manifest's claim
        // rests on.
        decode_png(
            &std::fs::read(&west_tmp).map_err(|e| format!("the raw west read back failed: {e}"))?,
            px,
        )?;
        decode_png(
            &std::fs::read(&east_tmp).map_err(|e| format!("the raw east read back failed: {e}"))?,
            px,
        )?;
        let west_path = dir.join(format!("eumetsat_raw_west_{px}.png"));
        let east_path = dir.join(format!("eumetsat_raw_east_{px}.png"));
        std::fs::rename(&west_tmp, &west_path)
            .map_err(|e| format!("the raw west commit failed: {e}"))?;
        std::fs::rename(&east_tmp, &east_path)
            .map_err(|e| format!("the raw east commit failed: {e}"))?;
        let manifest = RawManifest {
            version: 1,
            algorithm: ALGORITHM_VERSION,
            west_sha256: sha256_hex(west),
            east_sha256: sha256_hex(east),
            round: round.to_owned(),
        };
        let manifest =
            toml::to_string(&manifest).map_err(|e| format!("the raw manifest failed: {e}"))?;
        write_committed(
            &dir.join(format!("eumetsat_raw_{px}_meta.toml")),
            manifest.as_bytes(),
        )?;
        info!(px, "the raw pair is stored and committed");
        Ok(())
    }

    /// Load the stored raw pair, trusting it only when the manifest's hashes
    /// match what is on disk. `None` is "no usable pair", which the next
    /// network round replaces; the files are left alone either way.
    fn load_raw_pair(&self, px: u32) -> Option<RawPair> {
        let dir = self.cache_dir.as_ref()?;
        let manifest = std::fs::read(dir.join(format!("eumetsat_raw_{px}_meta.toml"))).ok()?;
        let manifest: RawManifest = toml::from_str(&String::from_utf8_lossy(&manifest)).ok()?;
        let west = std::fs::read(dir.join(format!("eumetsat_raw_west_{px}.png"))).ok()?;
        let east = std::fs::read(dir.join(format!("eumetsat_raw_east_{px}.png"))).ok()?;
        let (west_sha, east_sha) = (sha256_hex(&west), sha256_hex(&east));
        if manifest.west_sha256 != west_sha || manifest.east_sha256 != east_sha {
            warn!(
                px,
                "the stored raw pair does not match its manifest, ignoring it"
            );
            return None;
        }
        Some(RawPair {
            west,
            east,
            hashes: (west_sha, east_sha),
            round: manifest.round,
        })
    }

    /// Whether the stored derived map is reusable: it exists, and its
    /// metadata matches the raw hashes, the current parameters and the
    /// algorithm version, all together.
    #[expect(
        clippy::float_cmp,
        reason = "the metadata must equal the tuning bit for bit, not approximately"
    )]
    fn derived_matches(&self, px: u32, west_sha: &str, east_sha: &str) -> bool {
        let Some(dir) = &self.cache_dir else {
            return false;
        };
        let (width, height) = (2 * px, px);
        if !dir
            .join(format!("eumetsat_map_{width}x{height}.jpg"))
            .exists()
        {
            return false;
        }
        let Ok(meta) = std::fs::read(dir.join(format!("eumetsat_map_{width}x{height}_meta.toml")))
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
            && meta.west_sha256 == west_sha
            && meta.east_sha256 == east_sha
    }

    /// Process one round's raw pair into the derived map, and store it with
    /// its metadata. The staged float maps are dropped as the pipeline moves
    /// on; what stays is the JPEG and its metadata row.
    fn process_and_store(
        &self,
        px: u32,
        west: &GrayMap,
        east: &GrayMap,
        west_sha: &str,
        east_sha: &str,
        started: Instant,
    ) -> Result<Vec<u8>, String> {
        let stitched = stitch(west, east);
        let map = process_map(&stitched, &self.settings);
        drop(stitched);
        let bytes = encode_jpeg(&map);
        info!(
            bytes = bytes.len(),
            elapsed = ?started.elapsed(),
            "the EUMETSAT round is generated"
        );
        if let Some(dir) = &self.cache_dir {
            let (width, height) = (2 * px, px);
            let meta = DerivedMeta {
                version: 1,
                algorithm: ALGORITHM_VERSION,
                floor: self.settings.floor,
                ceiling: self.settings.ceiling,
                gamma: self.settings.gamma,
                blur: self.settings.blur,
                west_sha256: west_sha.to_owned(),
                east_sha256: east_sha.to_owned(),
            };
            let meta =
                toml::to_string(&meta).map_err(|e| format!("the derived metadata failed: {e}"))?;
            let committed = (|| -> Result<(), String> {
                write_committed(
                    &dir.join(format!("eumetsat_map_{width}x{height}.jpg")),
                    &bytes,
                )?;
                write_committed(
                    &dir.join(format!("eumetsat_map_{width}x{height}_meta.toml")),
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

/// Write `bytes` to `path` through a temporary file, the metadata's commit
/// point: readers see either the previous file or the complete new one.
pub(crate) fn write_committed(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).map_err(|e| format!("the temporary write failed: {e}"))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("the commit failed: {e}"))
}

/// The SHA-256 of `bytes`, as lowercase hex.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// The Retry-After header, in whole seconds. The HTTP-date form is not
/// honored: only a delay this round can actually wait out is.
/// The sub-bbox of tile `(row, col)` of a `tiles`-per-side grid over `bbox`,
/// in the same latitude,longitude order the hemisphere constants use.
fn sub_bbox(bbox: &str, row: u32, col: u32, tiles: u32) -> String {
    let parts: Vec<f64> = bbox
        .split(',')
        .filter_map(|v| v.trim().parse().ok())
        .collect();
    let [min_lat, min_lon, max_lat, max_lon] = parts[..] else {
        return bbox.to_owned();
    };
    let span_lat = (max_lat - min_lat) / f64::from(tiles);
    let span_lon = (max_lon - min_lon) / f64::from(tiles);
    let lat0 = min_lat + span_lat * f64::from(row);
    let lat1 = min_lat + span_lat * f64::from(row + 1);
    let lon0 = min_lon + span_lon * f64::from(col);
    let lon1 = min_lon + span_lon * f64::from(col + 1);
    format!("{lat0},{lon0},{lat1},{lon1}")
}

/// Paste a tile's pixels into the canvas at its grid position.
fn paste_tile(
    canvas: &mut [f32],
    px: u32,
    tile: &GrayMap,
    row: u32,
    col: u32,
    tile_px: u32,
    tiles: u32,
) {
    let (w, h) = (px as usize, tile_px as usize);
    let x0 = col as usize * h;
    // Tile rows count from the bbox's min_lat — south for both hemispheres —
    // while the canvas counts from its top, north: tile row 0 is the bottom
    // band, so its offset counts from the other end. Measured against the
    // service: a tile's top row is its max_lat edge, where the polar blind
    // zone sits.
    let y0 = (tiles - 1 - row) as usize * h;
    for y in 0..h {
        let dst = (y0 + y) * w + x0;
        canvas[dst..dst + h].copy_from_slice(&tile.pixels()[y * h..(y + 1) * h]);
    }
}

/// Encode a grayscale map as a lossless PNG, the raw pair's artifact: a
/// value the live round produced is exactly what a reprocessed round reads
/// back, since every canvas value already sits on the u8 grid the tiles
/// arrived on.
fn encode_png(map: &GrayMap) -> Vec<u8> {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "the values are already on the u8 grid the tiles arrived on"
    )]
    let bytes: Vec<u8> = map
        .pixels()
        .iter()
        .map(|&v| (v.clamp(0.0, 1.0) * 255.0) as u8)
        .collect();
    let mut out = Vec::new();
    image::codecs::png::PngEncoder::new(&mut out)
        .write_image(
            &bytes,
            map.width(),
            map.height(),
            image::ExtendedColorType::L8,
        )
        .expect("a grayscale PNG of an in-memory buffer");
    out
}

pub(crate) fn retry_after_header(headers: &ureq::http::HeaderMap) -> Option<Duration> {
    let value = headers.get("Retry-After")?.to_str().ok()?;
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use ureq::http::{Request, Response};
    use ureq::middleware::MiddlewareNext;
    use ureq::{Body, SendBody};

    use super::*;

    const FIXTURE_ROUND: &str = "2026-11-18T06:00:00Z";

    fn capabilities_response(round: &str) -> Response<Body> {
        Response::builder().status(200).body(Body::builder().data(format!(
            r#"<WMS_Capabilities xmlns="http://www.opengis.net/wms"><Capability><Layer><Layer><Name>{LAYER}</Name><Dimension name="time" default="{round}">2026-11-17T00:00:00Z/{round}/PT3H</Dimension></Layer></Layer></Capability></WMS_Capabilities>"#
        ).into_bytes())).unwrap()
    }

    fn gray(width: u32, height: u32, fill: f32) -> GrayMap {
        GrayMap::new(width, height, vec![fill; width as usize * height as usize]).unwrap()
    }

    fn png_bytes(width: u32, height: u32, value: u8) -> Vec<u8> {
        let img = image::GrayImage::from_pixel(width, height, image::Luma([value]));
        let mut out = Vec::new();
        image::DynamicImage::ImageLuma8(img)
            .write_to(&mut Cursor::new(&mut out), image::ImageFormat::Png)
            .unwrap();
        out
    }

    /// An agent whose responses come from `make`, one per call, with the call
    /// count left in the counter for the test to assert on.
    fn stub_agent(
        make: impl Fn(usize) -> Result<Response<Body>, ureq::Error> + Send + Sync + 'static,
    ) -> (ureq::Agent, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .middleware(move |req: Request<SendBody>, _: MiddlewareNext| {
                if req.uri().to_string().contains("GetCapabilities") {
                    return Ok(capabilities_response(FIXTURE_ROUND));
                }
                let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
                make(n)
            })
            .build()
            .new_agent();
        (agent, calls)
    }

    fn png_response(px: u32, value: u8) -> Response<Body> {
        Response::builder()
            .status(200)
            .header("Content-Type", "image/png")
            .body(Body::builder().data(png_bytes(px, px, value)))
            .unwrap()
    }

    #[test]
    fn capabilities_select_the_latest_time_of_only_the_cloud_layer() {
        let xml = r#"<w:WMS_Capabilities xmlns:w="http://www.opengis.net/wms"><w:Layer>
            <w:Layer><w:Name>other</w:Name><w:Dimension name="time">2026-11-19T00:00:00Z</w:Dimension></w:Layer>
            <w:Layer><w:Name>mumi:worldcloudmap_ir108</w:Name><w:Dimension name="time" default="2026-11-18T03:00:00Z">2026-11-18T09:00:00.000Z,2026-11-18T06:00:00Z</w:Dimension></w:Layer>
        </w:Layer></w:WMS_Capabilities>"#;
        assert_eq!(parse_latest_round(xml).unwrap(), "2026-11-18T09:00:00Z");
        for invalid in [
            "not xml",
            "<Layer/>",
            "<Layer><Name>mumi:worldcloudmap_ir108</Name><Dimension name=\"time\">invalid/PT3H</Dimension></Layer>",
        ] {
            assert!(parse_latest_round(invalid).is_err());
        }
    }

    #[test]
    fn failed_metadata_preserves_the_settled_round_and_fetches_no_tiles() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.settle(FIXTURE_ROUND);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        source.agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .middleware(move |req: Request<SendBody>, _: MiddlewareNext| {
                assert!(req.uri().to_string().contains("GetCapabilities"));
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(Response::builder()
                    .status(503)
                    .body(Body::builder().data(Vec::new()))
                    .unwrap())
            })
            .build()
            .new_agent();
        assert!(source.fetch_if_changed(None).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            source.publication().unwrap().latest,
            Some(time::macros::datetime!(2026-11-18 06:00 UTC))
        );
        assert_eq!(
            source.settled_note().as_deref(),
            Some("round 2026-11-18 06:00 UTC")
        );
        assert_eq!(source.status(), None);
    }

    #[test]
    fn identical_tiles_still_commit_the_new_round() {
        let (_scratch, mut source) = scratch_cache("eumetsat_identical_round");
        *source.size.lock().unwrap() = 16;
        let round = Arc::new(AtomicUsize::new(0));
        let current = Arc::clone(&round);
        let tiles = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&tiles);
        source.agent = ureq::Agent::config_builder()
            .middleware(move |req: Request<SendBody>, _: MiddlewareNext| {
                if req.uri().to_string().contains("GetCapabilities") {
                    return Ok(capabilities_response(
                        if current.load(Ordering::SeqCst) == 0 {
                            FIXTURE_ROUND
                        } else {
                            "2026-11-18T09:00:00Z"
                        },
                    ));
                }
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(png_response(16, 80))
            })
            .build()
            .new_agent();
        assert!(source.fetch_if_changed(None).unwrap().is_some());
        round.store(1, Ordering::SeqCst);
        assert!(source.fetch_if_changed(None).unwrap().is_none());
        assert_eq!(
            source.load_raw_pair(16).unwrap().round,
            "2026-11-18T09:00:00Z"
        );
        assert!(source.fetch_if_changed(None).unwrap().is_none());
        assert_eq!(tiles.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn documented_constants_are_the_generator_values() {
        assert_eq!(WMS_URL, "https://view.eumetsat.int/geoserver/ows");
        assert_eq!(LAYER, "mumi:worldcloudmap_ir108");
        assert_eq!(HEMISPHERES[0], ("west", "-90,-180,90,0"));
        assert_eq!(HEMISPHERES[1], ("east", "-90,0,90,180"));
        assert!((TRIM_THRESHOLD - 236.0 / 255.0).abs() < f32::EPSILON);
        assert_eq!(JPEG_QUALITY, 88);
        assert_eq!(REQUEST_TIMEOUT, Duration::from_mins(5));
        assert_eq!(CONNECT_TIMEOUT, Duration::from_secs(15));
        assert_eq!(ROUND_BUDGET, Duration::from_mins(25));
        assert_eq!(MAX_ATTEMPTS, 3);
        assert_eq!(
            RETRY_DELAYS,
            [Duration::from_secs(10), Duration::from_secs(20)]
        );
        assert_eq!(MAX_BODY_BYTES, 50 * 1024 * 1024);
        assert_eq!(MAX_HEMISPHERE_SIDE, 4096);
        assert_eq!(hemisphere_px(2048), 1024);
        assert_eq!(hemisphere_px(4096), 2048);
        assert_eq!(hemisphere_px(8192), 4096);
        assert!(hemisphere_px(8192) <= MAX_HEMISPHERE_SIDE);
    }

    #[test]
    fn the_wms_url_carries_the_documented_parameters_and_lat_lon_bbox() {
        let url = build_wms_url("-90,-180,90,0", 4096);
        assert!(url.starts_with("https://view.eumetsat.int/geoserver/ows?"));
        for part in [
            "service=WMS",
            "request=GetMap",
            "version=1.3.0",
            "layers=mumi:worldcloudmap_ir108",
            "styles=",
            "format=image/png",
            "crs=EPSG:4326",
            // WMS 1.3.0 with EPSG:4326 orders the bbox latitude first.
            "bbox=-90,-180,90,0",
            "width=4096&height=4096",
        ] {
            assert!(url.contains(part), "{part} missing from {url}");
        }
    }

    #[test]
    fn stitch_puts_west_left_and_makes_a_two_to_one_map() {
        let west = gray(4, 4, 0.25);
        let east = gray(4, 4, 0.75);
        let stitched = stitch(&west, &east);
        assert_eq!(stitched.width(), 8);
        assert_eq!(stitched.height(), 4);
        for y in 0..4 {
            assert!(stitched.pixels()[y * 8] < 0.3, "west's column is left");
            assert!(stitched.pixels()[y * 8 + 7] > 0.7, "east's column is right");
        }
    }

    /// Irregular caps spanning 74..81 N and 65..81 S, with antialiased white.
    fn irregular_caps() -> GrayMap {
        let (w, h) = (128usize, 180usize);
        let mut image = gray(128, 180, 0.0);
        for y in 0..h {
            for x in 0..w {
                let cap = y < 9 + (x * 7) % 8 || y >= h - (9 + (x * 11) % 17);
                image.pixels[y * w + x] = if cap {
                    f32::from([236u8, 247, 254, 255][(x + y) % 4]) / 255.0
                } else {
                    // Every valid sample is distinct, so resampling cannot hide.
                    #[expect(clippy::cast_precision_loss, reason = "small fixture indices")]
                    let value = 0.1 + (y * w + x) as f32 / (w * h) as f32 * 0.6;
                    value
                };
            }
        }
        image
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "unchanged samples and the mask must be exact"
    )]
    fn trim_poles_preserves_every_valid_coordinate_without_poleward_copies() {
        let image = irregular_caps();
        let (result, _) = trim_poles(&image);
        assert_eq!((result.width(), result.height()), (128, 180));
        for y in 0..180 {
            for x in 0..128 {
                let i = y * 128 + x;
                let cap = y < 9 + (x * 7) % 8 || y >= 180 - (9 + (x * 11) % 17);
                let expected = if cap { 0.0 } else { image.pixels[i] };
                assert_eq!(result.pixels[i], expected, "coordinate ({x}, {y}) moved");
            }
        }
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "masking must preserve exact zeros and untouched samples"
    )]
    fn all_white_columns_and_deep_caps_never_fall_back_to_valid_data() {
        let mut image = gray(8, 180, 0.4);
        for y in 0..180 {
            image.pixels[y * 8] = 247.0 / 255.0;
            image.pixels[y * 8 + 7] = 1.0;
            if !(80..100).contains(&y) {
                image.pixels[y * 8 + 3] = 1.0;
            }
        }
        let (result, _) = trim_poles(&image);
        let processed = process_map(
            &image,
            &EumetsatSettings {
                blur: 1.0,
                ..EumetsatSettings::default()
            },
        );
        for y in 0..180 {
            assert_eq!(
                processed.pixels[y * 8],
                0.0,
                "blur cannot revive an empty column"
            );
            assert_eq!(processed.pixels[y * 8 + 7], 0.0);
            assert_eq!(result.pixels[y * 8], 0.0);
            assert_eq!(result.pixels[y * 8 + 7], 0.0);
            assert_eq!(result.pixels[y * 8 + 1], 0.4, "neighbor remains valid");
            assert_eq!(
                result.pixels[y * 8 + 3],
                if (80..100).contains(&y) { 0.4 } else { 0.0 }
            );
        }
        for fill in [236.0 / 255.0, 1.0] {
            assert!(
                trim_poles(&gray(8, 180, fill))
                    .0
                    .pixels
                    .iter()
                    .all(|&v| v == 0.0)
            );
        }
    }

    #[test]
    fn one_row_and_full_data_maps_keep_their_boundaries() {
        let row = GrayMap::new(4, 1, vec![0.1, 1.0, 0.7, 247.0 / 255.0]).unwrap();
        assert_eq!(trim_poles(&row).0.pixels, [0.1, 0.0, 0.7, 0.0]);
        let mut full = gray(16, 16, 0.5);
        for y in 0..16 {
            for x in 0..16 {
                full.pixels[y * 16 + x] = f32::from(u8::try_from(x + y * 13).unwrap()) / 255.0;
            }
        }
        assert_eq!(trim_poles(&full).0.pixels, full.pixels);
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 1.0,
            blur: 0.0,
        };
        assert_eq!(process_map(&full, &settings).pixels, full.pixels);
        assert_eq!(process_map(&row, &settings).pixels, [0.1, 0.0, 0.7, 0.0]);
        // A single supported latitude row is retained, not duplicated into
        // the blind zone; the two fades meet at its nearest boundary.
        let mut single = gray(4, 180, 1.0);
        single.pixels[90 * 4..91 * 4].fill(0.5);
        let processed = process_map(&single, &settings);
        for y in 0..180 {
            let expected = if y == 90 { 0.5 * 0.028 } else { 0.0 };
            assert!((processed.pixels[y * 4] - expected).abs() < 1e-7);
        }
    }

    /// Exercise the real storage pipeline without a disk cache or network.
    fn processed_fixture(image: &GrayMap, settings: EumetsatSettings) -> image::GrayImage {
        let px = image.height();
        assert_eq!(image.width(), 2 * px);
        let mut west = Vec::new();
        let mut east = Vec::new();
        for row in image.pixels.chunks_exact(image.width() as usize) {
            west.extend_from_slice(&row[..px as usize]);
            east.extend_from_slice(&row[px as usize..]);
        }
        let source = EumetsatCloudSource::new(settings, None);
        let bytes = source
            .process_and_store(
                px,
                &GrayMap::new(px, px, west).unwrap(),
                &GrayMap::new(px, px, east).unwrap(),
                "fixture-west",
                "fixture-east",
                Instant::now(),
            )
            .unwrap();
        image::load_from_memory(&bytes).unwrap().into_luma8()
    }

    fn flat_caps(height: u32, top: u32, bottom: u32, value: f32) -> GrayMap {
        let mut image = gray(2 * height, height, value);
        let w = image.width() as usize;
        image.pixels[..top as usize * w].fill(247.0 / 255.0);
        image.pixels[bottom as usize * w..].fill(1.0);
        image
    }

    #[test]
    fn the_feather_is_five_degrees_smoothstep_after_gamma_at_sample_centers() {
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 0.5,
            blur: 0.0,
        };
        let coarse = processed_fixture(&flat_caps(180, 18, 162, 0.5), settings.clone());
        let fine = processed_fixture(&flat_caps(540, 54, 486, 0.5), settings);
        let mut previous = 0u8;
        for offset in 0..8u32 {
            let north = coarse.get_pixel(90, 18 + offset).0[0];
            let south = coarse.get_pixel(90, 161 - offset).0[0];
            let higher_resolution = fine.get_pixel(270, 54 + 3 * offset + 1).0[0];
            let t = ((f64::from(offset) + 0.5) / 5.0).min(1.0);
            let expected = 255.0 * 0.5f64.sqrt() * t * t * (3.0 - 2.0 * t);
            assert!(
                (f64::from(north) - expected).abs() <= 3.0,
                "row {offset}: {north} vs {expected}"
            );
            assert!(north >= previous, "fade must be monotonic into support");
            assert!(
                north.abs_diff(south) <= 3,
                "both boundaries use sample centers"
            );
            assert!(
                north.abs_diff(higher_resolution) <= 3,
                "latitude width must not depend on resolution"
            );
            previous = north;
        }
    }

    #[test]
    fn gamma_and_blur_do_not_resurrect_caps_or_blur_their_white_into_support() {
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 0.5,
            blur: 1.0,
        };
        let image = flat_caps(180, 18, 162, 0.25);
        let float_map = process_map(&image, &settings);
        let result = processed_fixture(&image, settings);
        // Independent reference: zero no-data BEFORE blur, feather AFTER it.
        let mut reference = gray(360, 180, 0.5);
        reference.pixels[..18 * 360].fill(0.0);
        reference.pixels[162 * 360..].fill(0.0);
        let reference = gaussian_blur(&reference, 1.0);
        for y in 0..180u32 {
            let distance = f64::from(y).min(179.0 - f64::from(y)) + 0.5 - 18.0;
            let t = (distance / 5.0).clamp(0.0, 1.0);
            let expected = f64::from(reference.pixels[y as usize * 360 + 90])
                * t
                * t
                * (3.0 - 2.0 * t)
                * 255.0;
            let actual = result.get_pixel(90, y).0[0];
            // JPEG is lossy, but the pre-encode no-data mask must be exact.
            assert!(
                (f64::from(actual) - expected).abs() <= 3.0,
                "row {y}: {actual} vs {expected}"
            );
            assert!(
                (f64::from(float_map.pixels[y as usize * 360 + 90]) - expected / 255.0).abs()
                    < 1e-7
            );
            if !(18..162).contains(&y) {
                assert!(
                    float_map.pixels[y as usize * 360..(y as usize + 1) * 360]
                        .iter()
                        .all(|&v| v == 0.0)
                );
            }
        }
    }

    #[test]
    fn bright_interior_clouds_are_retained_even_when_saturated() {
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 1.0,
            blur: 0.0,
        };
        let mut image = flat_caps(64, 8, 56, 0.4);
        for y in 24..40 {
            for x in 0..128 {
                image.pixels[y * 128 + x] = f32::from([236u8, 247, 254, 255][x / 32]) / 255.0;
            }
        }
        let result = processed_fixture(&image, settings);
        for (x, expected) in [(16, 236u8), (48, 247), (80, 254), (112, 255)] {
            let actual = result.get_pixel(x, 32).0[0];
            assert!(
                actual.abs_diff(expected) <= 2,
                "interior cloud at {x}: {actual}"
            );
        }
    }

    #[test]
    fn processing_does_not_fade_edges_without_a_cap() {
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 1.0,
            blur: 0.0,
        };
        for (top, bottom) in [(0, 64), (0, 56), (8, 64)] {
            let result = processed_fixture(&flat_caps(64, top, bottom, 0.4), settings.clone());
            if top == 0 {
                assert!(result.get_pixel(32, 0).0[0].abs_diff(102) <= 2);
            }
            if bottom == 64 {
                assert!(result.get_pixel(32, 63).0[0].abs_diff(102) <= 2);
            }
        }
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "saturated coverage must map to exactly one"
    )]
    fn coverage_keeps_saturated_values_after_the_support_mask() {
        let image = GrayMap::new(1, 2, vec![1.0, 253.0 / 255.0]).unwrap();
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 1.0,
            blur: 0.0,
        };
        let result = to_coverage(&image, &settings);
        assert_eq!(
            result.pixels()[0],
            1.0,
            "the mask, not brightness, rejects no-data"
        );
        assert!(result.pixels()[1] > 0.9, "253/255 is cloud, not no-data");
    }

    #[test]
    fn coverage_gamma_below_one_brightens_midtones() {
        let image = gray(1, 1, 0.5);
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 0.5,
            blur: 0.0,
        };
        let result = to_coverage(&image, &settings);
        assert!((result.pixels()[0] - 0.5f32.sqrt()).abs() < 0.01);
    }

    #[test]
    fn coverage_spans_the_floor_and_ceiling() {
        let image = GrayMap::new(1, 2, vec![100.0 / 255.0, 180.0 / 255.0]).unwrap();
        let settings = EumetsatSettings {
            floor: 100,
            ceiling: 180,
            gamma: 1.0,
            blur: 0.0,
        };
        let result = to_coverage(&image, &settings);
        assert!(result.pixels()[0].abs() < 0.01);
        assert!((result.pixels()[1] - 1.0).abs() < 0.01);
    }

    #[test]
    fn blur_smooths_a_spike_and_a_zero_sigma_is_a_no_op() {
        let mut pixels = vec![0.0f32; 9 * 9];
        pixels[4 * 9 + 4] = 1.0;
        let image = GrayMap::new(9, 9, pixels).unwrap();
        let blurred = gaussian_blur(&image, 1.0);
        let center = blurred.pixels()[4 * 9 + 4];
        let edge = blurred.pixels()[0];
        assert!(center < 1.0, "the spike spread");
        assert!(center > edge, "the center keeps the most");
        assert!(blurred.pixels().iter().all(|v| v.is_finite()));
        let untouched = gaussian_blur(&image, 0.0);
        assert_eq!(untouched.pixels(), image.pixels());
    }

    #[test]
    fn the_pipeline_keeps_north_up_and_west_first() {
        // The blind zone stays empty and each hemisphere's valid samples
        // keep their original longitude and latitude.
        let w = 64usize;
        let h = 32usize;
        let west = gray(32, 32, 0.2);
        let east = gray(32, 32, 0.8);
        let mut image = stitch(&west, &east);
        for x in 0..w {
            image.pixels[x] = 1.0;
            image.pixels[(h - 1) * w + x] = 1.0;
        }
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 1.0,
            blur: 0.0,
        };
        let covered = to_coverage(&trim_poles(&image).0, &settings);
        assert!(covered.pixels()[..w].iter().all(|&v| v == 0.0));
        assert!(covered.pixels()[(h - 1) * w..].iter().all(|&v| v == 0.0));
        let nw = covered.pixels()[w];
        let ne = covered.pixels()[w + w / 2];
        let sw = covered.pixels()[(h - 2) * w];
        let se = covered.pixels()[(h - 2) * w + w / 2];
        assert!(nw < 0.3, "north-west stays west-dim: {nw}");
        assert!(ne > 0.7, "north-east stays east-bright: {ne}");
        assert!(sw < 0.3, "south-west stays west-dim: {sw}");
        assert!(se > 0.7, "south-east stays east-bright: {se}");
    }

    #[test]
    fn decoded_png_must_be_a_png_of_the_exact_square_size() {
        let body = png_bytes(16, 16, 128);
        let map = decode_png(&body, 16).unwrap();
        assert_eq!(map.width(), 16);
        assert_eq!(map.height(), 16);
        assert!((map.pixels()[0] - 128.0 / 255.0).abs() < f32::EPSILON);
        for (label, body) in [
            ("wrong size", png_bytes(8, 8, 128)),
            ("non-square", png_bytes(8, 16, 128)),
        ] {
            let Err(error) = decode_png(&body, 16) else {
                panic!("{label} accepted");
            };
            assert!(error.contains("16x16"), "{label}: {error}");
        }
        let Err(error) = decode_png(b"<ServiceExceptionReport/>", 16) else {
            panic!("an XML error page accepted");
        };
        assert!(error.contains("PNG"), "XML page: {error}");
    }

    #[test]
    fn a_too_large_body_is_refused_without_a_decode() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        let (agent, calls) = stub_agent(|_| Ok(png_response(16, 128)));
        source.agent = agent;
        let url = build_wms_url("-90,-180,90,0", 16);
        let Err(error) = source.request_hemisphere("west", 0, &url, 16, 8) else {
            panic!("a body past the cap was read");
        };
        let FetchError::Permanent(message) = error else {
            panic!("a body past the cap is not retriable");
        };
        assert!(message.contains("cap"), "{message}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn an_attempt_never_starts_after_the_round_budget() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        let (agent, calls) = stub_agent(|_| Ok(png_response(16, 128)));
        source.agent = agent;
        let spent = Instant::now()
            .checked_sub(Duration::from_mins(1))
            .expect("a minute ago is representable");
        let Err(error) = source.fetch_hemisphere("west", 0, "-90,-180,90,0", 16, spent) else {
            panic!("an exhausted budget fetched anyway");
        };
        assert!(error.contains("budget"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn transient_failures_are_retried_and_a_retry_after_within_the_budget_wins() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        let (agent, calls) = stub_agent(|n| {
            if n == 1 {
                // A 429 with a Retry-After the budget can honor: it replaces
                // the shipped 10 s backoff, so the round stays fast.
                Ok(Response::builder()
                    .status(429)
                    .header("Retry-After", "0")
                    .body(Body::builder().data(Vec::new()))
                    .unwrap())
            } else {
                Ok(png_response(16, 128))
            }
        });
        source.agent = agent;
        let started = Instant::now();
        let (_, map) = source
            .fetch_hemisphere("west", 0, "-90,-180,90,0", 16, started + ROUND_BUDGET)
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2, "retried once");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "Retry-After honored"
        );
        assert_eq!(map.width(), 16);
    }

    #[test]
    fn a_transport_failure_is_transient_and_uses_the_backoff() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        let (agent, calls) = stub_agent(|n| {
            if n == 1 {
                Err(ureq::Error::Io(std::io::Error::other("connection died")))
            } else {
                Ok(png_response(16, 128))
            }
        });
        source.agent = agent;
        let (_, map) = source
            .fetch_hemisphere(
                "west",
                0,
                "-90,-180,90,0",
                16,
                Instant::now() + ROUND_BUDGET,
            )
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(map.width(), 16);
    }

    #[test]
    fn permanent_refusals_and_invalid_images_are_not_retried() {
        for (label, make) in [
            (
                "a permanent status",
                Box::new(|_: usize| {
                    Ok(Response::builder()
                        .status(403)
                        .body(Body::builder().data(Vec::new()))
                        .unwrap()) as Result<Response<Body>, ureq::Error>
                })
                    as Box<dyn Fn(usize) -> Result<Response<Body>, ureq::Error> + Send + Sync>,
            ),
            (
                "an XML error page under 200",
                Box::new(|_: usize| {
                    Ok(Response::builder()
                        .status(200)
                        .header("Content-Type", "text/xml")
                        .body(Body::builder().data(b"<ServiceExceptionReport/>".to_vec()))
                        .unwrap()) as Result<Response<Body>, ureq::Error>
                }),
            ),
            (
                "a disguised body that is not an image",
                Box::new(|_: usize| {
                    Ok(Response::builder()
                        .status(200)
                        .header("Content-Type", "image/png")
                        .body(Body::builder().data(b"not an image at all".to_vec()))
                        .unwrap()) as Result<Response<Body>, ureq::Error>
                }),
            ),
            (
                "a size this round did not ask for",
                Box::new(|_: usize| {
                    Ok(png_response(8, 128)) as Result<Response<Body>, ureq::Error>
                }),
            ),
        ] {
            let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
            let (agent, calls) = stub_agent(make);
            source.agent = agent;
            let Err(error) = source.fetch_hemisphere(
                "west",
                0,
                "-90,-180,90,0",
                16,
                Instant::now() + ROUND_BUDGET,
            ) else {
                panic!("{label} produced a map");
            };
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{label} retried");
            assert!(!error.is_empty(), "{label}");
        }
    }

    #[test]
    fn a_cached_round_does_not_hide_a_newly_published_round() {
        let (_scratch, mut source) = scratch_cache("eumetsat_new_publication");
        store_round(&source, 60, 90);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        source.agent = ureq::Agent::config_builder()
            .middleware(move |req: Request<SendBody>, _: MiddlewareNext| {
                let url = req.uri().to_string();
                seen.lock().unwrap().push(url.clone());
                if url.contains("GetCapabilities") {
                    Ok(Response::builder().status(200).body(Body::builder().data(
                        br#"<WMS_Capabilities><Capability><Layer><Layer><Name>mumi:worldcloudmap_ir108</Name><Dimension name="time" default="2026-11-18T12:00:00Z">2026-11-17T00:00:00Z/2026-11-18T12:00:00Z/PT3H</Dimension></Layer></Layer></Capability></WMS_Capabilities>"#.to_vec()
                    )).unwrap())
                } else {
                    Ok(png_response(16, 128))
                }
            })
            .build().new_agent();
        assert!(
            source.fetch_if_changed(None).unwrap().is_some(),
            "startup checks upstream even with a recent cache"
        );
        let urls = requests.lock().unwrap();
        assert_eq!(urls.len(), 3);
        assert!(urls[0].contains("GetCapabilities"));
        for url in &urls[1..] {
            assert!(url.contains("time=2026-11-18T12:00:00Z"), "{url}");
        }
        drop(urls);
        assert!(source.fetch_if_changed(None).unwrap().is_none());
        assert_eq!(
            requests.lock().unwrap().len(),
            4,
            "only metadata is fetched for an unchanged round"
        );
    }

    #[test]
    fn fetch_if_changed_rounds_two_hemispheres_into_one_jpeg_and_ignores_etags() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        // The 2048 texture resolution halves into 1024 px hemispheres, each
        // fetched as a 2x2 grid of 512 px tiles, which the stub serves. Both
        // hemispheres are one identical 128/255 fill, so the stitched
        // 2048x1024 map is uniform before trim, coverage and encode.
        let (agent, calls) = stub_agent(|_| Ok(png_response(512, 128)));
        source.agent = agent;
        source.prepare(2048);
        let image = source.fetch_if_changed(Some("\"old\"")).unwrap().unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            8,
            "one round: two hemispheres of four tiles"
        );
        assert!(image.etag.is_none(), "no conditional requests upstream");
        let decoded = image::load_from_memory(&image.bytes).unwrap();
        assert_eq!(decoded.width(), 2048);
        assert_eq!(decoded.height(), 1024);
        match decoded {
            image::DynamicImage::ImageLuma8(gray) => {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "the curve runs in f32, as the pipeline does"
                )]
                let expected = {
                    let settings = EumetsatSettings::default();
                    let v = f32::from(128u8) / 255.0;
                    let t = (v - f32::from(settings.floor) / 255.0)
                        / (f32::from(settings.ceiling) / 255.0 - f32::from(settings.floor) / 255.0);
                    t.clamp(0.0, 1.0).powf(settings.gamma as f32) * 255.0
                };
                for pixel in gray.pixels() {
                    let value = f32::from(pixel.0[0]);
                    assert!(
                        (value - expected).abs() <= 2.0,
                        "{value} against {expected}"
                    );
                }
            }
            other => panic!("not a grayscale JPEG: {other:?}"),
        }
        // No 304 semantics: the second poll fetches a fresh round regardless.
        source.fetch_if_changed(Some("\"old\"")).unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 16);
    }

    #[test]
    fn prepare_tracks_resolution_and_hashed_identity() {
        let source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        let wide = source.prepare(8192);
        assert!(wide.cache_key.starts_with("clouds_cache_eumetsat_"));
        assert_eq!(wide.wait, super::super::cloud_source::CloudWait::LongFetch);
        let narrow = source.prepare(2048);
        assert_ne!(wide.cache_key, narrow.cache_key, "the size is in the key");
        assert_eq!(source.px(), 1024);
        let retuned = EumetsatCloudSource::new(
            EumetsatSettings {
                floor: 61,
                ..EumetsatSettings::default()
            },
            None,
        );
        assert_eq!(
            retuned.prepare(2048).cache_key,
            narrow.cache_key,
            "the parameters are out of the key: one derived file per size,              rewritten by a tuning change"
        );
        assert!(
            source
                .describe()
                .starts_with("eumetsat mumi:worldcloudmap_ir108")
        );
    }

    #[test]
    fn algorithm_v2_invalidates_the_v1_startup_cache_identity() {
        let source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        let legacy = CloudTarget::eumetsat(&format!("{LAYER}|1024"));
        let current = source.prepare(2048);
        assert_ne!(
            current.cache_key, legacy.cache_key,
            "stretched v1 startup map must not be served"
        );
        assert_eq!(ALGORITHM_VERSION, 2);
        assert_eq!(
            EumetsatCloudSource::identity(1024),
            format!("{LAYER}|1024|v{ALGORITHM_VERSION}")
        );
    }

    // -----------------------------------------------------------------------
    // The raw pair and the derived map caches
    // -----------------------------------------------------------------------

    /// A source with a scratch cache directory, and the directory.
    fn scratch_cache(name: &str) -> (crate::test_support::ScratchDir, EumetsatCloudSource) {
        let scratch = crate::test_support::ScratchDir::new(name);
        let mut source = EumetsatCloudSource::new(
            EumetsatSettings::default(),
            Some(scratch.path().to_path_buf()),
        );
        source.agent = stub_agent(|_| panic!("unexpected imagery download")).0;
        (scratch, source)
    }

    /// One hemisphere's raw body at 16 px, uniform at `value`.
    fn raw_body(value: u8) -> Vec<u8> {
        png_bytes(16, 16, value)
    }

    /// Store a raw pair and its derivation at 16 px, and point `source` at
    /// that size: the state a poll between rounds reads.
    fn store_round(source: &EumetsatCloudSource, west_value: u8, east_value: u8) {
        let west = raw_body(west_value);
        let east = raw_body(east_value);
        let hashes = (sha256_hex(&west), sha256_hex(&east));
        source
            .store_raw_pair(16, &west, &east, FIXTURE_ROUND)
            .expect("stored");
        let west_map = decode_png(&west, 16).expect("the west decodes");
        let east_map = decode_png(&east, 16).expect("the east decodes");
        source
            .process_and_store(
                16,
                &west_map,
                &east_map,
                &hashes.0,
                &hashes.1,
                Instant::now(),
            )
            .expect("the derivation is stored");
        *source.size.lock().expect("the size lock") = 16;
    }

    #[test]
    fn the_raw_pair_is_stored_validated_and_loaded_back_only_intact() {
        let (scratch, source) = scratch_cache("eumetsat_raw_pair");
        let west = raw_body(60);
        let east = raw_body(90);
        source
            .store_raw_pair(16, &west, &east, FIXTURE_ROUND)
            .expect("the pair is stored");
        assert!(scratch.join("eumetsat_raw_west_16.png").is_file());
        assert!(scratch.join("eumetsat_raw_east_16.png").is_file());
        assert!(
            !scratch.join("eumetsat_raw_west_16.png.tmp").exists(),
            "no staging file is left behind"
        );
        let pair = source.load_raw_pair(16).expect("the pair loads");
        assert_eq!(pair.west, west);
        assert_eq!(pair.east, east);
        assert_eq!(pair.hashes.0, sha256_hex(&west));
        assert_eq!(pair.hashes.1, sha256_hex(&east));
        // A file that no longer matches its manifest makes the pair unusable
        // until a network round replaces it; the files are left alone either
        // way.
        let corrupted = scratch.join("eumetsat_raw_west_16.png");
        let mut bytes = std::fs::read(&corrupted).expect("the raw west is read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&corrupted, &bytes).expect("the raw west is corrupted");
        assert!(
            source.load_raw_pair(16).is_none(),
            "the manifest's hashes no longer match what is on disk"
        );
    }

    #[test]
    fn the_derived_map_is_reused_only_when_its_metadata_matches() {
        let (scratch, source) = scratch_cache("eumetsat_derived_meta");
        let west = raw_body(60);
        let east = raw_body(90);
        let hashes = (sha256_hex(&west), sha256_hex(&east));
        source
            .store_raw_pair(16, &west, &east, FIXTURE_ROUND)
            .expect("stored");
        assert!(
            !source.derived_matches(16, &hashes.0, &hashes.1),
            "there is no derivation yet"
        );
        let west_map = decode_png(&west, 16).expect("the west decodes");
        let east_map = decode_png(&east, 16).expect("the east decodes");
        source
            .process_and_store(
                16,
                &west_map,
                &east_map,
                &hashes.0,
                &hashes.1,
                Instant::now(),
            )
            .expect("the derivation is stored");
        assert!(source.derived_matches(16, &hashes.0, &hashes.1));
        // The derived file is one per size: its name carries no parameters.
        assert!(scratch.join("eumetsat_map_32x16.jpg").is_file());
        // A different tuning stops the match, which is what sends a parameter
        // change back to the raw pair.
        let retuned = EumetsatCloudSource::new(
            EumetsatSettings {
                floor: 61,
                ..EumetsatSettings::default()
            },
            Some(scratch.path().to_path_buf()),
        );
        assert!(
            !retuned.derived_matches(16, &hashes.0, &hashes.1),
            "the tuning moved, the derivation is stale"
        );
    }

    #[test]
    fn v1_derived_metadata_is_rebuilt_without_redownloading_or_rewriting_raw() {
        let (scratch, mut source) = scratch_cache("eumetsat_v1_reprocess");
        store_round(&source, 60, 90);
        let raw_path = scratch.join("eumetsat_raw_16_meta.toml");
        let mut raw_meta: RawManifest =
            toml::from_str(&std::fs::read_to_string(&raw_path).unwrap()).unwrap();
        raw_meta.algorithm = 1;
        let raw_text = toml::to_string(&raw_meta).unwrap();
        std::fs::write(&raw_path, &raw_text).unwrap();
        let meta_path = scratch.join("eumetsat_map_32x16_meta.toml");
        let mut old: DerivedMeta =
            toml::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        old.algorithm = 1;
        std::fs::write(&meta_path, toml::to_string(&old).unwrap()).unwrap();
        let pair = source.load_raw_pair(16).expect("v1 raw is reusable");
        let (agent, calls) = stub_agent(|_| panic!("algorithm migration must not download"));
        source.agent = agent;
        let result = source.fetch_if_changed(None).unwrap();
        assert!(result.is_some(), "the v1 derived map must be rebuilt");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let meta: DerivedMeta =
            toml::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert_eq!(meta.algorithm, 2);
        assert!(source.derived_matches(16, &pair.hashes.0, &pair.hashes.1));
        assert_eq!(std::fs::read_to_string(raw_path).unwrap(), raw_text);
        assert_eq!(
            std::fs::read(scratch.join("eumetsat_raw_west_16.png")).unwrap(),
            pair.west
        );
        assert_eq!(
            std::fs::read(scratch.join("eumetsat_raw_east_16.png")).unwrap(),
            pair.east
        );
    }

    #[test]
    fn a_parameter_change_reprocesses_the_raw_pair_without_the_network() {
        let (scratch, _dir) = scratch_cache("eumetsat_reprocess");
        let west = raw_body(60);
        let east = raw_body(90);
        let hashes = (sha256_hex(&west), sha256_hex(&east));
        let old = EumetsatCloudSource::new(
            EumetsatSettings {
                floor: 0,
                ceiling: 255,
                gamma: 1.0,
                blur: 0.0,
            },
            Some(scratch.path().to_path_buf()),
        );
        old.store_raw_pair(16, &west, &east, FIXTURE_ROUND)
            .expect("stored");
        let old_west = decode_png(&west, 16).expect("decodes");
        let old_east = decode_png(&east, 16).expect("decodes");
        old.process_and_store(
            16,
            &old_west,
            &old_east,
            &hashes.0,
            &hashes.1,
            Instant::now(),
        )
        .expect("the old derivation is stored");

        // The new instance's agent refuses to be used: reaching the network
        // here would panic inside the middleware instead of passing.
        let mut retuned = EumetsatCloudSource::new(
            EumetsatSettings::default(),
            Some(scratch.path().to_path_buf()),
        );
        let (agent, calls) = stub_agent(|_| panic!("the reprocess must not touch the network"));
        retuned.agent = agent;
        *retuned.size.lock().expect("the size lock") = 16;
        let image = retuned
            .fetch_if_changed(None)
            .expect("the round succeeds")
            .expect("a map");
        let decoded = image::load_from_memory(&image.bytes).expect("the map decodes");
        assert_eq!((decoded.width(), decoded.height()), (32, 16));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "the network was never touched"
        );
        // The metadata now carries the new tuning, so the next poll's
        // unchanged check finds the derivation current.
        assert!(retuned.derived_matches(16, &hashes.0, &hashes.1));
    }

    // -----------------------------------------------------------------------
    // The upstream round gate and the user stop
    // -----------------------------------------------------------------------

    /// The capabilities list still naming the stored round, every poll — a
    /// refresh pressed twice, the schedule's five-minute recheck — answers
    /// from disk without downloading imagery.
    #[test]
    fn repeated_polls_of_the_current_round_are_served_from_disk() {
        let (scratch, mut source) = scratch_cache("eumetsat_current_round");
        store_round(&source, 60, 90);
        source.fetch_if_changed(None).expect("the first fetch");
        let (agent, calls) = stub_agent(|_| panic!("the current round must not touch the imagery"));
        source.agent = agent;
        assert!(
            source
                .fetch_if_changed(None)
                .expect("the gate holds")
                .is_none(),
            "a pair of the current round with a current derivation is reused"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(scratch);
    }

    /// A stored round behind the capabilities' latest is old news: the poll
    /// downloads again and commits what it got.
    #[test]
    fn a_stored_round_behind_the_latest_is_fetched_again() {
        let (scratch, mut source) = scratch_cache("eumetsat_outdated_round");
        store_round(&source, 60, 90);
        source.fetch_if_changed(None).expect("the first fetch");
        let path = scratch.join("eumetsat_raw_16_meta.toml");
        let mut manifest: RawManifest =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        manifest.round = "2026-11-18T03:00:00Z".to_owned();
        std::fs::write(path, toml::to_string(&manifest).unwrap()).unwrap();
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        let (agent, calls) = stub_agent(|_| Ok(png_response(16, 128)));
        source.agent = agent;
        let image = source
            .fetch_if_changed(None)
            .expect("the refetch runs")
            .expect("a new round");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one tile per hemisphere at this size"
        );
        assert_eq!(image::load_from_memory(&image.bytes).unwrap().width(), 32);
    }

    /// The current round whose derivation no longer matches — its metadata
    /// gone, say — is reprocessed from disk rather than downloaded again.
    #[test]
    fn the_current_round_with_a_stale_derivation_is_reprocessed_from_disk() {
        let (scratch, mut source) = scratch_cache("eumetsat_stale_derivation");
        store_round(&source, 60, 90);
        source.fetch_if_changed(None).expect("the first fetch");
        std::fs::remove_file(scratch.join("eumetsat_map_32x16_meta.toml"))
            .expect("the derivation's metadata is taken away");
        let (agent, calls) = stub_agent(|_| panic!("the current round must not touch the imagery"));
        source.agent = agent;
        assert!(
            source
                .fetch_if_changed(None)
                .expect("the reprocess runs")
                .is_some(),
            "the stored pair is processed again"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// A stop standing before a round starts answers at once, and nothing
    /// is asked of the service.
    #[test]
    fn a_stop_before_a_round_touches_nothing() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.set_cancel(Arc::new(AtomicBool::new(true)));
        let (agent, calls) = stub_agent(|_| panic!("a stopped round must not touch the network"));
        source.agent = agent;
        let Err(error) = source.fetch_if_changed(None) else {
            panic!("the stop answers");
        };
        assert!(error.contains("stopped"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// A stop landing mid-round drops the round wherever it is: the error is
    /// the stop, and no pair is committed to the cache.
    #[test]
    fn a_stop_during_a_round_drops_it_uncommitted() {
        let (scratch, mut source) = scratch_cache("eumetsat_stop_mid_round");
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        *source.size.lock().expect("the size lock") = 16;
        let stop = Arc::new(AtomicBool::new(false));
        source.set_cancel(Arc::clone(&stop));
        // The stop goes up the moment the service is asked for anything, so
        // the workers unwind at their nearest check point.
        let stopper = Arc::clone(&stop);
        let (agent, calls) = stub_agent(move |_| {
            stopper.store(true, Ordering::SeqCst);
            Ok(png_response(16, 128))
        });
        source.agent = agent;
        let Err(error) = source.fetch_if_changed(None) else {
            panic!("the round is stopped");
        };
        assert!(error.contains("stopped"), "{error}");
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "the stop lands on a round that was running"
        );
        assert!(
            !scratch.join("eumetsat_raw_16_meta.toml").exists(),
            "a stopped round commits nothing"
        );
    }

    /// The two hemispheres fetch in parallel: the middleware holds the first
    /// request until the second arrives, and a timeout is a permanent 403.
    /// A sequential fetch would therefore fail in two seconds, while the
    /// parallel one is through in milliseconds.
    #[test]
    fn the_two_hemispheres_fetch_in_parallel() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        // The first request waits up to two seconds for its peer; a timeout
        // is a permanent 403. A sequential fetch would therefore fail, while
        // the parallel one is through in milliseconds.
        let (peer_tx, peer_rx) = crossbeam_channel::bounded::<()>(1);
        let arrived = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peer_tx = Arc::new(Mutex::new(Some(peer_tx)));
        let peer_rx = Arc::new(Mutex::new(peer_rx));
        let (agent, calls) = stub_agent(move |_| {
            use std::sync::atomic::Ordering as AtomicOrdering;
            if arrived.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                let rx = peer_rx.lock().unwrap_or_else(PoisonError::into_inner);
                if rx.recv_timeout(Duration::from_secs(2)).is_err() {
                    return Ok(Response::builder()
                        .status(403)
                        .body(Body::builder().data(Vec::new()))
                        .unwrap());
                }
            } else if let Some(tx) = peer_tx
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                let _ = tx.send(());
            }
            Ok(png_response(16, 128))
        });
        source.agent = agent;
        *source.size.lock().expect("the size lock") = 16;
        let started = Instant::now();
        let image = source
            .fetch_if_changed(None)
            .expect("both hemispheres arrive")
            .expect("a map comes out");
        let elapsed = started.elapsed();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one request per hemisphere"
        );
        assert_eq!(image::load_from_memory(&image.bytes).unwrap().width(), 32);
        assert!(
            elapsed < Duration::from_secs(2),
            "the round waited on the barrier, so the fetches were sequential: {elapsed:?}"
        );
    }

    /// A finished round leaves nothing behind: the phase and the download
    /// lines are the round's, and an idle source says nothing, which is what
    /// lets the worker's own status — ready, stopped, a retry's delay — be
    /// the answer between rounds.
    #[test]
    fn a_finished_round_leaves_no_status_behind() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        *source.size.lock().expect("the size lock") = 16;
        let (agent, _calls) = stub_agent(|_| Ok(png_response(16, 128)));
        source.agent = agent;
        source
            .fetch_if_changed(None)
            .expect("the round succeeds")
            .expect("a map comes out");
        assert_eq!(
            source.status(),
            None,
            "an idle source says nothing after a round"
        );

        // The same is true of the round after it, whether it comes back
        // unchanged or fails on its way out.
        source.fetch_if_changed(None).expect("the next round runs");
        assert_eq!(source.status(), None, "nothing after an unchanged round");
        let (agent, _calls) = stub_agent(|_| {
            Ok(Response::builder()
                .status(403)
                .body(Body::builder().data(Vec::new()))
                .unwrap())
        });
        source.agent = agent;
        assert!(source.fetch_if_changed(None).is_err(), "the round fails");
        assert_eq!(source.status(), None, "nothing after a failed round");
    }

    #[test]
    fn the_status_composes_the_phase_and_the_download_lines() {
        let source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        assert_eq!(source.status(), None, "an idle source says nothing");
        source.set_phase("downloading the two hemispheres");
        assert_eq!(
            source.status().as_deref(),
            Some("downloading the two hemispheres")
        );
        source.set_progress(0, "west: 1.2 of 2.8 MiB (450 KiB/s) — 43%");
        assert_eq!(
            source.status().as_deref(),
            Some("downloading the two hemispheres — west: 1.2 of 2.8 MiB (450 KiB/s) — 43%")
        );
        source.set_progress(1, "east: 0.3 of 2.8 MiB (120 KiB/s) — 11%");
        let status = source.status().expect("both lines are up");
        assert!(status.contains("west: 1.2"), "{status}");
        assert!(status.contains("east: 0.3"), "{status}");
        // A phase change ends the download lines with it.
        source.set_phase("processing locally");
        assert_eq!(
            source.status().as_deref(),
            Some("processing locally"),
            "stale download lines must not survive into the next phase"
        );
    }

    #[test]
    fn the_transfer_figures_use_the_units_that_read_best() {
        assert_eq!(mib(0), "0.0 MiB");
        assert_eq!(mib(2_811_907), "2.7 MiB");
        assert_eq!(speed_text(300.0 * 1024.0), "300 KiB/s");
        assert_eq!(speed_text(2.4 * 1024.0 * 1024.0), "2.4 MiB/s");
    }

    /// A middleware that serves each tile's fill value from its bbox: tile
    /// (row, col) of the west hemisphere comes back uniform at
    /// `100 + 40*row + 40*col`.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "the bbox grid indexes are small whole numbers by construction"
    )]
    fn per_tile_agent(
        fail_plan: Arc<Mutex<std::collections::HashMap<String, usize>>>,
    ) -> (ureq::Agent, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .middleware(
                move |req: ureq::http::Request<ureq::SendBody>,
                      _: ureq::middleware::MiddlewareNext| {
                    seen.fetch_add(1, Ordering::SeqCst);
                    let url = req.uri().to_string();
                    let bbox = url
                        .split("bbox=")
                        .nth(1)
                        .unwrap_or_default()
                        .split('&')
                        .next()
                        .unwrap_or_default()
                        .to_owned();
                    let numbers: Vec<f64> = bbox
                        .split(',')
                        .filter_map(|v| v.trim().parse().ok())
                        .collect();
                    let fill = if numbers.len() == 4 {
                        // West hemisphere: lat -90..90, lon -180..0.
                        let row = ((numbers[0] + 90.0) / 90.0).round() as usize;
                        let col = ((numbers[1] + 180.0) / 90.0).round() as usize;
                        let planned = fail_plan
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .get_mut(&bbox)
                            .is_some_and(|left| {
                                let fire = *left > 0;
                                if fire {
                                    *left -= 1;
                                }
                                fire
                            });
                        if planned {
                            return Ok(Response::builder()
                                .status(503)
                                .body(Body::builder().data(Vec::new()))
                                .unwrap());
                        }
                        u8::try_from(100 + 40 * row + 40 * col).unwrap_or(255)
                    } else {
                        128
                    };
                    Ok(png_response(512, fill))
                },
            )
            .build()
            .new_agent();
        (agent, calls)
    }

    /// The tiles of one hemisphere fetch concurrently: the stub holds the
    /// first tile request until a peer arrives (2 s grace). A sequential
    /// fetch would hold it alone, time out into a 403, and fail the round.
    #[test]
    fn tiles_fetch_concurrently() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        let (peer_tx, peer_rx) = crossbeam_channel::bounded::<()>(1);
        let arrived = Arc::new(AtomicUsize::new(0));
        let peer_tx = Mutex::new(Some(peer_tx));
        let peer_rx = Mutex::new(peer_rx);
        let (agent, calls) = {
            let arrived = Arc::clone(&arrived);
            stub_agent(move |_| {
                if arrived.fetch_add(1, Ordering::SeqCst) == 0 {
                    let wait = peer_rx
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .recv_timeout(Duration::from_secs(2));
                    if wait.is_err() {
                        return Ok(Response::builder()
                            .status(403)
                            .body(Body::builder().data(Vec::new()))
                            .unwrap());
                    }
                } else if let Some(tx) = peer_tx
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                {
                    let _ = tx.send(());
                }
                Ok(png_response(512, 128))
            })
        };
        source.agent = agent;
        let (_, map) = source
            .fetch_hemisphere(
                "west",
                0,
                "-90,-180,90,0",
                1024,
                Instant::now() + ROUND_BUDGET,
            )
            .expect("the peers arrive and every tile serves");
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(map.width(), 1024);
    }

    #[test]
    fn tiles_are_stitched_at_their_grid_positions() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        let (agent, calls) = per_tile_agent(Arc::default());
        source.agent = agent;
        let (_, map) = source
            .fetch_hemisphere(
                "west",
                0,
                "-90,-180,90,0",
                1024,
                Instant::now() + ROUND_BUDGET,
            )
            .expect("the tiled hemisphere arrives");
        assert_eq!(calls.load(Ordering::SeqCst), 4, "a 2x2 grid of tiles");
        // Tile (row, col) fills 100 + 40*row + 40*col, and each tile is
        // 512 px. Tile rows count from the bbox's min_lat — south — while
        // the canvas counts from its top, north: tile row 0 lands at the
        // BOTTOM of the canvas, so the first pixel of each quadrant names
        // the tile that owns it, with rows swapped.
        let expected = |row: usize, col: usize| {
            f32::from(u8::try_from(100 + 40 * row + 40 * col).unwrap_or(255)) / 255.0
        };
        approx::assert_relative_eq!(map.pixels()[0], expected(1, 0));
        approx::assert_relative_eq!(map.pixels()[512], expected(1, 1));
        approx::assert_relative_eq!(map.pixels()[512 * 1024], expected(0, 0));
        approx::assert_relative_eq!(map.pixels()[512 * 1024 + 512], expected(0, 1));
    }

    #[test]
    fn failed_tiles_are_retried_in_a_second_pass() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default(), None);
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        // Tile west (0,1) — bbox "-90,-90,0,0" — fails its three pass-1
        // attempts and lands on the fourth (pass 2).
        let mut plan = std::collections::HashMap::new();
        plan.insert("-90,-90,0,0".to_owned(), 3usize);
        let (agent, calls) = per_tile_agent(Arc::new(Mutex::new(plan)));
        source.agent = agent;
        let (_, map) = source
            .fetch_hemisphere(
                "west",
                0,
                "-90,-180,90,0",
                1024,
                Instant::now() + ROUND_BUDGET,
            )
            .expect("the second pass recovers the failed tile");
        // Three other tiles once, the failed one four times.
        assert_eq!(calls.load(Ordering::SeqCst), 7);
        // Tile (0,1) is the bottom-right quadrant now the rows are north-up.
        approx::assert_relative_eq!(map.pixels()[512 * 1024 + 512], 140.0 / 255.0);
    }

    /// A new instance over a complete cache — the startup poll's shape —
    /// reads the capabilities list and answers without downloading imagery.
    #[test]
    fn a_new_instance_with_the_current_round_costs_nothing() {
        let (_scratch, mut source) = scratch_cache("eumetsat_new_instance");
        let west = raw_body(60);
        let east = raw_body(90);
        let hashes = (sha256_hex(&west), sha256_hex(&east));
        source
            .store_raw_pair(16, &west, &east, FIXTURE_ROUND)
            .expect("stored");
        let west_map = decode_png(&west, 16).expect("decodes");
        let east_map = decode_png(&east, 16).expect("decodes");
        source
            .process_and_store(
                16,
                &west_map,
                &east_map,
                &hashes.0,
                &hashes.1,
                Instant::now(),
            )
            .expect("stored");
        let (agent, calls) = stub_agent(|_| panic!("the current round must not touch the imagery"));
        source.agent = agent;
        *source.size.lock().expect("the size lock") = 16;
        assert!(
            source
                .fetch_if_changed(None)
                .expect("the round succeeds")
                .is_none(),
            "the stored pair and its derivation are current"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
