//! The native EUMETSAT cloud source: fetch the IR layer over WMS and process
//! it into the grayscale map the cloud pipeline already decodes.
//!
//! This is the Rust port of the `eumetsat` command in `tools/cloud-fetch`
//! (`plans/2026-11-13-cloud-map-generator.md`), which was verified against the
//! app's renderer. One round fetches the two hemispheres of
//! `mumi:worldcloudmap_ir108` from `EUMETView`'s WMS 1.3.0 endpoint (EPSG:4326,
//! no auth), stitches them west-first into a 2:1 equirectangular map, trims
//! the geostationary blind zone at the poles column by column, maps IR
//! brightness to cloud coverage, and encodes the quality-88 grayscale JPEG the
//! fetcher stores and decodes.
//!
//! The imagery requires the "Contains modified EUMETSAT data" attribution
//! (EUMETSAT data licensing). Two approximations are inherent to the method
//! and carry over from the generator: the per-column pole trim stretches each
//! column's retained rows across the full height, which shifts latitudes near
//! the poles (a visualization approximation, not a reprojection), and the two
//! hemispheres are "latest" requests taken seconds apart, not a strictly
//! simultaneous instant.

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

/// Grayscale values at or above this are leftover blind-zone white, not cloud.
pub(crate) const NO_DATA_THRESHOLD: f32 = 254.0 / 255.0;
/// Blind-zone detection for the pole trim. Below the no-data threshold so the
/// disc edge antialiased to near-white (236..253) is trimmed away too, which
/// the coverage curve would otherwise clip to full cloud.
pub(crate) const TRIM_THRESHOLD: f32 = 236.0 / 255.0;
/// Median window half width for the column edges of the pole trim.
pub(crate) const TRIM_SMOOTH_HALF: usize = 24;
/// Fixed trim band, per pole, when a column's own edges look wrong.
pub(crate) const TRIM_FALLBACK: f64 = 0.09;
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
fn mib(bytes: usize) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// A transfer speed in the unit that reads best for it.
fn speed_text(bytes_per_second: f64) -> String {
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
/// Upstream's own refresh cadence for the IR108 layer: both hemispheres are
/// "latest" imagery republished this often. A round fetched within the
/// window cannot differ from the stored one, so a poll inside it serves the
/// pair from disk rather than downloading it again — the manual refresh
/// pressed twice, or an hourly poll landing just behind a restart, costs
/// nothing.
const UPSTREAM_REFRESH: Duration = Duration::from_mins(15);

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

/// Slide a window over `values` and take the window's middle element.
///
/// The window is clamped at the ends; with an even count the upper middle is
/// taken, as the generator's `np.sort` window does.
fn median_smooth(values: &[usize], half: usize) -> Vec<usize> {
    values
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let lo = i.saturating_sub(half);
            let hi = (i + half + 1).min(values.len());
            let mut window = values[lo..hi].to_vec();
            window.sort_unstable();
            window[window.len() / 2]
        })
        .collect()
}

/// Drop the polar blind zone column by column, then stretch what is left.
///
/// The service paints the unseen polar cap solid white and the disc edge
/// around it is antialiased to near-white, which the coverage curve reads as
/// thick cloud. That boundary is a curve in equirectangular space, so a
/// single horizontal cut cannot remove it: each column finds its own first
/// and last usable row within the top and bottom thirds, a span that starts
/// too low or ends too high makes both edges fall back to a fixed band, the
/// edges are median-smoothed to kill single-column spikes, and what remains
/// is stretched across the full height.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "row indices and fractions of a fixed-size image"
)]
pub(crate) fn trim_poles(image: &GrayMap) -> GrayMap {
    let width = image.width as usize;
    let height = image.height as usize;
    let third = height / 3;
    let quarter = height / 4;
    let fallback_top = (TRIM_FALLBACK * height as f64) as usize;
    let fallback_bottom = height.saturating_sub(fallback_top);

    let mut tops = Vec::with_capacity(width);
    let mut bottoms = Vec::with_capacity(width);
    for x in 0..width {
        // First row from the top (within the top third) that is not
        // blind-zone white. An all-white window ends the search at its edge,
        // as the generator's argmax of an all-false window does.
        let top = (0..third)
            .find(|&y| image.pixels[y * width + x] < TRIM_THRESHOLD)
            .unwrap_or(third);
        // Last row from the bottom (within the bottom third) that is not
        // blind-zone white; an all-white window stops at 2h/3.
        let bottom = (third * 2..height)
            .rev()
            .find(|&y| image.pixels[y * width + x] < TRIM_THRESHOLD)
            .unwrap_or(third * 2);
        tops.push(top);
        bottoms.push(bottom);
    }
    // A usable span that starts too low or ends too high is suspicious; the
    // generator falls back to a fixed band, for both edges together.
    for i in 0..width {
        if tops[i] >= quarter || bottoms[i] <= 3 * quarter {
            tops[i] = fallback_top;
            bottoms[i] = fallback_bottom;
        }
    }
    let tops = median_smooth(&tops, TRIM_SMOOTH_HALF);
    let bottoms = median_smooth(&bottoms, TRIM_SMOOTH_HALF);

    let mut out = vec![0.0f32; width * height];
    for x in 0..width {
        // Slice indices stay in range, the way Python's slice clamps them;
        // only a fixture smaller than the fallback band can reach this.
        let top = tops[x].min(height.saturating_sub(1));
        let bottom = bottoms[x].min(height.saturating_sub(1)).max(top);
        let n = bottom - top + 1;
        for y in 0..height {
            // np.interp of the column over np.linspace(0, 1, n), sampled at
            // np.linspace(0, 1, height): position y/(h-1)*(n-1), mixed
            // linearly between its neighboring column rows.
            let pos = if height > 1 {
                y as f64 / (height as f64 - 1.0) * (n as f64 - 1.0)
            } else {
                0.0
            };
            let i = (pos as usize).min(n - 1);
            let frac = (pos - i as f64) as f32;
            let a = image.pixels[(top + i) * width + x];
            let b = if i + 1 < n {
                image.pixels[(top + i + 1) * width + x]
            } else {
                a
            };
            out[y * width + x] = a + (b - a) * frac;
        }
    }
    GrayMap::new(image.width, image.height, out).expect("trimmed rectangle")
}

/// Turn IR brightness into cloud coverage.
///
/// Values at or above [`NO_DATA_THRESHOLD`] are leftover blind-zone white,
/// not cloud, and map to clear sky. `gamma` follows the generator's
/// `v ** gamma` convention (gamma below 1 brightens midtones):
/// `clamp((255v - floor) / (ceiling - floor), 0, 1) ** gamma`.
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
            if v >= NO_DATA_THRESHOLD {
                0.0
            } else {
                let t = ((f64::from(v) - floor) / span).clamp(0.0, 1.0) as f32;
                t.powf(gamma)
            }
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
/// There are no conditional requests against this source: upstream serves
/// "latest" imagery, so every poll fetches a fresh round and `known_etag` is
/// ignored on purpose. The two hemispheres are "latest" requests taken
/// seconds apart; the round's start and end times are logged, not claimed to
/// be one satellite instant.
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
    /// Whether the next fetch is this instance's first. A parameter or
    /// algorithm change arrives as a new instance, and its first fetch
    /// reprocesses the stored raw pair instead of downloading again.
    first_fetch: AtomicBool,
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
const ALGORITHM_VERSION: u32 = 1;

/// The raw pair's manifest, the commit point of a stored round: written last,
/// after both PNGs are on disk and verified.
#[derive(serde::Serialize, serde::Deserialize)]
struct RawManifest {
    version: u32,
    algorithm: u32,
    west_sha256: String,
    east_sha256: String,
    /// When the round started, Unix seconds. The freshness gate reads it: a
    /// round younger than [`UPSTREAM_REFRESH`] cannot differ from what
    /// upstream has, so a poll inside the window skips the network. `default`
    /// lets a manifest from before the gate read as unknown, which is never
    /// fresh.
    #[serde(default)]
    fetched_epoch_secs: u64,
}

/// The stored raw pair, its bytes and the hashes its manifest named.
struct RawPair {
    west: Vec<u8>,
    east: Vec<u8>,
    hashes: (String, String),
    /// The manifest's fetch time, Unix seconds; zero means unknown.
    fetched_epoch_secs: u64,
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

        // The stored pair is served from disk on this instance's first fetch
        // — a parameter or algorithm change arrives as a new instance, and
        // its first fetch reprocesses rather than downloads again — and on
        // any poll inside the window a round can still be young in: upstream
        // cannot have published anything the pair does not hold.
        let cached_raw = self.load_raw_pair(px);
        let first = self.first_fetch.swap(false, Ordering::SeqCst);
        let serves_from_disk = cached_raw
            .as_ref()
            .is_some_and(|pair| first || Self::round_is_fresh(pair));
        if let Some(pair) = &cached_raw
            && serves_from_disk
        {
            if self.derived_matches(px, &pair.hashes.0, &pair.hashes.1) {
                info!(
                    px,
                    fresh = !first,
                    "the stored raw pair and its derivation are still current"
                );
                return Ok(None);
            }
            self.set_phase("processing locally");
            info!(
                px,
                first, "reprocessing the stored raw pair without the network"
            );
            let west = decode_png(&pair.west, px)?;
            let east = decode_png(&pair.east, px)?;
            let bytes =
                self.process_and_store(px, &west, &east, &pair.hashes.0, &pair.hashes.1, started)?;
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
        // Upstream unchanged and the derivation current: nothing to report,
        // and nothing to process.
        if let Some(pair) = &cached_raw
            && west_body == pair.west
            && east_body == pair.east
            && self.derived_matches(px, &pair.hashes.0, &pair.hashes.1)
        {
            debug!(px, "the upstream round matches the stored one");
            return Ok(None);
        }
        self.store_raw_pair(px, &west_body, &east_body)?;
        let bytes = self.process_and_store(px, &west, &east, &west_sha, &east_sha, started)?;
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
            first_fetch: AtomicBool::new(true),
            retry_delays: RETRY_DELAYS,
            phase: Mutex::new(String::new()),
            progress: Mutex::new([None, None]),
            cancel: Mutex::new(Arc::new(AtomicBool::new(false))),
            agent,
        }
    }

    /// What one round's cache entry is named after: the layer and the
    /// hemisphere size. The processing parameters are deliberately absent —
    /// a parameter change must reuse the same derived file rather than
    /// accumulate one per tuning — so the key names the imagery, not the
    /// look.
    fn identity(px: u32) -> String {
        format!("{LAYER}|{px}")
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

    /// Whether the stored round is too young for upstream to have published
    /// anything new. Both hemispheres are "latest" imagery the service
    /// republishes every [`UPSTREAM_REFRESH`], so a pair fetched inside the
    /// window is the present state whatever the poll would ask for. A
    /// manifest without a usable time — one from before the gate, a clock
    /// set backwards — is never fresh.
    fn round_is_fresh(pair: &RawPair) -> bool {
        if pair.fetched_epoch_secs == 0 {
            return false;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now >= pair.fetched_epoch_secs && now - pair.fetched_epoch_secs < UPSTREAM_REFRESH.as_secs()
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
        let url = build_wms_url(&tile_bbox, tile_px);
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
    fn store_raw_pair(&self, px: u32, west: &[u8], east: &[u8]) -> Result<(), String> {
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
            fetched_epoch_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
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
            fetched_epoch_secs: manifest.fetched_epoch_secs,
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
        let map = trim_poles(&stitched);
        drop(stitched);
        let map = to_coverage(&map, &self.settings);
        let map = if self.settings.blur > 0.0 {
            gaussian_blur(&map, self.settings.blur)
        } else {
            map
        };
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
fn write_committed(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).map_err(|e| format!("the temporary write failed: {e}"))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("the commit failed: {e}"))
}

/// The SHA-256 of `bytes`, as lowercase hex.
fn sha256_hex(bytes: &[u8]) -> String {
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

fn retry_after_header(headers: &ureq::http::HeaderMap) -> Option<Duration> {
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
            .middleware(move |_: Request<SendBody>, _: MiddlewareNext| {
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
    fn documented_constants_are_the_generator_values() {
        assert_eq!(WMS_URL, "https://view.eumetsat.int/geoserver/ows");
        assert_eq!(LAYER, "mumi:worldcloudmap_ir108");
        assert_eq!(HEMISPHERES[0], ("west", "-90,-180,90,0"));
        assert_eq!(HEMISPHERES[1], ("east", "-90,0,90,180"));
        assert!((NO_DATA_THRESHOLD - 254.0 / 255.0).abs() < f32::EPSILON);
        assert!((TRIM_THRESHOLD - 236.0 / 255.0).abs() < f32::EPSILON);
        assert_eq!(TRIM_SMOOTH_HALF, 24);
        assert!((TRIM_FALLBACK - 0.09).abs() < f64::EPSILON);
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

    #[test]
    fn a_single_column_spike_is_removed_by_the_median_window() {
        let values = [5usize, 5, 9, 5, 5];
        assert_eq!(median_smooth(&values, 2), [5, 5, 5, 5, 5]);
    }

    /// White polar caps whose edge follows a gentle curve over the columns,
    /// the same fixture the generator's tests use.
    fn blind_zone_image(width: u32, height: u32) -> GrayMap {
        let w = width as usize;
        let h = height as usize;
        let mut pixels = vec![0.3f32; w * h];
        for x in 0..w {
            let edge = 12 + (4 * x) / w;
            for y in 0..edge {
                pixels[y * w + x] = 1.0;
            }
            for y in h - edge..h {
                pixels[y * w + x] = 1.0;
            }
        }
        GrayMap::new(width, height, pixels).unwrap()
    }

    #[test]
    fn trim_poles_removes_the_curved_caps_and_keeps_the_interior() {
        let image = blind_zone_image(128, 96);
        let result = trim_poles(&image);
        assert_eq!(result.width(), 128);
        assert_eq!(result.height(), 96);
        // The solid white caps disappear from the stretched output. The
        // median-smoothed edges can still catch a column or two of the curved
        // rim, so the guarantee is about the cap as a region.
        let w = 128usize;
        let input_white = (0..9 * w).filter(|&i| image.pixels()[i] >= 0.99).count();
        assert_eq!(input_white, 9 * w);
        let output_white = (0..9 * w).filter(|&i| result.pixels()[i] >= 0.99).count();
        // 0.05 as a ratio of counts: one twentieth.
        assert!(
            output_white * 20 < 9 * w,
            "{output_white} white pixels left in the top nine rows"
        );
        #[expect(clippy::cast_precision_loss, reason = "a mean over a small fixture")]
        let mean: f32 = result.pixels().iter().sum::<f32>() / result.pixels().len() as f32;
        assert!((mean - 0.3).abs() < 0.05, "interior mean {mean} moved");
    }

    #[test]
    fn near_white_caps_are_trimmed_under_the_236_threshold() {
        let mut image = blind_zone_image(128, 96);
        // 247/255 is below the no-data cutoff but above the trim threshold:
        // the disc edge antialiased to near-white must still be trimmed.
        let w = 128usize;
        for i in 0..12 * w {
            image.pixels[i] = 247.0 / 255.0;
            image.pixels[84 * w + i] = 247.0 / 255.0;
        }
        let result = trim_poles(&image);
        let output_white = (0..9 * 128).filter(|&i| result.pixels()[i] >= 0.99).count();
        // 0.05 as a ratio of counts: one twentieth.
        assert!(
            output_white * 20 < 9 * 128,
            "{output_white} white pixels left in the top nine rows"
        );
    }

    #[test]
    fn an_all_white_map_falls_back_to_the_fixed_band_and_stays_finite() {
        let image = gray(128, 96, 1.0);
        let result = trim_poles(&image);
        assert_eq!(result.width(), 128);
        assert_eq!(result.height(), 96);
        assert!(result.pixels().iter().all(|v| v.is_finite()));
    }

    #[test]
    fn coverage_maps_no_data_white_to_clear_sky() {
        let image = GrayMap::new(1, 2, vec![1.0, 253.0 / 255.0]).unwrap();
        let settings = EumetsatSettings {
            floor: 0,
            ceiling: 255,
            gamma: 1.0,
            blur: 0.0,
        };
        let result = to_coverage(&image, &settings);
        assert!(result.pixels()[0] < 0.01, "no-data maps to clear sky");
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
        // A west half at 0.2 and an east half at 0.8, blind zone included:
        // after trim and coverage, the west stays the dimmer left half and
        // the north row matches the south row, which the per-column stretch
        // preserves because it works along latitude only.
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
        let covered = to_coverage(&trim_poles(&image), &settings);
        let nw = covered.pixels()[0];
        let ne = covered.pixels()[w / 2];
        let sw = covered.pixels()[(h - 1) * w];
        let se = covered.pixels()[(h - 1) * w + w / 2];
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

    // -----------------------------------------------------------------------
    // The raw pair and the derived map caches
    // -----------------------------------------------------------------------

    /// A source with a scratch cache directory, and the directory.
    fn scratch_cache(name: &str) -> (crate::test_support::ScratchDir, EumetsatCloudSource) {
        let scratch = crate::test_support::ScratchDir::new(name);
        let source = EumetsatCloudSource::new(
            EumetsatSettings::default(),
            Some(scratch.path().to_path_buf()),
        );
        (scratch, source)
    }

    /// One hemisphere's raw body at 16 px, uniform at `value`.
    fn raw_body(value: u8) -> Vec<u8> {
        png_bytes(16, 16, value)
    }

    /// Rewrite the stored manifest's fetch time to `secs_ago` before now, so
    /// a test can age a round past the upstream window without waiting for
    /// it.
    fn backdate_pair(scratch: &crate::test_support::ScratchDir, px: u32, secs_ago: u64) {
        let path = scratch.join(&format!("eumetsat_raw_{px}_meta.toml"));
        let text = std::fs::read_to_string(&path).expect("the manifest reads");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is set")
            .as_secs();
        let updated = text
            .lines()
            .map(|line| {
                if line.starts_with("fetched_epoch_secs") {
                    format!("fetched_epoch_secs = {}", now.saturating_sub(secs_ago))
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, format!("{updated}\n")).expect("the manifest is rewritten");
    }

    /// Store a raw pair and its derivation at 16 px, and point `source` at
    /// that size: the state a poll between rounds reads.
    fn store_round(source: &EumetsatCloudSource, west_value: u8, east_value: u8) {
        let west = raw_body(west_value);
        let east = raw_body(east_value);
        let hashes = (sha256_hex(&west), sha256_hex(&east));
        source.store_raw_pair(16, &west, &east).expect("stored");
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
            .store_raw_pair(16, &west, &east)
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
        source.store_raw_pair(16, &west, &east).expect("stored");
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
        old.store_raw_pair(16, &west, &east).expect("stored");
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
    // The upstream freshness gate and the user stop
    // -----------------------------------------------------------------------

    /// A round inside the upstream window is the present state whatever the
    /// poll would ask for: the poll after the first — the shape of the hourly
    /// check, or of a refresh pressed twice — answers from disk.
    #[test]
    fn a_round_within_the_upstream_window_is_served_from_disk() {
        let (scratch, mut source) = scratch_cache("eumetsat_fresh_gate");
        store_round(&source, 60, 90);
        source.fetch_if_changed(None).expect("the first fetch");
        let (agent, calls) = stub_agent(|_| panic!("a young round must not touch the network"));
        source.agent = agent;
        assert!(
            source
                .fetch_if_changed(None)
                .expect("the gate holds")
                .is_none(),
            "a fresh pair with a current derivation is reused"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(scratch);
    }

    /// A round past the window is old news: the poll downloads again and
    /// commits what it got.
    #[test]
    fn a_round_past_the_upstream_window_is_fetched_again() {
        let (scratch, mut source) = scratch_cache("eumetsat_stale_gate");
        store_round(&source, 60, 90);
        source.fetch_if_changed(None).expect("the first fetch");
        backdate_pair(&scratch, 16, UPSTREAM_REFRESH.as_secs() + 60);
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

    /// A young round whose derivation no longer matches — its metadata gone,
    /// say — is reprocessed from disk rather than downloaded again.
    #[test]
    fn a_fresh_pair_without_a_current_derivation_is_reprocessed_from_disk() {
        let (scratch, mut source) = scratch_cache("eumetsat_fresh_reprocess");
        store_round(&source, 60, 90);
        source.fetch_if_changed(None).expect("the first fetch");
        std::fs::remove_file(scratch.join("eumetsat_map_32x16_meta.toml"))
            .expect("the derivation's metadata is taken away");
        let (agent, calls) = stub_agent(|_| panic!("a young round must not touch the network"));
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

    #[test]
    fn a_first_fetch_with_a_current_derivation_costs_nothing() {
        let (_scratch, mut source) = scratch_cache("eumetsat_current");
        let west = raw_body(60);
        let east = raw_body(90);
        let hashes = (sha256_hex(&west), sha256_hex(&east));
        source.store_raw_pair(16, &west, &east).expect("stored");
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
        let (agent, calls) = stub_agent(|_| panic!("nothing changed, so nothing is fetched"));
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
