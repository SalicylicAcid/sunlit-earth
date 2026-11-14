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
use std::sync::Mutex;
use std::time::{Duration, Instant};

use image::ImageEncoder;
use tracing::info;

use super::cloud_fetcher::cloud_variant;
use super::cloud_source::{CloudImage, CloudSource, CloudTarget, Download};
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

/// One WMS request at most, however slow the service is today.
const REQUEST_TIMEOUT: Duration = Duration::from_mins(5);
/// TCP/TLS handshake cap, so a dead endpoint is known in seconds.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// One whole round (both hemispheres and their retries) at most.
const ROUND_BUDGET: Duration = Duration::from_mins(15);
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
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "kernel offsets around a fixed-size image, mirrored at the boundary"
)]
pub(crate) fn gaussian_blur(image: &GrayMap, sigma: f64) -> GrayMap {
    if sigma <= 0.0 {
        return image.clone();
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

    let width = image.width as usize;
    let height = image.height as usize;
    // Horizontal pass, then vertical on the horizontal result.
    let mut horizontal = vec![0.0f32; width * height];
    for y in 0..height {
        for x in 0..width {
            let mut acc = 0.0f32;
            for (k, &weight) in kernel.iter().enumerate() {
                let dx = k as isize - radius as isize;
                let sx = mirror(x as isize + dx, width as isize) as usize;
                acc += image.pixels[y * width + sx] * weight;
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
    GrayMap::new(image.width, image.height, out).expect("same rectangle")
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
enum FetchError {
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
pub struct EumetsatCloudSource {
    settings: EumetsatSettings,
    /// Hemisphere size in pixels currently prepared. `prepare` and the fetch
    /// both run on the engine's single cloud worker; the lock is for
    /// construction and for tests, like `HttpCloudSource`'s URL.
    size: Mutex<u32>,
    /// Backoff between transient attempts; the shipped values are
    /// [`RETRY_DELAYS`] and tests shorten them.
    retry_delays: [Duration; (MAX_ATTEMPTS - 1) as usize],
    agent: ureq::Agent,
}

/// The hemisphere size a texture resolution's map needs: the map's width
/// halves into two square hemispheres.
pub(crate) fn hemisphere_px(resolution: u32) -> u32 {
    cloud_variant(resolution).1
}

impl EumetsatCloudSource {
    pub fn new(settings: EumetsatSettings) -> Self {
        let user_agent = format!("sunlit.earth/{}", env!("CARGO_PKG_VERSION"));
        let agent = ureq::Agent::config_builder()
            .user_agent(&user_agent)
            // Errors are classified here, from the status and its headers.
            .http_status_as_error(false)
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_per_call(Some(REQUEST_TIMEOUT))
            .build()
            .new_agent();
        Self {
            settings,
            size: Mutex::new(hemisphere_px(DEFAULT_TEXTURE_RESOLUTION)),
            retry_delays: RETRY_DELAYS,
            agent,
        }
    }

    /// What one round's cache entry is named after: the layer, the processing
    /// settings and the hemisphere size. Hashed, so the key names no other
    /// source's entry and carries no settings in the clear.
    fn identity(&self, px: u32) -> String {
        let s = &self.settings;
        format!(
            "{LAYER}|{}|{}|{}|{}|{px}",
            s.floor, s.ceiling, s.gamma, s.blur
        )
    }

    fn px(&self) -> u32 {
        *self
            .size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Fetch one hemisphere with up to [`MAX_ATTEMPTS`] tries, transient
    /// failures backed off and a Retry-After honored within `deadline`.
    ///
    /// No attempt starts after `deadline`; a request already in flight ends
    /// when it ends, and the next attempt then sees the exhausted budget.
    fn fetch_hemisphere(&self, bbox: &str, px: u32, deadline: Instant) -> Result<GrayMap, String> {
        let url = build_wms_url(bbox, px);
        for attempt in 1..=MAX_ATTEMPTS {
            if Instant::now() >= deadline {
                return Err(format!(
                    "EUMETSAT {bbox} hemisphere: the round budget is spent before attempt {attempt}"
                ));
            }
            match self.request_hemisphere(&url, px, MAX_BODY_BYTES) {
                Ok(map) => return Ok(map),
                // A permanent refusal or an invalid image: repeating this
                // round verbatim cannot help, the next scheduled one may.
                Err(FetchError::Permanent(message)) => {
                    return Err(format!("EUMETSAT {bbox} hemisphere: {message}"));
                }
                Err(FetchError::Transient(message, retry_after)) => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(format!(
                            "EUMETSAT {bbox} hemisphere failed after {MAX_ATTEMPTS} attempts: {message}"
                        ));
                    }
                    let delay = retry_after.unwrap_or(self.retry_delays[(attempt - 1) as usize]);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if delay >= remaining {
                        return Err(format!(
                            "EUMETSAT {bbox} hemisphere: retrying after {delay:?} would pass the round budget"
                        ));
                    }
                    info!(attempt, bbox, delay = ?delay, "retrying the EUMETSAT hemisphere");
                    std::thread::sleep(delay);
                }
            }
        }
        unreachable!("every loop path returns")
    }

    /// One attempt of one hemisphere: request, classify, read, validate.
    fn request_hemisphere(&self, url: &str, px: u32, max_body: u64) -> Result<GrayMap, FetchError> {
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
        let body = response
            .body_mut()
            .with_config()
            .limit(max_body)
            .read_to_vec()
            .map_err(|e| match e {
                ureq::Error::BodyExceedsLimit(_) => {
                    FetchError::Permanent(format!("the response exceeds the {max_body} byte cap"))
                }
                other => FetchError::Transient(format!("body read: {other}"), None),
            })?;
        decode_png(&body, px).map_err(FetchError::Permanent)
    }
}

impl CloudSource for EumetsatCloudSource {
    fn prepare(&self, resolution: u32) -> CloudTarget {
        let px = hemisphere_px(resolution);
        let mut size = self
            .size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *size = px;
        CloudTarget::eumetsat(&self.identity(px))
    }

    fn fetch_if_changed(&self, _known_etag: Option<&str>) -> Result<Option<CloudImage>, String> {
        let px = self.px();
        let started = Instant::now();
        let deadline = started + ROUND_BUDGET;
        // Both hemispheres of one round, in order; only a complete pair goes
        // on to processing, so no half a map is ever stitched to history.
        let west = self.fetch_hemisphere(HEMISPHERES[0].1, px, deadline)?;
        info!(elapsed = ?started.elapsed(), px, "the west hemisphere arrived");
        let east = self.fetch_hemisphere(HEMISPHERES[1].1, px, deadline)?;
        info!(elapsed = ?started.elapsed(), px, "both hemispheres arrived");
        let stitched = stitch(&west, &east);
        drop(west);
        drop(east);
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
        Ok(Some(CloudImage {
            bytes: Download::new(bytes),
            etag: None,
            last_modified: None,
        }))
    }

    fn describe(&self) -> String {
        format!("eumetsat {LAYER}")
    }
}

/// The Retry-After header, in whole seconds. The HTTP-date form is not
/// honored: only a delay this round can actually wait out is.
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
        assert_eq!(ROUND_BUDGET, Duration::from_mins(15));
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
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default());
        let (agent, calls) = stub_agent(|_| Ok(png_response(16, 128)));
        source.agent = agent;
        let url = build_wms_url("-90,-180,90,0", 16);
        let Err(error) = source.request_hemisphere(&url, 16, 8) else {
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
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default());
        let (agent, calls) = stub_agent(|_| Ok(png_response(16, 128)));
        source.agent = agent;
        let spent = Instant::now()
            .checked_sub(Duration::from_mins(1))
            .expect("a minute ago is representable");
        let Err(error) = source.fetch_hemisphere("-90,-180,90,0", 16, spent) else {
            panic!("an exhausted budget fetched anyway");
        };
        assert!(error.contains("budget"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn transient_failures_are_retried_and_a_retry_after_within_the_budget_wins() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default());
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
        let map = source
            .fetch_hemisphere("-90,-180,90,0", 16, started + ROUND_BUDGET)
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
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default());
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        let (agent, calls) = stub_agent(|n| {
            if n == 1 {
                Err(ureq::Error::Io(std::io::Error::other("connection died")))
            } else {
                Ok(png_response(16, 128))
            }
        });
        source.agent = agent;
        let map = source
            .fetch_hemisphere("-90,-180,90,0", 16, Instant::now() + ROUND_BUDGET)
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
            let mut source = EumetsatCloudSource::new(EumetsatSettings::default());
            let (agent, calls) = stub_agent(make);
            source.agent = agent;
            let Err(error) =
                source.fetch_hemisphere("-90,-180,90,0", 16, Instant::now() + ROUND_BUDGET)
            else {
                panic!("{label} produced a map");
            };
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{label} retried");
            assert!(!error.is_empty(), "{label}");
        }
    }

    #[test]
    fn fetch_if_changed_rounds_two_hemispheres_into_one_jpeg_and_ignores_etags() {
        let mut source = EumetsatCloudSource::new(EumetsatSettings::default());
        source.retry_delays = [Duration::ZERO, Duration::ZERO];
        // The 2048 texture resolution halves into 1024 px hemispheres, which
        // the stub serves. Both hemispheres are one identical 128/255 fill,
        // so the stitched 2048x1024 map is uniform before trim, coverage and
        // encode.
        let (agent, calls) = stub_agent(|_| Ok(png_response(1024, 128)));
        source.agent = agent;
        source.prepare(2048);
        let image = source.fetch_if_changed(Some("\"old\"")).unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2, "one round, two requests");
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
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn prepare_tracks_resolution_and_hashed_identity() {
        let source = EumetsatCloudSource::new(EumetsatSettings::default());
        let wide = source.prepare(8192);
        assert!(wide.cache_key.starts_with("clouds_cache_eumetsat_"));
        assert_eq!(wide.wait, super::super::cloud_source::CloudWait::LongFetch);
        let narrow = source.prepare(2048);
        assert_ne!(wide.cache_key, narrow.cache_key, "the size is in the key");
        assert_eq!(source.px(), 1024);
        let retuned = EumetsatCloudSource::new(EumetsatSettings {
            floor: 61,
            ..EumetsatSettings::default()
        });
        assert_ne!(
            retuned.prepare(2048).cache_key,
            narrow.cache_key,
            "the settings are in the key"
        );
        assert!(
            source
                .describe()
                .starts_with("eumetsat mumi:worldcloudmap_ir108")
        );
    }
}
