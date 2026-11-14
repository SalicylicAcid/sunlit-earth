//! The NASA `GIBS` cloud source: one WMS `GetMap` request for the day's global
//! MODIS Terra cloud fraction, converted to the grayscale map the cloud
//! pipeline already decodes.
//!
//! GIBS is CDN-fronted and measured an order of magnitude faster than
//! EUMETSAT from East Asia (a full 4096x2048 map in ~25 s). The trade-offs
//! are inherent to the product: it is a daily composite, not a 15-minute
//! one, and the polar-orbiting satellite covers only part of the globe per
//! day (~53% measured) — the uncovered rest renders as clear sky. It is the
//! fast fallback, and EUMETSAT stays the near-real-time source.
//!
//! The layer's PNG encodes cloud fraction in the red channel (0-255 = 0-100%
//! cover), alpha 255 where the satellite saw the surface and 0 for its swath
//! gaps. Conversion is per pixel: fraction where visible, clear sky where
//! not. The imagery requires NASA's attribution; see the About window.

use std::io::Read as _;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use image::ImageEncoder;
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use super::cloud_fetcher::cloud_variant;
use super::cloud_source::{CloudImage, CloudSource, CloudTarget, CloudWait, Download};
use super::eumetsat::{FetchError, blur_f32};
use crate::config::DEFAULT_TEXTURE_RESOLUTION;

/// GIBS' WMS 1.1.1 endpoint for the Geographic projection, unauthenticated.
pub(crate) const WMS_URL: &str = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi";
/// The daily MODIS Terra cloud fraction product: near-complete daytime
/// coverage of one full orbit day, the widest of GIBS' cloud-cover layers.
pub(crate) const LAYER: &str = "MODIS_Terra_Cloud_Fraction_Day";

/// Quality of the derived grayscale JPEG, matching the other sources.
const JPEG_QUALITY: u8 = 88;
/// The processing algorithm's version, stored in the derived map's metadata.
const ALGORITHM_VERSION: u32 = 3;
/// The no-data sentinel a converted map carries where the satellite's swath
/// missed, and what [`fill_line`] treats as a gap to bridge.
const NO_DATA: f32 = -1.0;

/// One WMS request at most.
const REQUEST_TIMEOUT: Duration = Duration::from_mins(2);
/// TCP/TLS handshake cap.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Attempts per round for transient failures.
const MAX_ATTEMPTS: u32 = 2;
/// Backoff between attempts.
const RETRY_DELAYS: [Duration; (MAX_ATTEMPTS - 1) as usize] = [Duration::from_secs(10)];
/// Compressed response cap.
const MAX_BODY_BYTES: u64 = 50 * 1024 * 1024;
/// Decoded map cap, matched to the largest resolution the app offers.
const MAX_MAP_SIDE: u32 = 8192;
/// How often a download refreshes its progress line.
const DOWNLOAD_REPORT_EVERY: Duration = Duration::from_millis(500);

/// A grayscale map as row-major f32 in [0, 1], north up, -180..180 west to
/// east — the same orientation every other source produces.
pub(crate) struct GrayMap {
    width: u32,
    height: u32,
    pixels: Vec<f32>,
}

impl GrayMap {
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

    #[cfg(test)]
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

/// The raw PNG's own metadata: the commit point of a stored download.
#[derive(serde::Serialize, serde::Deserialize)]
struct RawManifest {
    version: u32,
    algorithm: u32,
    raw_sha256: String,
    fetched_epoch_secs: u64,
}

/// The derived map's metadata: reused only when everything matches.
#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
struct DerivedMeta {
    version: u32,
    algorithm: u32,
    raw_sha256: String,
}

/// The native GIBS source: one request per round, no parameters.
pub struct GibsCloudSource {
    /// Map width in pixels currently prepared (the height is width / 2).
    size: Mutex<u32>,
    /// Where the raw PNG and the derived map are cached.
    cache_dir: Option<PathBuf>,
    /// Whether the next fetch is this instance's first: a fresh instance with
    /// a stored pair and a current derivation costs nothing.
    first_fetch: AtomicBool,
    /// What the round is doing right now, for the UI's source status.
    status: Mutex<String>,
    agent: ureq::Agent,
}

/// The map width a texture resolution asks for: GIBS serves the full 2:1 map
/// in one request, unlike the EUMETSAT hemisphere pairs.
pub(crate) fn map_width(resolution: u32) -> u32 {
    cloud_variant(resolution).0
}

impl GibsCloudSource {
    pub fn new(cache_dir: Option<PathBuf>) -> Self {
        let user_agent = format!("sunlit.earth/{}", env!("CARGO_PKG_VERSION"));
        let agent = ureq::Agent::config_builder()
            .user_agent(&user_agent)
            .http_status_as_error(false)
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_per_call(Some(REQUEST_TIMEOUT))
            // The user's own proxy route, if they run one.
            .proxy(ureq::Proxy::try_from_env())
            .build()
            .new_agent();
        Self {
            size: Mutex::new(map_width(DEFAULT_TEXTURE_RESOLUTION)),
            cache_dir,
            first_fetch: AtomicBool::new(true),
            status: Mutex::new(String::new()),
            agent,
        }
    }

    fn identity(width: u32) -> String {
        format!("{LAYER}|{width}x{}", width / 2)
    }

    fn width(&self) -> u32 {
        *self.size.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_status(&self, text: &str) {
        text.clone_into(&mut self.status.lock().unwrap_or_else(PoisonError::into_inner));
    }

    /// Fetch the day's map with up to [`MAX_ATTEMPTS`] tries.
    fn fetch_map(&self, width: u32, deadline: Instant) -> Result<(Vec<u8>, GrayMap), String> {
        let height = width / 2;
        let url = format!(
            "{WMS_URL}?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&LAYERS={LAYER}\
             &SRS=EPSG:4326&BBOX=-90,-180,90,180&WIDTH={width}&HEIGHT={height}&FORMAT=image/png"
        );
        for attempt in 1..=MAX_ATTEMPTS {
            if Instant::now() >= deadline {
                return Err(format!(
                    "GIBS: the round budget is spent before attempt {attempt}"
                ));
            }
            self.set_status(&format!(
                "downloading the map (attempt {attempt}/{MAX_ATTEMPTS})"
            ));
            match self.request_map(&url, width, MAX_BODY_BYTES) {
                Ok(pair) => return Ok(pair),
                Err(FetchError::Permanent(message)) => {
                    return Err(format!("GIBS: {message}"));
                }
                Err(FetchError::Transient(message, retry_after)) => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(format!(
                            "GIBS failed after {MAX_ATTEMPTS} attempts: {message}"
                        ));
                    }
                    let delay = retry_after.unwrap_or(RETRY_DELAYS[(attempt - 1) as usize]);
                    info!(attempt, delay = ?delay, "retrying the GIBS round");
                    std::thread::sleep(delay);
                }
            }
        }
        unreachable!("every loop path returns")
    }

    /// One attempt: request, classify, read with live progress, validate.
    fn request_map(
        &self,
        url: &str,
        width: u32,
        max_body: u64,
    ) -> Result<(Vec<u8>, GrayMap), FetchError> {
        let mut response = self
            .agent
            .get(url)
            .call()
            .map_err(|e| FetchError::Transient(format!("transport: {e}"), None))?;
        let status = response.status().as_u16();
        if status == 429 || status >= 500 {
            return Err(FetchError::Transient(
                format!("HTTP {status}"),
                retry_after_header(response.headers()),
            ));
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
            return Err(FetchError::Permanent(format!(
                "the service answered {content_type:?}, not an image"
            )));
        }
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
                let line = progress_line(&body, total, &request_started);
                self.set_status(&line);
            }
        }
        let map = decode_png(&body, width).map_err(FetchError::Permanent)?;
        Ok((body, map))
    }

    /// Store the raw PNG: written to a temporary file, verified by decoding
    /// what landed, renamed, and the manifest written last as the commit
    /// point.
    fn store_raw(&self, width: u32, raw: &[u8]) -> Result<(), String> {
        let Some(dir) = &self.cache_dir else {
            return Ok(());
        };
        let tmp = dir.join(format!("{LAYER}_{width}.png.tmp"));
        std::fs::write(&tmp, raw).map_err(|e| format!("the raw file failed: {e}"))?;
        decode_png(
            &std::fs::read(&tmp).map_err(|e| format!("the raw read back failed: {e}"))?,
            width,
        )?;
        std::fs::rename(&tmp, dir.join(format!("{LAYER}_{width}.png")))
            .map_err(|e| format!("the raw commit failed: {e}"))?;
        let manifest = RawManifest {
            version: 1,
            algorithm: ALGORITHM_VERSION,
            raw_sha256: sha256_hex(raw),
            fetched_epoch_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        let manifest =
            toml::to_string(&manifest).map_err(|e| format!("the raw manifest failed: {e}"))?;
        write_committed(
            &dir.join(format!("{LAYER}_{width}_meta.toml")),
            manifest.as_bytes(),
        )
    }

    /// Load the stored raw PNG, trusting it only when the manifest's hash
    /// matches what is on disk.
    fn load_raw(&self, width: u32) -> Option<(Vec<u8>, String)> {
        let dir = self.cache_dir.as_ref()?;
        let manifest = std::fs::read(dir.join(format!("{LAYER}_{width}_meta.toml"))).ok()?;
        let manifest: RawManifest = toml::from_str(&String::from_utf8_lossy(&manifest)).ok()?;
        let raw = std::fs::read(dir.join(format!("{LAYER}_{width}.png"))).ok()?;
        let raw_sha = sha256_hex(&raw);
        if manifest.raw_sha256 != raw_sha {
            warn!("the stored raw map does not match its manifest, ignoring it");
            return None;
        }
        Some((raw, raw_sha))
    }

    /// Whether the stored derived map is reusable: it exists and its
    /// metadata matches the raw hash and the algorithm version.
    fn derived_matches(&self, width: u32, raw_sha: &str) -> bool {
        let Some(dir) = &self.cache_dir else {
            return false;
        };
        let height = width / 2;
        if !dir.join(format!("gibs_map_{width}x{height}.jpg")).exists() {
            return false;
        }
        let Ok(meta) = std::fs::read(dir.join(format!("gibs_map_{width}x{height}_meta.toml")))
        else {
            return false;
        };
        let Ok(meta) = toml::from_str::<DerivedMeta>(&String::from_utf8_lossy(&meta)) else {
            return false;
        };
        meta.version == 1 && meta.algorithm == ALGORITHM_VERSION && meta.raw_sha256 == raw_sha
    }

    /// Convert the raw RGBA map to grayscale, fill the orbit gaps, smooth
    /// the product's native grid, and store the result with its metadata.
    fn process_and_store(&self, width: u32, raw: &[u8], raw_sha: &str) -> Result<Vec<u8>, String> {
        let decoded = decode_png(raw, width)?;
        // The product's native grid is about one degree: at this width the
        // data is blocky, and the satellite's orbit gaps cut hard through
        // cloud bands. Small gaps are bridged by interpolation along their
        // row, the remainder along their column, and what is left — a region
        // the satellite never saw today — stays clear sky. A blur scaled to
        // the native grid smooths both the blockiness and the fill seams.
        let mut pixels = decoded.pixels().to_vec();
        fill_gaps(
            width,
            decoded.height(),
            &mut pixels,
            // Row runs up to 12% of the width are bridged; longer runs are
            // regions the satellite never saw today and stay clear sky.
            width as usize / 8,
        );
        blur_f32(&pixels, width, decoded.height(), f64::from(width) / 1024.0);
        let filled = GrayMap::new(width, decoded.height(), pixels).expect("the filled rectangle");
        let bytes = encode_jpeg(&filled);
        info!(bytes = bytes.len(), "the GIBS map is generated");
        if let Some(dir) = &self.cache_dir {
            let height = width / 2;
            let meta = DerivedMeta {
                version: 1,
                algorithm: ALGORITHM_VERSION,
                raw_sha256: raw_sha.to_owned(),
            };
            let committed = (|| -> Result<(), String> {
                write_committed(&dir.join(format!("gibs_map_{width}x{height}.jpg")), &bytes)?;
                let meta = toml::to_string(&meta)
                    .map_err(|e| format!("the derived metadata failed: {e}"))?;
                write_committed(
                    &dir.join(format!("gibs_map_{width}x{height}_meta.toml")),
                    meta.as_bytes(),
                )
            })();
            if let Err(e) = committed {
                // The bytes are in hand and about to be posted; a failed cache
                // write costs the next start its fast path, not this round.
                warn!(error = %e, "the derived map could not be cached");
            }
        }
        Ok(bytes)
    }
}

/// The RGBA map's cloud fraction as grayscale: the red channel where the
/// satellite saw the surface, clear sky where its swath missed.
pub(crate) fn rgba_to_gray(img: &image::RgbaImage, width: u32) -> Result<GrayMap, String> {
    let (w, h) = (img.width(), img.height());
    if w != width || h != width / 2 {
        return Err(format!(
            "GIBS response is {w}x{h}, expected {width}x{}",
            width / 2
        ));
    }
    let pixels = img
        .pixels()
        .map(|p| {
            if p.0[3] == 255 {
                f32::from(p.0[0]) / 255.0
            } else {
                NO_DATA
            }
        })
        .collect();
    Ok(GrayMap::new(w, h, pixels).expect("the decoded rectangle"))
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the generator's float_to_uint8 truncates toward zero"
)]
fn encode_jpeg(map: &GrayMap) -> Vec<u8> {
    let bytes: Vec<u8> = map
        .pixels
        .iter()
        .map(|&v| (v.clamp(0.0, 1.0) * 255.0) as u8)
        .collect();
    let mut out = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, JPEG_QUALITY)
        .write_image(&bytes, map.width, map.height, image::ColorType::L8.into())
        .expect("a grayscale JPEG of an in-memory buffer");
    out
}

/// Decode the raw PNG into a [`GrayMap`], validating format and exact size
/// before any decode allocation happens.
fn decode_png(body: &[u8], width: u32) -> Result<GrayMap, String> {
    const PNG_SIGNATURE: [u8; 8] = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
    if !body.starts_with(&PNG_SIGNATURE) {
        return Err("the GIBS response is not a PNG".to_owned());
    }
    if body.len() < 24 {
        return Err("the GIBS PNG is too short to hold its dimensions".to_owned());
    }
    let w = u32::from_be_bytes([body[16], body[17], body[18], body[19]]);
    let h = u32::from_be_bytes([body[20], body[21], body[22], body[23]]);
    if w != width || h != width / 2 {
        return Err(format!(
            "GIBS response is {w}x{h}, expected {width}x{}",
            width / 2
        ));
    }
    if w > MAX_MAP_SIDE {
        return Err(format!(
            "the GIBS map {w}x{h} exceeds the {MAX_MAP_SIDE} side cap"
        ));
    }
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_MAP_SIDE);
    limits.max_image_height = Some(MAX_MAP_SIDE);
    let mut reader = image::ImageReader::new(std::io::Cursor::new(body));
    reader.set_format(image::ImageFormat::Png);
    reader.limits(limits);
    let img = reader
        .decode()
        .map_err(|e| format!("the GIBS PNG did not decode: {e}"))?;
    rgba_to_gray(&img.to_rgba8(), width)
}

/// Linearly interpolate no-data runs no longer than `run_max` along one
/// dimension. A pixel is no-data when it holds the negative sentinel; runs
/// longer than the bound stay negative — regions the satellite never saw
/// stay clear sky rather than streaking across half the map.
fn fill_line(values: &[f32], run_max: usize) -> Vec<f32> {
    let mut out = values.to_vec();
    let len = values.len();
    let mut position = 0;
    while position < len {
        if values[position] >= 0.0 {
            position += 1;
            continue;
        }
        let start = position;
        while position < len && values[position] < 0.0 {
            position += 1;
        }
        let run = position - start;
        if run > run_max {
            continue;
        }
        let left = start.checked_sub(1).map(|i| values[i]);
        let right = (position < len).then(|| values[position]);
        match (left, right) {
            (Some(l), Some(r)) =>
            {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "a run position is far inside the f32 mantissa"
                )]
                for (offset, i) in (start..position).enumerate() {
                    let t = (offset + 1) as f32 / (run + 1) as f32;
                    out[i] = l + (r - l) * t;
                }
            }
            (Some(l), None) => {
                out[start..position].fill(l);
            }
            (None, Some(r)) => {
                out[start..position].fill(r);
            }
            (None, None) => {}
        }
    }
    out
}

/// Bridge the satellite's orbit gaps: small no-data runs are interpolated
/// along their row, the remainder along their column, and what no pass
/// reaches — a region the satellite never saw today — is cleared to clear
/// sky afterwards.
fn fill_gaps(width: u32, height: u32, pixels: &mut [f32], run_row: usize) {
    let (w, h) = (width as usize, height as usize);
    // No-data pixels carry the [`NO_DATA`] sentinel through the fill pass.
    // A row that is mostly sentinel is a polar band the satellite barely
    // saw: bridging there stretches single sightings across hundreds of
    // pixels, and a column pass would smear mid-latitude values into the
    // polar band vertically, so both are left alone — such lines keep their
    // sightings, and their sentinels clear to cloudless sky at the end. The
    // orbit tracks the slits follow run pole to pole, so a row crossing one
    // sees a short run: the row pass is the one that bridges them.
    for y in 0..h {
        let line = &mut pixels[y * w..(y + 1) * w];
        if valid_fraction(line) < 0.5 {
            continue;
        }
        let filled = fill_line(line, run_row);
        line.copy_from_slice(&filled);
    }
    for pixel in pixels.iter_mut() {
        if *pixel < 0.0 {
            *pixel = 0.0;
        }
    }
}

/// The share of a line's pixels the satellite actually saw.
#[expect(
    clippy::cast_precision_loss,
    reason = "a pixel count is far inside the f32 mantissa"
)]
fn valid_fraction(line: &[f32]) -> f32 {
    line.iter().filter(|&&v| v >= 0.0).count() as f32 / line.len() as f32
}

fn progress_line(body: &[u8], total: Option<u64>, started: &Instant) -> String {
    #[expect(
        clippy::cast_precision_loss,
        reason = "a byte count is far inside the f64 mantissa"
    )]
    let speed = body.len() as f64 / started.elapsed().as_secs_f64().max(0.001);
    match total.filter(|&total| total > 0) {
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
                "the map: {} of {} ({}) — {:.0}%",
                mib(body.len()),
                mib(total),
                speed_text(speed),
                percent
            )
        }
        None => format!("the map: {} ({})", mib(body.len()), speed_text(speed)),
    }
}

fn write_committed(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).map_err(|e| format!("the temporary write failed: {e}"))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("the commit failed: {e}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn retry_after_header(headers: &ureq::http::HeaderMap) -> Option<Duration> {
    let value = headers.get("Retry-After")?.to_str().ok()?;
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

fn mib(bytes: usize) -> String {
    #[expect(
        clippy::cast_precision_loss,
        reason = "a byte count is far inside the f64 mantissa"
    )]
    fn as_mib(bytes: usize) -> f64 {
        bytes as f64 / (1024.0 * 1024.0)
    }
    format!("{:.1} MiB", as_mib(bytes))
}

fn speed_text(bytes_per_second: f64) -> String {
    if bytes_per_second >= 1024.0 * 1024.0 {
        format!("{:.1} MiB/s", bytes_per_second / (1024.0 * 1024.0))
    } else {
        format!("{:.0} KiB/s", bytes_per_second / 1024.0)
    }
}

impl CloudSource for GibsCloudSource {
    fn prepare(&self, resolution: u32) -> CloudTarget {
        let width = map_width(resolution);
        let mut size = self.size.lock().unwrap_or_else(PoisonError::into_inner);
        *size = width;
        CloudTarget::gibs(&Self::identity(width))
    }

    fn wait(&self) -> Duration {
        CloudWait::Existing.duration()
    }

    fn status(&self) -> Option<String> {
        let status = self.status.lock().unwrap_or_else(PoisonError::into_inner);
        (!status.is_empty()).then(|| status.clone())
    }

    fn fetch_if_changed(&self, _known_etag: Option<&str>) -> Result<Option<CloudImage>, String> {
        let width = self.width();
        let started = Instant::now();
        let deadline = started + Duration::from_mins(3);

        let cached_raw = self.load_raw(width);
        if self.first_fetch.swap(false, Ordering::SeqCst)
            && let Some((raw, raw_sha)) = &cached_raw
        {
            if self.derived_matches(width, raw_sha) {
                info!(width, "the stored GIBS map is still current");
                return Ok(None);
            }
            info!(
                width,
                "reprocessing the stored GIBS map without the network"
            );
            let bytes = self.process_and_store(width, raw, raw_sha)?;
            return Ok(Some(CloudImage {
                bytes: Download::new(bytes),
                etag: None,
                last_modified: None,
            }));
        }

        let (raw_body, _map) = self.fetch_map(width, deadline)?;
        let raw_sha = sha256_hex(&raw_body);
        if let Some((stored, stored_sha)) = &cached_raw
            && raw_body == *stored
            && self.derived_matches(width, stored_sha)
        {
            return Ok(None);
        }
        self.set_status("processing the map");
        self.store_raw(width, &raw_body)?;
        let bytes = self.process_and_store(width, &raw_body, &raw_sha)?;
        self.set_status("");
        info!(
            bytes = bytes.len(),
            elapsed = ?started.elapsed(),
            "the GIBS round is generated"
        );
        Ok(Some(CloudImage {
            bytes: Download::new(bytes),
            etag: None,
            last_modified: None,
        }))
    }

    fn describe(&self) -> String {
        format!("gibs {LAYER}")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn rgba_png(width: u32, height: u32, paint: &dyn Fn(u32, u32) -> [u8; 4]) -> Vec<u8> {
        let mut img = image::RgbaImage::new(width, height);
        for (x, y, pixel) in img.enumerate_pixels_mut() {
            let [r, g, b, a] = paint(x, y);
            *pixel = image::Rgba([r, g, b, a]);
        }
        let mut out = Vec::new();
        image::DynamicImage::ImageRgba8(img)
            .write_to(&mut std::io::Cursor::new(&mut out), image::ImageFormat::Png)
            .unwrap();
        out
    }

    fn scratch(name: &str) -> (crate::test_support::ScratchDir, GibsCloudSource) {
        let scratch = crate::test_support::ScratchDir::new(name);
        let source = GibsCloudSource::new(Some(scratch.path().to_path_buf()));
        (scratch, source)
    }

    /// Opaque left half at fraction 200/255, transparent right half.
    fn sample_map() -> Vec<u8> {
        rgba_png(16, 8, &|x, _| {
            if x < 8 {
                [200, 200, 200, 255]
            } else {
                [0, 0, 0, 0]
            }
        })
    }

    #[test]
    fn the_rgba_map_converts_fraction_and_no_data() {
        let raw = sample_map();
        let map = decode_png(&raw, 16).expect("the map decodes");
        assert_eq!(map.width(), 16);
        // Opaque: the red channel's fraction. Transparent: the no-data
        // sentinel the gap fill bridges.
        approx::assert_relative_eq!(map.pixels()[0], 200.0 / 255.0);
        assert!(
            map.pixels()[8] == NO_DATA,
            "the sentinel survives the decode"
        );
        // The left half stays opaque on the last row.
        approx::assert_relative_eq!(map.pixels()[7 * 16], 200.0 / 255.0);
    }

    #[test]
    fn a_wrong_size_is_refused_before_any_decode() {
        let raw = sample_map();
        let Err(error) = decode_png(&raw, 32) else {
            panic!("a wrong size decoded");
        };
        assert!(error.contains("32x16"), "{error}");
    }

    #[test]
    fn the_raw_pair_is_stored_validated_and_loaded_back_only_intact() {
        let (scratch, source) = scratch("gibs_raw");
        let raw = sample_map();
        source.store_raw(16, &raw).expect("stored");
        assert!(scratch.join(format!("{LAYER}_16.png").as_str()).is_file());
        assert!(
            !scratch
                .join(format!("{LAYER}_16.png.tmp").as_str())
                .exists()
        );
        let (loaded, sha) = source.load_raw(16).expect("loads");
        assert_eq!(loaded, raw);
        assert_eq!(sha, sha256_hex(&raw));
        let corrupted = scratch.join(format!("{LAYER}_16.png").as_str());
        let mut bytes = std::fs::read(&corrupted).expect("read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&corrupted, &bytes).expect("corrupted");
        assert!(
            source.load_raw(16).is_none(),
            "the manifest's hash no longer matches"
        );
    }

    #[test]
    fn the_derived_map_is_reused_only_when_its_metadata_matches() {
        let (scratch, source) = scratch("gibs_derived");
        let raw = sample_map();
        source.store_raw(16, &raw).expect("stored");
        let sha = sha256_hex(&raw);
        assert!(!source.derived_matches(16, &sha));
        source.process_and_store(16, &raw, &sha).expect("stored");
        assert!(source.derived_matches(16, &sha));
        assert!(scratch.join("gibs_map_16x8.jpg").is_file());
        // A new algorithm version stops the match without touching anything.
        assert!(!source.derived_matches(16, "different-hash"));
    }

    #[test]
    fn a_first_fetch_with_a_current_derivation_costs_nothing() {
        let (_scratch, mut source) = scratch("gibs_current");
        let raw = sample_map();
        source.store_raw(16, &raw).expect("stored");
        let sha = sha256_hex(&raw);
        source.process_and_store(16, &raw, &sha).expect("stored");
        let (agent, calls) = stub_agent(|| panic!("nothing changed, so nothing is fetched"));
        source.agent = agent;
        *source.size.lock().expect("the size lock") = 16;
        assert!(
            source.fetch_if_changed(None).expect("succeeds").is_none(),
            "the stored map and its derivation are current"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn fill_gaps_bridges_short_runs_and_clears_long_ones() {
        // One row: a 3-px gap (bridged) and a 10-px gap (kept, cleared),
        // separated by a valid pixel so the two runs are distinct.
        let mut pixels = vec![NO_DATA; 32 * 8];
        for y in 0..8 {
            for x in 0..32 {
                pixels[y * 32 + x] = if x < 4 {
                    0.2
                } else if x < 7 {
                    NO_DATA
                } else if x < 8 {
                    0.5
                } else if x < 18 {
                    NO_DATA
                } else {
                    0.8
                };
            }
        }
        fill_gaps(32, 8, &mut pixels, 32 / 8);
        // The 3-px gap between 0.2 and 0.5 is bridged; the 10-px gap (run_max
        // 4) is cleared sky, not streaked across.
        approx::assert_relative_eq!(pixels[0], 0.2);
        let bridged = &pixels[4..7];
        assert!(
            bridged[0] > 0.2 && bridged[2] < 0.5,
            "the short run is interpolated: {bridged:?}"
        );
        assert!(
            pixels[8..18].iter().all(|&v| v == 0.0),
            "the long run is clear sky"
        );
        approx::assert_relative_eq!(pixels[18], 0.8);
        assert!(
            pixels.iter().all(|&v| v.is_finite() && v >= 0.0),
            "no sentinel survives the fill"
        );
    }

    #[test]
    fn a_mostly_empty_line_is_not_filled() {
        // A polar band: 90% sentinel, two sightings far apart. Bridging them
        // would streak the pair across the band, so the line keeps its
        // sightings and its sentinels clear.
        let mut row = vec![NO_DATA; 32];
        row[2] = 0.4;
        row[20] = 0.9;
        let mut pixels = vec![0.5f32; 32 * 8];
        pixels[..32].copy_from_slice(&row);
        fill_gaps(32, 8, &mut pixels, 32 / 8);
        // Row 0 is 6% valid — untouched: its sentinels clear, its two
        // sightings stay.
        // The sightings survive bit-exact: the line was skipped, not written.
        assert_eq!(pixels[2].to_bits(), 0.4f32.to_bits());
        assert_eq!(pixels[20].to_bits(), 0.9f32.to_bits());
        assert!(
            pixels[0] == 0.0 && pixels[10] == 0.0 && pixels[30] == 0.0,
            "the skipped row's sentinels clear"
        );
        // The fully valid rows are untouched (nothing to fill).
        assert_eq!(pixels[32].to_bits(), 0.5f32.to_bits());
    }

    #[test]
    fn the_status_composes_the_rounds_phases() {
        let source = GibsCloudSource::new(None);
        assert_eq!(source.status(), None);
        source.set_status("downloading the map (attempt 1/2)");
        assert_eq!(
            source.status().as_deref(),
            Some("downloading the map (attempt 1/2)")
        );
        source.set_status("");
        assert_eq!(source.status(), None);
    }

    fn stub_agent(
        make: impl Fn() -> Result<ureq::http::Response<ureq::Body>, ureq::Error> + Send + Sync + 'static,
    ) -> (ureq::Agent, Arc<std::sync::atomic::AtomicUsize>) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .middleware(
                move |_: ureq::http::Request<ureq::SendBody>,
                      _: ureq::middleware::MiddlewareNext| {
                    seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    make()
                },
            )
            .build()
            .new_agent();
        (agent, calls)
    }
}
