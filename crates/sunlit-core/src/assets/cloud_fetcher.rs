//! Cloud texture caching, decoding, and polling.
//!
//! A `CloudUpdater` owns the on-disk cache and turns a [`CloudSource`] into
//! decoded frames parked in the texture mailbox. The engine drives it from its
//! own worker thread on its own schedule.
//!
//! Two values can be overridden through the environment so tests can point the
//! fetcher at a local stub server: `SUNLIT_EARTH_CLOUD_URL` and
//! `SUNLIT_EARTH_CLOUD_POLL_SECS`. The cache folder is
//! [`crate::user_state::cache_dir`]'s, which `SUNLIT_EARTH_CACHE_DIR` moves.

use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::{DEFAULT_TEXTURE_RESOLUTION, TEXTURE_RESOLUTIONS};

use super::cloud_haze;
use super::cloud_source::{CloudSource, CloudTarget, Download};
use super::mailbox::{CloudFrame, TextureMailbox};
use super::texture_loader::{self, Channels, DecodedImage, Pixels};

/// Callback invoked after a new frame has been posted, so a client that only
/// works on demand knows there is something waiting. Headless callers that poll
/// on their own schedule pass a no-op.
pub(crate) type NotifyFn = Arc<dyn Fn() + Send + Sync>;

/// A notify callback that does nothing.
#[cfg(test)]
pub(crate) fn no_notify() -> NotifyFn {
    Arc::new(|| {})
}

/// First delay after a failed poll. Doubles up to [`MAX_RETRY_DELAY`].
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(15);
/// Ceiling for the retry backoff, so a service outage does not turn into an
/// hourly poll that misses the recovery by 59 minutes.
const MAX_RETRY_DELAY: Duration = Duration::from_mins(5);

/// Exponential backoff for failed cloud polls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryBackoff {
    delay: Duration,
}

impl RetryBackoff {
    pub fn new() -> Self {
        Self {
            delay: INITIAL_RETRY_DELAY,
        }
    }

    /// How long to wait before the next attempt.
    #[cfg(test)]
    pub fn delay(&self) -> Duration {
        self.delay
    }

    /// Record a failure and return how long to wait before retrying.
    pub fn fail(&mut self) -> Duration {
        let current = self.delay;
        self.delay = (self.delay * 2).min(MAX_RETRY_DELAY);
        current
    }

    /// Record a success, so the next outage starts from the short delay again.
    pub fn reset(&mut self) {
        self.delay = INITIAL_RETRY_DELAY;
    }
}

impl Default for RetryBackoff {
    fn default() -> Self {
        Self::new()
    }
}

/// Environment variable overriding the poll interval, in seconds.
const ENV_POLL_SECS: &str = "SUNLIT_EARTH_CLOUD_POLL_SECS";

/// Metadata cached alongside the cloud JPEG for freshness checks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CacheMeta {
    etag: Option<String>,
    last_modified: Option<String>,
}

/// The cloud image variant that goes with a surface texture resolution.
///
/// The upstream service publishes exactly the three widths
/// [`TEXTURE_RESOLUTIONS`] offers, each at 2:1, so on those three the mapping
/// is one to one and the clouds are as detailed as the surface under them. The
/// renderer takes any width as a cap, though, so a width from outside the three
/// takes the widest variant that does not exceed it, and one below all of them
/// takes the smallest: sharper clouds than surface is the one combination worth
/// ruling out.
pub(crate) fn cloud_variant(resolution: u32) -> (u32, u32) {
    let width = TEXTURE_RESOLUTIONS
        .iter()
        .copied()
        .filter(|&offered| offered <= resolution)
        .max()
        .or_else(|| TEXTURE_RESOLUTIONS.iter().copied().min())
        .unwrap_or(DEFAULT_TEXTURE_RESOLUTION);
    (width, width / 2)
}

/// Resolve the poll interval from an optional environment override.
///
/// Values that do not parse as a positive number of seconds are ignored
/// with a warning and the config's minutes are used instead. `None` is "no
/// usable override".
fn resolve_poll_interval(raw: Option<&str>) -> Option<Duration> {
    let value = raw?;
    match value.trim().parse::<u64>() {
        Ok(secs) if secs > 0 => Some(Duration::from_secs(secs)),
        _ => {
            warn!(
                value,
                env = ENV_POLL_SECS,
                "invalid poll interval, using the config value"
            );
            None
        }
    }
}

/// The configured poll interval: the env override when it holds, else the
/// config's minutes. `SUNLIT_EARTH_CLOUD_POLL_SECS` stays the strongest
/// voice, as it has been since before the setting existed.
pub fn poll_interval(mins: u32) -> Duration {
    resolve_poll_interval(crate::env_override(ENV_POLL_SECS).as_deref())
        .unwrap_or_else(|| Duration::from_mins(u64::from(mins)))
}

/// The cache directory, as [`crate::user_state::cache_dir`] resolves it.
pub fn cache_dir() -> Option<PathBuf> {
    crate::user_state::cache_dir()
}

fn load_cache_meta(path: &Path) -> Option<CacheMeta> {
    let contents = fs::read_to_string(path).ok()?;
    toml::from_str(&contents).ok()
}

fn save_cache_meta(meta: &CacheMeta, path: &Path) {
    crate::files::write_toml(meta, path, "cloud cache meta");
}

/// Write the cached JPEG, reporting whether the entry now holds it.
///
/// A failure takes the partial file with it. `fs::write` truncates before it
/// writes, so a half-written JPEG can be left behind, and the caller must be
/// able to read the answer as "there is an image here" or "there is not"
/// without a third state: it decides whether to keep the freshness sidecar on
/// that answer alone.
fn save_cache_image(bytes: &[u8], path: &Path) -> bool {
    if let Some(parent) = path.parent()
        && let Err(e) = fs::create_dir_all(parent)
    {
        warn!(path = %parent.display(), error = %e, "could not create cloud cache directory");
        return false;
    }
    if let Err(e) = fs::write(path, bytes) {
        warn!(path = %path.display(), error = %e, "could not cache cloud image");
        let _ = fs::remove_file(path);
        return false;
    }
    true
}

/// Remove a cache entry's freshness sidecar, if it has one.
///
/// Called when the image beside it could not be written, so that what is left
/// on disk is an entry with nothing in it rather than an `ETag` claiming an
/// image that is not there.
fn discard_cache_meta(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            warn!(path = %path.display(), error = %e, "could not discard cloud cache meta");
        }
    }
}

/// Decode a JPEG cloud image from raw bytes into its red channel, which is the
/// one the cloud shader reads.
///
/// The channel comes straight out of the decoder's buffer, so the image is
/// never held as RGBA: a gray source is one channel already, and a color one,
/// which is how upstream ships its gray maps, costs its three-byte decode and
/// the one-byte copy.
///
/// Through [`texture_loader::orient`], so the overlay lands in the same UV
/// layout as the file-backed maps and cannot drift from them.
#[tracing::instrument(skip(bytes), fields(bytes_len = bytes.len()))]
fn decode_cloud_jpeg(bytes: &[u8]) -> Result<DecodedImage, String> {
    let img =
        image::load_from_memory(bytes).map_err(|e| format!("Failed to decode cloud JPEG: {e}"))?;

    let width = img.width();
    let height = img.height();
    let pixels = match img {
        image::DynamicImage::ImageLuma8(gray) => gray.into_raw(),
        other => other.into_rgb8().pixels().map(|pixel| pixel[0]).collect(),
    };
    let mut decoded = DecodedImage {
        pixels: Pixels::new(pixels),
        width,
        height,
        channels: Channels::Red,
    };
    texture_loader::orient(&mut decoded);

    Ok(decoded)
}

/// What one call to [`CloudUpdater::poll_once`] achieved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PollOutcome {
    /// A new frame was decoded and posted to the mailbox.
    Updated,
    /// The source reported no change.
    Unchanged,
    /// The fetch or the decode failed; the caller decides about backoff.
    Failed,
    /// A new image was downloaded after the gate closed, and was dropped
    /// undecoded with nothing written, so the next poll downloads it again.
    Abandoned,
}

/// Turns a [`CloudSource`] into decoded frames in the texture mailbox, keeping
/// the on-disk cache in step.
pub(crate) struct CloudUpdater {
    source: Arc<dyn CloudSource>,
    mailbox: TextureMailbox,
    notify: NotifyFn,
    /// The cloud generation in force, shared with the renderer. Read at post
    /// time so a pair carries the generation it was made for; the worker bumps
    /// it when the target changes.
    generation: Arc<AtomicU64>,
    cache_dir: Option<PathBuf>,
    /// Avoid preparing the same offered resolution on every worker poll.
    variant: (u32, u32),
    /// The source, not this updater, owns URL resolution and cache identity.
    target: CloudTarget,
    image_path: Option<PathBuf>,
    meta_path: Option<PathBuf>,
    cached_meta: Option<CacheMeta>,
    /// Whether a map has been posted since construction, from the cache or
    /// the network. A cell because posting reads the updater and changes
    /// nothing else.
    posted: Cell<bool>,
    /// Whether a download that has come back is still wanted, read between the
    /// download and its decode. `None` wants every one.
    gate: Option<Arc<AtomicBool>>,
}

/// Where a cache entry's JPEG and its metadata sidecar live.
fn cache_paths(cache_dir: Option<&Path>, stem: &str) -> (Option<PathBuf>, Option<PathBuf>) {
    (
        cache_dir.map(|dir| dir.join(format!("{stem}.jpg"))),
        cache_dir.map(|dir| dir.join(format!("{stem}_meta.toml"))),
    )
}

impl CloudUpdater {
    /// Build an updater against `source`, fetching the variant that goes with
    /// `resolution` and caching into `cache_dir` (skipped entirely when `None`).
    ///
    /// The source is pointed at the URL the cache entry is named after, so the
    /// name and the bytes behind it cannot disagree. `generation` is the shared
    /// cloud generation: read at post time, bumped by the worker when the
    /// target changes.
    pub fn new(
        source: Arc<dyn CloudSource>,
        mailbox: TextureMailbox,
        notify: NotifyFn,
        generation: Arc<AtomicU64>,
        cache_dir: Option<PathBuf>,
        resolution: u32,
    ) -> Self {
        let variant = cloud_variant(resolution);
        let target = source.prepare(resolution);
        let (image_path, meta_path) = cache_paths(cache_dir.as_deref(), &target.cache_key);
        let cached_meta = meta_path.as_ref().and_then(|p| load_cache_meta(p));
        Self {
            source,
            mailbox,
            notify,
            generation,
            cache_dir,
            variant,
            target,
            image_path,
            meta_path,
            cached_meta,
            posted: Cell::new(false),
            gate: None,
        }
    }

    /// Follow a change of texture resolution to the cloud variant that goes
    /// with it.
    ///
    /// Retargets the source, moves to that variant's cache entry, picks up
    /// whatever freshness metadata was left there, and posts whatever image was
    /// left there so the switch is visible immediately. A variant with nothing
    /// on disk posts nothing, which leaves the previous overlay up while the
    /// download runs. `docs/architecture.md` has the argument for both.
    ///
    /// A resolution that maps to the variant already in force is a no-op, which
    /// is what lets the worker call this before every poll.
    ///
    /// Returns whether it moved to another cache entry, whose image it has
    /// then posted if there was one.
    pub fn set_resolution(&mut self, resolution: u32) -> bool {
        let variant = cloud_variant(resolution);
        if variant == self.variant {
            return false;
        }
        self.variant = variant;

        let target = self.source.prepare(resolution);
        let changed = target.cache_key != self.target.cache_key;
        self.target = target;
        if !changed {
            // A fixed URL keeps its image, in-memory ETag and cache entry.
            return false;
        }
        // The target moved, so the pair of the previous one is no longer
        // wanted. Bumped before the cached image of the new target is posted,
        // which is what makes that posting land with the generation now in
        // force; the worker reads the same counter for its own decisions.
        self.generation.fetch_add(1, Ordering::SeqCst);
        info!(
            resolution,
            "cloud source target follows the texture resolution"
        );

        let (image_path, meta_path) =
            cache_paths(self.cache_dir.as_deref(), &self.target.cache_key);
        self.image_path = image_path;
        self.meta_path = meta_path;
        self.cached_meta = self.meta_path.as_ref().and_then(|p| load_cache_meta(p));
        self.post_cached();
        true
    }

    /// Drop every download that comes back while `gate` reads false, before it
    /// is decoded or anything of it is written.
    pub fn set_gate(&mut self, gate: Arc<AtomicBool>) {
        self.gate = Some(gate);
    }

    /// Whether a map has been posted since construction.
    pub fn has_posted(&self) -> bool {
        self.posted.get()
    }

    /// Decode the cached image, if any, and post it so clouds appear without
    /// waiting for the network.
    pub fn post_cached(&self) -> bool {
        let Some(path) = self.image_path.as_ref().filter(|p| p.exists()) else {
            return false;
        };
        match fs::read(path).map(Download::new) {
            Ok(bytes) => match decode_cloud_jpeg(&bytes) {
                Ok(img) => {
                    info!(
                        width = img.width,
                        height = img.height,
                        path = %path.display(),
                        "loaded cached cloud image"
                    );
                    self.post(img);
                    true
                }
                Err(e) => {
                    warn!(error = %e, "cached cloud image decode failed");
                    false
                }
            },
            Err(e) => {
                warn!(error = %e, "could not read cached cloud image");
                false
            }
        }
    }

    /// One freshness check, download, decode, cache, and post cycle.
    pub fn poll_once(&mut self) -> PollOutcome {
        let known_etag = self.cached_meta.as_ref().and_then(|m| m.etag.as_deref());
        let start = std::time::Instant::now();
        let fetched = match self.source.fetch_if_changed(known_etag) {
            Ok(Some(image)) => image,
            Ok(None) => return PollOutcome::Unchanged,
            Err(e) => {
                warn!(error = %e, "cloud fetch failed");
                return PollOutcome::Failed;
            }
        };

        // Before the decode and before the cache: the bytes are dropped with
        // no image, sidecar or ETag written, so the next poll is a download
        // rather than a 304 with nothing to post.
        if self
            .gate
            .as_ref()
            .is_some_and(|gate| !gate.load(Ordering::SeqCst))
        {
            info!(
                bytes = fetched.bytes.len(),
                "the cloud layer was switched off during the download, dropping it"
            );
            return PollOutcome::Abandoned;
        }

        info!(
            bytes = fetched.bytes.len(),
            elapsed_secs = format_args!("{:.1}", start.elapsed().as_secs_f64()),
            "downloaded cloud image"
        );

        // Decoded before anything is persisted. The sidecar is a claim that a
        // decodable image for its ETag is on disk, and a body that does not
        // decode (a captive portal answering 200 with an error page and an
        // ETag, say) must not leave that claim anywhere: on disk it defeats
        // `post_cached` and every later switch, and in memory it turns the
        // retry the backoff exists for into a 304 with nothing to post.
        // Failing here keeps the previous entry and the previous ETag, so the
        // retry is a genuine re-download.
        let img = match decode_cloud_jpeg(&fetched.bytes) {
            Ok(img) => img,
            Err(e) => {
                warn!(error = %e, "cloud image decode failed");
                return PollOutcome::Failed;
            }
        };
        info!(
            width = img.width,
            height = img.height,
            "decoded cloud image"
        );
        crate::memory::log_memory_usage("after cloud decode");

        let cached = self
            .image_path
            .as_deref()
            .is_some_and(|path| save_cache_image(&fetched.bytes, path));

        let meta = CacheMeta {
            etag: fetched.etag,
            last_modified: fetched.last_modified,
        };
        if let Some(path) = self.meta_path.as_deref() {
            // The sidecar goes only where its image went; `discard_cache_meta`
            // says why an entry must not keep one without the other.
            if cached {
                save_cache_meta(&meta, path);
            } else {
                discard_cache_meta(path);
            }
        }
        // Kept in memory whatever the disk did. The frame below is about to be
        // on the GPU, so a 304 on the next poll is the right answer either way,
        // and with no cache directory at all this is the only copy there is.
        self.cached_meta = Some(meta);

        self.post(img);
        PollOutcome::Updated
    }

    /// Post a map and its clear-sky background, the background first.
    ///
    /// Neither carries a generation: the two slots are never purged, so there
    /// is nothing for a stamp to protect.
    fn post(&self, img: DecodedImage) {
        let start = std::time::Instant::now();
        let background = cloud_haze::background(&img);
        info!(
            width = img.width,
            height = img.height,
            elapsed_ms = format_args!("{:.1}", start.elapsed().as_secs_f64() * 1000.0),
            "estimated the clear-sky background"
        );
        // One pair, not two slot messages: the renderer uploads both and
        // rebuilds the cloud bind group once, so a new map is never drawn
        // against the previous background.
        self.mailbox.post_cloud_frame(CloudFrame {
            map: img,
            background,
            generation: self.generation.load(Ordering::SeqCst),
        });
        self.posted.set(true);
        (self.notify)();
    }
}

#[cfg(test)]
mod tests {
    use super::super::cloud_source::HttpCloudSource;
    use super::*;
    use crate::config::CloudSourceConfig;

    fn resolve_cloud_url(raw: Option<&str>, resolution: u32) -> String {
        let remote = CloudSourceConfig::default().resolve(raw).remote;
        let (w, h) = cloud_variant(resolution);
        remote.expand(&format!("{w}x{h}"))
    }

    fn variant_cloud_url(resolution: u32) -> String {
        let (w, h) = cloud_variant(resolution);
        format!("https://clouds.matteason.co.uk/images/{w}x{h}/clouds.jpg")
    }

    fn remote_cache_key(resolution: u32, override_url: Option<&str>) -> String {
        HttpCloudSource::from_config(CloudSourceConfig::default().resolve(override_url).remote)
            .prepare(resolution)
            .cache_key
    }
    use crate::test_support::ScratchDir;

    #[test]
    fn load_cache_meta_nonexistent_returns_none() {
        let scratch = ScratchDir::new("cloud_meta_nonexistent");
        assert!(load_cache_meta(&scratch.join("meta.toml")).is_none());
    }

    /// The sidecar is written into a directory that does not exist yet, so the
    /// same test covers the parent creation the first save depends on.
    #[test]
    fn save_and_load_cache_meta_round_trip() {
        let scratch = ScratchDir::new("cloud_meta_roundtrip");
        let dir = scratch.join("nested");

        for (name, meta) in [
            (
                "both",
                CacheMeta {
                    etag: Some("\"abc123\"".to_owned()),
                    last_modified: Some("Thu, 01 Jan 2026 00:00:00 GMT".to_owned()),
                },
            ),
            (
                "etag_only",
                CacheMeta {
                    etag: Some("\"etag-only\"".to_owned()),
                    last_modified: None,
                },
            ),
            ("neither", CacheMeta::default()),
        ] {
            let path = dir.join(format!("{name}.toml"));
            save_cache_meta(&meta, &path);
            assert!(path.exists(), "{name}: the parent must be created");
            let loaded = load_cache_meta(&path).expect("should load saved meta");
            assert_eq!(loaded.etag, meta.etag, "{name}");
            assert_eq!(loaded.last_modified, meta.last_modified, "{name}");
        }
    }

    #[test]
    fn an_offered_width_is_its_own_variant() {
        for &width in &TEXTURE_RESOLUTIONS {
            assert_eq!(cloud_variant(width).0, width);
        }
    }

    #[test]
    fn every_variant_is_two_to_one() {
        for &width in &TEXTURE_RESOLUTIONS {
            let (w, h) = cloud_variant(width);
            assert_eq!(w, h * 2);
        }
    }

    #[test]
    fn a_width_the_set_does_not_offer_takes_the_widest_that_fits() {
        let mut offered = TEXTURE_RESOLUTIONS;
        offered.sort_unstable();
        let narrowest = offered[0];
        let widest = offered[offered.len() - 1];

        for below in [0, 1, narrowest - 1] {
            assert_eq!(
                cloud_variant(below),
                cloud_variant(narrowest),
                "nothing fits under {narrowest}, so the narrowest variant is what is left"
            );
        }
        assert_eq!(cloud_variant(widest + 1), cloud_variant(widest));
        for pair in offered.windows(2) {
            assert_eq!(
                cloud_variant(pair[1] - 1),
                cloud_variant(pair[0]),
                "a width just under {} belongs to {}",
                pair[1],
                pair[0]
            );
        }
    }

    #[test]
    fn resolve_cloud_url_without_override_names_the_variant() {
        for &width in &TEXTURE_RESOLUTIONS {
            let (w, h) = cloud_variant(width);
            let url = resolve_cloud_url(None, width);
            assert_eq!(url, variant_cloud_url(width));
            assert!(url.contains(&format!("{w}x{h}")), "unexpected URL: {url}");
        }
    }

    #[test]
    fn invalid_environment_urls_fall_back_without_expanding_templates() {
        for raw in [
            "",
            "  ",
            " https://example.org/map",
            "https://example.org/map ",
            "ftp://example.org/map",
            "//example.org/map",
            "https:///map",
            "https://u:secret@example.org/map",
            "https://example.org/map#secret",
            "https://example.org/{size}",
            "https://example.org:bad/map",
            "https://example.org:65536/map",
            "https://example.org/a b",
            "https://example.org/map?token=a\nb",
        ] {
            assert_eq!(
                resolve_cloud_url(Some(raw), 4096),
                variant_cloud_url(4096),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn a_fixed_http_source_is_not_retargeted_or_given_the_legacy_cache() {
        use super::super::cloud_source::HttpCloudSource;
        let scratch = ScratchDir::new("cloud_fixed_cache");
        let source = Arc::new(HttpCloudSource::new(
            "https://example.org/map?token=one".to_owned(),
        ));
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let mut updater = CloudUpdater::new(
            Arc::clone(&source) as Arc<dyn CloudSource>,
            mailbox.clone(),
            no_notify(),
            test_generation(),
            Some(scratch.path().to_path_buf()),
            8192,
        );
        assert_eq!(source.describe(), "https://example.org/map");
        let image_path = updater.image_path.clone();
        assert_ne!(image_path, Some(scratch.join("clouds_cache_8192x4096.jpg")));
        assert_ne!(image_path, Some(scratch.join("clouds_cache_override.jpg")));
        updater.cached_meta = Some(CacheMeta {
            etag: Some("kept".to_owned()),
            last_modified: None,
        });
        assert!(!updater.set_resolution(2048));
        assert_eq!(updater.image_path, image_path);
        assert_eq!(
            updater.cached_meta.as_ref().unwrap().etag.as_deref(),
            Some("kept")
        );
        assert!(mailbox.take_all().is_empty());
    }

    #[test]
    fn custom_urls_never_read_another_sources_cache_or_legacy_override() {
        use super::super::cloud_source::HttpCloudSource;
        let scratch = ScratchDir::new("cloud_cache_identity");
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        for name in [
            "clouds_cache_override",
            "clouds_cache",
            "clouds_cache_8192x4096",
        ] {
            save_cache_image(
                &ScriptedSource::new().jpeg,
                &scratch.join(&format!("{name}.jpg")),
            );
            save_cache_meta(
                &CacheMeta {
                    etag: Some("foreign".to_owned()),
                    last_modified: None,
                },
                &scratch.join(&format!("{name}_meta.toml")),
            );
        }
        let make = |url: &str| {
            CloudUpdater::new(
                Arc::new(HttpCloudSource::new(url.to_owned())),
                mailbox.clone(),
                no_notify(),
                test_generation(),
                Some(scratch.path().to_path_buf()),
                8192,
            )
        };
        let first = make("https://example.org/clouds?token=one");
        assert!(!first.post_cached());
        assert!(first.cached_meta.is_none());
        save_cache_image(
            &ScriptedSource::new().jpeg,
            first.image_path.as_deref().unwrap(),
        );
        save_cache_meta(
            &CacheMeta {
                etag: Some("one".to_owned()),
                last_modified: None,
            },
            first.meta_path.as_deref().unwrap(),
        );
        let second = make("https://example.org/clouds?token=two");
        assert_ne!(first.image_path, second.image_path);
        assert!(!second.post_cached());
        assert!(second.cached_meta.is_none());
        let restarted = make("https://example.org/clouds?token=one");
        assert_eq!(first.image_path, restarted.image_path);
        assert!(restarted.post_cached());
        assert_eq!(restarted.cached_meta.unwrap().etag.as_deref(), Some("one"));
    }

    #[test]
    fn environment_override_wins_over_the_variant() {
        for resolution in TEXTURE_RESOLUTIONS {
            assert_eq!(
                resolve_cloud_url(Some("http://127.0.0.1:8080/clouds.jpg"), resolution),
                "http://127.0.0.1:8080/clouds.jpg"
            );
        }
    }

    #[test]
    fn each_variant_has_its_own_cache_entry() {
        let dir = PathBuf::from("C:/tmp/sunlit");
        let mut seen = std::collections::HashSet::new();
        for &width in &TEXTURE_RESOLUTIONS {
            let (image, meta) = cache_paths(Some(&dir), &remote_cache_key(width, None));
            let image = image.expect("a cache dir was given");
            let meta = meta.expect("a cache dir was given");
            assert_ne!(image, meta);
            assert!(seen.insert(image), "two variants share a JPEG path");
            assert!(seen.insert(meta), "two variants share a meta path");
        }
    }

    #[test]
    fn the_environment_override_gets_a_cache_entry_of_its_own() {
        let overridden = remote_cache_key(4096, Some("https://example.org/map.jpg"));
        for &width in &TEXTURE_RESOLUTIONS {
            assert_ne!(remote_cache_key(width, None), overridden);
        }
        // And one entry whatever the resolution, since the URL is the same.
        for &width in &TEXTURE_RESOLUTIONS {
            assert_eq!(
                remote_cache_key(width, Some("https://example.org/map.jpg")),
                overridden
            );
        }
    }

    #[test]
    fn no_cache_directory_means_no_cache_paths() {
        let (image, meta) = cache_paths(None, &remote_cache_key(4096, None));
        assert!(image.is_none());
        assert!(meta.is_none());
    }

    #[test]
    fn resolve_poll_interval_without_override_yields_none() {
        assert_eq!(resolve_poll_interval(None), None);
    }

    #[test]
    fn resolve_poll_interval_parses_seconds() {
        assert_eq!(
            resolve_poll_interval(Some("5")),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            resolve_poll_interval(Some(" 42 ")),
            Some(Duration::from_secs(42))
        );
    }

    #[test]
    fn resolve_poll_interval_rejects_invalid_values() {
        for value in ["0", "-1", "abc", "1.5", ""] {
            assert_eq!(
                resolve_poll_interval(Some(value)),
                None,
                "expected fallback for {value:?}"
            );
        }
    }

    #[test]
    fn decode_cloud_jpeg_invalid_bytes() {
        let result = decode_cloud_jpeg(&[0, 1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_cloud_jpeg_valid_minimal() {
        let mut buf = std::io::Cursor::new(Vec::new());
        let img = image::RgbaImage::from_pixel(2, 2, image::Rgba([128, 64, 32, 255]));
        let rgb_img = image::DynamicImage::ImageRgba8(img).into_rgb8();
        rgb_img
            .write_to(&mut buf, image::ImageFormat::Jpeg)
            .expect("should encode minimal JPEG");

        let result = decode_cloud_jpeg(buf.get_ref());
        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert_eq!(decoded.width, 2);
        assert_eq!(decoded.height, 2);
        assert_eq!(decoded.channels, Channels::Red);
        assert_eq!(decoded.pixels.len(), 2 * 2);
    }

    /// The overlay keeps the channel the shader samples, red, rather than a
    /// luminance, which for this color would be about 75.
    #[test]
    fn a_decoded_cloud_image_is_its_red_channel() {
        let mut buf = std::io::Cursor::new(Vec::new());
        let img = image::RgbImage::from_pixel(8, 8, image::Rgb([128, 64, 32]));
        image::DynamicImage::ImageRgb8(img)
            .write_to(&mut buf, image::ImageFormat::Png)
            .expect("encode");

        let decoded = decode_cloud_jpeg(buf.get_ref()).expect("a decode");
        assert!(
            decoded.pixels.iter().all(|&red| red == 128),
            "{:?}",
            decoded.pixels
        );

        let mut buf = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageLuma8(image::GrayImage::from_pixel(8, 8, image::Luma([77])))
            .write_to(&mut buf, image::ImageFormat::Png)
            .expect("encode");
        let decoded = decode_cloud_jpeg(buf.get_ref()).expect("a decode");
        assert!(
            decoded.pixels.iter().all(|&gray| gray == 77),
            "{:?}",
            decoded.pixels
        );
    }

    // -----------------------------------------------------------------------
    // CloudUpdater against a scripted source
    // -----------------------------------------------------------------------

    /// A source that serves a fixed image and flips its `ETag` on demand, so the
    /// updater's freshness logic can be tested without a server.
    struct ScriptedSource {
        jpeg: Vec<u8>,
        version: std::sync::atomic::AtomicU64,
        fetches: std::sync::atomic::AtomicU64,
        /// Number of upcoming calls that report a transport error.
        failures: std::sync::atomic::AtomicU64,
        /// Number of upcoming calls that serve a body that is not a JPEG.
        corrupt: std::sync::atomic::AtomicU64,
        /// Every URL this source has been pointed at, in order.
        retargets: std::sync::Mutex<Vec<String>>,
    }

    impl ScriptedSource {
        fn new() -> Self {
            let mut buf = std::io::Cursor::new(Vec::new());
            let img = image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
                4,
                2,
                image::Rgba([200, 200, 200, 255]),
            ))
            .into_rgb8();
            img.write_to(&mut buf, image::ImageFormat::Jpeg)
                .expect("encode fixture");
            Self {
                jpeg: buf.into_inner(),
                version: std::sync::atomic::AtomicU64::new(1),
                fetches: std::sync::atomic::AtomicU64::new(0),
                failures: std::sync::atomic::AtomicU64::new(0),
                corrupt: std::sync::atomic::AtomicU64::new(0),
                retargets: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn retargets(&self) -> Vec<String> {
            self.retargets.lock().expect("retarget log").clone()
        }

        /// Drop the retarget construction performed, so a test can talk about
        /// the ones a switch performs.
        fn forget_retargets(&self) {
            self.retargets.lock().expect("retarget log").clear();
        }

        /// Make the next `count` calls fail, as an offline service would.
        fn fail_next(&self, count: u64) {
            self.failures
                .store(count, std::sync::atomic::Ordering::SeqCst);
        }

        /// Make the next `count` calls serve a body that is not a JPEG, as a
        /// captive portal answering 200 with an error page would.
        fn corrupt_next(&self, count: u64) {
            self.corrupt
                .store(count, std::sync::atomic::Ordering::SeqCst);
        }

        fn publish(&self) {
            self.version
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        fn fetches(&self) -> u64 {
            self.fetches.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl CloudSource for ScriptedSource {
        fn fetch_if_changed(&self, known_etag: Option<&str>) -> Result<Option<CloudImage>, String> {
            if self
                .failures
                .try_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |n| (n > 0).then(|| n - 1),
                )
                .is_ok()
            {
                return Err("scripted transport failure".to_owned());
            }
            let current = format!(
                "v{}",
                self.version.load(std::sync::atomic::Ordering::SeqCst)
            );
            if known_etag == Some(current.as_str()) {
                return Ok(None);
            }
            self.fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let bytes = if self
                .corrupt
                .try_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |n| (n > 0).then(|| n - 1),
                )
                .is_ok()
            {
                b"an error page, not a JPEG".to_vec()
            } else {
                self.jpeg.clone()
            };
            Ok(Some(CloudImage {
                bytes: Download::new(bytes),
                etag: Some(current),
                last_modified: None,
            }))
        }

        fn describe(&self) -> String {
            "scripted".to_owned()
        }

        fn retarget(&self, url: &str) {
            self.retargets
                .lock()
                .expect("retarget log")
                .push(url.to_owned());
        }
    }

    use super::super::cloud_source::CloudImage;

    /// Enough slots for the grid and the two file-backed textures; the cloud
    /// pair no longer travels through the slots at all.
    const SLOT_COUNT: usize = 3;

    /// The cloud generation every test runs at unless it bumps its own.
    fn test_generation() -> Arc<std::sync::atomic::AtomicU64> {
        Arc::new(std::sync::atomic::AtomicU64::new(0))
    }

    /// Take every parked pair and count the maps, each of which must have come
    /// with a background of the grid's size.
    fn maps_posted(mailbox: &TextureMailbox) -> usize {
        let mut pairs = Vec::new();
        while let Some(frame) = mailbox.take_cloud_frame() {
            assert_eq!(
                (
                    frame.background.width as usize,
                    frame.background.height as usize
                ),
                (cloud_haze::GRID_WIDTH, cloud_haze::GRID_HEIGHT)
            );
            pairs.push(frame);
        }
        assert!(
            mailbox.take_all().is_empty(),
            "the cloud pair must not travel through the file-texture slots"
        );
        pairs.len()
    }

    fn updater_for(source: Arc<ScriptedSource>, mailbox: &TextureMailbox) -> CloudUpdater {
        CloudUpdater::new(
            source,
            mailbox.clone(),
            no_notify(),
            test_generation(),
            None,
            DEFAULT_TEXTURE_RESOLUTION,
        )
    }

    // -----------------------------------------------------------------------
    // RetryBackoff
    // -----------------------------------------------------------------------

    #[test]
    fn backoff_starts_short_and_doubles() {
        let mut backoff = RetryBackoff::new();
        assert_eq!(backoff.fail(), INITIAL_RETRY_DELAY);
        assert_eq!(backoff.fail(), INITIAL_RETRY_DELAY * 2);
        assert_eq!(backoff.fail(), INITIAL_RETRY_DELAY * 4);
    }

    #[test]
    fn backoff_saturates_at_the_ceiling() {
        let mut backoff = RetryBackoff::new();
        for _ in 0..20 {
            backoff.fail();
        }
        assert_eq!(backoff.delay(), MAX_RETRY_DELAY);
        assert_eq!(backoff.fail(), MAX_RETRY_DELAY);
    }

    #[test]
    fn backoff_resets_after_a_success() {
        let mut backoff = RetryBackoff::new();
        backoff.fail();
        backoff.fail();
        assert_ne!(backoff.delay(), INITIAL_RETRY_DELAY);
        backoff.reset();
        assert_eq!(backoff.delay(), INITIAL_RETRY_DELAY);
    }

    // -----------------------------------------------------------------------
    // CloudUpdater
    // -----------------------------------------------------------------------

    /// One pass of the lifecycle the cloud worker runs: the first poll
    /// downloads and posts, the polls after it revalidate into a 304 and post
    /// nothing, and a new publication upstream is downloaded and posted again.
    #[test]
    fn updater_downloads_then_revalidates_then_republishes() {
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let mut updater = updater_for(Arc::clone(&source), &mailbox);

        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        assert_eq!(maps_posted(&mailbox), 1);

        assert_eq!(updater.poll_once(), PollOutcome::Unchanged);
        assert_eq!(updater.poll_once(), PollOutcome::Unchanged);
        assert_eq!(
            source.fetches(),
            1,
            "only the changed version is downloaded"
        );
        assert!(mailbox.take_all().is_empty());

        source.publish();
        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        assert_eq!(source.fetches(), 2);
        assert_eq!(maps_posted(&mailbox), 1);
    }

    /// A failure posts nothing and leaves the freshness the last success
    /// established, so the recovery is an ordinary poll and the poll after it
    /// can still short-circuit on a 304.
    #[test]
    fn updater_reports_failure_and_recovers() {
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let mut updater = updater_for(Arc::clone(&source), &mailbox);

        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        mailbox.take_all();

        source.fail_next(2);
        assert_eq!(updater.poll_once(), PollOutcome::Failed);
        assert_eq!(updater.poll_once(), PollOutcome::Failed);
        assert!(
            mailbox.take_all().is_empty(),
            "a failed poll must not post a frame"
        );

        assert_eq!(
            updater.poll_once(),
            PollOutcome::Unchanged,
            "the ETag that worked is still the one on file"
        );

        source.publish();
        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        assert_eq!(maps_posted(&mailbox), 1, "the recovery posts a frame");
    }

    #[test]
    fn updater_notifies_after_posting() {
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let notify: NotifyFn = Arc::new(move || {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let mut updater = CloudUpdater::new(
            source,
            mailbox,
            notify,
            test_generation(),
            None,
            DEFAULT_TEXTURE_RESOLUTION,
        );

        updater.poll_once();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn updater_without_a_cache_dir_posts_nothing_from_disk() {
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let updater = updater_for(source, &mailbox);
        assert!(!updater.post_cached());
        assert!(mailbox.take_all().is_empty());
    }

    #[test]
    fn updater_round_trips_through_the_disk_cache() {
        let scratch = ScratchDir::new("cloud_updater_cache");
        let dir = scratch.path();
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);

        let mut first = cached_updater(&source, &mailbox, dir, DEFAULT_TEXTURE_RESOLUTION);
        first.poll_once();
        mailbox.take_all();

        // A fresh updater reads the ETag back and does not re-download.
        let mut second = cached_updater(&source, &mailbox, dir, DEFAULT_TEXTURE_RESOLUTION);
        assert_eq!(second.poll_once(), PollOutcome::Unchanged);
        assert_eq!(source.fetches(), 1);
        assert!(second.post_cached(), "the cached JPEG should decode");
        assert_eq!(maps_posted(&mailbox), 1);
    }

    /// A download that comes back while the gate is closed is dropped with
    /// nothing decoded, posted or written, so the next poll with the gate open
    /// downloads it again rather than revalidating into a 304.
    #[test]
    fn a_download_returning_through_a_closed_gate_leaves_nothing_behind() {
        let scratch = ScratchDir::new("cloud_updater_gate");
        let dir = scratch.path();
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let gate = Arc::new(AtomicBool::new(false));
        let mut updater = cached_updater(&source, &mailbox, dir, DEFAULT_TEXTURE_RESOLUTION);
        updater.set_gate(Arc::clone(&gate));

        assert_eq!(updater.poll_once(), PollOutcome::Abandoned);
        assert_eq!(source.fetches(), 1);
        assert!(mailbox.take_all().is_empty(), "something was posted");
        assert!(!updater.has_posted());
        assert_eq!(
            std::fs::read_dir(dir).expect("the cache directory").count(),
            0,
            "something was written to the cache"
        );

        gate.store(true, Ordering::SeqCst);
        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        assert_eq!(source.fetches(), 2, "the open gate downloads again");
        assert_eq!(maps_posted(&mailbox), 1);
    }

    /// An updater on a disk cache at a chosen resolution.
    fn cached_updater(
        source: &Arc<ScriptedSource>,
        mailbox: &TextureMailbox,
        dir: &Path,
        resolution: u32,
    ) -> CloudUpdater {
        CloudUpdater::new(
            Arc::clone(source) as Arc<dyn CloudSource>,
            mailbox.clone(),
            no_notify(),
            test_generation(),
            Some(dir.to_path_buf()),
            resolution,
        )
    }

    // -----------------------------------------------------------------------
    // Following the texture resolution
    // -----------------------------------------------------------------------

    #[test]
    fn construction_points_the_source_at_the_variant_it_will_cache_under() {
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let _updater = CloudUpdater::new(
            Arc::clone(&source) as Arc<dyn CloudSource>,
            mailbox,
            no_notify(),
            test_generation(),
            None,
            8192,
        );
        let retargets = source.retargets();
        assert_eq!(retargets.len(), 1, "expected one retarget: {retargets:?}");
        assert!(
            retargets[0].contains("8192x4096"),
            "unexpected URL: {}",
            retargets[0]
        );
    }

    #[test]
    fn set_resolution_points_the_source_at_the_new_variant() {
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let mut updater = updater_for(Arc::clone(&source), &mailbox);
        source.forget_retargets();

        updater.set_resolution(2048);
        let retargets = source.retargets();
        assert_eq!(retargets.len(), 1, "expected one retarget: {retargets:?}");
        assert!(
            retargets[0].contains("2048x1024"),
            "unexpected URL: {}",
            retargets[0]
        );
    }

    /// The worker calls `set_resolution` before every poll, so a resolution
    /// that leaves the variant where it is has to cost neither a retarget nor a
    /// cache reload. That covers both the same width again and a different
    /// width that shares the variant.
    #[test]
    fn a_resolution_that_keeps_the_variant_does_not_retarget() {
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let mut updater = updater_for(Arc::clone(&source), &mailbox);
        source.forget_retargets();

        for _ in 0..3 {
            updater.set_resolution(DEFAULT_TEXTURE_RESOLUTION);
        }
        assert_eq!(source.retargets(), [] as [String; 0]);

        let shared = DEFAULT_TEXTURE_RESOLUTION + 1;
        assert_eq!(
            cloud_variant(shared),
            cloud_variant(DEFAULT_TEXTURE_RESOLUTION)
        );
        updater.set_resolution(shared);
        assert_eq!(source.retargets(), [] as [String; 0]);
    }

    /// The widest and narrowest widths on offer, which is what a switch has to
    /// move between for the two entries to be distinct.
    fn widest_and_narrowest() -> (u32, u32) {
        let mut offered = TEXTURE_RESOLUTIONS;
        offered.sort_unstable();
        (offered[offered.len() - 1], offered[0])
    }

    /// An image that could not be cached must leave no freshness claim behind,
    /// including one an earlier poll left there.
    ///
    /// The write is made to fail by putting a directory where the JPEG goes,
    /// which fails on both platforms and, unlike an unwritable parent, leaves
    /// the sidecar beside it perfectly writable. That is what makes the two
    /// halves separable here.
    #[test]
    fn an_image_that_could_not_be_cached_leaves_no_freshness_claim() {
        let scratch = ScratchDir::new("cloud_failed_image_write");
        let dir = scratch.path();
        let stem = remote_cache_key(8192, None);
        let image = dir.join(format!("{stem}.jpg"));
        let meta = dir.join(format!("{stem}_meta.toml"));

        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let mut updater = cached_updater(&source, &mailbox, dir, 8192);

        // A healthy poll first, so there is an entry for the failure to have to
        // clear rather than merely decline to create.
        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        mailbox.take_all();
        assert!(image.is_file() && meta.is_file());

        fs::remove_file(&image).expect("remove the cached image");
        fs::create_dir(&image).expect("occupy the image path with a directory");
        source.publish();

        assert_eq!(
            updater.poll_once(),
            PollOutcome::Updated,
            "a cache that cannot be written must not stop the frame reaching the screen"
        );
        assert_eq!(maps_posted(&mailbox), 1);
        assert!(
            !meta.is_file(),
            "an ETag was recorded for an image that is not on disk"
        );

        // A restart is a fresh updater reading that entry: it has to download
        // rather than revalidate into a 304 with nothing to post.
        let mut restarted = cached_updater(&source, &mailbox, dir, 8192);
        assert!(!restarted.post_cached(), "there is no image to post");
        assert_eq!(restarted.poll_once(), PollOutcome::Updated);
        assert_eq!(source.fetches(), 3);
    }

    #[test]
    fn an_undecodable_body_leaves_no_freshness_claim() {
        let scratch = ScratchDir::new("cloud_undecodable_body");
        let dir = scratch.path();
        let stem = remote_cache_key(8192, None);
        let image = dir.join(format!("{stem}.jpg"));
        let meta = dir.join(format!("{stem}_meta.toml"));

        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);
        let mut updater = cached_updater(&source, &mailbox, dir, 8192);

        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        mailbox.take_all();
        let good_image = fs::read(&image).expect("the healthy poll cached its image");
        let good_meta = fs::read(&meta).expect("and its sidecar");

        source.publish();
        source.corrupt_next(1);
        assert_eq!(
            updater.poll_once(),
            PollOutcome::Failed,
            "a body that does not decode is a failed poll, not an update"
        );
        assert!(mailbox.take_all().is_empty());
        assert_eq!(
            fs::read(&image).expect("the last good image stays"),
            good_image
        );
        assert_eq!(
            fs::read(&meta).expect("with the sidecar that matches it"),
            good_meta
        );

        assert_eq!(
            updater.poll_once(),
            PollOutcome::Updated,
            "the retry must re-download, not revalidate into a 304"
        );
        assert_eq!(maps_posted(&mailbox), 1);
        assert_eq!(source.fetches(), 3);
    }

    #[test]
    fn a_switch_with_nothing_cached_and_no_network_posts_nothing() {
        let scratch = ScratchDir::new("cloud_switch_offline");
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);

        let mut updater = cached_updater(&source, &mailbox, scratch.path(), 8192);
        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        mailbox.take_all();

        source.fail_next(1);
        updater.set_resolution(2048);
        assert_eq!(updater.poll_once(), PollOutcome::Failed);
        assert!(
            mailbox.take_all().is_empty(),
            "nothing to post means nothing posted, so the old overlay stays"
        );
    }

    #[test]
    fn each_variant_keeps_its_own_cached_image_and_a_switch_back_shows_it() {
        let scratch = ScratchDir::new("cloud_variant_cache");
        let (widest, narrowest) = widest_and_narrowest();
        let source = Arc::new(ScriptedSource::new());
        let mailbox = TextureMailbox::new(SLOT_COUNT);

        let mut updater = cached_updater(&source, &mailbox, scratch.path(), widest);
        assert_eq!(updater.poll_once(), PollOutcome::Updated);
        mailbox.take_all();

        // Down to a variant with nothing on disk: the switch posts nothing,
        // which is what leaves the wide overlay up while the download runs, and
        // the ETag that satisfied the wide variant must not satisfy this one.
        updater.set_resolution(narrowest);
        assert!(
            mailbox.take_all().is_empty(),
            "a variant with no cache entry has nothing to post"
        );
        assert_eq!(
            updater.poll_once(),
            PollOutcome::Updated,
            "the new variant must be fetched, not served from the old entry"
        );
        assert_eq!(source.fetches(), 2);
        mailbox.take_all();

        // Back up, inside the upstream refresh window: the wide entry is still
        // on disk with its own ETag.
        updater.set_resolution(widest);
        assert_eq!(
            maps_posted(&mailbox),
            1,
            "the switch must post the cached wide image"
        );
        assert_eq!(
            updater.poll_once(),
            PollOutcome::Unchanged,
            "the entry is still fresh, so the poll posts nothing"
        );
        assert_eq!(source.fetches(), 2, "and costs no third download");
    }
}
