//! Where cloud imagery comes from.
//!
//! The engine talks to a `CloudSource` rather than to HTTP directly, so tests
//! can publish frames on a simulated schedule without a network or a server.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tracing::info;

use super::cloud_fetcher::cloud_variant;
use crate::config::CloudSourceConfig;
use crate::config::CloudSourceMode;
use crate::config::clouds::{DEFAULT_REMOTE_URL, ENV_CLOUD_URL, RemoteCloudUrl};

/// One fetched cloud image plus the freshness metadata that came with it.
pub struct CloudImage {
    pub bytes: Download,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// The downloaded cloud images of this process, as [`downloads`] counts them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DownloadCount {
    /// Buffers alive now.
    pub buffers: u64,
    /// Bytes the live buffers hold.
    pub bytes: u64,
    /// Buffers made since the process started, alive or not.
    pub made: u64,
}

static LIVE_BUFFERS: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static MADE_BUFFERS: AtomicU64 = AtomicU64::new(0);

/// The [`Download`]s alive in this process, and how many there have been.
///
/// A download lives only through the poll that fetched it, or the cache read
/// that loaded it, while it is decoded, and for a poll written to the cache, so between
/// updates the live count is zero, and a buffer that stays alive is a download
/// parked in a queue, a cache or a long-lived struct. `made` tells "dropped"
/// from "not fetched yet", as it does for the decoded frames.
pub fn downloads() -> DownloadCount {
    DownloadCount {
        made: MADE_BUFFERS.load(Ordering::SeqCst),
        buffers: LIVE_BUFFERS.load(Ordering::SeqCst),
        bytes: LIVE_BYTES.load(Ordering::SeqCst),
    }
}

/// The body of a cloud image as it came over the wire, or back off the disk
/// cache, counted in [`downloads`] for as long as it exists.
///
/// Counted on creation and uncounted on drop, on the buffer rather than as
/// `Drop` on [`CloudImage`], whose freshness fields the updater moves out.
pub struct Download(Vec<u8>);

impl Download {
    /// Count `bytes` as one download.
    pub fn new(bytes: Vec<u8>) -> Self {
        LIVE_BYTES.fetch_add(bytes.capacity() as u64, Ordering::SeqCst);
        LIVE_BUFFERS.fetch_add(1, Ordering::SeqCst);
        MADE_BUFFERS.fetch_add(1, Ordering::SeqCst);
        Self(bytes)
    }
}

impl Drop for Download {
    fn drop(&mut self) {
        LIVE_BYTES.fetch_sub(self.0.capacity() as u64, Ordering::SeqCst);
        LIVE_BUFFERS.fetch_sub(1, Ordering::SeqCst);
    }
}

impl std::ops::Deref for Download {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

/// Metadata for a source's startup wait; consumers opt in separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloudWait {
    Existing,
    LongFetch,
    SlowFetch,
}

impl CloudWait {
    pub fn duration(self) -> Duration {
        match self {
            Self::Existing => Duration::from_secs(60),
            Self::LongFetch => Duration::from_mins(26),
            Self::SlowFetch => Duration::from_mins(5),
        }
    }
}

/// Identity of the image prepared for the next poll. No URL secrets in the key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudTarget {
    pub cache_key: String,
    pub wait: CloudWait,
}

/// Resolve a persisted cloud source into the runtime source, the way the
/// startup and a runtime switch both do: the environment override forces a
/// fixed remote URL, and the saved mode picks the native or the remote source
/// otherwise. One implementation, so neither path can grow its own opinion of
/// what a configuration means.
pub fn resolve(
    config: &CloudSourceConfig,
    env_url: Option<&str>,
    cache_dir: Option<&std::path::Path>,
) -> Arc<dyn CloudSource> {
    let effective = config.resolve(env_url);
    match effective.mode {
        CloudSourceMode::Remote => Arc::new(HttpCloudSource::from_config(effective.remote)),
        CloudSourceMode::Eumetsat => Arc::new(super::eumetsat::EumetsatCloudSource::new(
            effective.eumetsat,
            cache_dir.map(std::path::Path::to_path_buf),
        )),
        CloudSourceMode::Nsmc => Arc::new(super::nsmc::NsmcCloudSource::new(
            effective.nsmc,
            cache_dir.map(std::path::Path::to_path_buf),
        )),
    }
}

impl CloudTarget {
    fn remote(url: &str, legacy_template: bool) -> Self {
        // Only the exact original template may reuse a pre-source cache entry.
        let legacy = legacy_template
            .then(|| {
                crate::config::TEXTURE_RESOLUTIONS
                    .iter()
                    .find_map(|&width| {
                        let (w, h) = cloud_variant(width);
                        let size = format!("{w}x{h}");
                        (url == DEFAULT_REMOTE_URL.replace("{size}", &size))
                            .then(|| format!("clouds_cache_{size}"))
                    })
            })
            .flatten();
        Self {
            cache_key: legacy.unwrap_or_else(|| {
                use std::fmt::Write;
                let mut key = "clouds_cache_url_".to_owned();
                for byte in Sha256::digest(url.as_bytes()) {
                    let _ = write!(key, "{byte:02x}");
                }
                key
            }),
            wait: CloudWait::Existing,
        }
    }
}

fn prepare_remote(remote: &RemoteCloudUrl, resolution: u32) -> (String, CloudTarget) {
    let (w, h) = cloud_variant(resolution);
    let url = remote.expand(&format!("{w}x{h}"));
    let target = CloudTarget::remote(&url, remote.is_default_template());
    (url, target)
}

impl CloudTarget {
    /// Hash the EUMETSAT layer, hemisphere size and algorithm version, excluding tuning parameters.
    pub(crate) fn eumetsat(identity: &str) -> Self {
        use std::fmt::Write;
        let mut key = "clouds_cache_eumetsat_".to_owned();
        for byte in Sha256::digest(identity.as_bytes()) {
            let _ = write!(key, "{byte:02x}");
        }
        Self {
            cache_key: key,
            wait: CloudWait::LongFetch,
        }
    }

    /// Hash the NSMC layer, map size and algorithm version, excluding tuning parameters.
    pub(crate) fn nsmc(identity: &str) -> Self {
        use std::fmt::Write;
        let mut key = "clouds_cache_nsmc_".to_owned();
        for byte in Sha256::digest(identity.as_bytes()) {
            let _ = write!(key, "{byte:02x}");
        }
        Self {
            cache_key: key,
            wait: CloudWait::SlowFetch,
        }
    }
}

/// A source of equirectangular cloud imagery.
///
/// Implementations must be cheap to call when nothing has changed: the engine
/// polls on a schedule and expects `None` most of the time.
pub trait CloudSource: Send + Sync {
    /// Select the image and cache identity before polling. The default keeps
    /// URL-driven fixtures following the original service (and env override);
    /// fixed fixtures can continue to ignore `retarget`.
    fn prepare(&self, resolution: u32) -> CloudTarget {
        let effective =
            CloudSourceConfig::default().resolve(crate::env_override(ENV_CLOUD_URL).as_deref());
        let (url, target) = prepare_remote(&effective.remote, resolution);
        self.retarget(&url);
        target
    }

    /// How long a client waits for this source's first answer: a remote image
    /// is one download, the native source a whole generation budget.
    fn wait(&self) -> Duration {
        CloudWait::Existing.duration()
    }

    /// What the source is doing right now, when it knows. The fetch worker's
    /// own states (checking, ready, failed) are the coarse answer; a source
    /// whose work has visible phases says which it is in, and the UI shows
    /// that instead.
    fn status(&self) -> Option<String> {
        None
    }

    /// Which imagery the last settled fetch left on screen, when the source
    /// names it. Appended to the worker's coarse status, so the idle line says
    /// what is drawn rather than only that something is: a source that
    /// publishes one dated round an hour needs it, since a stale map and a
    /// current one look exactly alike otherwise.
    fn settled_note(&self) -> Option<String> {
        None
    }

    /// Fetch the current image if it differs from `known_etag`.
    ///
    /// Returns `Ok(None)` when the source is unchanged.
    fn fetch_if_changed(&self, known_etag: Option<&str>) -> Result<Option<CloudImage>, String>;

    /// Hand the source the flag a user stop reads: set while a running fetch
    /// is wanted gone, and the source unwinds to its nearest check point.
    /// A default no-op, because a source that serves one thing in one call
    /// has nowhere between pieces of work to look; the sources that read a
    /// body in chunks override it and check per chunk.
    fn set_cancel(&self, _flag: Arc<AtomicBool>) {}

    /// Human-readable description for logs.
    fn describe(&self) -> String;

    /// Point this source at a different URL.
    ///
    /// The texture resolution selects which cloud image variant to fetch, and
    /// the variant is a path segment of the URL, so a resolution switch has to
    /// move the source rather than replace it: the engine holds it behind an
    /// `Arc` shared with a worker thread that is mid-poll as often as not.
    /// Takes `&self` for that reason, and does nothing by default, which is the
    /// right answer for every source that serves one fixed thing.
    fn retarget(&self, _url: &str) {}
}

/// The production source: an HTTP endpoint polled with HEAD + `If-None-Match`.
pub struct HttpCloudSource {
    agent: ureq::Agent,
    /// Behind a lock because [`CloudSource::retarget`] moves it while the
    /// worker thread owns the only handle.
    url: Mutex<String>,
    /// Kept intact when a resolution expands a new request URL.
    configured: Option<RemoteCloudUrl>,
    /// The user stop, checked once per body chunk while a download reads.
    /// Behind a lock because [`CloudSource::set_cancel`] replaces it, and
    /// never `None`: before a worker attaches one, a private flag nobody
    /// sets reads false.
    cancel: Mutex<Arc<AtomicBool>>,
}

impl HttpCloudSource {
    pub fn new(url: String) -> Self {
        let user_agent = format!("sunlit.earth/{}", env!("CARGO_PKG_VERSION"));
        let agent = ureq::Agent::config_builder()
            .user_agent(&user_agent)
            .timeout_global(Some(Duration::from_mins(2)))
            .build()
            .new_agent();
        Self {
            agent,
            url: Mutex::new(url),
            configured: None,
            cancel: Mutex::new(Arc::new(AtomicBool::new(false))),
        }
    }

    /// Whether the user's stop is standing.
    fn cancelled(&self) -> bool {
        self.cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .load(Ordering::SeqCst)
    }

    /// Construct a source from the validated runtime remote selection.
    pub fn from_config(remote: RemoteCloudUrl) -> Self {
        let (url, _) = prepare_remote(&remote, crate::config::DEFAULT_TEXTURE_RESOLUTION);
        let mut source = Self::new(url);
        source.configured = Some(remote);
        source
    }

    /// The URL as of right now.
    ///
    /// A copy rather than a guard: the request below outlives any sensible
    /// lock hold, and a poisoned lock means a panic elsewhere rather than a
    /// reason to stop fetching clouds.
    fn url(&self) -> String {
        self.url
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Check if the remote image has changed using HEAD + `If-None-Match`.
    ///
    /// Returns `Ok(true)` if unchanged (304), `Ok(false)` if new content is
    /// available (200), or `Err` on transport/server errors.
    #[tracing::instrument(skip_all)]
    fn check_freshness(&self, url: &str, etag: &str) -> Result<bool, String> {
        let response = self
            .agent
            .head(url)
            .header("If-None-Match", etag)
            .call()
            .map_err(|_| "Cloud freshness check failed (transport or HTTP status)".to_owned())?;

        Ok(response.status().as_u16() == 304)
    }

    /// Download the cloud image unconditionally.
    #[tracing::instrument(skip_all)]
    fn download(&self, url: &str) -> Result<CloudImage, String> {
        let mut response = self
            .agent
            .get(url)
            .call()
            .map_err(|_| "Cloud download failed (transport or HTTP status)".to_owned())?;

        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        };
        let etag = header("ETag");
        let last_modified = header("Last-Modified");

        // 8K cloud JPEG can be ~15 MB, raise the default 10 MB limit
        let mut reader = response
            .body_mut()
            .with_config()
            .limit(50 * 1024 * 1024)
            .reader();
        let mut bytes = Vec::new();
        #[expect(
            clippy::large_stack_arrays,
            reason = "64 KiB is the chunk a download reads in, on the worker thread"
        )]
        let mut chunk = [0u8; 64 * 1024];
        loop {
            use std::io::Read as _;
            // The stop is checked once per chunk: a stop lands within about
            // one 64 KiB read rather than at the two-minute global timeout.
            if self.cancelled() {
                return Err("the cloud download was stopped".to_owned());
            }
            let read = reader
                .read(&mut chunk)
                .map_err(|_| "Failed to read cloud image body".to_owned())?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..read]);
        }

        Ok(CloudImage {
            bytes: Download::new(bytes),
            etag,
            last_modified,
        })
    }
}

impl CloudSource for HttpCloudSource {
    fn prepare(&self, resolution: u32) -> CloudTarget {
        if let Some(remote) = &self.configured {
            let (url, target) = prepare_remote(remote, resolution);
            self.retarget(&url);
            target
        } else {
            CloudTarget::remote(&self.url(), false)
        }
    }

    fn set_cancel(&self, flag: Arc<AtomicBool>) {
        *self
            .cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = flag;
    }

    fn fetch_if_changed(&self, known_etag: Option<&str>) -> Result<Option<CloudImage>, String> {
        let url = self.url();
        if let Some(etag) = known_etag {
            if self.check_freshness(&url, etag)? {
                info!("cloud image unchanged (304 Not Modified)");
                return Ok(None);
            }
            info!("cloud image has changed, downloading");
        } else {
            info!("no cached cloud ETag, downloading");
        }
        self.download(&url).map(Some)
    }

    fn describe(&self) -> String {
        let url = self.url();
        if url
            .parse::<ureq::http::Uri>()
            .ok()
            .and_then(|uri| uri.authority().cloned())
            .is_none_or(|authority| authority.as_str().contains('@'))
            || url.contains('#')
        {
            return "remote cloud image".to_owned();
        }
        url.split('?').next().unwrap_or_default().to_owned()
    }

    fn retarget(&self, url: &str) {
        let mut current = self
            .url
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.clear();
        current.push_str(url);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, OnceLock};
    use ureq::http::{Method, Request, Response};
    use ureq::middleware::MiddlewareNext;
    use ureq::{Body, SendBody};

    #[test]
    fn templates_survive_multiple_resolution_changes_and_default_keys_stay_compatible() {
        let config = CloudSourceConfig {
            remote_url: "https://example.org/{size}/map?size={size}&token=secret".to_owned(),
            ..CloudSourceConfig::default()
        };
        let source = HttpCloudSource::from_config(config.resolve(None).remote);
        let wide = source.prepare(8192);
        assert_eq!(
            source.url(),
            "https://example.org/8192x4096/map?size=8192x4096&token=secret"
        );
        let narrow = source.prepare(2048);
        assert_eq!(
            source.url(),
            "https://example.org/2048x1024/map?size=2048x1024&token=secret"
        );
        assert_ne!(wide.cache_key, narrow.cache_key);
        assert!(!wide.cache_key.contains("secret"));
        assert_eq!(source.prepare(8192), wide);
        let restarted = HttpCloudSource::from_config(config.resolve(None).remote);
        assert_eq!(restarted.prepare(2048), narrow);
        let default =
            HttpCloudSource::from_config(CloudSourceConfig::default().resolve(None).remote);
        for width in crate::config::TEXTURE_RESOLUTIONS {
            assert_eq!(
                default.prepare(width).cache_key,
                format!("clouds_cache_{width}x{}", width / 2)
            );
            let fixed = HttpCloudSource::new(default.url());
            assert_ne!(
                fixed.prepare(width).cache_key,
                default.prepare(width).cache_key
            );
        }
        assert_eq!(wide.wait, CloudWait::Existing);
        assert_eq!(CloudWait::Existing.duration(), Duration::from_secs(60));
        assert_eq!(CloudWait::LongFetch.duration(), Duration::from_mins(26));
        assert_eq!(CloudWait::SlowFetch.duration(), Duration::from_mins(5));
    }

    #[test]
    fn a_query_at_sign_is_not_userinfo_and_its_token_is_hidden() {
        let source = HttpCloudSource::new(
            "https://example.org/map?mail=user@example.org&token=secret".to_owned(),
        );
        assert_eq!(source.describe(), "https://example.org/map");
    }

    #[test]
    fn description_hides_query_and_invalid_credentials() {
        let source =
            HttpCloudSource::new("https://example.org/map.jpg?token=secret&key=hidden".to_owned());
        assert_eq!(source.describe(), "https://example.org/map.jpg");
        source.retarget("https://user:secret@example.org/map.jpg#hidden");
        let description = source.describe();
        assert!(!description.contains("secret") && !description.contains("hidden"));
    }

    #[test]
    fn head_and_get_use_one_url_snapshot_even_when_retargeted() {
        let holder: Arc<OnceLock<std::sync::Weak<HttpCloudSource>>> = Arc::default();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let source_ref = Arc::clone(&holder);
        let log = Arc::clone(&requests);
        let agent = ureq::Agent::config_builder()
            .middleware(move |req: Request<SendBody>, _: MiddlewareNext| {
                log.lock()
                    .unwrap()
                    .push((req.method().clone(), req.uri().to_string()));
                if req.method() == Method::HEAD {
                    assert_eq!(req.headers()["If-None-Match"], "old");
                    source_ref
                        .get()
                        .unwrap()
                        .upgrade()
                        .unwrap()
                        .retarget("https://example.org/new.jpg?token=new");
                }
                Ok(Response::builder()
                    .status(200)
                    .header("ETag", "fresh")
                    .body(Body::builder().data(b"image".to_vec()))
                    .unwrap())
            })
            .build()
            .new_agent();
        let mut source = HttpCloudSource::new("https://example.org/old.jpg?token=old".to_owned());
        source.agent = agent;
        let source = Arc::new(source);
        holder.set(Arc::downgrade(&source)).unwrap();
        let image = source.fetch_if_changed(Some("old")).unwrap().unwrap();
        assert_eq!(image.etag.as_deref(), Some("fresh"));
        assert_eq!(&*image.bytes, b"image");
        assert_eq!(
            *requests.lock().unwrap(),
            vec![
                (
                    Method::HEAD,
                    "https://example.org/old.jpg?token=old".to_owned()
                ),
                (
                    Method::GET,
                    "https://example.org/old.jpg?token=old".to_owned()
                ),
            ]
        );
        assert_eq!(source.url(), "https://example.org/new.jpg?token=new");
    }

    #[test]
    fn transport_errors_do_not_echo_secret_urls() {
        let agent = ureq::Agent::config_builder()
            .middleware(|_: Request<SendBody>, _: MiddlewareNext| {
                Err(ureq::Error::Io(std::io::Error::other(
                    "https://example.org/map?token=secret",
                )))
            })
            .build()
            .new_agent();
        let mut source = HttpCloudSource::new("https://example.org/map?token=secret".to_owned());
        source.agent = agent;
        for etag in [None, Some("old")] {
            let Err(error) = source.fetch_if_changed(etag) else {
                panic!("expected a transport failure")
            };
            assert!(!error.contains("secret"), "{error}");
        }
    }
}
