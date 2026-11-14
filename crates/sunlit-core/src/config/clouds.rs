//! Persisted cloud sources and validation, independent of the fetch worker.

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};
use tracing::warn;

pub const DEFAULT_REMOTE_URL: &str = "https://clouds.matteason.co.uk/images/{size}/clouds.jpg";
pub const ENV_CLOUD_URL: &str = "SUNLIT_EARTH_CLOUD_URL";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CloudSourceMode {
    #[default]
    Remote,
    Eumetsat,
    Gibs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EumetsatSettings {
    pub floor: u8,
    pub ceiling: u8,
    pub gamma: f64,
    pub blur: f64,
}

impl Default for EumetsatSettings {
    fn default() -> Self {
        Self {
            floor: 60,
            ceiling: 180,
            gamma: 0.8,
            blur: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CloudSourceConfig {
    pub mode: CloudSourceMode,
    pub remote_url: String,
    pub eumetsat: EumetsatSettings,
}

impl Default for CloudSourceConfig {
    fn default() -> Self {
        Self {
            mode: CloudSourceMode::Remote,
            remote_url: DEFAULT_REMOTE_URL.to_owned(),
            eumetsat: EumetsatSettings::default(),
        }
    }
}

/// A validated runtime selection. An environment URL changes this value only,
/// never the saved configuration. `remote` keeps its template/fixed distinction.
pub struct EffectiveCloudSource {
    pub mode: CloudSourceMode,
    pub remote: RemoteCloudUrl,
    pub eumetsat: EumetsatSettings,
}

#[derive(Clone)]
pub struct RemoteCloudUrl {
    url: String,
    template: bool,
}

impl RemoteCloudUrl {
    pub(crate) fn expand(&self, size: &str) -> String {
        if self.template {
            self.url.replace("{size}", size)
        } else {
            self.url.clone()
        }
    }

    pub(crate) fn is_default_template(&self) -> bool {
        self.template && self.url == DEFAULT_REMOTE_URL
    }
}

impl CloudSourceConfig {
    /// How long a client waits for this source's first answer: the same
    /// budget the resolved source reports through `CloudSource::wait`, so a
    /// one-shot command and the wallpaper follow the source rather than a
    /// constant sized for the widest case.
    pub fn wait(&self, env_url: Option<&str>) -> Duration {
        crate::assets::cloud_source::resolve(self, env_url, None).wait()
    }

    /// Resolve a value obtained via `env_override(ENV_CLOUD_URL)`. Invalid
    /// overrides warn without their contents and fall back to the saved source.
    pub fn resolve(&self, env_url: Option<&str>) -> EffectiveCloudSource {
        let saved = if self.valid() {
            self.clone()
        } else {
            warn!("invalid cloud source settings, using source defaults");
            Self::default()
        };
        if let Some(url) = env_url.filter(|v| !v.trim().is_empty()) {
            if valid_remote_url(url, false) {
                return EffectiveCloudSource {
                    mode: CloudSourceMode::Remote,
                    remote: RemoteCloudUrl {
                        url: url.to_owned(),
                        template: false,
                    },
                    eumetsat: saved.eumetsat,
                };
            }
            warn!(
                env = ENV_CLOUD_URL,
                "invalid cloud URL override, using saved source"
            );
        }
        EffectiveCloudSource {
            mode: saved.mode,
            remote: RemoteCloudUrl {
                url: saved.remote_url,
                template: true,
            },
            eumetsat: saved.eumetsat,
        }
    }

    /// The public face of the window's validation: the same rule the
    /// resolution enforces, so an Apply and a load cannot disagree.
    pub fn is_valid(&self) -> bool {
        self.valid()
    }

    fn valid(&self) -> bool {
        let settings = &self.eumetsat;
        valid_remote_url(&self.remote_url, true)
            && settings.floor < settings.ceiling
            && settings.gamma.is_finite()
            && (0.1..=4.0).contains(&settings.gamma)
            && settings.blur.is_finite()
            && (0.0..=10.0).contains(&settings.blur)
    }

    /// Isolate schema errors in this field, rather than failing the entire file.
    /// Do not format serde's error: it can contain a URL with credentials.
    pub(super) fn deserialize_value<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let value = toml::Value::deserialize(deserializer)?;
        match value.try_into::<Self>() {
            Ok(config) if config.valid() => Ok(config),
            _ => {
                warn!("invalid cloud source settings, using source defaults");
                Ok(Self::default())
            }
        }
    }
}

/// Validate before using `Uri`: its parser discards fragments. A template may
/// change only the path/query, never the authority. Encoded braces stay literal.
fn valid_remote_url(raw: &str, template: bool) -> bool {
    if raw.chars().any(|c| c.is_whitespace() || c.is_control()) || raw.contains('#') {
        return false;
    }
    let expanded = if template {
        raw.replace("{size}", "2048x1024")
    } else {
        raw.to_owned()
    };
    if expanded.contains(['{', '}']) {
        return false;
    }
    let mut bytes = expanded.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%'
            && (!bytes.next().is_some_and(|b| b.is_ascii_hexdigit())
                || !bytes.next().is_some_and(|b| b.is_ascii_hexdigit()))
        {
            return false;
        }
    }
    let Ok(uri) = expanded.parse::<ureq::http::Uri>() else {
        return false;
    };
    if !uri
        .scheme_str()
        .is_some_and(|s| s.eq_ignore_ascii_case("http") || s.eq_ignore_ascii_case("https"))
    {
        return false;
    }
    let Some(authority) = uri.authority() else {
        return false;
    };
    let authority = authority.as_str();
    if authority.contains('@')
        || raw.split_once("://").is_none_or(|(_, rest)| {
            rest.split(['/', '?'])
                .next()
                .unwrap_or_default()
                .contains(['{', '}'])
        })
    {
        return false;
    }
    let host = uri.host().unwrap_or_default();
    if host.is_empty() {
        return false;
    }
    let port = if host.starts_with('[') {
        if host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::Ipv6Addr>()
            .is_err()
        {
            return false;
        }
        &authority[host.len()..]
    } else {
        if !host
            .strip_suffix('.')
            .unwrap_or(host)
            .split('.')
            .all(|label| {
                !label.is_empty()
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                    && !label.starts_with('-')
                    && !label.ends_with('-')
            })
        {
            return false;
        }
        &authority[host.len()..]
    };
    port.is_empty()
        || port.strip_prefix(':').is_some_and(|p| {
            !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) && p.parse::<u16>().is_ok()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_boundaries_accept_complete_http_urls_and_safe_queries() {
        for url in [
            "HTTPS://EXAMPLE.ORG:443/map.jpg",
            "http://localhost:8080/map",
            "https://example.org./map",
            "http://[::1]:8080/map",
            "https://example.org/map?a=user@example.org&next=https://other.org/a&token=x%23y",
            "https://example.org/%7Bsize%7D?token=%40%2F%3F",
            "https://example.org",
            "https://example.org?token=abc",
        ] {
            assert!(valid_remote_url(url, false), "{url}");
        }
    }

    #[test]
    fn url_boundaries_reject_ambiguous_authorities_and_invalid_escapes() {
        for url in [
            "https://example.org:/map",
            "https://example.org:65536/map",
            "https://example.org:+80/map",
            "https://@example.org/map",
            "https://u%40p@example.org/map",
            "https://example.org\\@evil.org/map",
            "https://%65xample.org/map",
            "https://[broken]/map",
            "https://example..org/map",
            "https://-example.org/map",
            "https://example.org/map#",
            "https://example.org/map?token=#hidden",
            "https://example.org/map?token=%",
            "https://example.org/map?token=%GG",
            "https://example.org/map\t",
            "https://example.org/map?token=white space",
            "https://{size}.example.org/map",
            "https://example.org/{other}",
        ] {
            assert!(!valid_remote_url(url, true), "{url}");
        }
    }

    #[test]
    fn runtime_override_is_fixed_forces_remote_and_does_not_change_saved_config() {
        let config = CloudSourceConfig {
            mode: CloudSourceMode::Eumetsat,
            remote_url: "https://saved.org/{size}?token=saved".to_owned(),
            ..CloudSourceConfig::default()
        };
        let original = config.clone();
        let effective = config.resolve(Some("https://override.org/%7Bsize%7D?token=override"));
        assert_eq!(effective.mode, CloudSourceMode::Remote);
        assert_eq!(
            effective.remote.expand("8192x4096"),
            "https://override.org/%7Bsize%7D?token=override"
        );
        assert_eq!(
            effective.remote.expand("2048x1024"),
            effective.remote.expand("8192x4096")
        );
        assert_eq!(config, original);
        for raw in [
            None,
            Some(""),
            Some("  "),
            Some("file:///bad"),
            Some("https://override.org/{size}"),
            Some("https://u:secret@override.org/map"),
        ] {
            let effective = config.resolve(raw);
            assert_eq!(effective.mode, CloudSourceMode::Eumetsat);
            assert_eq!(effective.eumetsat, config.eumetsat);
            assert_eq!(
                effective.remote.expand("2048x1024"),
                "https://saved.org/2048x1024?token=saved"
            );
        }
    }

    #[test]
    fn eumetsat_settings_accept_both_inclusive_range_limits() {
        for (gamma, blur) in [(0.1, 0.0), (4.0, 10.0)] {
            let config = CloudSourceConfig {
                mode: CloudSourceMode::Eumetsat,
                eumetsat: EumetsatSettings {
                    floor: 0,
                    ceiling: 255,
                    gamma,
                    blur,
                },
                ..CloudSourceConfig::default()
            };
            assert!(config.valid());
            let effective = config.resolve(None);
            assert_eq!(effective.mode, CloudSourceMode::Eumetsat);
            assert_eq!(effective.eumetsat, config.eumetsat);
        }
    }
}
