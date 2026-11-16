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
    Eumetsat,
    Remote,
    Nsmc,
}

/// The render curve one cloud source's map is drawn through: what `fs_cloud`
/// takes off as haze, and the floor and gamma it applies after that, each in a
/// land and an ocean value.
///
/// Every source serves a different quantity on the same 0-1 scale, so this
/// belongs to the source rather than to the window. A brightness map has a
/// clear-sky veil over cold ground and needs a floor under it; NSMC's alpha is
/// an overlay opacity that is already zero where the sky is clear, and a floor
/// under it only eats thin cloud. `docs/rendering.md` has the shader's side.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CloudCurve {
    pub haze_equator: f32,
    pub haze_pole: f32,
    pub floor_land: f32,
    pub gamma_land: f32,
    pub floor_ocean: f32,
    pub gamma_ocean: f32,
}

impl CloudCurve {
    /// What an infrared brightness map needs: clear mid-latitude land reads
    /// about 0.8 of full scale, so the haze removal takes the region's own
    /// level off and the floor catches what it leaves.
    pub const BRIGHTNESS: Self = Self {
        haze_equator: 0.9,
        haze_pole: 1.3,
        floor_land: 0.25,
        gamma_land: 1.0,
        floor_ocean: 0.1,
        gamma_ocean: 0.75,
    };

    /// What the NSMC mosaic needs: its alpha is zero over clear sky and its
    /// own curve has already lifted the thin cloud, so the land floor is a
    /// noise gate rather than a veil remover, a twentieth instead of a
    /// quarter.
    pub const NSMC: Self = Self {
        floor_land: 0.05,
        // Above one the thin end is lifted, which is what a mosaic whose
        // median alpha is a tenth of the scale needs over land.
        gamma_land: 1.2,
        ..Self::BRIGHTNESS
    };
}

impl Default for CloudCurve {
    fn default() -> Self {
        Self::BRIGHTNESS
    }
}

/// One [`CloudCurve`] per source, so switching sources switches the curve with
/// it and a curve tuned on one source is waiting when the user comes back.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CloudCurves {
    pub eumetsat: CloudCurve,
    pub remote: CloudCurve,
    pub nsmc: CloudCurve,
}

impl CloudCurves {
    #[must_use]
    pub fn for_mode(&self, mode: CloudSourceMode) -> CloudCurve {
        match mode {
            CloudSourceMode::Eumetsat => self.eumetsat,
            CloudSourceMode::Remote => self.remote,
            CloudSourceMode::Nsmc => self.nsmc,
        }
    }

    pub fn for_mode_mut(&mut self, mode: CloudSourceMode) -> &mut CloudCurve {
        match mode {
            CloudSourceMode::Eumetsat => &mut self.eumetsat,
            CloudSourceMode::Remote => &mut self.remote,
            CloudSourceMode::Nsmc => &mut self.nsmc,
        }
    }
}

impl Default for CloudCurves {
    fn default() -> Self {
        Self {
            eumetsat: CloudCurve::BRIGHTNESS,
            remote: CloudCurve::BRIGHTNESS,
            nsmc: CloudCurve::NSMC,
        }
    }
}

impl CloudCurve {
    /// Hold the curve inside the ranges the panel's sliders offer, so a file
    /// cannot hand the shader a value no slider reaches.
    pub fn clamp(&mut self) {
        self.haze_equator = self.haze_equator.clamp(0.0, 2.0);
        self.haze_pole = self.haze_pole.clamp(0.0, 2.0);
        self.floor_land = self.floor_land.clamp(0.0, 0.5);
        self.floor_ocean = self.floor_ocean.clamp(0.0, 0.5);
        self.gamma_land = self.gamma_land.clamp(0.1, 2.0);
        self.gamma_ocean = self.gamma_ocean.clamp(0.1, 2.0);
    }
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

/// The NSMC source's derivation curve, over the mosaic's alpha coverage.
///
/// The curve is not a taste: it puts the mosaic on the scale `fs_cloud`
/// expects. That shader's floor and gamma were tuned against an infrared
/// *brightness* map, where clear tropical ocean reads near zero and clear
/// mid-latitude land around 0.8, so a land pixel under 0.25 of full scale is
/// discarded outright (`docs/rendering.md`, the haze removal). The mosaic's
/// alpha is an overlay opacity instead, and a low one: over one round, 39% of
/// its pixels are exactly zero, the median is 10 of 255 and the ninetieth
/// percentile 97. Passed through unchanged, nearly four fifths of the globe
/// sits under that floor and no cloud is drawn at all, which is what the
/// source shipped with and what the sliders are for.
///
/// The defaults below map the alpha's usable range onto the whole scale:
/// `ceiling` 165 is where the mosaic's deep convection begins to saturate, and
/// `gamma` 0.30 lifts the thin end clear of the floor. Measured against a
/// round of the same hour, the resulting map's percentiles are 110, 217 and
/// 255 at the 50th, 90th and 99th, against the EUMETSAT map's 121, 217 and
/// 255, so the two sources render at comparable weight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct NsmcSettings {
    pub floor: u8,
    pub ceiling: u8,
    pub gamma: f64,
    pub blur: f64,
    /// The coverage the mosaic's deepest cloud maps to, the top of a soft
    /// shoulder over the curve's result: it leaves the thin end almost where
    /// it was and pulls the saturated cores down. One is no shoulder, and the
    /// row ships there: a typhoon core reading as solid white is the honest
    /// picture of an overcast top, and turning it below one is a choice about
    /// seeing its structure instead. `to_coverage` has the curve.
    pub cap: f32,
}

impl Default for NsmcSettings {
    fn default() -> Self {
        Self {
            floor: 0,
            ceiling: 165,
            gamma: 0.3,
            blur: 0.0,
            cap: 1.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CloudSourceConfig {
    pub mode: CloudSourceMode,
    pub remote_url: String,
    pub eumetsat: EumetsatSettings,
    pub nsmc: NsmcSettings,
}

impl Default for CloudSourceConfig {
    fn default() -> Self {
        Self {
            mode: CloudSourceMode::default(),
            remote_url: DEFAULT_REMOTE_URL.to_owned(),
            eumetsat: EumetsatSettings::default(),
            nsmc: NsmcSettings::default(),
        }
    }
}

/// A validated runtime selection. An environment URL changes this value only,
/// never the saved configuration. `remote` keeps its template/fixed distinction.
pub struct EffectiveCloudSource {
    pub mode: CloudSourceMode,
    pub remote: RemoteCloudUrl,
    pub eumetsat: EumetsatSettings,
    pub nsmc: NsmcSettings,
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
                    nsmc: saved.nsmc,
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
            nsmc: saved.nsmc,
        }
    }

    /// The public face of the window's validation: the same rule the
    /// resolution enforces, so an Apply and a load cannot disagree.
    pub fn is_valid(&self) -> bool {
        self.valid()
    }

    fn valid(&self) -> bool {
        let settings = &self.eumetsat;
        let nsmc = &self.nsmc;
        valid_remote_url(&self.remote_url, true)
            && settings.floor < settings.ceiling
            && settings.gamma.is_finite()
            && (0.1..=4.0).contains(&settings.gamma)
            && settings.blur.is_finite()
            && (0.0..=10.0).contains(&settings.blur)
            && nsmc.floor < nsmc.ceiling
            && nsmc.gamma.is_finite()
            && (0.1..=4.0).contains(&nsmc.gamma)
            && nsmc.blur.is_finite()
            && (0.0..=10.0).contains(&nsmc.blur)
            && nsmc.cap.is_finite()
            && (0.3..=1.0).contains(&nsmc.cap)
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

    #[test]
    fn nsmc_settings_follow_the_same_curve_rules() {
        let mut config = CloudSourceConfig {
            mode: CloudSourceMode::Nsmc,
            ..CloudSourceConfig::default()
        };
        assert!(config.valid());
        let effective = config.resolve(None);
        assert_eq!(effective.mode, CloudSourceMode::Nsmc);
        assert_eq!(effective.nsmc, config.nsmc);
        config.nsmc = NsmcSettings {
            floor: 10,
            ceiling: 10,
            gamma: 1.0,
            blur: 0.0,
            cap: 0.8,
        };
        assert!(!config.valid());
        for (gamma, blur) in [(0.1, 0.0), (4.0, 10.0)] {
            config.nsmc = NsmcSettings {
                floor: 0,
                ceiling: 255,
                gamma,
                blur,
                cap: 0.8,
            };
            assert!(config.valid());
        }
        config.nsmc = NsmcSettings {
            floor: 0,
            ceiling: 255,
            gamma: 4.1,
            blur: 0.0,
            cap: 0.8,
        };
        assert!(!config.valid());
    }
}
