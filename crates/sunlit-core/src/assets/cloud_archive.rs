//! The per-round archive of cloud imagery.
//!
//! One directory per cloud source entry, one manifest, one row per round: the
//! raw imagery as it came over the wire, the rendered map the pipeline serves,
//! and whether the round is pinned. A round enters the archive through the live
//! poll (unpinned) or a visit from the time bar (pinned); retention sweeps the
//! unpinned ones past the window, and the disk budget takes unpinned first.
//!
//! The manifest format is fixed at version 2 and stable: entries written by
//! earlier versions of this module load unchanged.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::eumetsat::write_committed;

/// The archive root under the cache directory.
const ARCHIVE_ROOT: &str = "clouds_history";
/// The manifest format this module reads and writes.
const MANIFEST_VERSION: i64 = 2;

/// The stamp format every archived round is filed and named under:
/// `20260816T090000`, in UTC.
fn stamp_format() -> Vec<time::format_description::BorrowedFormatItem<'static>> {
    time::format_description::parse("[year][month][day]T[hour][minute][second]")
        .expect("the stamp format parses")
}

/// A round's stamp, in UTC — the zone the upstream names its rounds in,
/// whatever offset the caller's scene instant carries:
/// `20260816T090000`.
pub(crate) fn stamp_of_round(round: time::OffsetDateTime) -> String {
    round
        .to_offset(time::OffsetDateTime::UNIX_EPOCH.offset())
        .format(&stamp_format())
        .unwrap_or_default()
}

/// Read a stamp back into a round, in UTC.
pub(crate) fn parse_stamp(stamp: &str) -> Option<time::OffsetDateTime> {
    if stamp.len() != 15 || !stamp.bytes().take(8).all(|b| b.is_ascii_digit()) {
        return None;
    }
    time::PrimitiveDateTime::parse(stamp, &stamp_format())
        .ok()
        .map(time::PrimitiveDateTime::assume_utc)
}

/// One raw file of a round, as the manifest names it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawFile {
    pub name: String,
    pub sha256: String,
    pub bytes: u64,
}

/// The rendered map a round serves, as the manifest names it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DerivedRow {
    pub sha256: String,
    pub identity: String,
    pub width: u32,
    pub height: u32,
}

/// One round's manifest row.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
struct ManifestRound {
    stamp: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pinned: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    raw: Vec<RawFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    derived: Option<DerivedRow>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Manifest {
    version: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rounds: Vec<ManifestRound>,
}

/// The in-memory state of the active entry.
struct State {
    key: Option<String>,
    rounds: Vec<ManifestRound>,
    /// The round the source itself holds (the live one), folded into the index
    /// even before its files land in the archive.
    current: Option<time::OffsetDateTime>,
    /// Retention window in hours; `None` keeps everything.
    limit_hours: Option<u32>,
    /// Disk budget in megabytes; `None` is unlimited.
    budget_mb: Option<u32>,
    /// Whether rendered maps are kept beside the raw imagery. `false` (the
    /// raw-only cache mode) records raws only, and nothing claims a map.
    /// The default is the default cache mode's: keep them.
    keep_derived: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            key: None,
            rounds: Vec::new(),
            current: None,
            limit_hours: None,
            budget_mb: None,
            keep_derived: true,
        }
    }
}

/// The per-round archive, shared by the engine (index, settings) and the
/// worker (records, loads). `None` for the cache directory makes it a
/// memory-only index, which is how tests and cache-less sessions run.
#[derive(Clone)]
pub(crate) struct CloudArchive {
    root: Option<PathBuf>,
    state: Arc<Mutex<State>>,
}

impl CloudArchive {
    pub(crate) fn new(cache_dir: Option<&Path>) -> Self {
        Self {
            root: cache_dir.map(|dir| dir.join(ARCHIVE_ROOT)),
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Point the archive at an entry's directory, scan what is there, and
    /// remember the round the source itself holds.
    pub(crate) fn set_active(&mut self, key: &str, current: Option<time::OffsetDateTime>) {
        let rounds = self.scan_entry(key);
        let mut state = self.state();
        state.key = Some(key.to_owned());
        state.rounds = rounds;
        state.current = current;
    }

    /// Follow a change of the live round (a poll that landed a newer one).
    pub(crate) fn set_current(&self, current: Option<time::OffsetDateTime>) {
        self.state().current = current;
    }

    /// The round the source itself holds.
    pub(crate) fn current(&self) -> Option<time::OffsetDateTime> {
        self.state().current
    }

    /// Every round the archive can speak for: the archived stamps plus the
    /// source's own round, sorted and deduplicated. The engine's coverage
    /// decision reads this; it is in-memory only.
    pub(crate) fn rounds(&self) -> Vec<time::OffsetDateTime> {
        let state = self.state();
        let mut rounds: Vec<time::OffsetDateTime> = state
            .rounds
            .iter()
            .filter_map(|row| parse_stamp(&row.stamp))
            .chain(state.current)
            .collect();
        rounds.sort();
        rounds.dedup();
        rounds
    }

    /// Retention window, disk budget and cache mode, as the settings apply
    /// them.
    pub(crate) fn set_limit(&self, hours: Option<u32>) {
        self.state().limit_hours = hours;
    }

    pub(crate) fn set_budget(&self, mb: Option<u32>) {
        self.state().budget_mb = mb;
    }

    pub(crate) fn set_keep_derived(&self, keep: bool) {
        self.state().keep_derived = keep;
    }

    /// Record one round: the raw imagery, and the rendered map when the cache
    /// mode keeps one. `pin` marks a round the time bar visited, which
    /// retention never sweeps.
    ///
    /// The manifest row is written last, as the commit point: a failure in
    /// the files leaves no row, and a later attempt overwrites cleanly.
    pub(crate) fn record(
        &self,
        key: &str,
        round: time::OffsetDateTime,
        raws: &[(&str, &[u8])],
        derived: Option<(&[u8], &str, u32, u32)>,
        pin: bool,
    ) {
        let stamp = stamp_of_round(round);
        let derived_row = derived.filter(|_| self.state().keep_derived).map(
            |(bytes, identity, width, height)| DerivedRow {
                sha256: super::eumetsat::sha256_hex(bytes),
                identity: identity.to_owned(),
                width,
                height,
            },
        );
        let Some(dir) = self.entry_dir(key) else {
            // Memory-only: the index still learns the round, which is what
            // coverage decisions and tests read.
            let mut state = self.state();
            state.rounds.push(ManifestRound {
                stamp,
                pinned: pin,
                raw: Vec::new(),
                derived: derived_row,
            });
            return;
        };
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!(dir = %dir.display(), error = %e, "the archive directory could not be created");
            return;
        }
        let mut raw_rows = Vec::new();
        for (name, bytes) in raws {
            let path = dir.join(format!("{stamp}_{name}"));
            if let Err(e) = write_committed(&path, bytes) {
                warn!(path = %path.display(), error = %e, "the archive raw file failed");
                return;
            }
            raw_rows.push(RawFile {
                name: format!("{stamp}_{name}"),
                sha256: super::eumetsat::sha256_hex(bytes),
                bytes: bytes.len() as u64,
            });
        }
        if let Some((bytes, ..)) = derived.filter(|_| self.state().keep_derived) {
            let jpg = dir.join(format!("{stamp}.jpg"));
            if let Err(e) = write_committed(&jpg, bytes) {
                warn!(path = %jpg.display(), error = %e, "the archive rendered map failed");
            }
        }
        self.with_manifest(key, |rounds| {
            rounds.retain(|row| row.stamp != stamp);
            rounds.push(ManifestRound {
                stamp,
                pinned: pin,
                raw: raw_rows,
                derived: derived_row,
            });
            rounds.sort_by(|a, b| a.stamp.cmp(&b.stamp));
        });
        info!(round = %stamp_of_round(round), pin, "a round entered the cloud archive");
    }

    /// Load a round's rendered map, when the cache mode kept one.
    pub(crate) fn load_jpeg(&self, key: &str, round: time::OffsetDateTime) -> Option<Vec<u8>> {
        let stamp = stamp_of_round(round);
        let state = self.state();
        let row = state.rounds.iter().find(|row| row.stamp == stamp)?;
        row.derived.as_ref()?;
        let dir = self.entry_dir(key)?;
        std::fs::read(dir.join(format!("{stamp}.jpg"))).ok()
    }

    /// A round's raw files, as stored: paths beside their manifest rows.
    pub(crate) fn raw_files(&self, key: &str, round: time::OffsetDateTime) -> Vec<PathBuf> {
        let stamp = stamp_of_round(round);
        let state = self.state();
        let Some(row) = state.rounds.iter().find(|row| row.stamp == stamp) else {
            return Vec::new();
        };
        let Some(dir) = self.entry_dir(key) else {
            return Vec::new();
        };
        row.raw.iter().map(|file| dir.join(&file.name)).collect()
    }

    /// Sweep what retention and the budget take, and refresh the index.
    ///
    /// Retention cuts unpinned rounds older than the window; the budget cuts
    /// unpinned rounds oldest-first until the entries fit. Pinned rounds stand
    /// deliberately outside the window and are taken only when the budget
    /// cannot be met any other way.
    pub(crate) fn prune(&self) {
        let (limit, budget) = {
            let state = self.state();
            (state.limit_hours, state.budget_mb)
        };
        let Some(root) = self.root.as_ref() else {
            return;
        };
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                warn!(root = %root.display(), error = %e, "the archive root is unreadable");
                return;
            }
        };
        let cutoff = limit
            .map(|hours| time::OffsetDateTime::now_utc() - time::Duration::hours(i64::from(hours)));
        for entry in entries.flatten() {
            let dir = entry.path();
            if !dir.is_dir() {
                continue;
            }
            let key = entry.file_name().to_string_lossy().into_owned();
            let Some(mut manifest) = read_manifest(&dir) else {
                continue;
            };
            let before = manifest.rounds.clone();
            if let Some(cutoff) = cutoff {
                manifest.rounds.retain(|row| {
                    row.pinned || parse_stamp(&row.stamp).is_none_or(|round| round >= cutoff)
                });
            }
            if let Some(budget) = budget {
                prune_to_budget(&dir, &mut manifest, u64::from(budget) * 1024 * 1024);
            }
            if manifest.rounds != before {
                write_manifest(&dir, &manifest);
                if self.state().key.as_deref() == Some(key.as_str()) {
                    self.state().rounds.clone_from(&manifest.rounds);
                }
            }
        }
    }

    fn entry_dir(&self, key: &str) -> Option<PathBuf> {
        self.root.as_ref().map(|root| root.join(key))
    }

    fn scan_entry(&self, key: &str) -> Vec<ManifestRound> {
        let Some(dir) = self.entry_dir(key) else {
            return Vec::new();
        };
        read_manifest(&dir)
            .map(|manifest| manifest.rounds)
            .unwrap_or_default()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Read the active entry's manifest, hand its rounds to `edit`, and write
    /// it back when the edit changed anything. Memory-only archives edit
    /// their index in place instead.
    fn with_manifest(&self, key: &str, edit: impl FnOnce(&mut Vec<ManifestRound>)) {
        let Some(dir) = self.entry_dir(key) else {
            edit(&mut self.state().rounds);
            return;
        };
        let mut manifest = read_manifest(&dir).unwrap_or(Manifest {
            version: MANIFEST_VERSION,
            rounds: Vec::new(),
        });
        let before = manifest.rounds.clone();
        edit(&mut manifest.rounds);
        if manifest.rounds != before {
            write_manifest(&dir, &manifest);
            if self.state().key.as_deref() == Some(key) {
                self.state().rounds.clone_from(&manifest.rounds);
            }
        }
    }
}

fn read_manifest(dir: &Path) -> Option<Manifest> {
    let bytes = std::fs::read(dir.join("manifest.toml")).ok()?;
    let manifest: Manifest = toml::from_str(&String::from_utf8_lossy(&bytes)).ok()?;
    Some(manifest)
}

fn write_manifest(dir: &Path, manifest: &Manifest) {
    match toml::to_string(manifest) {
        Ok(text) => {
            if let Err(e) = write_committed(&dir.join("manifest.toml"), text.as_bytes()) {
                warn!(dir = %dir.display(), error = %e, "the archive manifest failed");
            }
        }
        Err(e) => warn!(error = %e, "the archive manifest did not serialize"),
    }
}

/// Take unpinned rounds oldest-first until the entry fits the budget. Sizes
/// come from the files on disk, which is what the budget is about; the
/// pinned rounds are never taken here, whatever they weigh.
fn prune_to_budget(dir: &Path, manifest: &mut Manifest, budget_bytes: u64) {
    let entry_bytes = || -> u64 {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|entry| entry.metadata().ok())
                    .map(|meta| meta.len())
                    .sum()
            })
            .unwrap_or(0)
    };
    let mut taken: Vec<String> = Vec::new();
    while entry_bytes() > budget_bytes {
        let Some(stamp) = manifest
            .rounds
            .iter()
            .filter(|row| !row.pinned && !taken.contains(&row.stamp))
            .map(|row| row.stamp.clone())
            .min()
        else {
            break;
        };
        taken.push(stamp.clone());
        remove_round(dir, &stamp);
    }
    if !taken.is_empty() {
        manifest.rounds.retain(|row| !taken.contains(&row.stamp));
    }
}

/// Remove every file of one round, prefix-matched by its stamp.
fn remove_round(dir: &Path, stamp: &str) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(stamp)
            && let Err(e) = std::fs::remove_file(entry.path())
        {
            warn!(path = %entry.path().display(), error = %e, "an archive file could not be removed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ScratchDir;

    fn round(h: i64) -> time::OffsetDateTime {
        time::OffsetDateTime::UNIX_EPOCH + time::Duration::hours(h)
    }

    /// Now, at whole seconds: the archive files rounds by the second, and a
    /// test instant with nanoseconds would not compare equal to its own
    /// stamp.
    fn now() -> time::OffsetDateTime {
        let at = time::OffsetDateTime::now_utc();
        at.replace_nanosecond(0).expect("a whole second")
    }

    fn jpeg(label: u8) -> Vec<u8> {
        vec![label; 32]
    }

    fn archive(scratch: &ScratchDir) -> CloudArchive {
        CloudArchive::new(Some(scratch.as_ref()))
    }

    const KEY: &str = "entry";

    #[test]
    fn stamps_round_trip_and_files_follow_their_row() {
        let scratch = ScratchDir::new("archive_roundtrip");
        let mut archive = archive(&scratch);
        archive.set_active(KEY, None);
        let at = round(24 * 400 + 5);
        archive.record(
            KEY,
            at,
            &[("raw.png", b"rawbytes")],
            Some((&jpeg(1), "identity", 8, 4)),
            false,
        );
        assert_eq!(archive.rounds(), vec![at]);
        let stamp = stamp_of_round(at);
        assert_eq!(stamp.len(), 15, "{stamp}");
        assert_eq!(parse_stamp(&stamp), Some(at));
        // The stamp is UTC, whatever offset the caller's instant carries.
        let shifted = at.to_offset(time::macros::offset!(+8));
        assert_eq!(stamp_of_round(shifted), stamp);
        let dir = scratch.join("clouds_history").join(KEY);
        assert!(dir.join(format!("{stamp}.jpg")).exists());
        assert_eq!(archive.load_jpeg(KEY, at).as_deref(), Some(&jpeg(1)[..]));
        assert_eq!(archive.raw_files(KEY, at).len(), 1);
    }

    #[test]
    fn a_second_record_of_the_same_round_replaces_the_row() {
        let scratch = ScratchDir::new("archive_replace");
        let mut archive = archive(&scratch);
        archive.set_active(KEY, None);
        let at = round(99);
        archive.record(KEY, at, &[("raw.png", b"one")], None, false);
        archive.record(KEY, at, &[("raw.png", b"two")], None, false);
        let dir = scratch.join("clouds_history").join(KEY);
        assert_eq!(read_manifest(&dir).expect("manifest").rounds.len(), 1);
        assert_eq!(archive.raw_files(KEY, at).len(), 1);
    }

    #[test]
    fn the_raw_only_mode_writes_no_rendered_map_and_no_claim_of_one() {
        let scratch = ScratchDir::new("archive_raw_only");
        let mut archive = archive(&scratch);
        archive.set_keep_derived(false);
        archive.set_active(KEY, None);
        let at = round(5);
        archive.record(
            KEY,
            at,
            &[("raw.png", b"raw")],
            Some((&jpeg(1), "id", 8, 4)),
            false,
        );
        let dir = scratch.join("clouds_history").join(KEY);
        let manifest = read_manifest(&dir).expect("manifest");
        assert!(manifest.rounds[0].derived.is_none());
        assert!(archive.load_jpeg(KEY, at).is_none());
    }

    #[test]
    fn pinning_survives_retention() {
        let scratch = ScratchDir::new("archive_retention");
        let mut archive = archive(&scratch);
        archive.set_active(KEY, None);
        archive.set_limit(Some(1));
        let old = now() - time::Duration::hours(48);
        let old_pinned = old - time::Duration::hours(1);
        archive.record(KEY, old, &[], None, false);
        archive.record(KEY, old_pinned, &[], None, true);
        archive.prune();
        let rounds = archive.rounds();
        assert!(!rounds.contains(&old), "the unpinned old round is swept");
        assert!(rounds.contains(&old_pinned), "the pinned one stands");
    }

    #[test]
    fn the_budget_takes_unpinned_first_oldest_first() {
        let scratch = ScratchDir::new("archive_budget");
        let mut archive = archive(&scratch);
        archive.set_active(KEY, None);
        let big = vec![7u8; 4096];
        let pinned = now() - time::Duration::hours(30);
        let old = now() - time::Duration::hours(20);
        let new = now() - time::Duration::hours(10);
        archive.record(KEY, pinned, &[("raw.png", &big)], None, true);
        archive.record(KEY, old, &[("raw.png", &big)], None, false);
        archive.record(KEY, new, &[("raw.png", &big)], None, false);
        // The entry is ~12 KiB; a budget of nothing takes both unpinned
        // rounds and gives up before it touches the pinned one.
        archive.set_budget(Some(0));
        archive.prune();
        let rounds = archive.rounds();
        assert!(rounds.contains(&pinned), "the pinned round survives");
        assert!(
            !rounds.contains(&old),
            "the oldest unpinned round goes first"
        );
        assert!(
            !rounds.contains(&new),
            "the budget cannot be met, but unpinned rounds went first"
        );
    }

    #[test]
    fn the_current_round_is_indexed_before_its_files_land() {
        let mut archive = CloudArchive::new(Some(Path::new("Z:/definitely-missing")));
        archive.set_active(KEY, Some(round(7)));
        assert_eq!(archive.rounds(), vec![round(7)]);
        assert_eq!(archive.current(), Some(round(7)));
    }

    #[test]
    fn a_memory_only_archive_still_answers_coverage() {
        let mut archive = CloudArchive::new(None);
        archive.set_active(KEY, None);
        archive.record(KEY, round(3), &[], None, true);
        assert_eq!(archive.rounds(), vec![round(3)]);
        assert!(archive.load_jpeg(KEY, round(3)).is_none());
    }
}
