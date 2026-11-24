//! The engine: one thread that owns the GPU device, the renderer, the asset
//! mailbox, and the schedule.
//!
//! Clients (the Slint shell, the `render` subcommand, tests) send commands and
//! receive events. Nothing the engine does depends on a window existing, which
//! is the whole point: hiding the main window removes a client, it does not
//! half-suspend the machinery.

pub mod clock;
mod cloud_timeline;
mod cloud_worker;
pub mod excursion;
mod handle;
mod loading;
mod protocol;
mod publish;
mod schedule;
mod surface;
mod tile_loader;
pub mod wallpaper_sink;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use tracing::{debug, info, warn};

use crate::assets::cloud_archive::CloudArchive;
use crate::assets::mailbox::TextureMailbox;
use crate::assets::tiles::TranscoderConfig;
use crate::config::CloudBackfillPolicy;
use crate::config::CloudSourceConfig;
use crate::config::QualityTier;
use crate::display::layout::{self, Framing};
use crate::params::{OutputParams, SceneParams};
use crate::renderer::CloudGate;
use crate::renderer::residency::{Output, surfaces_for};
use crate::renderer::{
    RenderOutcome, Renderer, RendererConfig, SlotLayout, SurfaceFormats, resolve_sample_count,
    surface_sampler_descriptor,
};
use crate::scene::sky::{self, SkyState};

use crate::assets::cloud_fetcher::EnsureOutcome;
use clock::Clock;
use cloud_timeline::{covering_round, floor_for, round_text};
use cloud_worker::{CloudWorker, FirstAnswer, spawn_cloud_worker};
use excursion::{Excursion, ExcursionCommand};
use handle::AdapterReport;
pub use handle::{EngineConfig, EngineHandle, start};
pub use loading::TILES_NAMED_AFTER;
pub use protocol::{EngineCommand, EngineEvent, ExportError, ExportLimit};
use schedule::Schedule;
use surface::SurfaceFeed;
use tile_loader::{LoaderConfig, View, Watched};
pub use tile_loader::{TileGate, TileReport};
use wallpaper_sink::WallpaperSink;

/// How long the loop blocks on the command channel before re-checking the
/// schedule. Short enough that a real-clock deadline is never missed by more
/// than this, cheap enough to ignore: the wake does nothing when nothing is due.
const TICK: Duration = Duration::from_millis(50);

/// The longest the loop waits while an excursion runs and the preview is
/// shown: about 30 frames a second where the adapter keeps up. The time comes
/// from the clock, so a slower adapter drops frames and keeps the speed.
const EXCURSION_TICK: Duration = Duration::from_millis(33);

/// How often decoded textures are uploaded to the GPU. Unconditional: this is
/// what keeps cloud updates flowing while no window is visible.
const DRAIN_INTERVAL: Duration = Duration::from_secs(5);

/// How often the sky state (the sun, the sky rotation, the planets) is
/// recomputed when rendering live time, unless [`sky_interval`] says the
/// preview needs it sooner.
const SKY_INTERVAL: Duration = Duration::from_mins(2);

/// The shortest interval [`sky_interval`] gives the sky schedule: the floor
/// under how often a shown preview of a Sun-fixed camera is redrawn to keep up
/// with the Earth's turn, which at the closest zoom moves a pixel every two
/// seconds. `docs/rendering.md` has the measurement behind it.
const SKY_FLOOR: Duration = Duration::from_secs(5);

/// The Earth's turn against the Sun, in radians a second: one turn a solar
/// day.
const EARTH_TURN: f32 = std::f32::consts::TAU / 86_400.0;

/// How often a memory sample is appended to the metrics CSV.
const METRICS_INTERVAL: Duration = Duration::from_mins(10);

/// How long a burst of display-change hints is allowed to settle before the
/// monitors are asked for.
///
/// A layout change is a burst on both platforms: `RandR` sends one event per CRTC
/// and one per output, and Windows sends one `WM_DISPLAYCHANGE` per applied
/// step, with a docking station bringing its screens up one at a time. Waiting
/// costs nothing anybody can see, and settling too early costs a second full
/// render and a second visible swap when the late step lands. It is read off
/// the injected clock and checked on the tick the loop already makes, so it
/// adds no timer and no wakeup.
///
/// Public because the integration tests step over it. A test that spelled the
/// number itself would still pass when this one moved, having quietly stopped
/// stepping over anything.
pub const DISPLAY_SETTLE: Duration = Duration::from_secs(2);

/// How long an export waits for the tiles its frame wants, on the injected
/// clock, before it is made with what is resident in their place: their
/// resident ancestors, or the floor. A wallpaper made that way is made again
/// once the tiles are resident.
///
/// Public because the integration tests step over it.
pub const TILE_WAIT: Duration = Duration::from_secs(5);

/// How long a wallpaper waits for the cloud layer's first answer, on the
/// injected clock, before it is made without clouds. It is the figure the
/// one-shot commands wait for a slow 8192 px download and its decode, so a map
/// that is merely slow still makes it in.
///
/// Public because the integration tests step over it.
pub const CLOUD_WAIT: Duration = Duration::from_secs(60);

/// How long the scene's landing on one round waits before that round is
/// fetched: the drag's settle. Every pointer move re-floors the instant, and
/// a fetch started for each of them would supersede itself forever; one
/// second of stillness is the difference between dragging and having
/// stopped.
const CLOUD_SETTLE: Duration = Duration::from_secs(1);

/// How long a historical fetch that came back empty-handed waits before it
/// is asked again: a transport-class failure, not an upstream refusal.
const CLOUD_FETCH_RETRY: Duration = Duration::from_secs(2);

/// How long the engine waits before asking again for a neighbour the GPU
/// cache did not receive: the worker drops a warm while a real request is
/// in flight.
const CLOUD_PREFETCH_RETRY: Duration = Duration::from_secs(2);

/// How long after the last time-bar move the session still counts as
/// browsing history. Past it the cache falls back to the worn round alone.
const CLOUD_BROWSE_IDLE: Duration = Duration::from_secs(20);

/// Two moves closer together than this are one drag gesture.
const CLOUD_BROWSE_RAPID: Duration = Duration::from_millis(350);

/// Moves within [`CLOUD_BROWSE_RAPID`] that make a drag fast: fast drags
/// warm two rounds either side instead of one.
const CLOUD_BROWSE_RAPID_MOVES: u32 = 4;

/// Which time-bar track the last browse came from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CloudBrowseKind {
    Time,
    Date,
}

/// The cache span and neighbour rounds one tick's browse activity calls for.
struct CloudBrowsePlan {
    cap: usize,
    neighbours: Vec<time::OffsetDateTime>,
    /// The browse window has just run out; the caller clears it.
    expired: bool,
}

/// What the session's browse state calls for at `now`: one worn round when
/// quiet, the rounds either side while the time track is in use (two either
/// side during a fast drag), or the days either side after a date-track move.
fn cloud_browse_plan(
    kind: CloudBrowseKind,
    moves: u32,
    last: Option<Duration>,
    until: Option<Duration>,
    now: Duration,
    round: time::OffsetDateTime,
    interval: Duration,
) -> CloudBrowsePlan {
    let Some(until) = until else {
        return CloudBrowsePlan {
            cap: 1,
            neighbours: Vec::new(),
            expired: false,
        };
    };
    if now >= until {
        return CloudBrowsePlan {
            cap: 1,
            neighbours: Vec::new(),
            expired: true,
        };
    }
    let rapid = last.is_some_and(|last| now.saturating_sub(last) <= CLOUD_BROWSE_RAPID)
        && moves >= CLOUD_BROWSE_RAPID_MOVES;
    match kind {
        CloudBrowseKind::Date => {
            let day = time::Duration::hours(24);
            CloudBrowsePlan {
                cap: 3,
                neighbours: vec![round - day, round + day],
                expired: false,
            }
        }
        CloudBrowseKind::Time if rapid => CloudBrowsePlan {
            cap: 5,
            neighbours: vec![
                round - interval,
                round + interval,
                round - interval * 2,
                round + interval * 2,
            ],
            expired: false,
        },
        CloudBrowseKind::Time => CloudBrowsePlan {
            cap: 3,
            neighbours: vec![round - interval, round + interval],
            expired: false,
        },
    }
}

/// How long a round the upstream has not published yet waits before it is
/// asked for again: the publication check's own cadence, so a lagging round
/// is picked up within minutes of its appearing.
const CLOUD_PUBLICATION_RETRY: Duration = Duration::from_mins(5);

/// The engine's own state, private to its thread.
///
/// The bools are independent latches on a private struct rather than
/// parameters anyone passes, which is the confusion the lint is about.
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent latches on a private struct, which is not the confusion the lint is about"
)]
struct Engine {
    rx: Receiver<EngineCommand>,
    clock: Arc<dyn Clock>,
    on_event: Arc<dyn Fn(EngineEvent) + Send + Sync>,
    wallpaper: Arc<dyn WallpaperSink>,
    renderer: Renderer,
    params: SceneParams,
    quality: QualityTier,
    /// The adapter slug the memory report names itself after.
    adapter_key: String,
    /// MSAA sample counts the adapter supports for the render format. Every
    /// requested count is resolved against this before it can reach wgpu.
    supported_sample_counts: Vec<u32>,
    /// The last count a client asked for, so the clamp is logged when the
    /// request changes rather than on every slider tick.
    requested_sample_count: u32,
    preview: PreviewState,
    /// A wallpaper update was asked for while a texture was still on its way,
    /// and happens as soon as it arrives.
    wallpaper_owed: bool,
    /// The renders the wallpaper owed, or to be made again, is made of, whose
    /// tiles join the wanted set until it is made from them.
    wallpaper_shots: Vec<publish::Shot>,
    /// When the wallpaper owed began to wait for its tiles alone, the cubes
    /// being resident, which is what [`TILE_WAIT`] counts from, and how many
    /// packs had been opened for their tiles then: one opened since starts
    /// the count over.
    tiles_awaited_since: Option<(Duration, u64)>,
    /// When the wallpaper owed began to wait for the cloud layer's first
    /// answer, the cubes being resident, which is what [`CLOUD_WAIT`] counts
    /// from.
    clouds_awaited_since: Option<Duration>,
    /// A wallpaper went out after [`TILE_WAIT`] with tiles missing, and is
    /// made again when they are resident.
    reexport: bool,
    /// `RenderToFile` and `ExportPixels` requests waiting for their tiles.
    exports: Vec<publish::WaitingExport>,
    /// The sky the wanted set in force was computed under. The outputs an
    /// export waits on are resolved under it too: the set answers for the
    /// cameras it was computed for, and a Sun-fixed camera resolved at another
    /// instant is not one of them.
    wanted_sky: Option<SkyState>,
    /// A client asked for a wallpaper update. The publish happens in `tick`, so
    /// several requests drained together cost one native-resolution render
    /// rather than one each.
    publish_asked: bool,
    /// How this session's monitors relate to each other, and which one the plan
    /// is built around. Read only when a wallpaper is published.
    display_mode: crate::display::layout::DisplayMode,
    anchor_monitor: Option<String>,
    /// When the monitors are next worth asking for, set by a display-change
    /// hint and pushed out again by every hint that follows it.
    display_recheck: Option<Duration>,
    /// The last monitor list the engine saw, from a publish or from a recheck.
    /// `None` until one of the two has happened, which is what makes the first
    /// hint an announcement rather than a comparison against nothing.
    known_monitors: Option<Vec<crate::display::Monitor>>,
    /// The last monitor list the engine saw from any source, the startup
    /// enumeration included, which the preview's anchor screen is resolved
    /// against. Kept apart from `known_monitors`, so the display hint
    /// comparison and its republish rule do not depend on it.
    preview_monitors: Vec<crate::display::Monitor>,
    /// The anchor screen's size, which the preview is framed for (plan
    /// decision 4), or `None` where no screen is known.
    preview_screen: Option<(u32, u32)>,
    /// Whether the desk is holding a picture this process made. Set when a sink
    /// accepted one and never on a refusal, so a layout change cannot produce a
    /// wallpaper nobody asked for or an error out of nowhere.
    published: bool,
    /// The preview's time while it is away from the setting, running or
    /// paused.
    excursion: Option<Excursion>,
    /// The instant the scene is held at, from `Hold(true)` to `Hold(false)`:
    /// the preview and every export draw at it, and wallpaper publishes wait.
    held: Option<time::OffsetDateTime>,
    /// The instant the last draw was drawn at, which the preview frame read
    /// back from it shows.
    drawn_instant: time::OffsetDateTime,
    /// The short edge the last draw was framed on, in frame pixels, which the
    /// preview frame carries.
    drawn_short_edge: f32,
    /// Set when something happened that the next render must pick up.
    dirty: bool,
    textures_ready: bool,
    /// The generation the last `CloudsSettled` went out for. One per
    /// generation: a source switch announces afresh, and the same source's
    /// later polls announce nothing.
    clouds_settled_for: Option<u64>,
    /// The cloud status last forwarded, so the event goes out on changes only.
    cloud_status: Option<String>,
    /// Whether a cloud fetch run was in flight at the last tick, so the busy
    /// event goes out on changes only.
    cloud_busy: bool,
    /// The per-round archive, shared with the worker: the engine reads its
    /// index for coverage and holds its settings; the worker records into it.
    cloud_archive: CloudArchive,
    /// What the layer does outside the archive's span.
    cloud_backfill: CloudBackfillPolicy,
    /// The historical fetch in flight: dispatched, no result yet.
    cloud_fetch_in_flight: Option<time::OffsetDateTime>,
    /// The neighbour rounds asked of the archive to warm the GPU cache, and
    /// when each ask went out, so a busy worker's drop is retried without
    /// flooding it.
    cloud_prefetch: HashMap<time::OffsetDateTime, Duration>,
    /// Which track the last time-bar move came from, the moves inside the
    /// current gesture, when the last one landed, and until when the
    /// session counts as browsing history.
    cloud_browse_kind: CloudBrowseKind,
    cloud_browse_moves: u32,
    cloud_browse_last: Option<Duration>,
    cloud_browse_until: Option<Duration>,
    /// The cache span last handed to the renderer.
    cloud_cache_cap: usize,
    /// The floor the scene has rested on and since when, the drag's settle.
    cloud_settle: Option<(time::OffsetDateTime, Duration)>,
    /// The rounds the upstream does not hold, learned by asking. Uncovered
    /// for good; the layer wears nothing for them.
    cloud_unavailable: Vec<time::OffsetDateTime>,
    /// The rounds newer than the upstream's publication edge, which have
    /// not published yet and will: retried on the publication cadence, and
    /// cleared when the edge moves past them.
    cloud_not_yet: Vec<time::OffsetDateTime>,
    /// The round a user stop stood down. Cleared when the scene lands
    /// elsewhere or an explicit press answers for it.
    cloud_stopped: Option<time::OffsetDateTime>,
    /// When a failed historical fetch may be asked again.
    cloud_retry_at: Option<Duration>,
    /// Whether the next historical dispatch bypasses the settle and a stop,
    /// which an explicit press asks for.
    cloud_fetch_forced: bool,
    /// The round line last forwarded, so the event goes out on changes only.
    cloud_round_line_sent: String,
    /// Whether the one debug-level memory dump has already been written. It
    /// goes out the first time the textures are ready, which is the first
    /// moment the numbers describe a loaded scene rather than a half-built one.
    memory_dumped: bool,
    last_status: String,
    drain: Schedule,
    sky: Schedule,
    metrics: Option<Schedule>,
    auto_refresh: Option<Schedule>,
    cloud: Option<CloudWorker>,
    /// The transcoder and the month in force, when the globe is drawn from the
    /// cube surface.
    surface: Option<SurfaceFeed>,
}

/// Whether preview frames are wanted, and whether one is owed right now.
///
/// Re-enabling the preview has to deliver a frame even though the scene did
/// not change, otherwise a window that was hidden and shown again would sit on
/// a stale image until the user touched something.
struct PreviewState {
    enabled: bool,
    owed: bool,
    /// Whether the last readback failed. A device that keeps failing is still
    /// retried on every tick, because it may come back, but it costs one line
    /// in the log rather than twenty a second.
    readback_failed: bool,
}

impl Engine {
    #[expect(
        clippy::too_many_lines,
        reason = "one linear setup sequence, most of it the state's fields one per line"
    )]
    fn new(
        config: EngineConfig,
        rx: Receiver<EngineCommand>,
        command_tx: &Sender<EngineCommand>,
        ready: &Sender<Result<AdapterReport, String>>,
    ) -> Result<Self, String> {
        let EngineConfig {
            force_software,
            texture_paths,
            preview_size,
            preview_enabled,
            mut params,
            quality,
            texture_resolution,
            clock,
            cloud,
            cloud_poll_interval,
            cloud_history_hours,
            cloud_cache_mode,
            cloud_archive_budget_mb,
            cloud_backfill,
            cache_dir,
            auto_refresh,
            wallpaper,
            display_mode,
            anchor_monitor,
            monitors,
            on_event,
            record_metrics,
            mailbox,
            cube_textures,
            tile_geometry,
            tile_layers,
            tile_gate,
            build_gate,
            every_floor,
        } = config;

        let slots = SlotLayout::new(texture_paths.len());
        let mailbox = checked_mailbox(mailbox, slots, texture_paths.len());
        let gpu = open_gpu(force_software, ready)?;
        if let Some(dir) = &cache_dir {
            crate::assets::texture_loader::remove_retired_downscales(dir);
        }

        // Every background producer wakes the engine loop through the same
        // command channel, so there is exactly one place that decides what to
        // do about new work. The channel is unbounded, so the send never
        // blocks, which the transcoder's notify callback relies on: it runs on
        // a thread that dropping the transcoder joins.
        let poke_tx = command_tx.clone();
        let notify: crate::assets::cloud_fetcher::NotifyFn = Arc::new(move || {
            let _ = poke_tx.send(EngineCommand::Poke);
        });

        let now = clock.elapsed();
        let month = crate::scene::month::month_in_force(&params.time, clock.now_utc());
        let drawn_instant = params.time.instant(clock.now_utc());
        // The cloud generation in force: bumped by the worker whenever the
        // effective target changes, read by the renderer to drop the frames of
        // a target the session has left.
        let cloud_generation = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let cpu_adapter = gpu.device_type == wgpu::DeviceType::Cpu;
        let formats = SurfaceFormats::for_adapter(
            gpu.device
                .features()
                .contains(wgpu::Features::TEXTURE_COMPRESSION_BC),
            cpu_adapter,
        );
        let mut surface = start_surface(
            cache_dir.as_ref(),
            TranscoderConfig {
                geometry: tile_geometry,
                gate: build_gate,
                ..TranscoderConfig::new(std::path::PathBuf::new(), cube_textures, month)
            },
            now,
            &notify,
            &LoaderConfig {
                decode: formats.color == wgpu::TextureFormat::Rgba8Unorm,
                anisotropy: surface_sampler_descriptor(cpu_adapter).anisotropy_clamp,
                workers: crate::assets::tiles::default_threads(),
                gate: tile_gate,
            },
        );
        if let Some(surface) = &mut surface {
            surface.keep_every_floor(every_floor.unwrap_or(!cpu_adapter));
        }

        let requested_sample_count = params.sample_count;
        params.sample_count = resolve_and_warn(
            requested_sample_count,
            &gpu.supported_sample_counts,
            quality.max_sample_count(),
            None,
        );
        info!(texture_resolution, "surface texture resolution");
        let (width, height) = preview_target_size(preview_size, quality);
        let renderer = Renderer::new(
            gpu.device,
            gpu.queue,
            RendererConfig {
                sample_count: params.sample_count,
                width,
                height,
                texture_paths,
                texture_resolution,
                mailbox: mailbox.clone(),
                notify: Arc::clone(&notify),
                cloud_generation: Arc::clone(&cloud_generation),
                cube_month: surface.as_ref().map(|_| month),
                cpu_adapter,
                tile_geometry,
                tile_layers,
            },
        );

        // The per-round archive: its settings are in force before the worker
        // exists, so the seed and the first records land under them, and the
        // sweep runs once at startup against whatever earlier sessions left.
        let cloud_archive = CloudArchive::new(cache_dir.as_deref());
        cloud_archive.set_limit(cloud_history_hours);
        cloud_archive.set_budget(cloud_archive_budget_mb);
        cloud_archive.set_keep_derived(cloud_cache_mode != crate::config::CloudCacheMode::RawOnly);
        cloud_archive.prune();

        let cloud = cloud.map(|source| {
            spawn_cloud_worker(
                &source,
                &mailbox,
                &notify,
                &cloud_generation,
                &cloud_archive,
                cache_dir.as_deref(),
                cloud_poll_interval,
                now,
                texture_resolution,
                params.clouds_enabled,
            )
        });

        let preview_screen = preview_screen_of(&monitors, anchor_monitor.as_deref());
        Ok(Self {
            rx,
            clock,
            on_event,
            wallpaper,
            renderer,
            params,
            quality,
            adapter_key: gpu.adapter_key,
            supported_sample_counts: gpu.supported_sample_counts,
            requested_sample_count,
            preview: PreviewState {
                enabled: preview_enabled,
                owed: false,
                readback_failed: false,
            },
            wallpaper_owed: false,
            wallpaper_shots: Vec::new(),
            tiles_awaited_since: None,
            clouds_awaited_since: None,
            reexport: false,
            exports: Vec::new(),
            wanted_sky: None,
            publish_asked: false,
            display_mode,
            anchor_monitor,
            display_recheck: None,
            known_monitors: None,
            preview_monitors: monitors,
            preview_screen,
            published: false,
            excursion: None,
            held: None,
            drawn_instant,
            drawn_short_edge: 0.0,
            dirty: true,
            textures_ready: false,
            clouds_settled_for: None,
            cloud_status: None,
            cloud_busy: false,
            cloud_archive,
            cloud_backfill,
            cloud_fetch_in_flight: None,
            cloud_prefetch: HashMap::new(),
            cloud_browse_kind: CloudBrowseKind::Time,
            cloud_browse_moves: 0,
            cloud_browse_last: None,
            cloud_browse_until: None,
            cloud_cache_cap: 1,
            cloud_settle: None,
            cloud_unavailable: Vec::new(),
            cloud_not_yet: Vec::new(),
            cloud_stopped: None,
            cloud_retry_at: None,
            cloud_fetch_forced: false,
            cloud_round_line_sent: String::new(),
            memory_dumped: false,
            last_status: String::new(),
            drain: Schedule::new(DRAIN_INTERVAL, now),
            sky: Schedule::new(
                sky_interval(
                    &params,
                    false,
                    layout::preview_short_edge(height, preview_screen),
                    preview_enabled,
                ),
                now,
            ),
            metrics: record_metrics.then(|| Schedule::new(METRICS_INTERVAL, now)),
            auto_refresh: auto_refresh.map(|i| Schedule::new(i, now)),
            cloud,
            surface,
        })
    }

    fn run(&mut self) {
        info!("engine started");
        if self.metrics.is_some() {
            // Leave a startup baseline so a short run is still comparable.
            crate::memory::record_metrics_sample(self.renderer.texture_resolution());
        }
        loop {
            match self.rx.recv_timeout(self.wait()) {
                Ok(cmd) => {
                    if !self.handle(cmd) {
                        break;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }

            // Drain whatever else queued up so a backlog of parameter updates
            // collapses into one render.
            let mut stop = false;
            loop {
                match self.rx.try_recv() {
                    Ok(cmd) => {
                        if !self.handle(cmd) {
                            stop = true;
                            break;
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        stop = true;
                        break;
                    }
                }
            }
            if stop {
                break;
            }

            self.tick();
        }
        info!("engine stopped");
    }

    /// Apply one command. Returns `false` when the engine should stop.
    #[expect(
        clippy::too_many_lines,
        reason = "one arm per command, each short; the length is the routing table"
    )]
    fn handle(&mut self, cmd: EngineCommand) -> bool {
        match cmd {
            EngineCommand::UpdateParams(params) => {
                if params.time != self.params.time {
                    self.set_excursion(None);
                    self.note_cloud_browse(CloudBrowseKind::Time);
                }
                self.params = *params;
                self.resolve_requested_sample_count();
                self.dirty = true;
                self.retime_sky();
            }
            EngineCommand::SetPreviewSize(w, h) => {
                let (qw, qh) = preview_target_size((w, h), self.quality);
                if (qw, qh) != self.renderer.size() {
                    self.renderer.resize(qw, qh);
                    self.dirty = true;
                }
                self.retime_sky();
            }
            EngineCommand::SetPreviewEnabled(enabled) => {
                if enabled && !self.preview.enabled {
                    self.preview.owed = true;
                }
                if !enabled {
                    self.renderer.release_preview_readback();
                    if let Some(mut excursion) = self.excursion.filter(Excursion::is_running) {
                        excursion.pause(self.clock.elapsed());
                        self.set_excursion(Some(excursion));
                    }
                }
                self.preview.enabled = enabled;
                self.retime_sky();
            }
            EngineCommand::RenderWallpaperNow => self.publish_asked = true,
            EngineCommand::Excursion(command) => self.excursion_command(command),
            EngineCommand::Hold(hold) => self.hold(hold),
            EngineCommand::SetDisplayPlan { mode, anchor } => {
                self.display_mode = mode;
                self.anchor_monitor = anchor;
                self.update_preview_screen();
            }
            EngineCommand::RenderToFile {
                path,
                width,
                height,
                reply,
            } => self.export(width, height, publish::ExportReply::File { path, reply }),
            EngineCommand::ExportPixels {
                width,
                height,
                reply,
            } => self.export(width, height, publish::ExportReply::Pixels(reply)),
            EngineCommand::SetTextureResolution(width) => self.set_texture_resolution(width),
            EngineCommand::ReportMemory { reply, settled } => {
                if settled {
                    self.renderer.flush_and_wait();
                }
                let _ = reply.send(Box::new(self.renderer.memory_report(&self.adapter_key)));
            }
            EngineCommand::ReportTiles { reply } => {
                let report = self
                    .surface
                    .as_ref()
                    .map(|surface| surface.tile_report(&self.renderer));
                let _ = reply.send(report.map(Box::new));
            }
            EngineCommand::ReportCloudCache { reply } => {
                let _ = reply.send(self.renderer.cloud_cached_rounds());
            }
            EngineCommand::SetAutoRefresh { enabled, interval } => {
                self.set_auto_refresh(enabled, interval);
            }
            EngineCommand::SetCloudSource(config) => self.set_cloud_source(config.as_ref()),
            EngineCommand::CloudBrowse { date } => {
                self.note_cloud_browse(if date {
                    CloudBrowseKind::Date
                } else {
                    CloudBrowseKind::Time
                });
            }
            EngineCommand::RefreshClouds => self.refresh_clouds(),
            EngineCommand::AbortClouds => self.abort_clouds(),
            EngineCommand::SetCloudPollInterval(mins) => self.set_cloud_poll_interval(mins),
            EngineCommand::SetCloudHistoryLimit(hours) => self.set_cloud_history_limit(hours),
            EngineCommand::SetCloudCacheMode(mode) => self.set_cloud_cache_mode(mode),
            EngineCommand::SetCloudArchiveBudget(budget) => self.set_cloud_archive_budget(budget),
            EngineCommand::SetCloudBackfillPolicy(policy) => self.cloud_backfill = policy,
            EngineCommand::DisplaysChanged => {
                // Trailing rather than leading: every hint pushes the deadline
                // out, so a burst is one query at the end of it.
                let due = self.clock.elapsed() + DISPLAY_SETTLE;
                self.display_recheck = Some(due);
                debug!(
                    settle_secs = DISPLAY_SETTLE.as_secs(),
                    "the display layout may have moved"
                );
            }
            EngineCommand::Poke => {
                // A poke means a producer has something waiting, so bring the
                // next drain forward instead of sitting out the interval.
                self.drain.next = Duration::ZERO;
            }
            EngineCommand::Shutdown => {
                // A publish asked for in the same drain batch as the shutdown,
                // or one still waiting for its tiles, is still someone's
                // request, and there is no tick left to do it in or to wait
                // in. The wallpaper shows the clock; an export keeps the held
                // instant.
                if std::mem::take(&mut self.publish_asked) || self.wallpaper_owed {
                    let held = self.held.take();
                    self.publish_wallpaper_at_exit();
                    self.held = held;
                }
                // A caller waiting on an export is answered with what is
                // resident rather than with a closed channel.
                if !self.exports.is_empty() {
                    self.prepare_export();
                    for export in std::mem::take(&mut self.exports) {
                        self.answer(export);
                    }
                }
                return false;
            }
        }
        true
    }

    /// How long the loop waits for a command: a frame's worth while an
    /// excursion runs and the preview is shown, [`TICK`] otherwise.
    fn wait(&self) -> Duration {
        if self.preview.enabled && self.excursion.as_ref().is_some_and(Excursion::is_running) {
            EXCURSION_TICK
        } else {
            TICK
        }
    }

    /// Apply a client's excursion command.
    fn excursion_command(&mut self, command: ExcursionCommand) {
        let now = self.clock.elapsed();
        let setting = self.params.time.instant(self.clock.now_utc());
        let next = excursion::apply(self.excursion, command, now, setting);
        self.set_excursion(next);
    }

    /// Replace the excursion, and when its state changed tell clients and
    /// draw the preview again.
    fn set_excursion(&mut self, next: Option<Excursion>) {
        let before = self.excursion.map(|excursion| excursion.state());
        let moved = self.excursion != next;
        self.excursion = next;
        let after = next.map(|excursion| excursion.state());
        if before != after {
            self.emit(EngineEvent::Excursion(after));
        }
        if moved {
            self.dirty = true;
            self.retime_sky();
        }
    }

    /// Take the hold or let it go (`EngineCommand::Hold`).
    ///
    /// Taking it pins the instant the preview shows now and pauses a running
    /// excursion, which stays paused after the release. The readiness latch
    /// reopens and the frame is drawn again at the pinned instant, so
    /// `TexturesReady` goes out afresh: on the next tick when everything is
    /// resident, else when it lands. Letting go draws the setting again, and a
    /// wallpaper asked for meanwhile goes out on the next tick.
    fn hold(&mut self, hold: bool) {
        if hold {
            if self.held.is_none() {
                if let Some(mut excursion) = self.excursion.filter(Excursion::is_running) {
                    excursion.pause(self.clock.elapsed());
                    self.set_excursion(Some(excursion));
                }
                self.held = Some(self.preview_instant());
                debug!("the scene is held");
            }
            self.textures_ready = false;
        } else if self.held.take().is_some() {
            debug!("the scene is released");
        }
        self.dirty = true;
        self.retime_sky();
    }

    /// The instant the preview shows now: the held one while the scene is
    /// held, else the excursion's while there is one, the time setting's
    /// otherwise.
    fn preview_instant(&self) -> time::OffsetDateTime {
        if let Some(held) = self.held {
            return held;
        }
        match &self.excursion {
            Some(excursion) => excursion.instant_at(self.clock.elapsed()),
            None => self.params.time.instant(self.clock.now_utc()),
        }
    }

    /// The instant of the time setting, which a wallpaper draws at.
    fn setting_instant(&self) -> time::OffsetDateTime {
        self.params.time.instant(self.clock.now_utc())
    }

    /// The instant every export draws at: the held one while the scene is
    /// held, the time setting's otherwise.
    fn export_instant(&self) -> time::OffsetDateTime {
        self.held.unwrap_or_else(|| self.setting_instant())
    }

    /// The instant the month in force follows in a draw at `at`: the exports'
    /// while a wallpaper or an export waits for its tiles, since they are of
    /// that instant's month and the set has one month, and `at` otherwise. An
    /// excursion in another month is drawn from the setting's month
    /// meanwhile, rather than the two months taking turns in force and each
    /// turn starting the tile wait over.
    fn month_instant(&self, at: time::OffsetDateTime) -> time::OffsetDateTime {
        if self.outputs_waiting() {
            self.export_instant()
        } else {
            at
        }
    }

    /// Whether a wallpaper or an export waits for its tiles.
    fn outputs_waiting(&self) -> bool {
        !self.wallpaper_shots.is_empty() || !self.exports.is_empty()
    }

    /// Point the renderer at another texture resolution's set of assets.
    fn set_texture_resolution(&mut self, width: u32) {
        if self.renderer.set_texture_resolution(width) {
            info!(texture_resolution = width, "surface texture resolution");
            // The tiles of the old cap go, the floors stay, and the next draw
            // asks for the tiles the new one allows.
            if let Some(surface) = &mut self.surface {
                surface.purge_tiles(&mut self.renderer);
                self.restart_export_waits();
            }
            // The textures the current mode needs are gone until the
            // reload lands, so the readiness latch has to reopen or
            // clients would never hear about the new ones.
            self.textures_ready = false;
            self.dirty = true;
            // The cloud variant follows the same setting, but its slot
            // is deliberately not purged: the switch must not depend on
            // the network, and a cloudless gap while a download runs
            // would be a worse picture than a cloud layer at the
            // previous variant. Asking for a poll now is the whole of
            // the change; the existing update path replaces texture,
            // view, and bind group together when it lands.
            if let Some(cloud) = &mut self.cloud
                && cloud.retarget(width)
            {
                cloud.owed = true;
            }
        }
    }

    /// Give the sky schedule the interval the scene and the preview now call
    /// for, counting it from now when that is a change.
    fn retime_sky(&mut self) {
        let interval = sky_interval(
            &self.params,
            self.excursion.is_some(),
            layout::preview_short_edge(self.renderer.size().1, self.preview_screen),
            self.preview.enabled,
        );
        if interval != self.sky.interval {
            debug!(
                interval_secs = interval.as_secs_f32(),
                "the sky schedule's interval changed"
            );
            self.sky.set_interval(interval, self.clock.elapsed());
        }
    }

    /// Start, restart or stop the auto-refresh schedule.
    fn set_auto_refresh(&mut self, enabled: bool, interval: Duration) {
        let now = self.clock.elapsed();
        if enabled {
            match &mut self.auto_refresh {
                Some(schedule) => schedule.set_interval(interval, now),
                None => self.auto_refresh = Some(Schedule::new(interval, now)),
            }
        } else {
            self.auto_refresh = None;
        }
        debug!(
            enabled,
            interval_secs = interval.as_secs(),
            "auto-refresh changed"
        );
    }

    /// Ask the worker for one poll now, ahead of the schedule — at live
    /// time. At a fixed instant the press means "fetch the round this
    /// instant calls for": it bypasses the drag's settle and stands any stop
    /// down, because the user pressed it just now, on purpose.
    fn refresh_clouds(&mut self) {
        let live = self.params.time.is_live() && self.excursion.is_none() && self.held.is_none();
        let interval = self
            .cloud
            .as_ref()
            .and_then(cloud_worker::CloudWorker::publication_interval);
        let instant = self.preview_instant();
        let Some(cloud) = self.cloud.as_mut() else {
            return;
        };
        if live {
            if !cloud.request() {
                cloud.owed = true;
            }
            return;
        }
        let Some(interval) = interval else {
            // A source without dated imagery: the poll is the only upstream
            // action there is, and its map is what every instant wears.
            if !cloud.request() {
                cloud.owed = true;
            }
            return;
        };
        let floor = floor_for(instant, interval);
        let Some(floor) = floor else {
            return;
        };
        self.cloud_unavailable.retain(|round| *round != floor);
        self.cloud_stopped = None;
        self.cloud_settle = None;
        self.cloud_retry_at = None;
        self.cloud_fetch_forced = false;
        info!(round = %round_text(floor), "the historical round is pressed for");
        cloud.fetch(floor);
        self.cloud_fetch_in_flight = Some(floor);
    }

    /// Stop the cloud fetch in flight: the attempt's own stop is raised, so
    /// its source unwinds at its next check point and commits nothing. What
    /// was in flight stands down — the scene wears nothing there until a
    /// drag lands elsewhere or the press answers for it — and the tick stops
    /// re-asking. The next check of the live schedule is the regular one.
    fn abort_clouds(&mut self) {
        let Some(cloud) = self.cloud.as_mut() else {
            return;
        };
        cloud.abort();
        cloud.owed = false;
        if let Some(round) = self.cloud_fetch_in_flight.take() {
            self.cloud_stopped = Some(round);
        }
        self.cloud_settle = None;
        self.cloud_fetch_forced = false;
        info!("the cloud fetch was asked to stop");
    }

    /// Set the minutes between checks of the cloud source. The worker's
    /// schedule is the engine tick's own bookkeeping, so this is where the
    /// new interval lands; a check already owed stays owed.
    fn set_cloud_poll_interval(&mut self, mins: u32) {
        let Some(cloud) = self.cloud.as_mut() else {
            return;
        };
        let interval = Duration::from_mins(u64::from(mins.max(crate::config::CLOUD_POLL_MINS_MIN)));
        cloud.schedule.set_interval(interval, self.clock.elapsed());
        info!(mins, "the cloud check interval was set");
    }

    /// Set how many hours of cloud history the archive keeps, and sweep to
    /// it at once.
    fn set_cloud_history_limit(&mut self, hours: Option<u32>) {
        self.cloud_archive.set_limit(hours);
        self.cloud_archive.prune();
        info!(?hours, "the cloud history limit was set");
    }

    /// Set what the archive keeps per round. Already-rendered files stand
    /// until their round is swept.
    fn set_cloud_cache_mode(&mut self, mode: crate::config::CloudCacheMode) {
        self.cloud_archive
            .set_keep_derived(mode != crate::config::CloudCacheMode::RawOnly);
        info!(?mode, "the cloud cache mode was set");
    }

    /// Set the archive's disk budget, and sweep to it at once.
    fn set_cloud_archive_budget(&mut self, budget: Option<u32>) {
        self.cloud_archive.set_budget(budget);
        self.cloud_archive.prune();
        info!(?budget, "the cloud archive budget was set");
    }

    fn set_cloud_source(&mut self, config: &CloudSourceConfig) {
        let Some(cloud) = self.cloud.as_mut() else {
            info!("a cloud source arrived for a session without one, ignoring it");
            return;
        };
        let source = crate::assets::cloud_source::resolve(
            config,
            crate::env_override(crate::config::clouds::ENV_CLOUD_URL).as_deref(),
            crate::assets::cloud_fetcher::cache_dir().as_deref(),
        );
        info!(source = %source.describe(), "the cloud source is switching");
        cloud.set_source(source);
        // The timeline's memory belongs to the source that taught it: what
        // the old upstream did not hold says nothing about the new one.
        self.cloud_unavailable.clear();
        self.cloud_stopped = None;
        self.cloud_fetch_in_flight = None;
        self.cloud_settle = None;
        self.cloud_retry_at = None;
        self.cloud_prefetch.clear();
        // Worth a poll as soon as the worker can take one, ahead of schedule.
        cloud.owed = true;
        self.cloud_browse_until = None;
        if self.cloud_cache_cap != 1 {
            self.cloud_cache_cap = 1;
            self.renderer.set_cloud_cache_cap(1);
        }
    }

    /// Run everything the schedule says is due, then render if anything changed.
    #[expect(
        clippy::too_many_lines,
        reason = "one scheduled concern after another in the order they were added; splitting it would scatter the tick's ordering invariants"
    )]
    fn tick(&mut self) {
        let now = self.clock.elapsed();

        if self.drain.due(now) && self.renderer.drain_texture_updates() {
            self.dirty = true;
        }
        self.note_clouds();
        self.note_cloud_status();

        // Every tick rather than on the drain's schedule: a pack that landed is
        // one cheap check away, and the first frame of a first run waits on it.
        // A pack opened for its tiles that changes nothing drawn, the month
        // ahead's, has its tiles asked for without a frame.
        let mut opened = false;
        if let Some(surface) = &mut self.surface {
            let before = surface.packs_opened();
            if surface.drain(&mut self.renderer) {
                self.dirty = true;
            }
            opened = surface.packs_opened() != before;
        }
        if opened && !self.dirty {
            self.want_now();
        }
        // A pack that fails changes nothing drawn but can complete what the
        // latch waits for, and the latch is judged after a draw.
        if !self.textures_ready && !self.dirty && self.textures_ready() {
            self.dirty = true;
        }

        if let Some(cloud) = &mut self.cloud {
            // A request the worker gave up because the layer went off is owed
            // again, and goes out when it is switched back on.
            cloud.set_enabled(self.params.clouds_enabled);
            if cloud.take_abandoned() {
                cloud.owed = true;
            }
            // Scheduled checks follow the wallpaper's timer: with auto-refresh
            // off there is no next wallpaper a new map would reach, so a poll
            // that falls due stays unowed until the timer is on. The startup
            // check and the manual ones do not pass through here.
            if cloud.scheduled_due(now, self.clock.now_utc(), self.auto_refresh.is_some()) {
                cloud.owed = true;
            }
            // Retry on the next tick rather than waiting out another whole
            // interval when the worker was still busy with the previous poll.
            // While the layer is switched off a poll that falls due stays
            // owed, so switching it on sends one at once.
            if cloud.owed && self.params.clouds_enabled && cloud.request() {
                cloud.owed = false;
            }
            let busy = cloud.fetching();
            if self.cloud_busy != busy {
                self.cloud_busy = busy;
                // The stop shows against a fetch in flight, so the busy state
                // goes out the tick it changes, status line or not.
                self.emit(EngineEvent::CloudBusy(busy));
            }
        }
        self.sync_cloud_timeline(now);
        self.note_cloud_round();

        // Live time keeps moving even when nothing else changes, so the
        // terminator and the sky have to be recomputed on a schedule of their
        // own.
        if self.sky.due(now)
            && self.params.time.is_live()
            && self.excursion.is_none()
            && self.held.is_none()
        {
            self.dirty = true;
        }

        // A running excursion moves the preview on every tick it is shown,
        // and stops at the edge of the range.
        if let Some(mut excursion) = self.excursion.filter(Excursion::is_running) {
            if excursion.stop_at_the_edge(now) {
                self.set_excursion(Some(excursion));
            }
            if self.preview.enabled {
                self.dirty = true;
            }
        }

        if let Some(metrics) = &mut self.metrics
            && metrics.due(now)
        {
            crate::memory::record_metrics_sample(self.renderer.texture_resolution());
        }

        if self.surface.is_some() {
            let camera = self.watched_camera();
            if let Some(surface) = &mut self.surface
                && surface.settle_drag(&camera, now)
            {
                self.dirty = true;
            }
        }

        if let Some(schedule) = &mut self.auto_refresh
            && schedule.due(now)
        {
            info!("auto-refresh: updating wallpaper");
            self.publish_wallpaper();
        }

        // After the auto-refresh, so a recheck that lands on the same tick
        // compares against the list that refresh planned with rather than
        // publishing twice for one layout.
        if self.display_recheck.is_some_and(|due| now >= due) {
            self.display_recheck = None;
            self.recheck_displays();
        }

        // A frame just drawn goes out, and so does a debt from the preview
        // being turned back on: nothing changed while it was off, so the frame
        // already in the texture is the answer. Either way the readback happens
        // once per tick, and a debt no readback could pay stands until one can,
        // including the case where no frame exists yet.
        let rendered = self.render_if_dirty();
        if self.preview.enabled
            && (rendered || self.preview.owed)
            && self.renderer.has_frame()
            && self.emit_preview()
        {
            self.preview.owed = false;
        }

        // After the render, so that a publish held back by a reload goes out on
        // the tick the reload lands and in the order the events describe: the
        // textures became ready, and then the wallpaper was set from them.
        //
        // One publish however many asked for it: a double-click on "Set as
        // Wallpaper", or the tray and IPC arriving together, are one wallpaper.
        // A hold keeps the request for the release.
        let free = self.held.is_none();
        if free && (std::mem::take(&mut self.publish_asked) || self.wallpaper_owed) {
            self.publish_wallpaper();
        } else if free
            && self.reexport
            && !self.renderer.textures_pending(&self.params)
            && !self.shots_pending()
        {
            info!("the tiles a wallpaper went out without are resident; making it again");
            self.publish_wallpaper();
        }
        self.settle_exports();

        // Last, so that the gate opens only when nothing in this tick drew,
        // and so that a drag's pause counts from the end of the frame's work,
        // its readback included, which on a software adapter is most of it.
        if let Some(surface) = &mut self.surface {
            surface.relax(now);
            surface.end_tick(self.clock.elapsed());
        }
        self.note_status();
    }

    /// Replace the requested MSAA count with one [`resolve_and_warn`] allows,
    /// remembering the request so the same one is reported only once.
    fn resolve_requested_sample_count(&mut self) {
        let requested = self.params.sample_count;
        let resolved = resolve_and_warn(
            requested,
            &self.supported_sample_counts,
            self.quality.max_sample_count(),
            Some(self.requested_sample_count),
        );
        self.requested_sample_count = requested;
        self.params.sample_count = resolved;
    }

    /// The astronomy state the preview is drawn under now.
    fn sky_state(&self) -> SkyState {
        sky::compute_sky_state_for(self.preview_instant())
    }

    /// Derive the month in force from the date, as the sky state is, and move
    /// the surface to it when it changed.
    ///
    /// Outside the digest like the sun direction: the month is not a
    /// parameter anyone sets but a consequence of the date. A floor that
    /// is not resident yet reopens the readiness latch, so clients hear
    /// `TexturesReady` again when it lands. A change marks the frame dirty
    /// whichever path saw it first, a draw or a publish, so the next tick
    /// draws the new month and asks for its tiles; a publish that is owed
    /// and still waiting returns without drawing, and nothing else would.
    fn sync_month(&mut self, at: time::OffsetDateTime) {
        // The live clock only goes forward; a fixed time and an excursion can
        // go either way.
        let both_ways = !self.params.time.is_live() || self.excursion.is_some();
        let Some(surface) = &mut self.surface else {
            return;
        };
        let scene_time = crate::scene::scene_time::SceneTime::fixed(at);
        let month = crate::scene::month::month_in_force(&scene_time, at);
        let ahead = crate::scene::month::month_across_at(at, both_ways);
        if surface.set_month(month, ahead, &mut self.renderer) {
            self.dirty = true;
        }
        if !self.renderer.textures_ready(&self.params) {
            self.textures_ready = false;
        }
    }

    /// Whether everything the frame needs is resident: the cubes the mode
    /// needs, and every tile it wants in view at the 1 px threshold, a failed
    /// one counting as there (plan decision 10).
    fn textures_ready(&self) -> bool {
        self.renderer.textures_ready(&self.params)
            && self
                .surface
                .as_ref()
                .is_none_or(|surface| surface.tiles_complete(&self.renderer))
    }

    /// The engine is about to draw, or has just drawn: the transcoder's pause
    /// gate closes now and stays closed for a while.
    fn mark_busy(&mut self) {
        let now = self.clock.elapsed();
        if let Some(surface) = &mut self.surface {
            surface.mark_busy(now);
        }
    }

    /// The scene the preview frame draws, and the short edge it is framed on:
    /// the settings framed for the frame the renderer holds, after the tier's
    /// cap, and for the anchor screen (plan decision 4).
    fn preview_output(&self) -> (OutputParams, f32) {
        let (width, height) = self.renderer.size();
        let short_edge = layout::preview_short_edge(height, self.preview_screen);
        let framing =
            layout::output_framing(Framing::from(&self.params), width, height, short_edge);
        (framing.applied_to(&self.params), short_edge)
    }

    /// Take a monitor list in, from a publish or from a recheck that found a
    /// new layout, for the preview to resolve its anchor screen against.
    pub(super) fn see_monitors(&mut self, monitors: &[crate::display::Monitor]) {
        self.preview_monitors = monitors.to_vec();
        self.update_preview_screen();
    }

    /// Resolve the preview's anchor screen again, and draw the preview again
    /// when that changed its framing.
    ///
    /// A screen of another size but the same shape frames the preview the
    /// same way, and so does a landscape screen standing in for none, so the
    /// frame already drawn is the answer. Drawing it again would cost a frame
    /// and close the transcoder's pause gate for nothing.
    fn update_preview_screen(&mut self) {
        let screen = preview_screen_of(&self.preview_monitors, self.anchor_monitor.as_deref());
        if screen == self.preview_screen {
            return;
        }
        debug!(?screen, "the preview's anchor screen changed");
        let before = self.preview_output();
        self.preview_screen = screen;
        if self.preview_output() != before {
            self.dirty = true;
            self.retime_sky();
        }
    }

    /// Draw the scene as it stands, for the preview or for an export, and hand
    /// back the sky it was drawn under.
    ///
    /// Every path that draws comes through here first, the preview's and each
    /// export's, so that the pause gate closes before any of them draws: a
    /// build of the rest of the year gives way at its next check rather than
    /// running through the frame.
    ///
    /// The wanted set is computed under the preview's sky whichever is drawn,
    /// so an export during an excursion neither moves the preview's output
    /// in it nor shows the motion observer the setting's camera and Sun
    /// between two of the excursion's. Its month and Blend mode's Sun are the
    /// setting's while outputs wait, which `month_instant` and `want_tiles`
    /// decide.
    fn draw(&mut self, at: time::OffsetDateTime, preview: bool) -> (RenderOutcome, SkyState) {
        self.sync_month(self.month_instant(at));
        self.mark_busy();
        let sky = sky::compute_sky_state_for(at);
        if preview || self.excursion.is_none() {
            self.want_tiles(&sky);
        } else {
            self.want_tiles(&self.sky_state());
        }
        let (mut output, short_edge) = self.preview_output();
        // The strict binding's visible half: a fixed instant wears only its
        // covering round, and nothing at all where no round covers — not a
        // neighbouring era's imagery standing in.
        if !self.clouds_visible_at(at) {
            output.clouds_enabled = false;
        }
        let outcome = self.renderer.render(&output, &sky);
        self.drawn_instant = at;
        self.drawn_short_edge = short_edge;
        self.note_readiness();
        (outcome, sky)
    }

    /// Tell clients what is loading, and when everything the frame needs has
    /// become resident.
    ///
    /// After every draw, the preview's and an export's alike, so that
    /// `TexturesReady` goes out before a wallpaper drawn from what it
    /// announces. The latch follows readiness both ways, so new tiles wanted,
    /// a drag, a month or a resolution switch each reopen it, and clients hear
    /// it again when what they want is there.
    fn note_readiness(&mut self) {
        self.note_status();
        let ready = self.textures_ready();
        if ready && !self.textures_ready {
            debug!("textures ready");
            self.dump_memory_report();
            self.emit(EngineEvent::TexturesReady);
        }
        self.textures_ready = ready;
    }

    /// Tell clients what the loading line says now, when it changed: the pack
    /// the transcoder is preparing, and otherwise the surfaces the frame
    /// waits for, a cube the mode needs or, once they have been on their way
    /// for a moment, the tiles in view.
    ///
    /// After every draw and at the end of every tick, since the transcoder
    /// prepares the rest of the year while nothing is drawn.
    fn note_status(&mut self) {
        let (mut day, mut night) = self.renderer.cubes_waiting(&self.params);
        let mut preparing = None;
        if let Some(surface) = &mut self.surface {
            preparing = surface.preparing();
            let tiles = surface.tiles_waiting(&self.renderer, self.clock.elapsed());
            day |= tiles.0;
            night |= tiles.1;
        }
        let status = loading::text(preparing, day, night);
        if status != self.last_status {
            self.last_status.clone_from(&status);
            self.emit(EngineEvent::Status(status));
        }
    }

    /// Ask the tile loader for the tiles the preview wants under `sky`, and
    /// every export waiting for its tiles, before the frame is drawn, so the
    /// page table it draws with is held to the preview's cap.
    ///
    /// The preview is resolved under the sky of the instant it shows, and
    /// the exports under the time setting's, which is the one they are drawn
    /// at; the two are one sky but during an excursion. While an excursion
    /// runs, the motion observer watches the Sun as well as the camera, and
    /// the camera the preview shows rather than the one the settings hold, so
    /// in the Sun-fixed mode the read-ahead follows the Earth's turn; outside
    /// one it watches the held camera alone, as a drag always was. A run
    /// starting or stopping changes what is watched, which is not a move.
    ///
    /// In Blend mode the set has one Sun to choose the day and the night by:
    /// the setting's while a wallpaper or an export waits, as the month in
    /// force is (plan departure 9), and the preview's otherwise.
    fn want_tiles(&mut self, sky: &SkyState) {
        let exports_sky = if self.excursion.is_some() && self.held.is_none() {
            sky::compute_sky_state_for(self.export_instant())
        } else {
            sky.clone()
        };
        let sun = if self.outputs_waiting() {
            exports_sky.sun_direction
        } else {
            sky.sun_direction
        };
        let running = self.excursion.as_ref().is_some_and(Excursion::is_running);
        let (preview, _) = self.preview_output();
        let Some(surface) = &mut self.surface else {
            return;
        };
        let (width, height) = self.renderer.size();
        let mut outputs = vec![tile_output(&preview, sky, width, height, false)];
        outputs.extend(self.wallpaper_shots.iter().map(|shot| {
            tile_output(
                &shot.framing.applied_to(&self.params),
                &exports_sky,
                shot.width,
                shot.height,
                true,
            )
        }));
        outputs.extend(self.exports.iter().map(|export| {
            tile_output(
                &layout::screen_framing(Framing::from(&self.params), export.width, export.height)
                    .applied_to(&self.params),
                &exports_sky,
                export.width,
                export.height,
                true,
            )
        }));
        let view = View {
            outputs: &outputs,
            month: surface.month(),
            ahead: surface.ahead(),
            surfaces: surfaces_for(&self.params, sun),
            texture_resolution: self.renderer.texture_resolution(),
            drag: surface.drag(
                &if running {
                    self.params.camera_at(sky)
                } else {
                    self.params.held_camera()
                },
                // Outside a run the Sun's motion is the live clock's, which the
                // read-ahead has never treated as a drag.
                if running {
                    sky.sun_direction
                } else {
                    glam::Vec3::ZERO
                },
                if running { Watched::Run } else { Watched::Held },
                self.clock.elapsed(),
            ),
        };
        surface.want_tiles(&view, &mut self.renderer);
        self.wanted_sky = Some(exports_sky);
    }

    /// Bring the wanted set up to date with the outputs as they stand,
    /// without drawing: for renders planned while a cube is still on its
    /// way, whose tiles are read ahead of it.
    fn want_now(&mut self) {
        let sky = self.sky_state();
        self.want_tiles(&sky);
    }

    /// The camera the motion observer last watched, for deciding whether it
    /// has rested: the one the preview shows while an excursion runs.
    fn watched_camera(&self) -> crate::scene::camera::CameraParams {
        if self.excursion.as_ref().is_some_and(Excursion::is_running) {
            self.params.camera_at(&self.sky_state())
        } else {
            self.params.held_camera()
        }
    }

    /// Returns whether a new frame was drawn. Emitting it is `tick`'s, so that
    /// a tick asks the preview target for its pixels once however it got here.
    fn render_if_dirty(&mut self) -> bool {
        if !self.dirty {
            return false;
        }
        let (outcome, _) = self.draw(self.preview_instant(), true);
        self.dirty = false;
        if matches!(outcome, RenderOutcome::Rendered { first_frame: true }) {
            info!("first frame rendered");
        }
        if matches!(outcome, RenderOutcome::Rendered { .. }) {
            self.mark_busy();
        }
        matches!(outcome, RenderOutcome::Rendered { .. })
    }

    /// Write the memory report to the log once, the first time the scene is
    /// fully loaded.
    ///
    /// Behind the level check because assembling the report walks the
    /// allocator's live allocations behind its own lock, and a release build
    /// compiles `debug!` out without compiling out what feeds it.
    fn dump_memory_report(&mut self) {
        if self.memory_dumped || !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        self.memory_dumped = true;
        debug!("\n{}", self.renderer.memory_report(&self.adapter_key));
    }

    /// Read the preview target back and hand the pixels to the client, saying
    /// whether one got there.
    ///
    /// A readback that fails costs this frame and nothing more, and the caller
    /// keeps whatever debt it was paying, so the next tick tries again. Only the
    /// first failure of a run is logged, because the retry is every 50 ms and a
    /// device that is gone stays gone. A device that is gone for good says so on
    /// the paths that have somewhere to report it.
    fn emit_preview(&mut self) -> bool {
        let (width, height) = self.renderer.size();
        let rgba = match self.renderer.read_preview_pixels() {
            Ok(rgba) => rgba,
            Err(e) => {
                if !self.preview.readback_failed {
                    self.preview.readback_failed = true;
                    warn!(error = %e, "the preview frame could not be read back");
                }
                return false;
            }
        };
        self.preview.readback_failed = false;
        self.emit(EngineEvent::PreviewFrame {
            rgba,
            width,
            height,
            short_edge: self.drawn_short_edge,
            instant: self.drawn_instant,
        });
        true
    }

    /// Make sure a frame exists to replay, with a current sun direction.
    ///
    /// Nothing may have been rendered yet when the window started hidden, and
    /// the stored sun direction is as old as the last frame, which for a hidden
    /// window can be hours.
    fn prepare_export(&mut self) {
        self.renderer.drain_texture_updates();
        self.note_clouds();
        if let Some(surface) = &mut self.surface {
            surface.drain(&mut self.renderer);
        }
        let (outcome, sky) = self.draw(self.export_instant(), false);
        if self.excursion.is_some() && self.held.is_none() {
            // The preview target now holds the setting's frame, and the
            // preview has to go back to the excursion's.
            self.dirty = true;
        }
        if matches!(outcome, RenderOutcome::Skipped) {
            // A skipped frame kept the sky the last render was drawn with, and
            // for a hidden window that can be hours old. A rendered one is
            // already holding this one.
            self.renderer.set_sky_state(sky);
        }
    }

    /// Tell clients, once per target, what the cloud layer's first answer
    /// was: drawn when that target's map and its background are uploaded, not
    /// when its first attempt had nothing to show. The generation names the
    /// target, so a switch announces afresh and a client can tell "the
    /// current target is ready" from "an older target's image is still up".
    ///
    /// After every drain, and on every tick, since an answer of nothing posts
    /// nothing and so wakes nobody.
    fn note_clouds(&mut self) {
        let Some(cloud) = &self.cloud else {
            return;
        };
        let generation = cloud.cloud_generation();
        if self.clouds_settled_for == Some(generation) {
            return;
        }
        let drawn = if self.renderer.clouds_resident() {
            true
        } else if cloud.first_answer() == FirstAnswer::Unavailable {
            false
        } else {
            return;
        };
        self.clouds_settled_for = Some(generation);
        if drawn {
            debug!("the cloud layer is resident");
        } else {
            debug!("the cloud layer has nothing to show yet");
        }
        self.emit(EngineEvent::CloudsSettled { drawn, generation });
    }

    /// Forward what the cloud layer is doing when it changes. Called on every
    /// tick: a long native round's phases are the only movement the UI sees
    /// for minutes at a time.
    fn note_cloud_status(&mut self) {
        let Some(cloud) = &self.cloud else {
            return;
        };
        let status = cloud.status();
        if self.cloud_status.as_ref() == Some(&status) {
            return;
        }
        self.cloud_status = Some(status.clone());
        self.emit(EngineEvent::CloudStatus(status));
    }

    /// The cloud timeline's one step, on every tick.
    ///
    /// Results first: what the last historical attempt landed on clears the
    /// in-flight mark and teaches the timeline what the upstream holds.
    /// Then the scene's own answer: live time wears the newest round and
    /// leaves the worker alone; a fixed instant wears its covering round,
    /// dispatching the covering round's fetch where there is none — under
    /// the drag's settle, the policy in force, and any stop the user asked
    /// for. The renderer's gate follows the same answer, so a frame the
    /// session is no longer wearing never replaces what the GPU holds.
    #[expect(
        clippy::too_many_lines,
        reason = "the results, the live branch and the fixed-instant dispatch read as one sequence of the same state machine"
    )]
    fn sync_cloud_timeline(&mut self, now: Duration) {
        let Some(cloud) = &self.cloud else {
            return;
        };
        if let Some(result) = cloud.take_fetch_result() {
            if self.cloud_fetch_in_flight == Some(result.round) {
                self.cloud_fetch_in_flight = None;
            }
            match result.outcome {
                EnsureOutcome::Served => {}
                EnsureOutcome::Missing => {
                    // A round newer than the upstream's own publication edge
                    // is not missing — it has not published yet, and it will:
                    // the upstream lags wall-clock by hours. It retries on
                    // the publication cadence instead of standing down for
                    // good; the line says which.
                    let not_yet = self
                        .cloud
                        .as_ref()
                        .and_then(CloudWorker::publication_latest)
                        .is_none_or(|latest| result.round > latest);
                    if not_yet {
                        info!(
                            round = %round_text(result.round),
                            "the upstream has not published the round yet"
                        );
                        self.cloud_not_yet.push(result.round);
                        self.cloud_retry_at = Some(now + CLOUD_PUBLICATION_RETRY);
                    } else {
                        info!(
                            round = %round_text(result.round),
                            "the upstream does not hold the round"
                        );
                        if !self.cloud_unavailable.contains(&result.round) {
                            self.cloud_unavailable.push(result.round);
                        }
                    }
                }
                EnsureOutcome::Failed => {
                    if result.stopped {
                        self.cloud_stopped = Some(result.round);
                    } else {
                        self.cloud_retry_at = Some(now + CLOUD_FETCH_RETRY);
                    }
                }
            }
        }
        let live = self.params.time.is_live() && self.excursion.is_none() && self.held.is_none();
        if live {
            self.renderer.set_cloud_gate(CloudGate::Live {
                drawn: self.renderer.cloud_round_drawn(),
            });
            // Live is the current interval: no neighbours are worth keeping.
            if self.cloud_cache_cap != 1 {
                self.cloud_cache_cap = 1;
                self.renderer.set_cloud_cache_cap(1);
            }
            return;
        }
        if !self.params.clouds_enabled {
            self.renderer.set_cloud_gate(CloudGate::Nothing);
            return;
        }
        let Some(interval) = cloud.publication_interval() else {
            // A source without dated imagery: there is no round to bind to,
            // so its one image wears at every instant as at live time.
            self.renderer.set_cloud_gate(CloudGate::Live {
                drawn: self.renderer.cloud_round_drawn(),
            });
            return;
        };
        let at = self.preview_instant();
        let rounds = self.cloud_archive.rounds();
        let covered = covering_round(&rounds, at, interval);
        if let Some(round) = covered {
            self.renderer.set_cloud_gate(CloudGate::Round(round));
            // The covering round is on its way whenever what is drawn is not
            // it: the archive answers in a fraction of a second, so this
            // waits for no settle.
            let drawn = self.renderer.cloud_round_drawn();
            if drawn != Some(round) && self.cloud_fetch_in_flight != Some(round) {
                info!(round = %round_text(round), "the covering round is served");
                cloud.fetch(round);
                self.cloud_fetch_in_flight = Some(round);
                self.cloud_settle = None;
                self.cloud_retry_at = None;
            }
            let plan = cloud_browse_plan(
                self.cloud_browse_kind,
                self.cloud_browse_moves,
                self.cloud_browse_last,
                self.cloud_browse_until,
                now,
                round,
                interval,
            );
            if plan.expired {
                self.cloud_browse_until = None;
            }
            if plan.cap != self.cloud_cache_cap {
                self.cloud_cache_cap = plan.cap;
                self.renderer.set_cloud_cache_cap(plan.cap);
            }
            if !plan.neighbours.is_empty() {
                Self::prefetch_rounds(
                    cloud,
                    &self.cloud_archive,
                    &self.renderer,
                    &mut self.cloud_prefetch,
                    &plan.neighbours,
                    now,
                );
            }
            return;
        }
        self.renderer.set_cloud_gate(CloudGate::Nothing);
        // Nothing covers the instant: the floor is what would, and fetching
        // it is the policy's to allow.
        let Some(floor) = floor_for(at, interval) else {
            return;
        };
        // A stop stands down one round, not the timeline: the scene landing
        // elsewhere is a new ask.
        if self.cloud_stopped.is_some_and(|round| round != floor) {
            self.cloud_stopped = None;
        }
        // A round the upstream has since published is fetchable again: the
        // not-yet mark outlives only the lag, not the session.
        let published_edge = self
            .cloud
            .as_ref()
            .and_then(CloudWorker::publication_latest);
        self.cloud_not_yet
            .retain(|round| published_edge.is_none_or(|latest| *round > latest));
        let fetchable = match self.cloud_backfill {
            CloudBackfillPolicy::Never => false,
            CloudBackfillPolicy::RefreshOnly => self.cloud_fetch_forced,
            CloudBackfillPolicy::Automatic => true,
        };
        // Arm the settle for the floor the scene has landed on, so a fetch
        // starts once the drag has stopped, not for every move along the
        // way. A press bypasses the wait; a stop stands the round down.
        if self
            .cloud_settle
            .is_none_or(|(settle_floor, _)| settle_floor != floor)
        {
            self.cloud_settle = Some((floor, now));
        }
        let now_utc = self.clock.now_utc();
        let dispatch = fetchable
            && !self.cloud_unavailable.contains(&floor)
            && !self.cloud_not_yet.contains(&floor)
            && self.cloud_stopped != Some(floor)
            && self.cloud_fetch_in_flight != Some(floor)
            && self.cloud_retry_at.is_none_or(|due| now >= due)
            && floor <= now_utc;
        let settling = !self.cloud_fetch_forced
            && self.cloud_settle.is_some_and(|(settle_floor, since)| {
                settle_floor == floor && now.checked_sub(since).unwrap() < CLOUD_SETTLE
            });
        if !dispatch || settling {
            return;
        }
        info!(round = %round_text(floor), "the covering round is fetched");
        cloud.fetch(floor);
        self.cloud_fetch_in_flight = Some(floor);
        self.cloud_settle = None;
        self.cloud_retry_at = None;
        self.cloud_fetch_forced = false;
    }

    /// Record one time-bar move, from the UI's own hint or from a changed
    /// instant in `UpdateParams`, and keep the session in browsing mode for
    /// a while after it.
    fn note_cloud_browse(&mut self, kind: CloudBrowseKind) {
        let now = self.clock.elapsed();
        let rapid = self
            .cloud_browse_last
            .is_some_and(|last| now.saturating_sub(last) <= CLOUD_BROWSE_RAPID);
        self.cloud_browse_moves = if rapid {
            self.cloud_browse_moves.saturating_add(1)
        } else {
            1
        };
        self.cloud_browse_last = Some(now);
        self.cloud_browse_kind = kind;
        self.cloud_browse_until = Some(now + CLOUD_BROWSE_IDLE);
    }

    /// Ask the archive for `neighbours`, so the GPU cache holds them before
    /// the time bar crosses a boundary. Only rounds already in the archive
    /// are asked for: a warm never touches the network, and a miss costs a
    /// retry after [`CLOUD_PREFETCH_RETRY`] at most.
    fn prefetch_rounds(
        cloud: &CloudWorker,
        archive: &CloudArchive,
        renderer: &crate::renderer::Renderer,
        asked: &mut HashMap<time::OffsetDateTime, Duration>,
        neighbours: &[time::OffsetDateTime],
        now: Duration,
    ) {
        let rounds = archive.rounds();
        for &neighbor in neighbours {
            if !rounds.contains(&neighbor) {
                asked.remove(&neighbor);
                continue;
            }
            if renderer.cloud_round_cached(Some(neighbor)) {
                asked.remove(&neighbor);
                continue;
            }
            if asked
                .get(&neighbor)
                .is_some_and(|at| now.saturating_sub(*at) < CLOUD_PREFETCH_RETRY)
            {
                continue;
            }
            cloud.prefetch(neighbor);
            asked.insert(neighbor, now);
        }
        // A warm the worker never took must not be remembered for the whole
        // session: after this window it is asked for from scratch.
        let horizon = CLOUD_PREFETCH_RETRY * 16;
        asked.retain(|_, at| now.saturating_sub(*at) < horizon);
    }

    /// Whether the draw at `at` wears clouds: at live time, whatever is
    /// resident; at a fixed instant, exactly its covering round — and
    /// nothing where no round covers. This is the strict binding's visible
    /// half: imagery from another era never stands in for this instant's.
    fn clouds_visible_at(&self, at: time::OffsetDateTime) -> bool {
        let live = self.params.time.is_live() && self.excursion.is_none() && self.held.is_none();
        if live {
            return true;
        }
        let Some(cloud) = &self.cloud else {
            return true;
        };
        let Some(interval) = cloud.publication_interval() else {
            // No dated imagery: the source's one image wears everywhere.
            return true;
        };
        let rounds = self.cloud_archive.rounds();
        covering_round(&rounds, at, interval)
            .is_some_and(|round| self.renderer.cloud_round_drawn() == Some(round))
    }

    /// The cloud imagery's own line, recomputed from the timeline every tick
    /// and forwarded when it changes. Recomputed is the point: there is no
    /// second message stream to fall behind what the engine is doing. The
    /// worker's actionable note — a download's progress, a failed check —
    /// follows as its own sentence, so the panel carries ONE cloud line.
    fn note_cloud_round(&mut self) {
        let line = self.cloud_round_line();
        let note = self
            .cloud
            .as_ref()
            .and_then(CloudWorker::note)
            // A stopped landing has its own line below; the worker's stop
            // note would only say it a second time.
            .filter(|_| self.cloud_stopped.is_none());
        let line = match note {
            Some(note) if !line.is_empty() => format!("{line} {note}"),
            _ => line,
        };
        if self.cloud_round_line_sent == line {
            return;
        }
        self.cloud_round_line_sent.clone_from(&line);
        self.emit(EngineEvent::CloudRound { text: line });
    }

    /// What the round line says now.
    fn cloud_round_line(&self) -> String {
        let Some(cloud) = &self.cloud else {
            return String::new();
        };
        if !self.params.clouds_enabled {
            return String::new();
        }
        let live = self.params.time.is_live() && self.excursion.is_none() && self.held.is_none();
        let rounds = self.cloud_archive.rounds();
        if live {
            let round = self
                .cloud_archive
                .current()
                .or_else(|| rounds.last().copied());
            return round.map_or_else(
                || "The layer is waiting for its first round.".to_owned(),
                |round| {
                    // The live clock wears the newest round the source holds,
                    // which may be hours behind the clock when the upstream
                    // stalls. Say the age, so a stale map does not read as a
                    // current one.
                    let behind = (self.clock.now_utc() - round).whole_hours();
                    if behind >= 1 {
                        format!("Live round is {} UTC, {behind} h old.", round_text(round))
                    } else {
                        format!("Live round is {} UTC.", round_text(round))
                    }
                },
            );
        }
        let Some(interval) = cloud.publication_interval() else {
            return "This source carries no dated imagery.".to_owned();
        };
        let at = self.preview_instant();
        if let Some(round) = covering_round(&rounds, at, interval) {
            return format!("Historical round is {} UTC.", round_text(round));
        }
        let Some(floor) = floor_for(at, interval) else {
            return "This source carries no dated imagery.".to_owned();
        };
        if floor > self.clock.now_utc() {
            return format!("Round {} UTC is not published yet.", round_text(floor));
        }
        if self.cloud_fetch_in_flight == Some(floor) {
            return format!("Fetching round {} UTC.", round_text(floor));
        }
        let published_edge = self
            .cloud
            .as_ref()
            .and_then(CloudWorker::publication_latest);
        if published_edge.is_none_or(|latest| floor > latest) {
            return format!("Round {} UTC is not published yet.", round_text(floor));
        }
        if self.cloud_unavailable.contains(&floor) {
            return format!("The upstream does not hold {} UTC.", round_text(floor));
        }
        if self.cloud_stopped == Some(floor) {
            return "The fetch was stopped. Drag again or press Fetch.".to_owned();
        }
        if self.cloud_retry_at.is_some() {
            return format!("Round {} UTC failed to fetch; retrying.", round_text(floor));
        }
        match self.cloud_backfill {
            CloudBackfillPolicy::Never => {
                format!("Fetching is off for round {} UTC.", round_text(floor))
            }
            CloudBackfillPolicy::RefreshOnly => {
                format!("Press Fetch for round {} UTC.", round_text(floor))
            }
            CloudBackfillPolicy::Automatic => {
                format!("Fetching round {} UTC.", round_text(floor))
            }
        }
    }

    fn emit(&self, event: EngineEvent) {
        (self.on_event)(event);
    }
}

/// The mailbox the engine will use, with its slot count checked against the
/// textures this engine was configured with.
///
/// Checked rather than trusted, and before the device exists, because neither
/// way of getting it wrong is visible where it happens. Too few slots drops the
/// posts for the high ones, leaving those slots waiting for a load that was
/// thrown away; too many hands the consumer a slot index its own array does not
/// have, which is a panic in the middle of a session. Failing here means failing
/// before `ready` is sent, so `start` reports a thread that never got as far as
/// its adapter rather than handing back a handle to a thread that quietly died.
fn checked_mailbox(
    mailbox: Option<TextureMailbox>,
    slots: SlotLayout,
    paths: usize,
) -> TextureMailbox {
    if let Some(mailbox) = &mailbox {
        assert_eq!(
            mailbox.slot_count(),
            slots.count(),
            "the texture mailbox must have one slot per texture: the grid, \
             {paths} file-backed, the cloud overlay and its background"
        );
    }
    mailbox.unwrap_or_else(|| TextureMailbox::new(slots.count()))
}

/// One output of the wanted set: `framed` is the scene with the output's
/// framing applied, as the render takes it (`OutputFraming::applied_to`), and its
/// camera is resolved under `sky`, the sky the output is drawn under, so the
/// tiles and the picture cannot disagree on what is in the frame.
fn tile_output(
    framed: &OutputParams,
    sky: &SkyState,
    width: u32,
    height: u32,
    export: bool,
) -> Output {
    Output {
        camera: framed.camera_at(sky),
        width,
        height,
        export,
    }
}

/// The size of the screen a wallpaper plan would be anchored to, the one the
/// preview is framed for, or `None` where the list has no screen with pixels.
fn preview_screen_of(
    monitors: &[crate::display::Monitor],
    anchor: Option<&str>,
) -> Option<(u32, u32)> {
    let anchor = layout::resolve_anchor(monitors, anchor)?;
    let monitor = &monitors[anchor.index];
    (monitor.width > 0 && monitor.height > 0).then_some((monitor.width, monitor.height))
}

/// Start feeding the cube surface, when every face is there and there is a
/// cache directory to build its packs in.
fn start_surface(
    cache_dir: Option<&std::path::PathBuf>,
    mut transcoder: TranscoderConfig,
    now: Duration,
    wake: &crate::assets::cloud_fetcher::NotifyFn,
    tiles: &LoaderConfig,
) -> Option<SurfaceFeed> {
    if !transcoder.textures.is_complete() {
        return None;
    }
    let Some(dir) = cache_dir else {
        warn!(
            "the cube faces are there but no cache directory is, so the tile packs cannot be built"
        );
        return None;
    };
    transcoder.cache_dir.clone_from(dir);
    Some(SurfaceFeed::start(transcoder, now, Arc::clone(wake), tiles))
}

/// Open the GPU and tell `start` what was opened, or why nothing was.
///
/// One report either way, before anything else can fail: a caller blocked on
/// `ready` gets an adapter it can name or an error it can put in front of the
/// user, and never silence.
fn open_gpu(
    force_software: bool,
    ready: &Sender<Result<AdapterReport, String>>,
) -> Result<crate::wgpu_init::WgpuContext, String> {
    let gpu = match crate::wgpu_init::init(force_software) {
        Ok(gpu) => gpu,
        Err(reason) => {
            let _ = ready.send(Err(reason.clone()));
            return Err(reason);
        }
    };
    let _ = ready.send(Ok(AdapterReport {
        info: gpu.adapter_info.clone(),
        key: gpu.adapter_key.clone(),
        supported_sample_counts: gpu.supported_sample_counts.clone(),
    }));
    crate::memory::log_memory_usage("engine: after wgpu init");
    Ok(gpu)
}

/// Resolve a requested MSAA count against the adapter and the tier, saying so
/// when the answer is not what was asked for.
///
/// The single place a sample count becomes real: a config file, a combo box
/// built against a different adapter, or a stale saved setting all funnel
/// through here rather than into `create_render_textures`, where an unsupported
/// count is a wgpu validation error that kills the engine thread.
///
/// `announced` is the request that has already been reported, so a setting the
/// user pushes again is logged once rather than on every arrival. `None` where
/// nothing has been reported yet, which is the engine's own construction.
fn resolve_and_warn(requested: u32, supported: &[u32], max: u32, announced: Option<u32>) -> u32 {
    let resolved = resolve_sample_count(requested, supported, max);
    if resolved != requested && announced != Some(requested) {
        warn!(
            requested,
            using = resolved,
            supported = ?supported,
            "MSAA sample count is not available, falling back"
        );
    }
    resolved
}

/// Two sentences for the status line, where either may be empty.
fn join_notes(first: String, second: String) -> String {
    match (first.is_empty(), second.is_empty()) {
        (true, _) => second,
        (_, true) => first,
        _ => format!("{first}; {second}"),
    }
}

/// Clamp a requested preview size to the tier's cap, keeping the aspect ratio
/// as closely as whole pixels allow, and never below one pixel either way.
///
/// Every preview frame drawn after a [`EngineCommand::SetPreviewSize`] of
/// `requested` has this size, which is how a client tells the frame for the
/// size it sent last from one drawn before.
#[must_use]
pub fn preview_target_size(requested: (u32, u32), quality: QualityTier) -> (u32, u32) {
    let (mut w, mut h) = requested;
    let max_w = quality.max_preview_width();
    if w > max_w {
        let scaled = (u64::from(h) * u64::from(max_w) + u64::from(w) / 2) / u64::from(w);
        h = scaled.try_into().unwrap_or(u32::MAX);
        w = max_w;
    }
    (w.max(1), h.max(1))
}

/// How often the sky schedule redraws a scene drawn at live time, for a
/// preview framed on a short edge of `preview_short_edge` pixels
/// ([`layout::preview_short_edge`]) that is shown or not.
///
/// [`SKY_INTERVAL`], except for a Sun-fixed camera under live time with the
/// preview shown: the surface beneath that camera crosses the frame at about
/// `focal * EARTH_TURN / (distance - 1)` pixels a second, so the interval is
/// the time it takes to move one pixel, clamped between [`SKY_FLOOR`] and
/// [`SKY_INTERVAL`]. A hidden preview has nobody to show a frame to, and an
/// Earth-fixed camera or an overridden instant does not move with the turn.
fn sky_interval(
    params: &SceneParams,
    excursion: bool,
    preview_short_edge: f32,
    shown: bool,
) -> Duration {
    if !shown || !params.sun_fixed || !params.time.is_live() || excursion {
        return SKY_INTERVAL;
    }
    let camera = &params.camera;
    let distance = crate::scene::camera::zoom_to_distance(camera.zoom);
    let focal = preview_short_edge / (2.0 * (camera.fov_deg.to_radians() / 2.0).tan());
    let pixels_a_second = focal * EARTH_TURN / (distance - 1.0);
    Duration::try_from_secs_f32(pixels_a_second.recip())
        .map_or(SKY_INTERVAL, |pixel| pixel.clamp(SKY_FLOOR, SKY_INTERVAL))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_size_below_the_cap_comes_back_as_asked() {
        for tier in [QualityTier::Low, QualityTier::Medium, QualityTier::High] {
            let widest = tier.max_preview_width().min(3841);
            for size in [(1, 1), (widest / 3, 200), (widest - 1, 641), (widest, 2161)] {
                assert_eq!(preview_target_size(size, tier), size, "{tier:?}");
            }
        }
    }

    #[test]
    fn preview_size_above_the_cap_keeps_the_aspect_at_the_caps_width() {
        for tier in [QualityTier::Low, QualityTier::Medium] {
            let cap = tier.max_preview_width();
            for (rw, rh) in [
                (cap * 3, cap * 2 - 1),
                (cap + 641, 1439),
                (cap * 4, 917),
                (cap + 1, 3),
            ] {
                let (w, h) = preview_target_size((rw, rh), tier);
                assert_eq!(w, cap, "{rw}x{rh} at {tier:?}");
                let ideal = f64::from(rh) * f64::from(cap) / f64::from(rw);
                assert!(
                    (f64::from(h) - ideal).abs() <= 0.5,
                    "{rw}x{rh} at {tier:?} came back {w}x{h}, ideal height {ideal}"
                );
            }
        }
    }

    #[test]
    fn preview_size_is_never_zero() {
        for tier in [QualityTier::Low, QualityTier::Medium, QualityTier::High] {
            assert_eq!(preview_target_size((0, 0), tier), (1, 1));
            assert_eq!(preview_target_size((0, 300), tier), (1, 300));
            assert_eq!(preview_target_size((300, 0), tier), (300, 1));
        }
        assert_eq!(preview_target_size((100_000, 1), QualityTier::Low).1, 1);
    }

    #[test]
    fn preview_size_cap_grows_with_the_tier() {
        let low = preview_target_size((3840, 2160), QualityTier::Low);
        let medium = preview_target_size((3840, 2160), QualityTier::Medium);
        let high = preview_target_size((3840, 2160), QualityTier::High);
        assert!(low.0 < medium.0);
        assert!(medium.0 < high.0);
        assert_eq!(high.0, QualityTier::High.max_preview_width().min(3840));
    }

    fn sun_fixed_live(zoom: f32) -> SceneParams {
        let mut params = SceneParams {
            sun_fixed: true,
            ..SceneParams::default()
        };
        params.camera.zoom = zoom;
        params.time = crate::scene::scene_time::SceneTime::Live;
        params
    }

    /// One pixel of the turn at the camera's own lens and distance, by the
    /// geometry: the point beneath the camera moves `EARTH_TURN` radii a
    /// second at `distance - 1` from the eye.
    #[test]
    fn a_shown_sun_fixed_preview_redraws_once_a_pixel_of_the_turn() {
        let params = sun_fixed_live(crate::scene::camera::distance_to_zoom(8.0));
        let short_edge = 1080.0;
        let focal = short_edge / (2.0 * (params.camera.fov_deg.to_radians() / 2.0).tan());
        let expected = 7.0 / (focal * EARTH_TURN);
        let interval = sky_interval(&params, false, short_edge, true);
        approx::assert_relative_eq!(interval.as_secs_f32(), expected, max_relative = 1e-3);
        assert!(SKY_FLOOR < interval && interval < SKY_INTERVAL);
    }

    /// A closer camera and a taller preview both move the surface faster
    /// across the frame, down to the floor and never below it.
    #[test]
    fn the_interval_shortens_with_the_zoom_and_the_preview_down_to_the_floor() {
        let far = sky_interval(&sun_fixed_live(0.5), false, 720.0, true);
        let near = sky_interval(&sun_fixed_live(0.3), false, 720.0, true);
        let taller = sky_interval(&sun_fixed_live(0.5), false, 1440.0, true);
        assert!(near < far, "{near:?} against {far:?}");
        assert!(taller < far, "{taller:?} against {far:?}");
        assert_eq!(
            sky_interval(&sun_fixed_live(0.0), false, 2160.0, true),
            SKY_FLOOR
        );
        assert_eq!(
            sky_interval(&sun_fixed_live(1.0), false, 64.0, true),
            SKY_INTERVAL
        );
    }

    /// Hidden, Earth-fixed, or at an overridden instant, the picture has no
    /// reason to be redrawn more often than the Sun and the stars need.
    #[test]
    fn only_a_shown_sun_fixed_camera_at_live_time_shortens_the_interval() {
        let live = sun_fixed_live(0.0);
        assert!(sky_interval(&live, false, 1080.0, true) < SKY_INTERVAL);
        assert_eq!(sky_interval(&live, false, 1080.0, false), SKY_INTERVAL);
        let earth = SceneParams {
            sun_fixed: false,
            ..live
        };
        assert_eq!(sky_interval(&earth, false, 1080.0, true), SKY_INTERVAL);
        let mut overridden = live;
        overridden.time =
            crate::scene::scene_time::SceneTime::fixed(time::OffsetDateTime::UNIX_EPOCH);
        assert_eq!(sky_interval(&overridden, false, 1080.0, true), SKY_INTERVAL);
        assert_eq!(
            sky_interval(&live, true, 1080.0, true),
            SKY_INTERVAL,
            "an excursion counts as a fixed time"
        );
        assert_eq!(sky_interval(&live, false, 0.0, true), SKY_INTERVAL);
    }

    /// A quiet bar keeps the worn round alone; a move wakes the neighbours,
    /// and the window closing puts the cache back to one.
    #[test]
    fn a_quiet_time_bar_keeps_only_the_worn_round() {
        let round = time::OffsetDateTime::UNIX_EPOCH;
        let interval = Duration::from_hours(1);
        let quiet = cloud_browse_plan(
            CloudBrowseKind::Time,
            0,
            None,
            None,
            Duration::ZERO,
            round,
            interval,
        );
        assert_eq!(quiet.cap, 1);
        assert!(quiet.neighbours.is_empty());

        let moved = cloud_browse_plan(
            CloudBrowseKind::Time,
            1,
            Some(Duration::ZERO),
            Some(Duration::from_secs(20)),
            Duration::from_secs(1),
            round,
            interval,
        );
        assert_eq!(moved.cap, 3);
        assert_eq!(
            moved.neighbours,
            vec![
                round - time::Duration::hours(1),
                round + time::Duration::hours(1)
            ]
        );

        let expired = cloud_browse_plan(
            CloudBrowseKind::Time,
            1,
            Some(Duration::ZERO),
            Some(Duration::from_secs(20)),
            Duration::from_secs(21),
            round,
            interval,
        );
        assert!(expired.expired);
        assert_eq!(expired.cap, 1);
        assert!(expired.neighbours.is_empty());
    }

    /// A fast time drag widens the span to two rounds either side.
    #[test]
    fn a_fast_time_drag_warms_two_rounds_either_side() {
        let round = time::OffsetDateTime::UNIX_EPOCH;
        let plan = cloud_browse_plan(
            CloudBrowseKind::Time,
            CLOUD_BROWSE_RAPID_MOVES,
            Some(Duration::from_millis(100)),
            Some(Duration::from_secs(20)),
            Duration::from_millis(200),
            round,
            Duration::from_hours(1),
        );
        assert_eq!(plan.cap, 5);
        assert_eq!(
            plan.neighbours,
            vec![
                round - time::Duration::hours(1),
                round + time::Duration::hours(1),
                round - time::Duration::hours(2),
                round + time::Duration::hours(2)
            ]
        );
    }

    /// A date drag warms the day either side, not the rounds either side.
    #[test]
    fn a_date_drag_warms_the_days_either_side() {
        let round = time::OffsetDateTime::UNIX_EPOCH;
        let plan = cloud_browse_plan(
            CloudBrowseKind::Date,
            1,
            Some(Duration::ZERO),
            Some(Duration::from_secs(20)),
            Duration::from_secs(1),
            round,
            Duration::from_hours(1),
        );
        assert_eq!(plan.cap, 3);
        assert_eq!(
            plan.neighbours,
            vec![
                round - time::Duration::hours(24),
                round + time::Duration::hours(24)
            ]
        );
    }
}
