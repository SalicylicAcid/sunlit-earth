# Cloud history v4: rebuilt from 748f9d3

**Date:** 2026-11-22 (implementation). The user's ruling was that v1 to v3 were all abandoned — their uncommitted changes discarded — and the feature rebuilt from commit 748f9d3, iterating on the simplest design, with a hands-on acceptance run before delivery.

## Why v1 to v3 were thrown away

v1 to v3, none of them committed, accumulated fourteen interacting mutable flags in the engine's cloud block (wanted, neighbor, prefetched, retry, floor, floor_armed, stopped_floor, unavailable, refresh_pressed, serve_sent, repost, requested, and more). Every round of fixes introduced a new interaction bug: a short circuit on frame_settled twice over, a miss poisoning its own round, a poisoned seed, a status line that outlived its state. The logs from a real run showed the machinery "working" while the experience failed:

| Symptom | Log evidence (session of 2026-11-22, 12:33 to 12:57) | The structural answer in v4 |
|---|---|---|
| A new drag did not preempt a fetch in flight, costing about 35 s extra | Dragging to 08-16 while the worker was still fetching the 08-22 prefetch round; the user waited from 04:55:21 to 04:56:11 | The worker runs one attempt at a time on a thread of its own; a new request sets the old attempt's cancel flag, drops its result channel and starts at once (`cloud_worker.rs::coordinator`) |
| Stop could not interrupt a 50 s download | It could only exit at the "latest checkpoint" | The cancel flag is checked between every read chunk and inside the park slice; on the engine side Stop sets the stopped state immediately |
| A stretch with no archive showed nothing and said nothing | 05:05 to 08:53 drew no clouds with no explanation | The round line (`CloudRound`) is recomputed from the timeline's state on every tick: `fetching round X UTC…`, `has not been published yet`, `is not held by the upstream`, `stopped` |
| The status stuck on an older round | The round and status lines read 09:00 while the user had dragged to 03:00 | The round line is a direct reading of the state machine, so there is no second message stream to go stale |
| Every stop cost 55 s | NSMC's `compose` renders server-side for about 50 s | The archive accumulates (the live poll records, a landing pins), so revisiting a round costs no network at all; a settle delay after the drag avoids cancelling in a row |

## Design (the simplest one)

**Archive** `assets/cloud_archive.rs`: one `manifest.toml` v2 (stamp, raw, derived, pinned), a format compatible with the v3 data on disk, so the eighteen rounds an existing user already has are usable as they are. Six operations only: record, pin, prune (the retention window skips pinned rounds; the budget clears unpinned ones first), `rounds()` (the in-memory index), `load_jpeg` and `raw_files`. The seed is a real round out of the source's metadata, never a file's modification time pretending to be one.

**Source** `CloudSource` gains two methods: `fetch_round_at(round) -> Ok(Some)/Ok(None)/Err`, where `None` means the upstream's list does not hold that round, and `settled_round()` for the seed. NSMC asks for a wide list (`hourRange=10000`, held for the session, and a miss does not invalidate the list) and uses its existing `compose` endpoint; EUMETSAT requests a given time through its existing tile pipeline. Derivation goes through a "derive without writing" kernel (`derive_encode`, `process_map_only`), so a historical round never overwrites the live cache.

**Timeline** `engine::cloud_timeline`, pure functions (`floor_for`, `covering_round`, `round_text`), plus seven memory fields on the engine: in flight, settle, unavailable, stopped, retry at, forced, round line sent. Each tick: consume the result; at live time the gate is Live; at a fixed instant compute the covering round; with a cover, the gate is Round and a dispatch goes out whenever the drawn round differs from it (the archive answers in milliseconds); with no cover, dispatch a fetch under the policy, the 1 s settle (`CLOUD_SETTLE`) and the retry window. `renderer::CloudGate` refuses a frame the session is no longer wearing, so an abandoned fetch that lands late does not reach the screen.

**Visibility** `clouds_visible_at(at)`: a fixed instant wears only its covering round, and with no covering round it wears no clouds. A source without rounds (the remote image) wears its current map at every instant.

**Worker** `CloudRequest::{Poll, Fetch}`: Poll obeys the engine's busy guard and preempts nothing; Fetch is fire and forget, so a new landing preempts the fetch in flight. Results come back to the engine through `take_fetch_result()` in three states.

**UI**: the round line under the button row, reading `CloudRound`; **Fetch this moment**, which fetches the landing at once and is disabled while busy; **Stop**, shown only while busy; and four settings, Keep history, Cache imagery, Archive size limit and Missing imagery (three choices), each taking effect when chosen and persisting.

## Explicitly out of scope (the simplest boundary)

Background prefetch and concurrent probing; wearing the newest round at a fixed instant (v2's strict binding is kept); a database; a formal state machine enum; an LRU for served rounds; and identity checking for EUMETSAT's archived rounds, where identity is stored but never compared, so a mismatch falls back to fetching from upstream.

## Tests

- Archive, 7: round trip, replacement, raw-only, retention, budget, current, in-memory state.
- Timeline pure functions, 8: the three floor cases, coverage boundaries, a gap, a window in the future.
- Worker, 3: a source switch's generation, a clean recovery after Stop, and **a new fetch preempting the one in flight** — the regression proof this round exists for.
- Engine integration, 8, in `tests/engine/clouds_history.rs`: the archive answers at once, a fetch after the settle and its pin, **preemption**, Stop standing a fetch down and its recovery, no retry for a round upstream does not hold, both cache modes, and a round in the future.

## Implementation bugs found and fixed (recorded)

- The coordinator did not clear busy after an attempt ended, so the engine never saw an idle worker after the first poll. A hanging test exposed it.
- A source without rounds was hidden at a fixed instant, which broke the remote source and every fixture test. It now has the same semantics as live time.
- The archive's `State` default for `keep_derived` had to agree with `CloudCacheMode::default()`, which is true.
- Test timestamps carried nanoseconds while the archive compares whole seconds (the stamp is truncated), so the tests now round to whole seconds.

## Verification (all passed)

- **The whole workspace test suite green twice**, about 2,180 tests, including 19 new ones: 7 archive, 8 timeline pure functions, 3 worker and 8 engine integration — the archive answering at once, the fetch after the settle and its pin, **preemption**, Stop standing a fetch down and its recovery, no retry where upstream holds nothing, both cache modes, and a round in the future.
- **Clippy clean** across the workspace, all targets.
- **A release build with `cargo xtask bundle --verify`**: 90 files read back identical, plus a smoke render.
- **A hands-on run** with the dev build against the live NSMC service, a throwaway cache folder and the landing driven from the config, which is the same path a drag takes:

| Stage | Scenario | Result, from the logs |
|---|---|---|
| A | Fixed instant 11-22 04:53, archive empty | the settle dispatched, `compose` ran, `entered the cloud archive pin=true`, the round reached the screen |
| B | Fixed instant 04:20, the archive already holding 04:00 | `the archive serves the round` in 250 ms with no network, and it reached the screen; a startup transient, where the previous session's live frame showed for 1.5 s, was found there and fixed in the same session by defaulting the gate to Nothing |
| C | Back to live time | the newest round, 07:00, was worn, and a poll confirmed it was still current |
| D | **08-16 09:45, the case the user originally reported** | the settle dispatched, the wide list held 9942 entries, `compose` took 57.2 s, the round was **pinned into the archive** and reached the screen, with August's clouds worn correctly |

- The archive ended the run holding three rounds, two fetched and pinned and one live and unpinned, so the pinned and live semantics worked as designed.
- Known limit of this acceptance run: the session's synthetic mouse clicks could not reach the Slint panel's disclosure arrow or drag the time bar, because Slint did not hit the events at those positions. The landing was driven from the config's `scene_time` instead, which is the same path an `UpdateParams` landing takes. The drag's UI pipeline is unchanged since HEAD and covered by engine tests, including preemption and Stop. The round line, the buttons and the four settings are present and laid out (the build and the Slint UI tests pass); clicking them by hand is left to the user.

## How the user accepts it

1. After startup, drag the time bar back to August. About a second after the drag stops, the round line reads `fetching round 2026-08-16 09:00 UTC…`; about a minute later it reads `… (historical)` and August's clouds are on the screen.
2. Drag back to an instant already archived: the clouds appear **at once**, marked `historical`, with no network wait.
3. Drag somewhere else while a fetch is running: the old one is abandoned immediately and the new one starts, with no 50 s wait.
4. Press Stop during a fetch: it is interrupted at once and the round line reads `stopped — drag again or press Fetch this moment`; dragging again or pressing **Fetch this moment** resumes.
5. The round line is recomputed from the engine's state every tick: when it says "fetching" a fetch is running, when it says "historical" the round is on screen. It never sticks on an older wording.