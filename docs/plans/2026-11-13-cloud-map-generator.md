# Plan: Generating our own cloud maps from EUMETSAT

## Summary

Upstream's cloud maps stopped updating in November 2026, and the app has no way
to recover on its own: it keeps serving the last cached map. The fix is to
generate our own maps from the same data upstream uses -- EUMETSAT's
`mumi:worldcloudmap_ir108` EUMETView layer -- with a generator that now exists
as the `eumetsat` command in `tools/cloud-fetch`. Hosting the generated maps
and pointing the app at them (phase 2 and phase 4 of the
[hosting plan](2026-10-22-web-hosting-plan.md)) is deferred and recorded here
as the remaining work.

## Background

The app polls `https://clouds.matteason.co.uk/images/{size}/clouds.jpg` hourly
(`CLOUD_URL_TEMPLATE`, `cloud_fetcher.rs`). matteason/live-cloud-maps is a
single-developer hobby project whose [data sources
research](2026-03-19-cloud-data-sources.md) already flagged the reliability
risk in March 2026, with the recommendation to "fall back to EUMETSAT WMS if
it goes offline". In November 2026 the maps stopped changing: the service
still answers, but publishes the same stale image (the local install's newest
cache entry is from 2026-11-11), so every released build shows frozen clouds
and README's "usually less than 3 hours old" promise no longer holds. The app
detects staleness of neither kind.

The hosting plan anticipated exactly this in its phase 4 ("Our own cloud
maps"), which was waiting on "a method" for generating maps. The method now
exists: a standalone script, `make_cloudmap.py`, written by hand outside the
repository and verified against the app's renderer. It was productized into
`tools/cloud-fetch` as the `eumetsat` source.

## The method

EUMETView serves the global IR (10.8 um) cloud layer `mumi:worldcloudmap_ir108`
as WMS 1.3.0 over EPSG:4326 without authentication, reprojecting to plate
carree on request. The [data sources research](2026-03-19-cloud-data-sources.md)
identifies this layer as "the raw upstream data behind matteason", which makes
the output a candidate replacement for the map the app's haze removal, floors
and gammas were tuned on.

The generator, in one run:

1. Fetches two hemispheres (each a square 4096x4096 request; the WMS budget is
   81920 KB per request, so one full-globe 8192x4096 call is refused) and
   stitches them west-first into 8192x4096.
2. Trims the geostationary blind zone at the poles column by column -- the
   service paints unseen polar regions solid white and the disc edge around
   them is antialiased to near-white. The trim's blind-zone detection uses
   236 as its cutoff (below the no-data threshold, so near-white edge pixels
   are trimmed too); each column finds its own first and last usable row, the
   edges are median-smoothed, and what remains is stretched across the full
   height.
3. Maps IR brightness to cloud coverage with a levels curve (default floor
   100, ceiling 180, gamma 1.0, `v ** gamma` convention), treating values
   >= 254 as leftover no-data white and mapping them to clear sky.
4. Downscales the result to 4096x2048 and 2048x1024 (the three variants
   `cloud_variant` offers) and saves grayscale JPEGs, the format the app's
   fetcher downloads.

The `eumetsat` command exposes all of this as
`uv run cloud-fetch eumetsat -o ./output [--all-sizes] [--raw]` with the
levels, blur, quality and timeout knobs; `--size` also allows smaller maps.
Its gamma follows the script's `v ** gamma` convention, the inverse of the
Photoshop-style gamma the other two sources use; at the default 1.0 the two
agree.

## What exists now

- `tools/cloud-fetch/src/cloud_fetch/eumetsat.py` -- the fetch, stitch, pole
  trim, coverage mapping and variant derivation, reusing
  `processing.apply_levels` and friends. `decode_grayscale` moved from
  `matteason.py` to `processing.py`, where both sources share it.
- The `eumetsat` CLI command, with tests in `tests/test_eumetsat.py` and CLI
  cases in `tests/test_cli.py`.
- `tools/cloud-fetch/README.md` documents the command and the EUMETSAT
  attribution requirement ("Contains modified EUMETSAT data").

## Verification

- `uv run pytest` in `tools/cloud-fetch`: 99 passed.
- A real fetch at `--size 4096` on 2026-11-13: the west hemisphere took 166 s
  (9.7 MB), the east 125 s (9.2 MB), and the run produced the 8192x4096 main
  JPEG at 4.1 MB plus the 4096x2048 (1.4 MB) and 2048x1024 (0.5 MB) variants,
  all inside the app fetcher's size expectations.
- Pixel distribution, generated map against the last matteason 8K cache
  (8-bit grayscale, six 30-degree latitude bands north to south):

  | Map | mean | bands N->S | >= 254 |
  |---|---|---|---|
  | matteason (2026-11-11) | 180.9 | 234 199 104 112 200 236 | 0.01% |
  | generated, default levels (100/180/1.0) | 63.7 | 120 46 22 24 29 139 | 2.81% |
  | generated, tuned levels (60/180/0.8) | 122.9 | 179 119 70 74 106 190 | 2.99% |

  The structure agrees (bright poles, darkest tropics, storm-track mid
  latitudes), but matteason keeps a much higher overall veil: its
  multi-channel blend retains thin cloud and it fabricates the polar caps by
  mirroring rows, while IR-only data sees less thin cloud and this generator
  stretches whatever each column retains.

## Drop-in verdict

The last matteason 8K map and two generated maps were each rendered through
the app's `render` subcommand by pre-seeding `SUNLIT_EARTH_CACHE_DIR` with
`clouds_cache_override.jpg` (the override's cache stem) and pointing
`SUNLIT_EARTH_CLOUD_URL` at an unreachable port, so the render used exactly
one map with no network. All three renders show real clouds in plausible
places through the full shader path (haze estimate, land/ocean floors and
gammas, night side).

- Default levels: correct clouds but a patchy, hard-edged look -- the ceiling
  at 180 clips every IR value above it to flat white, and the shader's floors
  and haze, tuned on matteason's high baseline, read the near-zero clear sky
  as extremely clean.
- Tuned levels (60/180/0.8): visibly closer to matteason's continuous veil.

**Verdict: not a silent v1 drop-in.** The generated maps work end to end, but
the cloud defaults were retuned specifically on matteason's distribution
(1245e82), and the distributions above differ enough that shipping the
generator's output under v1 would change the look without a matching release.
Per hosting-plan decision 5, the generated maps go to `v2` and the release
that switches the app to them retunes the cloud defaults, starting from the
tuned levels above. If a future comparison against a same-day pair shows the
gap has closed, serving at v1 stays open as the cheaper path.

## What remains (deferred)

- **The clouds host.** Per the hosting plan's phase 2 and phase 4: a public
  `sunlit-earth/clouds` repository holding a static-assets Worker on
  `clouds.sunlit.earth` plus a scheduled workflow (every one to two hours,
  with the 60-day keepalive commit) that runs `cloud-fetch eumetsat
  --all-sizes`, validates the JPEGs, and deploys. Zero-cost guardrails from
  the hosting plan's decision 2 apply unchanged. The generator's WMS fetches
  take two to three minutes at 8K, well inside a standard runner's budget.
- **The app switch.** `CLOUD_URL_TEMPLATE` becomes
  `https://clouds.sunlit.earth/v2/{size}/clouds.jpg` (per the verdict), bundled
  with the user agent change to `sunlit.earth/<version> (<os>; <arch>)` from
  hosting-plan decision 7, with a retune of the cloud defaults against the
  generated maps, and with `assets/ATTRIBUTION.md`'s clouds line rewritten to
  credit the EUMETSAT-derived generator (keeping Matt Eason's credit for the
  map the current tuning was done on). Ships in the next release.
- **Released builds.** Until that release ships, published builds keep
  polling matteason directly and show frozen clouds; there is no server-side
  remedy for them. Upgrading is what restores the update.
