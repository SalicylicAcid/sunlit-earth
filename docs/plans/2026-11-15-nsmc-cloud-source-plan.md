# The NSMC Cloud Source (GEOS_IRX)

**Date:** 2026-11-15
**Status:** Implemented this day; the source ships as the Clouds group's third option.

## Why

The cloud source evaluation ([2026-03-19-cloud-data-sources.md](2026-03-19-cloud-data-sources.md)) settled on the app's own EUMETSAT pipeline after Matteason stopped publishing (2026-11). A third institutional source was wanted as a hedge and for regional preference. The National Satellite Meteorological Center (NSMC) of the China Meteorological Administration publishes a WMS-style imagery interface, officially documented at [www.nsmc.org.cn/nsmc/cn/image/wms.html](http://www.nsmc.org.cn/nsmc/cn/image/wms.html) ("WMS卫星影像接口说明", service name `FYWMS` in the GetCapabilities, `Fees=none`), and hands out its capability document at
`https://data.nsmc.org.cn/NSMCAPI/v1/nsmc/image/wms/ability?request=GetCapabilities`.

## What was measured, hands-on, 2026-11-15

| Question | Answer |
|---|---|
| Layers | `FY3F_MERSI` and `FY3H_MERSI` (polar-orbiter daily true-color mosaics) and `GEOS_IRX`, the global mosaic of the geostationary fleet's 10.8 µm infrared channels. The capabilities XML declares the latter two; the documentation names all three. |
| Freshness | `GEOS_IRX` publishes one round an hour; at 06:48 UTC the 06:00 round was available (latency under an hour). The day products are one image per day. |
| Image format | RGBA PNG (8-bit, color type 6). **The alpha channel carries the cloud intensity**: 0 is clear sky or no data, 255 the deepest convective cloud, with every value between in use (a 2048×1024 global render held ~1.3 M clear-to-thin pixels and ~733 K pixels over alpha 31, ITCZ cores at 219–255). The RGB channels only echo it as a binary companion — white wherever any coverage is present, black elsewhere. |
| Coverage | Global equirectangular, but the geostationary ring sees roughly ±60° of latitude; the polar bands are alpha zero. |
| Sizes | Full-globe 4096×2048 works (5.1 MB, ~31 s of server-side rendering). 8192×4096 full-globe comes back blank; so does every JPEG-format full-globe request (a fixed blank image, byte-identical across rounds) — the JPEG output channel is broken for this layer. Regional requests at the documented example sizes work. |
| Auth, CORS | None; `Access-Control-Allow-Origin: *`. Errors return 200 with an empty body or a blank image, never an HTTP error or a WMS exception page — the client must validate content. |
| Change detection | The service publishes the round list the doc page links: `https://data.nsmc.org.cn/nsmcapi/v1/nsmc/image/animation/datatime/mongodb?dataCode=GEO_MULT_GBAL_L2_GGM_IRX_GLL_YYYYMMDD_HHmm_4000M.PNG&hourRange=24`, JSON `{"ds":[{"dataDate":"20261115","dataTime":"060000",...},...]}` in chronological order, a day of hourly entries, ~1.6 KB. |
| FY-3 layers | One round per day (the previous day's mosaic, published ~06:26 UTC by `data.json`), true color with the surface fully visible — the evaluation's surface-contamination criterion rules them out for the cloud layer, and they were left out. |

## Decisions

- **Layer: `GEOS_IRX` only.** The only layer that meets the evaluation criteria (hourly, infrared, continuous intensity). The FY-3 true-color layers are not exposed.
- **Polar fill: stretch.** The blind bands run at nearly constant latitude, so the fill finds the outermost data-bearing rows once (a row is data when half its pixels carry coverage) and interpolates them across the full height — the same latitude-shift approximation the EUMETSAT pole trim makes, minus the per-column search that would chase clear-sky rows inside the data area.
- **Fetch: one 4096×2048 PNG per round, no tiling.** The service renders the full globe in one request and refuses to upscale past ~4K, so the raw mosaic is one file for every texture resolution and each variant's derived map is a client-side triangle resize of it. This is the main structural difference from the EUMETSAT source, which tiles because a hemisphere dies mid-transfer.
- **Round gate: the round list, not an age window.** The EUMETSAT source ages its pair because upstream has no list; NSMC does, so every poll costs one small JSON request and a stored round serves the cache with no imagery download. A round listed but not yet rendered comes back with a uniform alpha (all clear or all saturated), which is classified transient and retried within the round's budget.
- **Derivation:** alpha → polar fill → resize to the variant → `clamp((255v − floor)/(ceiling − floor), 0, 1) ** gamma` → optional Gaussian blur → the quality-88 grayscale JPEG the pipeline already decodes. Defaults `floor 0, ceiling 255, gamma 1, blur 0` pass the served coverage through unchanged; the sliders retune it. The raw PNG is cached verified with a manifest (hashes, round `datetime`, fetch time) committed last; the derived map's metadata names the raw hash, the parameters and the algorithm version, one per size.
- **Wait: `CloudWait::SlowFetch`, 5 minutes.** Between the remote image's 60 s and the EUMETSAT generation budget's 26 min: the service's render time (~31 s measured) plus the transfer and the derivation.
- **No serde_json.** The round list's JSON is scanned for its two field names instead, because `serde_json`'s `impl PartialEq<Value> for i32` links into every test binary of the tree and turns the existing `assert_eq!(rates, [])` comparisons into ambiguous inference. The list is flat, machine-generated, and carries no escapes, so the scan skips rather than guesses at anything it does not recognize.

## What is verified and what is not

- Verified live: GetCapabilities and GetMap unauthenticated, hourly cadence, the alpha semantics, the blank-image quirks, the round list. The unit tests (fixture agent, no network) cover the round flow, the derivation, the caches, the cancel and the error classes.
- Not verified: the source in the running window against the live service on any platform, and on Linux/macOS at the window. The engine-level machinery is shared with the EUMETSAT source and covered by the existing engine tests.

## Update, 2026-11-16: the curve did not survive the renderer

The pass-through defaults this plan settled on drew almost nothing. `fs_cloud`'s floor and gamma were tuned for an infrared *brightness* map — clear mid-latitude land around 0.8 of full scale — while the mosaic's alpha is an overlay opacity with a median of 10 of 255 and 39% of its pixels at exactly zero, so nearly four fifths of the globe fell under the land floor of 0.25 and a cloudy region such as southern China rendered as clear sky. Measured over the 05:00 UTC round of 2026-11-16, the derived map's 50th and 90th percentiles were 21 and 104 against the EUMETSAT map's 121 and 217.

The default curve is now `floor 0, ceiling 165, gamma 0.30`, which lands the same round at 110 and 217; a config file still holding the pass-through is migrated to it, and `NsmcSettings`' doc comment carries the measurements. The status line also names the settled round (`round 2026-11-16 05:00 UTC`), since the window gave no way to tell an hour-old map from a current one.
