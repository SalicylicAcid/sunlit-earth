# cloud-fetch

Fetch and process satellite cloud imagery for Sunlit Earth.

Three data sources are supported:

- **matteason** -- Pre-composited 8K equirectangular cloud JPEG from [clouds.matteason.co.uk](https://clouds.matteason.co.uk)
- **gmgsi** -- NOAA GMGSI Longwave IR band from the public `noaa-gmgsi-pds` S3 bucket, remapped to equirectangular and upscaled to 8K
- **eumetsat** -- Global IR (10.8 um) cloud layer [`mumi:worldcloudmap_ir108`](https://user.eumetsat.int/resources/user-guides/eumetview-user-guide) from EUMETView's WMS, stitched from two hemisphere requests into a 2:1 map, with the geostationary blind zone at the poles trimmed per column. This is the raw upstream data behind the matteason map; its output is a candidate drop-in replacement. The imagery requires the attribution "Contains modified EUMETSAT data" ([EUMETSAT data licensing](https://www.eumetsat.int/eumetsat-data-licensing)).

All sources apply Photoshop-style Levels adjustment (floor, ceiling, gamma) and optional Gaussian blur.

## Usage

```bash
# Matteason source (simplest, pre-composited)
uv run cloud-fetch matteason --output-dir ./output

# GMGSI source (latest available data)
uv run cloud-fetch gmgsi --output-dir ./output

# GMGSI with specific date/time
uv run cloud-fetch gmgsi --output-dir ./output --date 20260319 --hour 06

# EUMETSAT source (8K map plus the two downscaled variants)
uv run cloud-fetch eumetsat -o ./output --all-sizes

# Custom levels processing
uv run cloud-fetch matteason -o ./output --floor 30 --ceiling 200 --gamma 0.5

# Also save the unprocessed image
uv run cloud-fetch matteason -o ./output --raw
```

## Options

### `cloud-fetch matteason`

| Option | Default | Description |
|---|---|---|
| `--output-dir`, `-o` | cwd | Output directory |
| `--floor` | 50 | Levels black point (0--255) |
| `--ceiling` | 255 | Levels white point (0--255) |
| `--gamma` | 0.3 | Levels midtone gamma |
| `--blur` | 0.0 | Gaussian blur sigma (pixels) |
| `--raw` | off | Also save unprocessed image |

### `cloud-fetch gmgsi`

| Option | Default | Description |
|---|---|---|
| `--output-dir`, `-o` | cwd | Output directory |
| `--date` | auto | Force date (YYYYMMDD) |
| `--hour` | auto | Force UTC hour (HH) |
| `--floor` | 60 | Levels black point (0--255) |
| `--ceiling` | 215 | Levels white point (0--255) |
| `--gamma` | 0.7 | Levels midtone gamma |
| `--blur` | 3.0 | Gaussian blur sigma at 8K (pixels) |
| `--raw` | off | Also save unprocessed image |

### `cloud-fetch eumetsat`

| Option | Default | Description |
|---|---|---|
| `--output-dir`, `-o` | cwd | Output directory |
| `--size` | 4096 | Pixels per hemisphere (256--4096); the main output is (2x this) wide |
| `--floor` | 100 | Levels black point (0--255) |
| `--ceiling` | 180 | Levels white point (0--255) |
| `--gamma` | 1.0 | Coverage curve exponent (`v ** gamma`, < 1 brightens midtones) |
| `--blur` | 0.0 | Gaussian blur sigma (pixels) |
| `--quality` | 88 | JPEG quality (1--100) |
| `--timeout` | 900 | Seconds per WMS request |
| `--all-sizes` | off | Also save the two downscaled variants of the main map |
| `--raw` | off | Also save the unprocessed stitched image |

The WMS budget is 81920 KB per request, so a single full-globe 8192x4096 call is
refused; the tool fetches two 4096x4096 hemispheres instead and stitches them
(west left). Output files are named `clouds_eumetsat_<w>x<h>.jpg`, the format
Sunlit Earth's cloud fetcher downloads. The layer is near-realtime; the server
paints unseen polar regions solid white, which the pole trimming removes.

## Development

```bash
uv sync                          # Install dependencies
uv run pytest -v                 # Run tests
uv run ruff check src/ tests/   # Lint
uv run ruff format src/ tests/  # Format
```
