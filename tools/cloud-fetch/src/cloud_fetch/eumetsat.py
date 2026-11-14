"""EUMETSAT EUMETView global IR cloud map fetcher.

Fetches the global infrared (10.8 um) cloud layer ``mumi:worldcloudmap_ir108``
from EUMETView's WMS 1.3.0 endpoint (EPSG:4326, no auth), stitches the two
hemispheres into a 2:1 equirectangular greyscale map, patches the
geostationary blind zone at the poles, and turns IR brightness into cloud
coverage: white clouds on black, north up, -180..180 left to right.

The WMS budget is 81920 KB per request, so a single full-globe 8192x4096 call
is refused (98304 KB needed); two hemisphere requests always fit.

This layer is the raw upstream data behind clouds.matteason.co.uk, so the
output is a candidate drop-in replacement for the map Sunlit Earth's haze
removal, floors and gammas were tuned on. The imagery requires the
"Contains modified EUMETSAT data" attribution (EUMETSAT data licensing).
"""

import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path

import numpy as np
from PIL import Image

from cloud_fetch.processing import (
    apply_blur,
    apply_levels,
    decode_grayscale,
    float_to_uint8,
)

WMS_URL = "https://view.eumetsat.int/geoserver/ows"
LAYER = "mumi:worldcloudmap_ir108"
USER_AGENT = "SunlitEarth/1.0"

# WMS 1.3.0 with CRS EPSG:4326 uses latitude,longitude axis order. Each
# hemisphere is a square 180x180 degree bbox rendered at size x size, so the
# stitched map is (2 * size) x size, equirectangular.
HEMISPHERES = (
    ("west", "-90,-180,90,0"),
    ("east", "-90,0,90,180"),
)

DEFAULT_SIZE = 4096  # pixels per hemisphere; output width is 2x this
DEFAULT_FLOOR = 100
DEFAULT_CEILING = 180
DEFAULT_GAMMA = 1.0
DEFAULT_BLUR_SIGMA = 0.0
DEFAULT_QUALITY = 88
DEFAULT_TIMEOUT = 900  # seconds per WMS request

#: Grayscale values at or above this are leftover blind-zone white, not cloud.
NO_DATA_THRESHOLD = 254 / 255.0
#: Blind-zone detection for the pole trim. Below the no-data threshold so the
#: disc edge antialiased to near-white (236..253) is trimmed away too, which
#: the coverage curve would otherwise clip to full cloud.
TRIM_THRESHOLD = 236 / 255.0


def build_wms_url(bbox: str, px: int) -> str:
    """Build the WMS 1.3.0 GetMap URL for one hemisphere.

    :param bbox: Bounding box in latitude,longitude axis order.
    :param px: Width and height of the requested image in pixels.
    :returns: The complete request URL.
    """
    return (
        f"{WMS_URL}?service=WMS&request=GetMap&version=1.3.0&layers={LAYER}"
        f"&styles=&format=image/png&crs=EPSG:4326&bbox={bbox}"
        f"&width={px}&height={px}"
    )


def fetch_hemisphere(
    bbox: str, px: int, timeout: int = DEFAULT_TIMEOUT, retries: int = 3
) -> np.ndarray:
    """Fetch one hemisphere's cloud map as a float32 array in [0, 1].

    Retries with a growing delay (10 s, 20 s, ...) before giving up.

    :param bbox: Bounding box in latitude,longitude axis order.
    :param px: Width and height of the requested image in pixels.
    :param timeout: Seconds per request.
    :param retries: Number of attempts before raising.
    :returns: 2D float32 array with values in [0, 1].
    :raises RuntimeError: If every attempt fails.
    """
    url = build_wms_url(bbox, px)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        started = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                ctype = resp.headers.get("Content-Type", "")
            if not ctype.startswith("image/"):
                snippet = body[:300].decode("utf-8", "replace")
                raise RuntimeError(f"server refused the request: {snippet}")
            image = decode_grayscale(body)
            elapsed = time.monotonic() - started
            print(
                f"  {bbox}: {image.shape[1]}x{image.shape[0]}, "
                f"{len(body) / 1e6:.1f} MB, {elapsed:.0f}s"
            )
            return image
        except (urllib.error.URLError, RuntimeError, OSError) as exc:
            last = exc
            print(f"  attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                time.sleep(10 * attempt)
    raise RuntimeError(f"giving up on {bbox}: {last}") from last


def stitch(hemispheres: Sequence[np.ndarray]) -> np.ndarray:
    """Stitch hemisphere arrays side by side, west first.

    :param hemispheres: Hemisphere arrays of equal height, west then east.
    :returns: One array of the combined width.
    """
    parts = list(hemispheres)
    height = parts[0].shape[0]
    width = sum(part.shape[1] for part in parts)
    canvas = np.empty((height, width), dtype=np.float32)
    x = 0
    for part in parts:
        canvas[:, x : x + part.shape[1]] = part
        x += part.shape[1]
    return canvas


def _median_smooth(values: np.ndarray, half: int) -> np.ndarray:
    """Slide a window over *values* and take the window's middle element.

    :param values: 1D array to smooth.
    :param half: Half width of the window.
    :returns: Smoothed 1D array of the same length.
    """
    result = np.empty_like(values)
    for i in range(len(values)):
        window = np.sort(values[max(0, i - half) : i + half + 1])
        result[i] = window[len(window) // 2]
    return result


def trim_poles(
    image: np.ndarray, threshold: float = TRIM_THRESHOLD, smooth: int = 24
) -> np.ndarray:
    """Drop the polar blind zone column by column, then stretch what is left.

    The service paints the unseen polar cap solid white and the disc edge
    around it is antialiased to near-white, which the coverage curve reads
    as thick cloud. That boundary is a curve in equirectangular space, so a
    single horizontal cut cannot remove it: each column finds its own first
    and last usable row, the edges are median-smoothed to kill single-column
    spikes, and what remains is stretched across the full height.

    :param image: 2D float32 array with values in [0, 1].
    :param threshold: Values at or above this count as blind-zone white.
    :param smooth: Median window half width for the column edges.
    :returns: Full-height array with the blind zone stretched away.
    """
    height, width = image.shape
    fallback_top, fallback_bottom = int(0.09 * height), height - int(0.09 * height)

    # First row from the top (within the top third) that is not blind-zone
    # white, per column. ``argmax`` of a boolean array returns the first True.
    top_zone = image[: height // 3, :] < threshold
    raw_top = np.where(top_zone.any(axis=0), top_zone.argmax(axis=0), height // 3)

    # Last row from the bottom (within the bottom third) that is not
    # blind-zone white, per column. The search window starts at row 2h/3: when
    # that row is usable the loop this replaces stops on it and uses it, and
    # when the whole window is white both return 2h/3, so the results agree.
    bottom_zone = image[2 * height // 3 :, :] < threshold
    raw_bottom = np.where(
        bottom_zone.any(axis=0),
        2 * height // 3 + (bottom_zone.shape[0] - 1) - bottom_zone[::-1].argmax(axis=0),
        2 * height // 3,
    )

    # A usable span that starts too low or ends too high is suspicious; the
    # original script falls back to a fixed 82% band in that case, for both
    # edges together.
    fallback = (raw_top >= height // 4) | (raw_bottom <= 3 * height // 4)
    tops = np.where(fallback, fallback_top, raw_top)
    bottoms = np.where(fallback, fallback_bottom, raw_bottom)

    tops = _median_smooth(tops.astype(np.int64), smooth)
    bottoms = _median_smooth(bottoms.astype(np.int64), smooth)

    out = np.empty_like(image)
    target = np.linspace(0.0, 1.0, height)
    for x in range(width):
        column = image[int(tops[x]) : int(bottoms[x]) + 1, x]
        out[:, x] = np.interp(target, np.linspace(0.0, 1.0, column.size), column)
    print("  polar blind zone trimmed per column")
    return out


def to_coverage(
    image: np.ndarray,
    floor: int = DEFAULT_FLOOR,
    ceiling: int = DEFAULT_CEILING,
    gamma: float = DEFAULT_GAMMA,
) -> np.ndarray:
    """Turn IR brightness into cloud coverage.

    Values at or above :data:`NO_DATA_THRESHOLD` are leftover blind-zone
    white, not cloud, and map to clear sky. ``gamma`` follows the original
    script's ``v ** gamma`` convention (gamma below 1 brightens midtones),
    which is the inverse of :func:`processing.apply_levels`' Photoshop-style
    gamma.

    :param image: 2D float32 array with values in [0, 1].
    :param floor: Input black point (0--255).
    :param ceiling: Input white point (0--255).
    :param gamma: Coverage curve exponent (``v ** gamma``).
    :returns: 2D float32 array with values in [0, 1].
    """
    no_data = image >= NO_DATA_THRESHOLD
    covered = apply_levels(image, floor, ceiling, 1.0 / gamma)
    covered[no_data] = 0.0
    return covered


def resize_width(image: np.ndarray, width: int) -> np.ndarray:
    """Downscale a 2:1 array to a smaller width, keeping the 2:1 ratio.

    :param image: 2D float32 array.
    :param width: Target width, below the current one.
    :returns: Downscaled 2D float32 array.
    """
    height = width // 2
    pil = Image.fromarray(image, mode="F").resize((width, height), Image.LANCZOS)
    return np.asarray(pil).astype(np.float32)


def variant_widths(main_width: int) -> tuple[int, ...]:
    """The app's texture-resolution variants: the main width and its halves.

    For the default 8192 output these are exactly the three cloud image
    variants ``cloud_variant`` offers: 8192x4096, 4096x2048 and 2048x1024.

    :param main_width: Width of the main image.
    :returns: Widths from largest to smallest.
    """
    return (main_width, main_width // 2, main_width // 4)


def save_jpeg(
    image: np.ndarray, output_dir: Path, quality: int = DEFAULT_QUALITY
) -> Path:
    """Save a coverage array as the grayscale JPEG the app's fetcher expects.

    :param image: 2D float32 array with values in [0, 1].
    :param output_dir: Directory for the file (must exist).
    :param quality: JPEG quality (1--100).
    :returns: Path to the saved file, named ``clouds_eumetsat_<w>x<h>.jpg``.
    """
    height, width = image.shape
    out_path = output_dir / f"clouds_eumetsat_{width}x{height}.jpg"
    Image.fromarray(float_to_uint8(image), mode="L").save(
        str(out_path), "JPEG", quality=quality, optimize=True
    )
    size_kb = out_path.stat().st_size / 1024
    print(f"  Saved {out_path} ({width}x{height}, {size_kb:.0f} KB)")
    return out_path


def fetch_eumetsat(
    output_dir: Path,
    size: int = DEFAULT_SIZE,
    floor: int = DEFAULT_FLOOR,
    ceiling: int = DEFAULT_CEILING,
    gamma: float = DEFAULT_GAMMA,
    blur: float = DEFAULT_BLUR_SIGMA,
    quality: int = DEFAULT_QUALITY,
    timeout: int = DEFAULT_TIMEOUT,
    all_sizes: bool = False,
    raw: bool = False,
) -> Path:
    """Full EUMETSAT cloud map pipeline.

    Fetches both hemispheres, stitches them (west left), trims the polar
    blind zone, applies the coverage mapping and optional blur, and saves
    grayscale JPEGs. With ``all_sizes``, also saves the two downscaled
    variants below the main size.

    :param output_dir: Directory for output files (created if needed).
    :param size: Pixels per hemisphere; the main output is (2*size) x size.
    :param floor: Input black point (0--255).
    :param ceiling: Input white point (0--255).
    :param gamma: Coverage curve exponent (``v ** gamma``).
    :param blur: Gaussian blur sigma in pixels.
    :param quality: JPEG quality (1--100).
    :param timeout: Seconds per WMS request.
    :param all_sizes: Whether to also save the downscaled variants.
    :param raw: Whether to also save the unprocessed stitched image.
    :returns: Path to the saved main JPEG.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    hemispheres = []
    for name, bbox in HEMISPHERES:
        print(f"Fetching {name} hemisphere at {size}x{size}...")
        hemispheres.append(fetch_hemisphere(bbox, size, timeout=timeout))

    image = stitch(hemispheres)
    print(f"\nStitched: {image.shape[1]}x{image.shape[0]}")

    if raw:
        raw_path = output_dir / "clouds_eumetsat_raw.png"
        Image.fromarray(float_to_uint8(image), mode="L").save(str(raw_path))
        print(f"  Saved raw: {raw_path}")

    print("\nTrimming the polar blind zone...")
    image = trim_poles(image)

    print("Applying coverage mapping...")
    image = to_coverage(image, floor, ceiling, gamma)
    print(f"  floor={floor}, ceiling={ceiling}, gamma={gamma}")
    covered = np.count_nonzero(image > 0.01)
    print(f"  Pixels > 1%: {covered / image.size * 100:.1f}%")

    if blur > 0:
        print(f"  Applying Gaussian blur (sigma={blur})...")
        image = apply_blur(image, sigma=blur)

    main = save_jpeg(image, output_dir, quality=quality)

    if all_sizes:
        for width in variant_widths(image.shape[1]):
            if width < image.shape[1]:
                print(f"Downscaling to {width}x{width // 2}...")
                save_jpeg(resize_width(image, width), output_dir, quality=quality)

    return main
