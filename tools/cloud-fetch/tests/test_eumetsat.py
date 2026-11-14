"""Tests for the EUMETSAT cloud map fetcher."""

import io
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
from numpy.testing import assert_allclose
from PIL import Image

from cloud_fetch.eumetsat import (
    HEMISPHERES,
    NO_DATA_THRESHOLD,
    _median_smooth,
    build_wms_url,
    fetch_eumetsat,
    fetch_hemisphere,
    resize_width,
    stitch,
    to_coverage,
    trim_poles,
    variant_widths,
)

_URLOPEN = "cloud_fetch.eumetsat.urllib.request.urlopen"
_SLEEP = "cloud_fetch.eumetsat.time.sleep"


def _make_png(width: int = 16, height: int = 16, value: int = 128) -> bytes:
    """Create a synthetic grayscale PNG for testing."""
    img = Image.new("L", (width, height), value)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _mock_urlopen(body: bytes, content_type: str = "image/png") -> MagicMock:
    """Create a mock for urllib.request.urlopen."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = body
    mock_resp.headers.get.return_value = content_type
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


class TestBuildWmsUrl:
    def test_contains_request_parameters(self) -> None:
        url = build_wms_url("-90,-180,90,0", 4096)
        assert url.startswith("https://view.eumetsat.int/geoserver/ows?")
        for part in [
            "service=WMS",
            "request=GetMap",
            "version=1.3.0",
            "layers=mumi:worldcloudmap_ir108",
            "format=image/png",
            "crs=EPSG:4326",
            "width=4096&height=4096",
        ]:
            assert part in url

    def test_bbox_uses_lat_lon_axis_order(self) -> None:
        """WMS 1.3.0 with EPSG:4326 orders the bbox latitude first."""
        assert "bbox=-90,-180,90,0" in build_wms_url("-90,-180,90,0", 16)

    def test_hemispheres_cover_the_globe_west_first(self) -> None:
        assert HEMISPHERES[0] == ("west", "-90,-180,90,0")
        assert HEMISPHERES[1] == ("east", "-90,0,90,180")


class TestFetchHemisphere:
    def test_success_returns_float_array(self) -> None:
        mock_resp = _mock_urlopen(_make_png(16, 16, value=128))
        with patch(_URLOPEN, return_value=mock_resp):
            result = fetch_hemisphere("-90,-180,90,0", 16)
        assert result.shape == (16, 16)
        assert result.dtype == np.float32
        assert_allclose(result, 128 / 255, atol=0.01)

    def test_non_image_response_gives_up(self) -> None:
        """An XML service exception is retried, then raises."""
        mock_resp = _mock_urlopen(b"<ServiceExceptionReport/>", "text/xml")
        with (
            patch(_URLOPEN, return_value=mock_resp),
            patch(_SLEEP) as mock_sleep,
        ):
            try:
                fetch_hemisphere("-90,-180,90,0", 16)
            except RuntimeError as exc:
                assert "giving up" in str(exc)
            else:
                raise AssertionError("RuntimeError not raised")
        assert mock_sleep.call_count == 2  # between the three attempts

    def test_retries_then_succeeds(self) -> None:
        mock_resp = _mock_urlopen(_make_png(16, 16))
        with (
            patch(
                _URLOPEN,
                side_effect=[urllib.error.URLError("boom"), mock_resp],
            ),
            patch(_SLEEP),
        ):
            result = fetch_hemisphere("-90,-180,90,0", 16)
        assert result.shape == (16, 16)


class TestStitch:
    def test_west_goes_left(self) -> None:
        west = np.zeros((4, 4), dtype=np.float32)
        east = np.ones((4, 4), dtype=np.float32)
        result = stitch([west, east])
        assert result.shape == (4, 8)
        assert_allclose(result[:, :4], 0.0)
        assert_allclose(result[:, 4:], 1.0)

    def test_combined_width(self) -> None:
        parts = [np.zeros((4, w), dtype=np.float32) for w in (3, 5, 7)]
        assert stitch(parts).shape == (4, 15)


class TestMedianSmooth:
    def test_single_spike_is_removed(self) -> None:
        values = np.array([5, 5, 9, 5, 5], dtype=np.int64)
        assert_allclose(_median_smooth(values, half=2), 5)


class TestTrimPoles:
    def _blind_zone_image(self, width: int = 128, height: int = 96) -> np.ndarray:
        """White polar caps whose edge follows a gentle curve over the columns."""
        image = np.full((height, width), 0.3, dtype=np.float32)
        edge = 12 + (4 * np.arange(width)) // width  # 12..15, a gentle curve
        for x in range(width):
            image[: edge[x], x] = 1.0
            image[height - edge[x] :, x] = 1.0
        return image

    def test_caps_are_removed(self) -> None:
        """The solid white caps disappear from the stretched output.

        The median-smoothed edges can still catch a column or two of the
        curved rim, so the guarantee is about the cap as a region, not about
        every pixel.
        """
        image = self._blind_zone_image()
        result = trim_poles(image)
        assert result.shape == image.shape
        assert np.count_nonzero(image[:9] >= 0.99) == 9 * image.shape[1]
        output_white = np.count_nonzero(result[:9] >= 0.99)
        assert output_white < 0.05 * 9 * image.shape[1]

    def test_interior_survives(self) -> None:
        image = self._blind_zone_image()
        result = trim_poles(image)
        assert_allclose(np.mean(result), 0.3, atol=0.05)

    def test_fallback_on_all_white(self) -> None:
        """A fully white image uses the fallback band and stays finite."""
        image = np.ones((96, 128), dtype=np.float32)
        result = trim_poles(image)
        assert result.shape == image.shape
        assert np.all(np.isfinite(result))

    def test_near_white_caps_are_trimmed(self) -> None:
        """Caps at 247/255 count as blind zone under the 236 trim threshold.

        The disc edge is antialiased to near-white below the no-data cutoff;
        trimming must still catch it, or the coverage curve clips those pixels
        to full cloud.
        """
        image = self._blind_zone_image()
        image[:12, :] = 247 / 255
        image[84:, :] = 247 / 255
        result = trim_poles(image)
        assert np.count_nonzero(result[:9] >= 0.99) < 0.05 * 9 * image.shape[1]


class TestToCoverage:
    def test_no_data_maps_to_zero(self) -> None:
        image = np.array([[1.0], [253 / 255]], dtype=np.float32)
        result = to_coverage(image, floor=0, ceiling=255, gamma=1.0)
        assert_allclose(result[0, 0], 0.0)
        assert result[1, 0] > 0.9

    def test_identity_levels(self) -> None:
        image = np.full((2, 2), 128 / 255, dtype=np.float32)
        result = to_coverage(image, floor=0, ceiling=255, gamma=1.0)
        assert_allclose(result, 128 / 255, atol=0.01)

    def test_gamma_below_one_brightens_midtones(self) -> None:
        image = np.full((1, 1), 0.5, dtype=np.float32)
        result = to_coverage(image, floor=0, ceiling=255, gamma=0.5)
        assert_allclose(result[0, 0], np.sqrt(0.5), atol=0.01)

    def test_floor_ceiling_span(self) -> None:
        image = np.array([[100 / 255], [180 / 255]], dtype=np.float32)
        result = to_coverage(image, floor=100, ceiling=180, gamma=1.0)
        assert_allclose(result[0, 0], 0.0, atol=0.01)
        assert_allclose(result[1, 0], 1.0, atol=0.01)

    def test_default_threshold_is_the_script_value(self) -> None:
        assert_allclose(NO_DATA_THRESHOLD, 254 / 255)


class TestResizeWidth:
    def test_shape_and_ratio(self) -> None:
        image = np.full((32, 64), 0.5, dtype=np.float32)
        result = resize_width(image, 32)
        assert result.shape == (16, 32)

    def test_uniform_preserved(self) -> None:
        image = np.full((32, 64), 0.5, dtype=np.float32)
        assert_allclose(resize_width(image, 32), 0.5, atol=0.01)


class TestVariantWidths:
    def test_default_variants_match_the_app(self) -> None:
        assert variant_widths(8192) == (8192, 4096, 2048)


class TestFetchEumetsat:
    def test_pipeline_writes_main_jpeg(self, tmp_path: Path) -> None:
        mock_resp = _mock_urlopen(_make_png(16, 16, value=128))
        with patch(_URLOPEN, side_effect=[mock_resp, mock_resp]):
            result = fetch_eumetsat(tmp_path, size=16)

        assert result.name == "clouds_eumetsat_32x16.jpg"
        img = Image.open(result)
        assert img.mode == "L"
        assert img.size == (32, 16)

    def test_output_dir_is_created(self, tmp_path: Path) -> None:
        mock_resp = _mock_urlopen(_make_png(16, 16))
        out = tmp_path / "sub" / "dir"
        with patch(_URLOPEN, side_effect=[mock_resp, mock_resp]):
            result = fetch_eumetsat(out, size=16)
        assert out.is_dir()
        assert result.exists()

    def test_raw_flag_saves_stitched_png(self, tmp_path: Path) -> None:
        mock_resp = _mock_urlopen(_make_png(16, 16))
        with patch(_URLOPEN, side_effect=[mock_resp, mock_resp]):
            fetch_eumetsat(tmp_path, size=16, raw=True)
        assert (tmp_path / "clouds_eumetsat_raw.png").exists()

    def test_raw_flag_false_no_extra_file(self, tmp_path: Path) -> None:
        mock_resp = _mock_urlopen(_make_png(16, 16))
        with patch(_URLOPEN, side_effect=[mock_resp, mock_resp]):
            fetch_eumetsat(tmp_path, size=16, raw=False)
        assert not (tmp_path / "clouds_eumetsat_raw.png").exists()

    def test_all_sizes_saves_downscales(self, tmp_path: Path) -> None:
        mock_resp = _mock_urlopen(_make_png(16, 16))
        with patch(_URLOPEN, side_effect=[mock_resp, mock_resp]):
            fetch_eumetsat(tmp_path, size=16, all_sizes=True)
        assert (tmp_path / "clouds_eumetsat_32x16.jpg").exists()
        assert (tmp_path / "clouds_eumetsat_16x8.jpg").exists()
        assert (tmp_path / "clouds_eumetsat_8x4.jpg").exists()

    def test_fetch_failure_exits_with_error(self, tmp_path: Path) -> None:
        with (
            patch(
                _URLOPEN,
                side_effect=urllib.error.URLError("no network"),
            ),
            patch(_SLEEP),
        ):
            try:
                fetch_eumetsat(tmp_path, size=16)
            except RuntimeError:
                pass
            else:
                raise AssertionError("RuntimeError not raised")
