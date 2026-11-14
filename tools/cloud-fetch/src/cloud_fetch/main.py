"""CLI interface for the cloud-fetch tool."""

from pathlib import Path
from typing import Annotated

import typer

from cloud_fetch.eumetsat import (
    DEFAULT_BLUR_SIGMA as EUM_DEFAULT_BLUR,
)
from cloud_fetch.eumetsat import (
    DEFAULT_CEILING as EUM_DEFAULT_CEILING,
)
from cloud_fetch.eumetsat import (
    DEFAULT_FLOOR as EUM_DEFAULT_FLOOR,
)
from cloud_fetch.eumetsat import (
    DEFAULT_GAMMA as EUM_DEFAULT_GAMMA,
)
from cloud_fetch.eumetsat import (
    DEFAULT_QUALITY as EUM_DEFAULT_QUALITY,
)
from cloud_fetch.eumetsat import (
    DEFAULT_SIZE as EUM_DEFAULT_SIZE,
)
from cloud_fetch.eumetsat import (
    DEFAULT_TIMEOUT as EUM_DEFAULT_TIMEOUT,
)
from cloud_fetch.eumetsat import (
    fetch_eumetsat,
)
from cloud_fetch.gmgsi import (
    DEFAULT_BLUR_SIGMA as GMGSI_DEFAULT_BLUR,
)
from cloud_fetch.gmgsi import (
    DEFAULT_CEILING as GMGSI_DEFAULT_CEILING,
)
from cloud_fetch.gmgsi import (
    DEFAULT_FLOOR as GMGSI_DEFAULT_FLOOR,
)
from cloud_fetch.gmgsi import (
    DEFAULT_GAMMA as GMGSI_DEFAULT_GAMMA,
)
from cloud_fetch.gmgsi import (
    fetch_gmgsi,
)
from cloud_fetch.matteason import (
    DEFAULT_BLUR_SIGMA as MATT_DEFAULT_BLUR,
)
from cloud_fetch.matteason import (
    DEFAULT_CEILING as MATT_DEFAULT_CEILING,
)
from cloud_fetch.matteason import (
    DEFAULT_FLOOR as MATT_DEFAULT_FLOOR,
)
from cloud_fetch.matteason import (
    DEFAULT_GAMMA as MATT_DEFAULT_GAMMA,
)
from cloud_fetch.matteason import (
    fetch_matteason,
)

app = typer.Typer(
    name="cloud-fetch",
    help="Fetch and process satellite cloud imagery for Sunlit Earth.",
)

_DEFAULT_OUTPUT_DIR = Path.cwd()


def _validate_floor(value: int) -> int:
    if value < 0 or value > 255:
        raise typer.BadParameter("Floor must be between 0 and 255.")
    return value


def _validate_ceiling(value: int) -> int:
    if value < 0 or value > 255:
        raise typer.BadParameter("Ceiling must be between 0 and 255.")
    return value


def _validate_gamma(value: float) -> float:
    if value <= 0:
        raise typer.BadParameter("Gamma must be positive.")
    return value


def _validate_blur(value: float) -> float:
    if value < 0:
        raise typer.BadParameter("Blur sigma must be non-negative.")
    return value


def _validate_size(value: int) -> int:
    if value < 256 or value > 4096:
        raise typer.BadParameter(
            "Size must be between 256 and 4096 pixels per hemisphere."
        )
    return value


def _validate_quality(value: int) -> int:
    if value < 1 or value > 100:
        raise typer.BadParameter("Quality must be between 1 and 100.")
    return value


def _validate_timeout(value: int) -> int:
    if value < 1:
        raise typer.BadParameter("Timeout must be at least one second.")
    return value


@app.command()
def matteason(
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Directory for output PNG (default: current directory).",
            resolve_path=True,
        ),
    ] = _DEFAULT_OUTPUT_DIR,
    floor: Annotated[
        int,
        typer.Option(
            "--floor",
            help="Levels black point, 0-255.",
            callback=_validate_floor,
        ),
    ] = MATT_DEFAULT_FLOOR,
    ceiling: Annotated[
        int,
        typer.Option(
            "--ceiling",
            help="Levels white point, 0-255.",
            callback=_validate_ceiling,
        ),
    ] = MATT_DEFAULT_CEILING,
    gamma: Annotated[
        float,
        typer.Option(
            "--gamma",
            help="Levels midtone gamma.",
            callback=_validate_gamma,
        ),
    ] = MATT_DEFAULT_GAMMA,
    blur: Annotated[
        float,
        typer.Option(
            "--blur",
            help="Gaussian blur sigma in pixels.",
            callback=_validate_blur,
        ),
    ] = MATT_DEFAULT_BLUR,
    raw: Annotated[
        bool,
        typer.Option(
            "--raw",
            help="Also save the unprocessed image.",
        ),
    ] = False,
) -> None:
    """Fetch the matteason 8K cloud image and apply levels processing."""
    try:
        fetch_matteason(
            output_dir=output_dir,
            floor=floor,
            ceiling=ceiling,
            gamma=gamma,
            blur=blur,
            raw=raw,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def gmgsi(
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Directory for output PNG (default: current directory).",
            resolve_path=True,
        ),
    ] = _DEFAULT_OUTPUT_DIR,
    date: Annotated[
        str | None,
        typer.Option(
            "--date",
            help="Force date YYYYMMDD.",
        ),
    ] = None,
    hour: Annotated[
        str | None,
        typer.Option(
            "--hour",
            help="Force UTC hour HH.",
        ),
    ] = None,
    floor: Annotated[
        int,
        typer.Option(
            "--floor",
            help="Levels black point, 0-255.",
            callback=_validate_floor,
        ),
    ] = GMGSI_DEFAULT_FLOOR,
    ceiling: Annotated[
        int,
        typer.Option(
            "--ceiling",
            help="Levels white point, 0-255.",
            callback=_validate_ceiling,
        ),
    ] = GMGSI_DEFAULT_CEILING,
    gamma: Annotated[
        float,
        typer.Option(
            "--gamma",
            help="Levels midtone gamma.",
            callback=_validate_gamma,
        ),
    ] = GMGSI_DEFAULT_GAMMA,
    blur: Annotated[
        float,
        typer.Option(
            "--blur",
            help="Gaussian blur sigma in pixels at 8K output.",
            callback=_validate_blur,
        ),
    ] = GMGSI_DEFAULT_BLUR,
    raw: Annotated[
        bool,
        typer.Option(
            "--raw",
            help="Also save the unprocessed image.",
        ),
    ] = False,
) -> None:
    """Fetch NOAA GMGSI LW satellite imagery and produce a cloud texture."""
    try:
        fetch_gmgsi(
            output_dir=output_dir,
            date=date,
            hour=hour,
            floor=floor,
            ceiling=ceiling,
            gamma=gamma,
            blur=blur,
            raw=raw,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def eumetsat(
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Directory for output JPEG (default: current directory).",
            resolve_path=True,
        ),
    ] = _DEFAULT_OUTPUT_DIR,
    size: Annotated[
        int,
        typer.Option(
            "--size",
            help="Pixels per hemisphere; the main output is (2x this) wide.",
            callback=_validate_size,
        ),
    ] = EUM_DEFAULT_SIZE,
    floor: Annotated[
        int,
        typer.Option(
            "--floor",
            help="Levels black point, 0-255.",
            callback=_validate_floor,
        ),
    ] = EUM_DEFAULT_FLOOR,
    ceiling: Annotated[
        int,
        typer.Option(
            "--ceiling",
            help="Levels white point, 0-255.",
            callback=_validate_ceiling,
        ),
    ] = EUM_DEFAULT_CEILING,
    gamma: Annotated[
        float,
        typer.Option(
            "--gamma",
            help="Coverage curve exponent (v ** gamma, < 1 brightens midtones).",
            callback=_validate_gamma,
        ),
    ] = EUM_DEFAULT_GAMMA,
    blur: Annotated[
        float,
        typer.Option(
            "--blur",
            help="Gaussian blur sigma in pixels.",
            callback=_validate_blur,
        ),
    ] = EUM_DEFAULT_BLUR,
    quality: Annotated[
        int,
        typer.Option(
            "--quality",
            help="JPEG quality, 1-100.",
            callback=_validate_quality,
        ),
    ] = EUM_DEFAULT_QUALITY,
    timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            help="Seconds per WMS request.",
            callback=_validate_timeout,
        ),
    ] = EUM_DEFAULT_TIMEOUT,
    all_sizes: Annotated[
        bool,
        typer.Option(
            "--all-sizes",
            help="Also save the two downscaled variants of the main map.",
        ),
    ] = False,
    raw: Annotated[
        bool,
        typer.Option(
            "--raw",
            help="Also save the unprocessed stitched image.",
        ),
    ] = False,
) -> None:
    """Fetch the EUMETSAT global IR cloud layer and build a coverage map."""
    try:
        fetch_eumetsat(
            output_dir=output_dir,
            size=size,
            floor=floor,
            ceiling=ceiling,
            gamma=gamma,
            blur=blur,
            quality=quality,
            timeout=timeout,
            all_sizes=all_sizes,
            raw=raw,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e
