"""Small HTTP downloader with retries and atomic file replacement."""

from __future__ import annotations

import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
CHUNK_SIZE = 1024 * 1024


def download_file(
    url: str,
    destination: str | Path,
    validate: Callable[[Path], object],
    *,
    force: bool = False,
    timeout_seconds: int = 30,
    retries: int = 2,
) -> Path:
    """Download to a temporary file, validate it, then atomically rename it."""
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Source URL: %s", url)
    logger.info("Destination: %s", target)

    if target.exists() and not force:
        try:
            validate(target)
        except Exception as exc:
            raise ValueError(f"Existing file is invalid; use --force to replace {target}: {exc}") from exc
        logger.info("Existing valid file kept: %s", target)
        return target

    for attempt in range(retries + 1):
        temporary_path: Path | None = None
        try:
            request = Request(url, headers={"User-Agent": "nyc-taxi-bigdata/0.1 (course project)"})
            with urlopen(request, timeout=timeout_seconds) as response:
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status} from {url}")
                content_length = response.headers.get("Content-Length")
                if content_length:
                    logger.info("Expected download size: %s bytes", content_length)

                fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".part", dir=target.parent)
                temporary_path = Path(temp_name)
                received = 0
                with os.fdopen(fd, "wb") as output:
                    while chunk := response.read(CHUNK_SIZE):
                        output.write(chunk)
                        received += len(chunk)

            if content_length and received != int(content_length):
                raise ValueError(f"Incomplete download: expected {content_length} bytes, got {received}")
            validate(temporary_path)
            os.replace(temporary_path, target)
            logger.info("Downloaded and validated %s (%d bytes)", target, received)
            return target
        except HTTPError as exc:
            if exc.code not in RETRYABLE_STATUSES or attempt == retries:
                raise RuntimeError(f"HTTP {exc.code} downloading {url}") from exc
            logger.warning("HTTP %s; retrying %s (%d/%d)", exc.code, url, attempt + 1, retries)
        except (URLError, TimeoutError, ConnectionError) as exc:
            if attempt == retries:
                raise RuntimeError(f"Network error downloading {url}: {exc}") from exc
            logger.warning("Network error; retrying %s (%d/%d): %s", url, attempt + 1, retries, exc)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        time.sleep(2 ** attempt)

    raise AssertionError("Download retry loop ended unexpectedly")
