"""Stage 1: download the raw CSV (idempotent)."""
import logging
import urllib.request

from .config import DATASET_URL, RAW_PATH

log = logging.getLogger(__name__)


def download(force: bool = False) -> int:
    """Download the dataset if missing. Returns file size in bytes."""
    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    if RAW_PATH.exists() and not force:
        log.info("Raw file already present, skipping download")
    else:
        log.info("Downloading %s", DATASET_URL)
        urllib.request.urlretrieve(DATASET_URL, RAW_PATH)
    return RAW_PATH.stat().st_size
