# -*- coding: utf-8 -*-
"""
This module handles downloading FAERS quarterly data files from the FDA website.
"""
import hashlib
import logging
import re
import zipfile
from pathlib import Path
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

from .config import DownloaderSettings

logger = logging.getLogger(__name__)

FDA_FAERS_URL = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
DOWNLOAD_URL_TEMPLATE = "https://fis.fda.gov/content/Exports/faers_ascii_{quarter}.zip"


def _create_retry_session() -> requests.Session:
    """Create a requests session with a retry mechanism."""
    session = requests.Session()
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def find_latest_quarter() -> Optional[str]:
    """
    Find the latest available FAERS quarter by scraping the FDA website.

    :return: The latest quarter as a string (e.g., "2025q3"), or None if not found.
    """
    logger.info("Finding the latest FAERS quarter from the FDA website...")
    try:
        session = _create_retry_session()
        response = session.get(FDA_FAERS_URL, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Find all links that match the faers_ascii_YYYYqN.zip pattern
        links = soup.find_all("a", href=re.compile(r"faers_ascii_\d{4}q\d\.zip"))
        if not links:
            logger.warning("No FAERS ASCII download links found on the page.")
            return None

        quarters = []
        for link in links:
            if isinstance(link, Tag):
                href = link.get("href")
                if isinstance(href, str):
                    match = re.search(r"faers_ascii_(\d{4}q\d)\.zip", href)
                    if match:
                        quarters.append(match.group(1))

        if not quarters:
            logger.warning("Could not parse any quarter strings from the download links.")
            return None

        # Sort quarters to find the latest (e.g., "2025q2" > "2025q1")
        latest_quarter = sorted(quarters, reverse=True)[0]
        logger.info(f"Latest FAERS quarter found: {latest_quarter}")
        return latest_quarter

    except requests.RequestException as e:
        logger.error(f"Error while trying to access the FDA FAERS website: {e}")
        return None


def download_quarter(quarter: str, settings: DownloaderSettings) -> Optional[Tuple[Path, str]]:
    """
    Download a specific FAERS quarter data file.

    :param quarter: The quarter to download (e.g., "2025q1").
    :param settings: The downloader configuration settings.
    :return: A tuple containing the path to the downloaded file and its
             SHA-256 checksum, or None if download fails.
    """
    download_url = DOWNLOAD_URL_TEMPLATE.format(quarter=quarter)
    download_dir = Path(settings.download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    file_path = download_dir / f"faers_ascii_{quarter}.zip"

    logger.info(f"Downloading FAERS data for quarter {quarter} from {download_url}")

    try:
        session = _create_retry_session()
        response = session.get(download_url, stream=True, timeout=settings.timeout)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with open(file_path, "wb") as f, tqdm(
            desc=f"Downloading {quarter}",
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                size = f.write(chunk)
                bar.update(size)

        logger.info(f"Successfully downloaded to {file_path}")

        # R5: Verify the integrity of the downloaded ZIP file
        logger.info(f"Verifying integrity of {file_path}...")
        with zipfile.ZipFile(file_path) as zf:
            if zf.testzip() is not None:
                logger.error(f"Downloaded file {file_path} is corrupted.")
                file_path.unlink()  # Delete corrupted file
                return None
        logger.info(f"File {file_path} integrity verified.")

        # R5: Generate and log SHA-256 checksum
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        checksum = sha256_hash.hexdigest()
        logger.info(f"SHA-256 checksum for {file_path}: {checksum}")

        return file_path, checksum

    except requests.RequestException as e:
        logger.error(f"Failed to download {download_url}. Error: {e}")
        return None
    except zipfile.BadZipFile:
        logger.error(f"File at {file_path} is not a valid zip file or is corrupted.")
        if file_path.exists():
            file_path.unlink()
        return None
