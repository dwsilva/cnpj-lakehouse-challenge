"""Download dos ZIPs da RF (Casados Dados) por vintage e partição."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
import httpx
from loguru import logger

from scripts.config import (
    PREFIX_ALIASES,
    RF_BASE_URL,
    RF_VINTAGE,
    PARTITIONED_PREFIXES,
    SHARED_PREFIXES,
    raw_vintage_dir,
)

PARTITIONED_FILE = re.compile(
    r"^(Empresas|Estabelecimentos|Socios)(\d+)\.zip$",
    re.IGNORECASE,
)
SHARED_FILE = re.compile(r"^(Simples\.zip|Cnaes\.zip|CNAE\.csv)$", re.IGNORECASE)


def list_index_urls(vintage: str) -> list[str]:
    index_url = f"{RF_BASE_URL.rstrip('/')}/arquivos/{vintage}/"
    response = httpx.get(index_url, timeout=60.0, follow_redirects=True)
    response.raise_for_status()
    hrefs = re.findall(r'href="([^"]+)"', response.text, flags=re.IGNORECASE)
    files = []
    for href in hrefs:
        name = href.split("/")[-1]
        if PARTITIONED_FILE.match(name) or SHARED_FILE.match(name):
            files.append(name)
    return sorted(set(files))


def detect_partitions(vintage: str) -> list[int]:
    parts: set[int] = set()
    for name in list_index_urls(vintage):
        match = PARTITIONED_FILE.match(name)
        if match:
            parts.add(int(match.group(2)))
    return sorted(parts)


def _zip_name(prefix: str, partition_id: int | None = None) -> str:
    if partition_id is None:
        aliases = PREFIX_ALIASES.get(prefix, [prefix])
        return f"{aliases[0]}.zip" if prefix != "CNAE" else "Cnaes.zip"
    return f"{prefix}{partition_id}.zip"


def _remote_file_size(url: str) -> int | None:
    try:
        response = httpx.head(url, timeout=60.0, follow_redirects=True)
        if response.status_code == 200:
            content_length = response.headers.get("content-length")
            if content_length:
                return int(content_length)
    except httpx.HTTPError as exc:
        logger.debug("HEAD falhou para {}: {}", url, exc)
    return None


def download_file(vintage: str, filename: str, dest_dir: Path | None = None) -> Path:
    dest_dir = dest_dir or raw_vintage_dir(vintage)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename
    url = f"{RF_BASE_URL.rstrip('/')}/arquivos/{vintage}/{filename}"

    remote_size = _remote_file_size(url)
    if dest.exists():
        local_size = dest.stat().st_size
        if remote_size is not None:
            if local_size == remote_size:
                size_mb = local_size / (1024 * 1024)
                logger.info("{} já completo ({:.1f} MB), pulando download", dest.name, size_mb)
                return dest
            if local_size > remote_size:
                logger.warning(
                    "{} local ({:.1f} MB) maior que remoto ({:.1f} MB); baixando de novo.",
                    dest.name,
                    local_size / (1024 * 1024),
                    remote_size / (1024 * 1024),
                )
                dest.unlink()

    headers: dict[str, str] = {}
    mode = "wb"
    start_at = 0
    if dest.exists():
        start_at = dest.stat().st_size
        if start_at > 0:
            headers["Range"] = f"bytes={start_at}-"
            mode = "ab"

    with httpx.stream("GET", url, timeout=900.0, follow_redirects=True, headers=headers) as resp:
        if resp.status_code == 416:
            if dest.exists() and dest.stat().st_size > 0:
                size_mb = dest.stat().st_size / (1024 * 1024)
                logger.info("{} já completo ({:.1f} MB), pulando download", dest.name, size_mb)
                return dest
            resp.raise_for_status()

        resp.raise_for_status()

        if resp.status_code == 206:
            with dest.open(mode) as fh:
                for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                    fh.write(chunk)
        else:
            with dest.open("wb") as fh:
                for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                    fh.write(chunk)

    size_mb = dest.stat().st_size / (1024 * 1024)
    logger.info("Baixado {} ({:.1f} MB)", dest.name, size_mb)
    return dest


def download_partition(vintage: str, partition_id: int, include_shared: bool = True) -> list[Path]:
    downloaded: list[Path] = []
    for prefix in PARTITIONED_PREFIXES:
        filename = _zip_name(prefix, partition_id)
        downloaded.append(download_file(vintage, filename))

    if include_shared and partition_id == 0:
        for prefix in SHARED_PREFIXES:
            for name in list_index_urls(vintage):
                if prefix.lower() in name.lower():
                    downloaded.append(download_file(vintage, name))
                    break

    return downloaded


def download_all_partitions(vintage: str) -> list[Path]:
    partitions = detect_partitions(vintage)
    if not partitions:
        raise FileNotFoundError(f"Nenhuma partição listada em {vintage}")

    all_files: list[Path] = []
    for pid in partitions:
        all_files.extend(download_partition(vintage, pid, include_shared=(pid == 0)))
    return all_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Download CNPJ RF (Casados Dados)")
    parser.add_argument("--vintage", default=RF_VINTAGE)
    parser.add_argument("--partition", type=int, default=None)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all:
        download_all_partitions(args.vintage)
    elif args.partition is not None:
        download_partition(args.vintage, args.partition)
    else:
        download_partition(args.vintage, 0)


if __name__ == "__main__":
    main()
