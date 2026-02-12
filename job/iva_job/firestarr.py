
# job/iva_job/firestarr.py
# Strict parity mosaics: max mosaic, nearest resampling, exact 100m grid.
# Also supports "latest folder" discovery to mimic justTheIntersection.R.

import os
import tempfile
from datetime import date
from typing import List, Tuple, Optional
import logging
from pathlib import Path
from urllib.parse import urlparse, urlencode, parse_qsl
import xml.etree.ElementTree as ET

import requests
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.warp import transform_bounds, reproject, Resampling
from rasterio.transform import from_origin

log = logging.getLogger('iva.firestarr')
logging.basicConfig(level=logging.INFO)


def _ymd(d: date) -> str:
    return d.strftime('%Y%m%d')


def _split_base_and_sas(container_url: str) -> Tuple[str, str]:
    """Split FIRESTARR_BLOB_URL into base container URL and SAS token (if embedded)."""
    # FIRESTARR_BLOB_URL may include a sas query, but usually it doesn't.
    p = urlparse(container_url)
    base = f"{p.scheme}://{p.netloc}{p.path}".rstrip('/')
    sas = p.query
    return base, sas


def _sas_join(base: str, sas: str, extra: dict) -> str:
    """Attach SAS query parameters plus extra query params."""
    base_q = dict(parse_qsl(sas, keep_blank_values=True)) if sas else {}
    base_q.update(extra)
    return base + ('?' + urlencode(base_q) if base_q else '')


def _list_container(container_base: str, sas_query: str, prefix: str = '', delimiter: str = '/', max_results: int = 5000) -> Tuple[List[str], List[str]]:
    """List Azure Blob container using REST API.

    Returns (prefixes, blobs).
    """
    # Azure list blobs: restype=container&comp=list&prefix=...&delimiter=/
    url = _sas_join(
        container_base,
        sas_query,
        {
            'restype': 'container',
            'comp': 'list',
            'prefix': prefix,
            'delimiter': delimiter,
            'maxresults': str(max_results),
        },
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    root = ET.fromstring(r.text)

    ns = ''
    # Extract CommonPrefixes
    prefixes = []
    for cp in root.findall('.//CommonPrefixes'):
        name_el = cp.find('Name')
        if name_el is not None and name_el.text:
            prefixes.append(name_el.text)

    blobs = []
    for b in root.findall('.//Blobs/Blob'):
        name_el = b.find('Name')
        if name_el is not None and name_el.text:
            blobs.append(name_el.text)

    return prefixes, blobs


def discover_latest_tiles(run_date: date, horizon: int) -> List[str]:
    """Mimic the R logic:

    - list 'firestarr/' immediate prefixes
    - pick the latest prefix
    - within it, find a sub-prefix containing the target date string (run_date + horizon - 1)
    - list tif blobs under that prefix

    Returns: list of blob names (paths) within the container.
    """
    container_url = os.getenv('FIRESTARR_BLOB_URL', '').rstrip('/')
    if not container_url:
        raise ValueError('FIRESTARR_BLOB_URL is not set')

    # SAS can be provided separately or embedded
    sas_env = os.getenv('AZURE_SAS_TOKEN', '')
    base, sas_in_url = _split_base_and_sas(container_url)
    sas_query = sas_in_url or sas_env.lstrip('?')

    root_prefix = os.getenv('FIRESTARR_ROOT_PREFIX', 'firestarr').strip('/').strip()
    root_prefix = root_prefix + '/'

    # 1) list root prefixes
    prefixes, _ = _list_container(base, sas_query, prefix=root_prefix, delimiter='/')
    if not prefixes:
        raise RuntimeError(f'No prefixes found under {root_prefix}')

    # R uses tail(list_blobs(...), n=1). We approximate with lexicographic max.
    latest = sorted(prefixes)[-1]
    log.info(f'Discovered latest run prefix: {latest}')

    # 2) list immediate prefixes under latest
    sub_prefixes, _ = _list_container(base, sas_query, prefix=latest, delimiter='/')

    target_date = _ymd(run_date + (horizon - 1) * (run_date - run_date))  # placeholder to keep typing happy
    # compute target date string as in R: runDate + (day-1)
    from datetime import timedelta
    target_date = (run_date + timedelta(days=horizon - 1)).strftime('%Y%m%d')

    # Find a subprefix containing target date
    candidates = [p for p in sub_prefixes if target_date in p]
    if not candidates:
        # fallback: search blobs names if prefixes missing
        _, blobs = _list_container(base, sas_query, prefix=latest, delimiter='')
        candidates = sorted(set([b.rsplit('/', 1)[0] + '/' for b in blobs if target_date in b]))

    if not candidates:
        raise RuntimeError(f'No sub-prefix found under {latest} containing {target_date}')

    day_prefix = sorted(candidates)[-1]
    log.info(f'Discovered day prefix: {day_prefix}')

    # 3) list tif blobs under day_prefix
    _, blobs = _list_container(base, sas_query, prefix=day_prefix, delimiter='')
    tifs = [b for b in blobs if b.lower().endswith('.tif')]
    if not tifs:
        raise RuntimeError(f'No .tif blobs found under {day_prefix}')

    return tifs


def build_blob_urls(run_date: date, horizon: int, prefix: str, grid: List[Tuple[int, int]]) -> List[str]:
    """Build or discover blob URLs.

    If FIRESTARR_DISCOVER_LATEST=1, uses Azure listing to mimic R workflow.
    Otherwise uses TILE_TEMPLATE + grid.
    """
    container_url = os.getenv('FIRESTARR_BLOB_URL', '').rstrip('/')
    if not container_url:
        raise ValueError('FIRESTARR_BLOB_URL is not set')

    base, sas_in_url = _split_base_and_sas(container_url)
    sas_env = os.getenv('AZURE_SAS_TOKEN', '')
    sas_query = sas_in_url or sas_env.lstrip('?')

    discover = os.getenv('FIRESTARR_DISCOVER_LATEST', '0').strip() == '1'
    if discover:
        blob_names = discover_latest_tiles(run_date, horizon)
        return [
            _sas_join(f"{base}/{name}", sas_query, {})
            for name in blob_names
        ]

    tmpl = os.getenv('TILE_TEMPLATE', '{prefix}/{ymd}/D{horizon}/tile_{ix}_{iy}.tif')
    urls: List[str] = []
    for ix, iy in grid:
        path = tmpl.format(prefix=prefix, ymd=_ymd(run_date), horizon=horizon, ix=ix, iy=iy)
        urls.append(_sas_join(f"{base}/{path}", sas_query, {}))
    return urls


def _safe_name(url: str) -> str:
    p = urlparse(url)
    return Path(p.path).name


def download_tiles(urls: List[str]) -> List[Path]:
    tmpdir = Path(tempfile.mkdtemp(prefix='firestarr_'))
    local_paths: List[Path] = []

    for u in urls:
        fn = tmpdir / _safe_name(u)
        log.info(f'Downloading {u} -> {fn}')
        with requests.get(u, stream=True, timeout=180) as r:
            r.raise_for_status()
            with open(fn, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
        local_paths.append(fn)

    return local_paths


def _snap_bounds_to_res(bounds, res_m: float):
    xmin, ymin, xmax, ymax = bounds
    xmin_s = np.floor(xmin / res_m) * res_m
    ymin_s = np.floor(ymin / res_m) * res_m
    xmax_s = np.ceil(xmax / res_m) * res_m
    ymax_s = np.ceil(ymax / res_m) * res_m
    return float(xmin_s), float(ymin_s), float(xmax_s), float(ymax_s)


def mosaic_to_grid(tile_paths: List[Path], out_path: Path, dst_epsg: int = 3978, res_m: float = 100.0, mosaic_method: str = 'max') -> Path:
    if not tile_paths:
        raise ValueError('No tiles provided for mosaic')

    src_files = [rasterio.open(str(p)) for p in tile_paths]

    mosaic, mosaic_transform = merge(src_files, method=mosaic_method)
    src_meta = src_files[0].meta.copy()
    src_crs = src_meta['crs']

    for ds in src_files:
        ds.close()

    dst_crs = f'EPSG:{dst_epsg}'

    h, w = mosaic.shape[1], mosaic.shape[2]
    left, bottom, right, top = rasterio.transform.array_bounds(h, w, mosaic_transform)
    dst_bounds = transform_bounds(src_crs, dst_crs, left, bottom, right, top, densify_pts=21)
    xmin, ymin, xmax, ymax = _snap_bounds_to_res(dst_bounds, res_m)

    dst_transform = from_origin(xmin, ymax, res_m, res_m)
    dst_width = int(np.ceil((xmax - xmin) / res_m))
    dst_height = int(np.ceil((ymax - ymin) / res_m))

    dst_meta = src_meta.copy()
    dst_meta.update({
        'driver': 'GTiff',
        'height': dst_height,
        'width': dst_width,
        'transform': dst_transform,
        'crs': dst_crs,
        'compress': 'deflate',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
    })

    log.info(f'Writing mosaic to {out_path} (CRS={dst_crs}, res={res_m}m, size={dst_width}x{dst_height}, method={mosaic_method}, resampling=nearest)')

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_path, 'w', **dst_meta) as dst:
        for i in range(mosaic.shape[0]):
            reproject(
                source=mosaic[i],
                destination=rasterio.band(dst, i + 1),
                src_transform=mosaic_transform,
                src_crs=src_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=Resampling.nearest,
            )

    return out_path
