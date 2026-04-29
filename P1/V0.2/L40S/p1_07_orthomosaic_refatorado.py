#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import math
import subprocess
import sys
from pathlib import Path

import numpy as np
import rasterio
from pyproj import CRS, Transformer
from rasterio.transform import from_origin
from rasterio.windows import Window


def now_str():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_line(log_file: str, dataset: str, gpu: str, module: str, level: str, message: str) -> None:
    line = f"[{now_str()}] [{level}] [{dataset}] [{gpu}] [{module}] {message}"
    print(line, flush=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def metric(metrics_csv: str, dataset: str, gpu: str, module: str,
           metric_name: str, value, unit: str = "", status: str = "SUCCESS", notes: str = "") -> None:
    with open(metrics_csv, "a", encoding="utf-8") as f:
        f.write(f"{now_str()};{dataset};{gpu};{module};{metric_name};{value};{unit};{status};{notes}\n")


def detect_projected_epsg(crs):
    if crs is None:
        return None, None

    crs_str = None
    try:
        crs_str = crs.to_string()
    except Exception:
        crs_str = None

    if crs_str and crs_str.upper().startswith("EPSG:"):
        try:
            return int(crs_str.split(":")[1]), crs_str
        except Exception:
            pass

    try:
        auth = crs.to_authority()
        if auth and len(auth) == 2 and str(auth[0]).upper() == "EPSG":
            return int(auth[1]), crs_str
    except Exception:
        pass

    try:
        epsg = crs.to_epsg()
        if epsg is not None:
            return int(epsg), crs_str
    except Exception:
        pass

    try:
        wkt = crs.to_wkt()
        for code in ("31983", "31982", "31981"):
            if code in wkt:
                return int(code), crs_str
    except Exception:
        pass

    return None, crs_str


def make_utm_to_enu_transformers(meta):
    lat = float(meta["ref_lat"])
    lon = float(meta["ref_lon"])
    alt = float(meta["ref_alt"])
    epsg = int(meta["epsg"])

    crs_geodetic = CRS.from_epsg(4979)
    crs_ecef = CRS.from_epsg(4978)
    crs_utm = CRS.from_epsg(epsg)

    utm_to_geo = Transformer.from_crs(crs_utm, crs_geodetic, always_xy=True)
    geo_to_ecef = Transformer.from_crs(crs_geodetic, crs_ecef, always_xy=True)

    x0, y0, z0 = geo_to_ecef.transform(lon, lat, alt)

    phi = np.radians(lat)
    lam = np.radians(lon)

    sin_phi, cos_phi = np.sin(phi), np.cos(phi)
    sin_lam, cos_lam = np.sin(lam), np.cos(lam)

    R_enu_to_ecef = np.array([
        [-sin_lam,             -sin_phi * cos_lam,   cos_phi * cos_lam],
        [ cos_lam,             -sin_phi * sin_lam,   cos_phi * sin_lam],
        [ 0.0,                  cos_phi,             sin_phi]
    ], dtype=np.float64)

    R_ecef_to_enu = R_enu_to_ecef.T

    return {
        "utm_to_geo": utm_to_geo,
        "geo_to_ecef": geo_to_ecef,
        "ecef_to_enu_R": R_ecef_to_enu,
        "ecef_origin": np.array([x0, y0, z0], dtype=np.float64),
        "epsg": epsg,
    }


def utm_xyz_to_enu(xyz_utm, tfm):
    east = xyz_utm[:, 0]
    north = xyz_utm[:, 1]
    up = xyz_utm[:, 2]

    lon, lat, h = tfm["utm_to_geo"].transform(east, north, up)
    x, y, z = tfm["geo_to_ecef"].transform(lon, lat, h)

    ecef = np.stack([x, y, z], axis=1)
    delta = ecef - tfm["ecef_origin"][None, :]
    enu = (tfm["ecef_to_enu_R"] @ delta.T).T
    return enu


def parse_cameras_txt(path: Path):
    cameras = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            cam_id = int(parts[0])
            model = parts[1]
            width = int(parts[2])
            height = int(parts[3])
            params = np.array([float(x) for x in parts[4:]], dtype=np.float64)
            cameras[cam_id] = {
                "model": model,
                "width": width,
                "height": height,
                "params": params,
            }
    return cameras


def parse_images_txt(path: Path):
    images = []
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith("#"):
            i += 1
            continue

        parts = line.split()
        if len(parts) >= 10:
            image_id = int(parts[0])
            qw, qx, qy, qz = map(float, parts[1:5])
            tx, ty, tz = map(float, parts[5:8])
            camera_id = int(parts[8])
            name = parts[9]
            images.append({
                "image_id": image_id,
                "qvec": np.array([qw, qx, qy, qz], dtype=np.float64),
                "tvec": np.array([tx, ty, tz], dtype=np.float64),
                "camera_id": camera_id,
                "name": name,
            })
            i += 2
        else:
            i += 1

    return images


def qvec_to_rotmat(qw, qx, qy, qz):
    return np.array([
        [1 - 2*qy*qy - 2*qz*qz,     2*qx*qy - 2*qz*qw,     2*qx*qz + 2*qy*qw],
        [2*qx*qy + 2*qz*qw,         1 - 2*qx*qx - 2*qz*qz, 2*qy*qz - 2*qx*qw],
        [2*qx*qz - 2*qy*qw,         2*qy*qz + 2*qx*qw,     1 - 2*qx*qx - 2*qy*qy],
    ], dtype=np.float64)


def build_intrinsics(cam):
    model = cam["model"]
    p = cam["params"]

    if model in ("SIMPLE_PINHOLE", "SIMPLE_RADIAL", "RADIAL"):
        f, cx, cy = p[:3]
        fx = fy = f
    elif model in ("PINHOLE", "OPENCV", "FULL_OPENCV", "OPENCV_FISHEYE"):
        fx, fy, cx, cy = p[:4]
    else:
        raise ValueError(f"Modelo de câmera não suportado no M07 v1: {model}")

    K = np.array([
        [fx, 0.0, cx],
        [0.0, fy, cy],
        [0.0, 0.0, 1.0],
    ], dtype=np.float64)
    return K


def camera_center_world(R, t):
    return -R.T @ t


def camera_optical_axis_world(R):
    return R.T @ np.array([0.0, 0.0, 1.0], dtype=np.float64)


def project_points(K, R, t, xyz_world):
    x_cam = (R @ xyz_world.T + t[:, None]).T
    z = x_cam[:, 2]
    valid = z > 1e-8

    uv = np.full((xyz_world.shape[0], 2), np.nan, dtype=np.float64)
    if np.any(valid):
        xn = x_cam[valid, 0] / x_cam[valid, 2]
        yn = x_cam[valid, 1] / x_cam[valid, 2]
        proj = (K @ np.vstack([xn, yn, np.ones_like(xn)])).T
        uv[valid, 0] = proj[:, 0]
        uv[valid, 1] = proj[:, 1]
    return uv, valid, z


def bilinear_sample_rgb(src, u, v):
    if src.count < 3:
        raise RuntimeError(f"Imagem {src.name} não possui 3 bandas")

    x0 = np.floor(u).astype(np.int32)
    y0 = np.floor(v).astype(np.int32)
    x1 = x0 + 1
    y1 = y0 + 1

    inside = (
        (x0 >= 0) & (y0 >= 0) &
        (x1 < src.width) & (y1 < src.height)
    )

    out = np.zeros((u.shape[0], 3), dtype=np.uint8)
    if not np.any(inside):
        return out, inside

    xx0 = x0[inside]
    yy0 = y0[inside]
    xx1 = x1[inside]
    yy1 = y1[inside]

    col_min = int(xx0.min())
    col_max = int(xx1.max())
    row_min = int(yy0.min())
    row_max = int(yy1.max())

    win = Window(col_off=col_min, row_off=row_min,
                 width=col_max - col_min + 1,
                 height=row_max - row_min + 1)

    data = src.read([1, 2, 3], window=win)

    dx = u[inside] - xx0
    dy = v[inside] - yy0

    def pick(arr, rr, cc):
        rr_local = rr - row_min
        cc_local = cc - col_min
        return arr[:, rr_local, cc_local].astype(np.float64)

    c00 = pick(data, yy0, xx0)
    c10 = pick(data, yy0, xx1)
    c01 = pick(data, yy1, xx0)
    c11 = pick(data, yy1, xx1)

    w00 = (1 - dx) * (1 - dy)
    w10 = dx * (1 - dy)
    w01 = (1 - dx) * dy
    w11 = dx * dy

    rgb = (
        c00 * w00[None, :] +
        c10 * w10[None, :] +
        c01 * w01[None, :] +
        c11 * w11[None, :]
    ).T

    out[inside] = np.clip(rgb, 0, 255).astype(np.uint8)
    return out, inside


def maybe_make_preview(output_tif: Path, output_vrt: Path | None, output_preview: Path | None):
    if output_vrt:
        try:
            subprocess.run(
                ["gdal_translate", "-of", "VRT", str(output_tif), str(output_vrt)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        except Exception:
            pass

    if output_preview:
        try:
            subprocess.run(
                ["gdal_translate", "-b", "1", "-b", "2", "-b", "3", "-of", "JPEG",
                 "-outsize", "20%", "20%", str(output_tif), str(output_preview)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        except Exception:
            pass


def fill_small_holes_rgb(out_flat, alpha_flat, tile_h, tile_w, max_iters=2):
    """
    Preenche pequenos buracos locais usando média dos vizinhos válidos.
    Atua apenas onde alpha == 0 e existir vizinhança válida.
    """
    rgb = out_flat.reshape(tile_h, tile_w, 3).astype(np.float32).copy()
    alpha = alpha_flat.reshape(tile_h, tile_w).copy()

    for _ in range(max_iters):
        holes = alpha == 0
        if not np.any(holes):
            break

        rgb_new = rgb.copy()
        alpha_new = alpha.copy()

        for r in range(tile_h):
            r0 = max(0, r - 1)
            r1 = min(tile_h, r + 2)
            for c in range(tile_w):
                if alpha[r, c] != 0:
                    continue

                c0 = max(0, c - 1)
                c1 = min(tile_w, c + 2)

                neigh_alpha = alpha[r0:r1, c0:c1]
                neigh_rgb = rgb[r0:r1, c0:c1, :]
                valid = neigh_alpha > 0
                if np.any(valid):
                    vals = neigh_rgb[valid]
                    rgb_new[r, c, :] = vals.mean(axis=0)
                    alpha_new[r, c] = 255

        rgb = rgb_new
        alpha = alpha_new

    return (
        np.clip(rgb.reshape(tile_h * tile_w, 3), 0, 255).astype(np.uint8),
        alpha.reshape(tile_h * tile_w).astype(np.uint8),
    )



def rank_candidates_for_tile(image_models, xyz, max_candidates, probe_max_points=500):
    """
    Rankeia candidatos por cobertura geométrica potencial no tile,
    score angular médio e distância ao centro.
    """
    if xyz.shape[0] == 0:
        return []

    sample_step = max(1, xyz.shape[0] // max(1, probe_max_points))
    xyz_probe = xyz[::sample_step]
    center_xyz = xyz[xyz.shape[0] // 2]

    ranked = []
    for idx_img, m in enumerate(image_models):
        dist_center = float(np.linalg.norm(m["C"] - center_xyz))
        uv, ok_depth, _ = project_points(m["K"], m["R"], m["t"], xyz_probe)
        u = uv[:, 0]
        v = uv[:, 1]

        inside_img = (
            ok_depth &
            (u >= 0) & (u < m["width"] - 1) &
            (v >= 0) & (v < m["height"] - 1)
        )

        projected_count = int(np.count_nonzero(inside_img))
        if projected_count > 0:
            rays = xyz_probe - m["C"][None, :]
            ray_norm = np.linalg.norm(rays, axis=1) + 1e-12
            rays_unit = rays / ray_norm[:, None]
            cosang = np.einsum("ij,j->i", rays_unit, m["axis"])
            mean_score = float(np.mean(cosang[inside_img] / ray_norm[inside_img]))
        else:
            mean_score = -1e9

        rank_score = (projected_count * 1000.0) + (mean_score * 100.0) - dist_center
        ranked.append((rank_score, idx_img, projected_count, mean_score, dist_center))

    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[:max_candidates]

def safe_read_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Pipeline P1 - M07 Ortomosaico RGBA")
    parser.add_argument("--surface", required=True, help="DSM_closed.tif ou DTM_closed.tif")
    parser.add_argument("--images-dir", required=True, help="DENSE_PATH/images")
    parser.add_argument("--model-dir", required=True, help="Diretório TXT do modelo COLMAP")
    parser.add_argument("--enu-meta-json", required=True)
    parser.add_argument("--output-tif", required=True)
    parser.add_argument("--output-vrt", default="")
    parser.add_argument("--output-preview", default="")
    parser.add_argument("--resolution", type=float, required=True, help="m/pixel")
    parser.add_argument("--tile-size", type=int, default=1024)
    parser.add_argument("--blend-mode", choices=["best_angle", "first", "mean"], default="best_angle")
    parser.add_argument("--max-candidates", type=int, default=8)
    parser.add_argument("--retry-max-candidates", type=int, default=24)
    parser.add_argument("--retry-fill-threshold", type=float, default=0.985)
    parser.add_argument("--hole-fill-iters", type=int, default=2)
    parser.add_argument("--candidate-probe-max-points", type=int, default=500)
    parser.add_argument("--low-coverage-threshold", type=float, default=0.90)
    parser.add_argument("--compress", default="DEFLATE")
    parser.add_argument("--jpeg-quality", type=int, default=90)

    parser.add_argument("--log-file", required=True)
    parser.add_argument("--metrics-csv", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--gpu", required=True)
    parser.add_argument("--module", required=True)

    args = parser.parse_args()

    log_file = args.log_file
    metrics_csv = args.metrics_csv
    dataset = args.dataset
    gpu = args.gpu
    module = args.module

    surface_path = Path(args.surface)
    images_dir = Path(args.images_dir)
    model_dir = Path(args.model_dir)
    enu_meta_json = Path(args.enu_meta_json)
    output_tif = Path(args.output_tif)
    output_vrt = Path(args.output_vrt) if args.output_vrt else None
    output_preview = Path(args.output_preview) if args.output_preview else None

    cameras_txt = model_dir / "cameras.txt"
    images_txt = model_dir / "images.txt"

    if not surface_path.exists():
        log_line(log_file, dataset, gpu, module, "ERROR", f"Surface não encontrada: {surface_path}")
        return 2
    if not images_dir.exists():
        log_line(log_file, dataset, gpu, module, "ERROR", f"Diretório de imagens não encontrado: {images_dir}")
        return 3
    if not cameras_txt.exists() or not images_txt.exists():
        log_line(log_file, dataset, gpu, module, "ERROR", "Arquivos cameras.txt/images.txt não encontrados")
        return 4
    if not enu_meta_json.exists():
        log_line(log_file, dataset, gpu, module, "ERROR", f"ENU meta não encontrado: {enu_meta_json}")
        return 5

    meta = safe_read_json(enu_meta_json)
    expected_epsg = int(meta.get("epsg", 31983))
    utm_enu_tfm = make_utm_to_enu_transformers(meta)

    cameras = parse_cameras_txt(cameras_txt)
    images = parse_images_txt(images_txt)

    log_line(log_file, dataset, gpu, module, "INFO", f"Câmeras lidas: {len(cameras)}")
    log_line(log_file, dataset, gpu, module, "INFO", f"Imagens lidas: {len(images)}")

    metric(metrics_csv, dataset, gpu, module, "num_cameras", len(cameras), "count")
    metric(metrics_csv, dataset, gpu, module, "num_images_model", len(images), "count")
    metric(metrics_csv, dataset, gpu, module, "expected_epsg", expected_epsg, "code")
    metric(metrics_csv, dataset, gpu, module, "ortho_compress_requested", args.compress, "mode")
    metric(metrics_csv, dataset, gpu, module, "ortho_jpeg_quality", args.jpeg_quality, "value")

    with rasterio.open(surface_path) as surf:
        surface = surf.read(1)
        bounds = surf.bounds
        crs = surf.crs
        nodata = surf.nodata

        if crs is None:
            log_line(log_file, dataset, gpu, module, "ERROR", "Surface sem CRS")
            return 6

        detected_epsg, crs_str = detect_projected_epsg(crs)

        log_line(log_file, dataset, gpu, module, "INFO", f"Surface path: {surface_path}")
        log_line(log_file, dataset, gpu, module, "INFO", f"Surface CRS string: {crs_str if crs_str else 'None'}")
        log_line(log_file, dataset, gpu, module, "INFO", f"Expected EPSG from ENU meta: {expected_epsg}")

        metric(metrics_csv, dataset, gpu, module, "surface_epsg", detected_epsg if detected_epsg is not None else "None", "code")
        metric(metrics_csv, dataset, gpu, module, "surface_crs_string", crs_str if crs_str else "None", "srs")

        if detected_epsg is None:
            log_line(log_file, dataset, gpu, module, "WARN",
                     f"Não foi possível detectar EPSG formalmente; usando expected_epsg={expected_epsg}")
        elif int(detected_epsg) != int(expected_epsg):
            log_line(log_file, dataset, gpu, module, "WARN",
                     f"EPSG detectado ({detected_epsg}) difere do esperado ({expected_epsg}); "
                     f"seguindo com expected_epsg como referência operacional. CRS string: {crs_str}")

        res = float(args.resolution)
        out_width = int(math.ceil((bounds.right - bounds.left) / res))
        out_height = int(math.ceil((bounds.top - bounds.bottom) / res))
        out_transform = from_origin(bounds.left, bounds.top, res, res)

        metric(metrics_csv, dataset, gpu, module, "ortho_width", out_width, "px")
        metric(metrics_csv, dataset, gpu, module, "ortho_height", out_height, "px")
        metric(metrics_csv, dataset, gpu, module, "ortho_resolution", res, "m")

        log_line(log_file, dataset, gpu, module, "INFO",
                 f"Ortomosaico de saída: {out_width} x {out_height} px, resolução {res} m")

        output_crs = CRS.from_epsg(expected_epsg)

        compress = args.compress.upper()
        if compress == "JPEG":
            log_line(
                log_file, dataset, gpu, module, "WARN",
                "Compressao JPEG solicitada para GeoTIFF RGBA; usando DEFLATE para preservar alpha."
            )
            compress = "DEFLATE"

        profile = {
            "driver": "GTiff",
            "height": out_height,
            "width": out_width,
            "count": 4,  # RGBA
            "dtype": "uint8",
            "crs": output_crs,
            "transform": out_transform,
            "tiled": True,
            "compress": compress,
            "blockxsize": 512,
            "blockysize": 512,
            "BIGTIFF": "IF_SAFER",
        }

        metric(metrics_csv, dataset, gpu, module, "ortho_compress_effective", compress, "mode")

        output_tif.parent.mkdir(parents=True, exist_ok=True)

        image_models = []
        for img in images:
            cam = cameras.get(img["camera_id"])
            if cam is None:
                continue

            img_path = images_dir / img["name"]
            if not img_path.exists():
                continue

            R = qvec_to_rotmat(*img["qvec"])
            t = img["tvec"]
            C = camera_center_world(R, t)
            axis = camera_optical_axis_world(R)
            K = build_intrinsics(cam)

            image_models.append({
                "name": img["name"],
                "path": img_path,
                "R": R,
                "t": t,
                "C": C,
                "axis": axis,
                "K": K,
                "width": cam["width"],
                "height": cam["height"],
            })

        metric(metrics_csv, dataset, gpu, module, "num_images_available", len(image_models), "count")
        log_line(log_file, dataset, gpu, module, "INFO", f"Imagens válidas para ortho: {len(image_models)}")

        tile_size = int(args.tile_size)
        blend_mode = args.blend_mode
        max_candidates = int(args.max_candidates)
        num_tile_rows = int(math.ceil(out_height / tile_size))
        num_tile_cols = int(math.ceil(out_width / tile_size))
        total_tiles = num_tile_rows * num_tile_cols
        progress_interval = max(1, total_tiles // 20)

        metric(metrics_csv, dataset, gpu, module, "ortho_total_tiles", total_tiles, "count")
        log_line(log_file, dataset, gpu, module, "INFO",
                 f"Processando {total_tiles} tiles ({num_tile_rows} x {num_tile_cols})")

        low_coverage_tiles = 0
        sum_surface_ratio = 0.0
        sum_projected_ratio = 0.0
        sum_sampled_ratio = 0.0
        sum_final_ratio = 0.0
        max_retry_used = 0

        tile_index = 0
        with rasterio.open(output_tif, "w", **profile) as dst:
            for row0 in range(0, out_height, tile_size):
                for col0 in range(0, out_width, tile_size):
                    tile_index += 1
                    tile_h = min(tile_size, out_height - row0)
                    tile_w = min(tile_size, out_width - col0)

                    rows = np.arange(row0, row0 + tile_h)
                    cols = np.arange(col0, col0 + tile_w)
                    cc, rr = np.meshgrid(cols, rows)

                    xs = bounds.left + (cc.astype(np.float64) + 0.5) * res
                    ys = bounds.top - (rr.astype(np.float64) + 0.5) * res

                    xs_flat = xs.ravel()
                    ys_flat = ys.ravel()

                    surf_rows, surf_cols = rasterio.transform.rowcol(surf.transform, xs_flat, ys_flat)
                    surf_rows = np.asarray(surf_rows, dtype=np.int64)
                    surf_cols = np.asarray(surf_cols, dtype=np.int64)

                    inside_surface = (
                        (surf_rows >= 0) & (surf_rows < surf.height) &
                        (surf_cols >= 0) & (surf_cols < surf.width)
                    )

                    z_flat = np.full(xs_flat.shape, np.nan, dtype=np.float32)
                    z_flat[inside_surface] = surface[surf_rows[inside_surface], surf_cols[inside_surface]]

                    valid_flat = np.isfinite(z_flat)
                    if nodata is not None:
                        valid_flat &= (z_flat != nodata)

                    if not np.any(valid_flat):
                        tile_rgba = np.zeros((tile_h, tile_w, 4), dtype=np.uint8)
                        dst.write(tile_rgba[:, :, 0], 1, window=Window(col0, row0, tile_w, tile_h))
                        dst.write(tile_rgba[:, :, 1], 2, window=Window(col0, row0, tile_w, tile_h))
                        dst.write(tile_rgba[:, :, 2], 3, window=Window(col0, row0, tile_w, tile_h))
                        dst.write(tile_rgba[:, :, 3], 4, window=Window(col0, row0, tile_w, tile_h))
                        continue

                    xyz_utm = np.stack(
                        [xs_flat[valid_flat], ys_flat[valid_flat], z_flat[valid_flat]],
                        axis=1
                    )
                    xyz = utm_xyz_to_enu(xyz_utm, utm_enu_tfm)
                    valid_flat_idx = np.where(valid_flat)[0]

                    surface_valid_flat = np.zeros(tile_h * tile_w, dtype=bool)
                    surface_valid_flat[valid_flat_idx] = True

                    candidate_pool_sizes = [max_candidates]
                    if int(args.retry_max_candidates) > max_candidates:
                        candidate_pool_sizes.append(int(args.retry_max_candidates))

                    attempt_modes = [blend_mode]
                    if blend_mode != "mean":
                        attempt_modes.append("mean")

                    best_tile_rgba = None
                    best_fill_ratio = -1.0
                    best_projected_ratio = 0.0
                    best_sampled_ratio = 0.0
                    best_retry_level = 0
                    best_attempt_mode = blend_mode

                    for retry_level, cand_limit in enumerate(candidate_pool_sizes):
                        ranked = rank_candidates_for_tile(
                            image_models,
                            xyz,
                            cand_limit,
                            probe_max_points=int(args.candidate_probe_max_points),
                        )
                        candidate_indices = [idx for _, idx, _, _, _ in ranked]
                        candidates = [image_models[i] for i in candidate_indices]

                        for attempt_mode in attempt_modes:
                            if attempt_mode == "mean":
                                accum = np.zeros((tile_h * tile_w, 3), dtype=np.float64)
                                count = np.zeros(tile_h * tile_w, dtype=np.int32)
                                filled_flat = np.zeros(tile_h * tile_w, dtype=bool)
                            else:
                                best_score = np.full(tile_h * tile_w, -np.inf, dtype=np.float64)
                                best_rgb = np.zeros((tile_h * tile_w, 3), dtype=np.uint8)
                                filled_flat = np.zeros(tile_h * tile_w, dtype=bool)

                            projected_valid_flat = np.zeros(tile_h * tile_w, dtype=bool)
                            sampled_valid_flat = np.zeros(tile_h * tile_w, dtype=bool)

                            for cand in candidates:
                                R = cand["R"]
                                t = cand["t"]
                                C = cand["C"]
                                axis = cand["axis"]
                                K = cand["K"]

                                uv, ok_depth, zc = project_points(K, R, t, xyz)
                                u = uv[:, 0]
                                v = uv[:, 1]

                                inside_img = (
                                    ok_depth &
                                    (u >= 0) & (u < cand["width"] - 1) &
                                    (v >= 0) & (v < cand["height"] - 1)
                                )

                                if not np.any(inside_img):
                                    continue

                                projected_valid_flat[valid_flat_idx[inside_img]] = True

                                rays = xyz - C[None, :]
                                ray_norm = np.linalg.norm(rays, axis=1) + 1e-12
                                rays_unit = rays / ray_norm[:, None]

                                cosang = np.einsum("ij,j->i", rays_unit, axis)
                                score = np.full(xyz.shape[0], -np.inf, dtype=np.float64)
                                score[inside_img] = cosang[inside_img] / ray_norm[inside_img]

                                with rasterio.open(cand["path"]) as src_img:
                                    rgb, sampled = bilinear_sample_rgb(src_img, u, v)

                                use = inside_img & sampled
                                if not np.any(use):
                                    continue

                                flat_use_idx = valid_flat_idx[use]
                                sampled_valid_flat[flat_use_idx] = True

                                if attempt_mode == "first":
                                    new_mask = ~filled_flat[flat_use_idx]
                                    if np.any(new_mask):
                                        dst_idx = flat_use_idx[new_mask]
                                        src_rgb = rgb[use][new_mask]
                                        best_rgb[dst_idx] = src_rgb
                                        filled_flat[dst_idx] = True

                                elif attempt_mode == "mean":
                                    accum[flat_use_idx] += rgb[use].astype(np.float64)
                                    count[flat_use_idx] += 1
                                    filled_flat[flat_use_idx] = True

                                else:  # best_angle
                                    better = score[use] > best_score[flat_use_idx]
                                    if np.any(better):
                                        dst_idx = flat_use_idx[better]
                                        src_rgb = rgb[use][better]
                                        best_rgb[dst_idx] = src_rgb
                                        best_score[dst_idx] = score[use][better]
                                        filled_flat[dst_idx] = True

                            if attempt_mode == "mean":
                                out_flat = np.zeros((tile_h * tile_w, 3), dtype=np.uint8)
                                nz = count > 0
                                out_flat[nz] = np.clip(accum[nz] / count[nz, None], 0, 255).astype(np.uint8)
                            else:
                                out_flat = best_rgb

                            alpha_flat = np.where(filled_flat, 255, 0).astype(np.uint8)

                            out_flat, alpha_flat = fill_small_holes_rgb(
                                out_flat,
                                alpha_flat,
                                tile_h,
                                tile_w,
                                max_iters=int(args.hole_fill_iters),
                            )

                            surface_ratio = float(surface_valid_flat.sum()) / float(tile_h * tile_w)
                            projected_ratio = float(projected_valid_flat.sum()) / float(tile_h * tile_w)
                            sampled_ratio = float(sampled_valid_flat.sum()) / float(tile_h * tile_w)
                            fill_ratio = float((alpha_flat > 0).sum()) / float(tile_h * tile_w)

                            tile_rgba = np.zeros((tile_h, tile_w, 4), dtype=np.uint8)
                            tile_rgba[:, :, 0:3] = out_flat.reshape(tile_h, tile_w, 3)
                            tile_rgba[:, :, 3] = alpha_flat.reshape(tile_h, tile_w)

                            if fill_ratio > best_fill_ratio:
                                best_fill_ratio = fill_ratio
                                best_projected_ratio = projected_ratio
                                best_sampled_ratio = sampled_ratio
                                best_tile_rgba = tile_rgba
                                best_retry_level = retry_level
                                best_attempt_mode = attempt_mode

                            if fill_ratio >= float(args.retry_fill_threshold):
                                break

                        if best_fill_ratio >= float(args.retry_fill_threshold):
                            break

                    if best_tile_rgba is None:
                        best_tile_rgba = np.zeros((tile_h, tile_w, 4), dtype=np.uint8)

                    current_surface_ratio = float(surface_valid_flat.sum()) / float(tile_h * tile_w)
                    sum_surface_ratio += current_surface_ratio
                    sum_projected_ratio += best_projected_ratio
                    sum_sampled_ratio += best_sampled_ratio
                    sum_final_ratio += best_fill_ratio
                    max_retry_used = max(max_retry_used, best_retry_level)

                    if best_fill_ratio < float(args.low_coverage_threshold):
                        low_coverage_tiles += 1
                        log_line(
                            log_file, dataset, gpu, module, "WARN",
                            f"Tile {tile_index} com baixa cobertura final: {best_fill_ratio:.1%} "
                            f"(surface={current_surface_ratio:.1%}, projected={best_projected_ratio:.1%}, "
                            f"sampled={best_sampled_ratio:.1%}, modo={best_attempt_mode}, retry={best_retry_level})"
                        )

                    dst.write(best_tile_rgba[:, :, 0], 1, window=Window(col0, row0, tile_w, tile_h))
                    dst.write(best_tile_rgba[:, :, 1], 2, window=Window(col0, row0, tile_w, tile_h))
                    dst.write(best_tile_rgba[:, :, 2], 3, window=Window(col0, row0, tile_w, tile_h))
                    dst.write(best_tile_rgba[:, :, 3], 4, window=Window(col0, row0, tile_w, tile_h))
                    if tile_index == 1 or tile_index == total_tiles or (tile_index % progress_interval) == 0:
                        log_line(
                            log_file, dataset, gpu, module, "INFO",
                            f"Progresso tiles: {tile_index}/{total_tiles} "
                            f"({100.0 * tile_index / total_tiles:.1f}%) "
                            f"- alpha preenchido neste tile: {best_fill_ratio:.1%} "
                            f"- projected: {best_projected_ratio:.1%} "
                            f"- sampled: {best_sampled_ratio:.1%} "
                            f"- modo: {best_attempt_mode} "
                            f"- retry: {best_retry_level}"
                        )

    if total_tiles > 0:
        metric(metrics_csv, dataset, gpu, module, "ortho_avg_surface_ratio", sum_surface_ratio / total_tiles, "ratio")
        metric(metrics_csv, dataset, gpu, module, "ortho_avg_projected_ratio", sum_projected_ratio / total_tiles, "ratio")
        metric(metrics_csv, dataset, gpu, module, "ortho_avg_sampled_ratio", sum_sampled_ratio / total_tiles, "ratio")
        metric(metrics_csv, dataset, gpu, module, "ortho_avg_final_fill_ratio", sum_final_ratio / total_tiles, "ratio")
    metric(metrics_csv, dataset, gpu, module, "ortho_low_coverage_tiles", low_coverage_tiles, "count")
    metric(metrics_csv, dataset, gpu, module, "ortho_max_retry_used", max_retry_used, "count")

    maybe_make_preview(output_tif, output_vrt, output_preview)

    metric(metrics_csv, dataset, gpu, module, "ortho_exists", int(output_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "ortho_size_bytes",
           output_tif.stat().st_size if output_tif.exists() else 0, "bytes")
    metric(metrics_csv, dataset, gpu, module, "ortho_rgba", 1, "bool")

    if output_vrt and output_vrt.exists():
        metric(metrics_csv, dataset, gpu, module, "ortho_vrt_exists", 1, "bool")
    if output_preview and output_preview.exists():
        metric(metrics_csv, dataset, gpu, module, "ortho_preview_exists", 1, "bool")

    log_line(log_file, dataset, gpu, module, "INFO", f"ORTHO gerado em: {output_tif}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
    
