#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

import laspy
import numpy as np
from plyfile import PlyData
from pyproj import CRS, Transformer

from p1_logging import log_info, log_error, metric


def to_u16(arr):
    arr = np.asarray(arr)
    if np.issubdtype(arr.dtype, np.floating):
        arr = np.clip(np.rint(arr), 0, 255).astype(np.uint16)
        return arr * 257
    if arr.dtype.itemsize == 1:
        return arr.astype(np.uint16) * 257
    if arr.dtype.itemsize >= 2:
        return arr.astype(np.uint16)
    return np.clip(arr, 0, 255).astype(np.uint16) * 257


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Exporta nuvem ENU para LAS UTM com preservação de RGB."
    )
    parser.add_argument("--input-ply", required=True, help="Nuvem densa em PLY no referencial ENU.")
    parser.add_argument("--enu-meta-json", required=True, help="Arquivo enu_origin.json com a origem do ENU.")
    parser.add_argument("--output-las", required=True, help="Arquivo LAS de saída.")
    parser.add_argument("--log-file", required=False, help="Arquivo de log textual.")
    parser.add_argument("--metrics-csv", required=False, help="CSV de métricas.")
    parser.add_argument("--dataset", required=True, help="Nome do dataset.")
    parser.add_argument("--gpu", required=True, help="GPU usada no processamento.")
    parser.add_argument("--module", required=True, help="Nome do módulo.")
    args = parser.parse_args()

    log_file = args.log_file
    metrics_csv = args.metrics_csv
    dataset = args.dataset
    gpu = args.gpu
    module = args.module

    ply_path = Path(args.input_ply)
    meta_path = Path(args.enu_meta_json)
    out_file = Path(args.output_las)

    if not ply_path.exists():
        log_error(log_file, dataset, gpu, module, f"PLY não encontrado: {ply_path}")
        metric(metrics_csv, dataset, gpu, module, "input_ply_exists", 0, "bool", "FAILED", str(ply_path))
        return 1

    if not meta_path.exists():
        log_error(log_file, dataset, gpu, module, f"JSON não encontrado: {meta_path}")
        metric(metrics_csv, dataset, gpu, module, "enu_meta_exists", 0, "bool", "FAILED", str(meta_path))
        return 2

    out_file.parent.mkdir(parents=True, exist_ok=True)

    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    lat = float(meta["ref_lat"])
    lon = float(meta["ref_lon"])
    alt = float(meta["ref_alt"])
    epsg = int(meta["epsg"])

    log_info(log_file, dataset, gpu, module, f"Origem ENU: {lat} {lon} {alt}")
    log_info(log_file, dataset, gpu, module, f"EPSG destino: {epsg}")

    metric(metrics_csv, dataset, gpu, module, "ref_lat", lat, "deg")
    metric(metrics_csv, dataset, gpu, module, "ref_lon", lon, "deg")
    metric(metrics_csv, dataset, gpu, module, "ref_alt", alt, "m")
    metric(metrics_csv, dataset, gpu, module, "epsg", epsg, "code")

    crs_geodetic = CRS.from_epsg(4979)
    crs_ecef = CRS.from_epsg(4978)
    crs_utm = CRS.from_epsg(epsg)

    geo_to_ecef = Transformer.from_crs(crs_geodetic, crs_ecef, always_xy=True)
    ecef_to_geo = Transformer.from_crs(crs_ecef, crs_geodetic, always_xy=True)
    geo_to_utm = Transformer.from_crs(crs_geodetic, crs_utm, always_xy=True)

    x0, y0, z0 = geo_to_ecef.transform(lon, lat, alt)

    ply = PlyData.read(ply_path)
    v = ply["vertex"].data
    names = v.dtype.names

    x = np.asarray(v["x"], dtype=np.float64)
    y = np.asarray(v["y"], dtype=np.float64)
    z = np.asarray(v["z"], dtype=np.float64)

    phi = np.radians(lat)
    lam = np.radians(lon)
    sin_phi, cos_phi = np.sin(phi), np.cos(phi)
    sin_lam, cos_lam = np.sin(lam), np.cos(lam)

    # ENU -> ECEF
    R = np.array([
        [-sin_lam,             -sin_phi * cos_lam,   cos_phi * cos_lam],
        [ cos_lam,             -sin_phi * sin_lam,   cos_phi * sin_lam],
        [ 0.0,                  cos_phi,             sin_phi]
    ], dtype=np.float64)

    enu = np.vstack((x, y, z))
    ecef = R @ enu

    X = ecef[0] + x0
    Y = ecef[1] + y0
    Z = ecef[2] + z0

    lon_pts, lat_pts, h_pts = ecef_to_geo.transform(X, Y, Z)
    E, N, H = geo_to_utm.transform(lon_pts, lat_pts, h_pts)

    def get_color(name):
        if name in names:
            return np.asarray(v[name])
        return None

    red = get_color("red")
    green = get_color("green")
    blue = get_color("blue")
    has_rgb = red is not None and green is not None and blue is not None

    log_info(log_file, dataset, gpu, module, f"RGB presente no PLY: {has_rgb}")
    metric(metrics_csv, dataset, gpu, module, "has_rgb", int(has_rgb), "bool")

    header = laspy.LasHeader(point_format=3 if has_rgb else 1, version="1.4")
    header.offsets = [float(E.min()), float(N.min()), float(H.min())]
    header.scales = [0.001, 0.001, 0.001]
    header.add_crs(CRS.from_epsg(epsg))

    las = laspy.LasData(header)
    las.x = E
    las.y = N
    las.z = H

    if has_rgb:
        las.red = to_u16(red)
        las.green = to_u16(green)
        las.blue = to_u16(blue)

    las.write(out_file)

    point_count = len(E)
    e_min, e_max = float(E.min()), float(E.max())
    n_min, n_max = float(N.min()), float(N.max())
    h_min, h_max = float(H.min()), float(H.max())

    log_info(log_file, dataset, gpu, module, f"LAS escrito com sucesso: {out_file}")
    log_info(log_file, dataset, gpu, module, f"Pontos gravados: {point_count}")
    log_info(log_file, dataset, gpu, module, f"Bounds E: {e_min} {e_max}")
    log_info(log_file, dataset, gpu, module, f"Bounds N: {n_min} {n_max}")
    log_info(log_file, dataset, gpu, module, f"Bounds H: {h_min} {h_max}")

    metric(metrics_csv, dataset, gpu, module, "points", point_count, "count")
    metric(metrics_csv, dataset, gpu, module, "E_min", e_min, "m")
    metric(metrics_csv, dataset, gpu, module, "E_max", e_max, "m")
    metric(metrics_csv, dataset, gpu, module, "N_min", n_min, "m")
    metric(metrics_csv, dataset, gpu, module, "N_max", n_max, "m")
    metric(metrics_csv, dataset, gpu, module, "H_min", h_min, "m")
    metric(metrics_csv, dataset, gpu, module, "H_max", h_max, "m")
    metric(metrics_csv, dataset, gpu, module, "output_las_exists", 1, "bool", "SUCCESS", str(out_file))

    return 0


if __name__ == "__main__":
    sys.exit(main())
    