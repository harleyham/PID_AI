#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
import sys
from pathlib import Path

import rasterio

from p1_logging import log_info, log_error, metric


def count_features_gpkg(gpkg_path: Path, layer_name: str) -> int:
    conn = sqlite3.connect(str(gpkg_path))
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{layer_name}"')
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def raster_meta(raster_path: Path) -> dict:
    with rasterio.open(raster_path) as ds:
        return {
            "width": ds.width,
            "height": ds.height,
            "crs": ds.crs.to_string() if ds.crs else "UNKNOWN",
            "res_x": float(ds.res[0]),
            "res_y": float(abs(ds.res[1])),
            "count": ds.count,
        }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Calcula estatísticas do M08 - curvas de nível."
    )
    parser.add_argument("--input-vector", required=True, help="Arquivo vetorial de curvas.")
    parser.add_argument("--input-raster", required=True, help="Raster base das curvas.")
    parser.add_argument("--layer-name", required=True, help="Nome da camada vetorial.")
    parser.add_argument("--field-name", required=True, help="Campo de cota.")
    parser.add_argument("--format", required=True, help="Formato vetorial.")
    parser.add_argument("--log-file", required=False)
    parser.add_argument("--metrics-csv", required=False)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--gpu", required=True)
    parser.add_argument("--module", required=True)
    args = parser.parse_args()

    vector_path = Path(args.input_vector)
    raster_path = Path(args.input_raster)

    if not vector_path.exists():
        log_error(args.log_file, args.dataset, args.gpu, args.module,
                  f"Vetor de curvas não encontrado: {vector_path}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "contour_vector_exists", 0, "bool", "FAILED", str(vector_path))
        return 1

    if not raster_path.exists():
        log_error(args.log_file, args.dataset, args.gpu, args.module,
                  f"Raster base não encontrado: {raster_path}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "contour_raster_exists", 0, "bool", "FAILED", str(raster_path))
        return 2

    info = raster_meta(raster_path)

    log_info(args.log_file, args.dataset, args.gpu, args.module,
             f"Raster base: {raster_path}")
    log_info(args.log_file, args.dataset, args.gpu, args.module,
             f"CRS: {info['crs']}")
    log_info(args.log_file, args.dataset, args.gpu, args.module,
             f"Dimensões raster: {info['width']} x {info['height']}")
    log_info(args.log_file, args.dataset, args.gpu, args.module,
             f"Resolução raster: {info['res_x']} x {info['res_y']} m")

    metric(args.metrics_csv, args.dataset, args.gpu, args.module,
           "contour_raster_crs", info["crs"], "crs")
    metric(args.metrics_csv, args.dataset, args.gpu, args.module,
           "contour_raster_width", info["width"], "px")
    metric(args.metrics_csv, args.dataset, args.gpu, args.module,
           "contour_raster_height", info["height"], "px")
    metric(args.metrics_csv, args.dataset, args.gpu, args.module,
           "contour_raster_res_x", info["res_x"], "m")
    metric(args.metrics_csv, args.dataset, args.gpu, args.module,
           "contour_raster_res_y", info["res_y"], "m")
    metric(args.metrics_csv, args.dataset, args.gpu, args.module,
           "contour_field_name", args.field_name, "field")
    metric(args.metrics_csv, args.dataset, args.gpu, args.module,
           "contour_vector_format", args.format, "format")

    feature_count = -1
    if args.format.upper() == "GPKG":
        try:
            feature_count = count_features_gpkg(vector_path, args.layer_name)
            log_info(args.log_file, args.dataset, args.gpu, args.module,
                     f"Quantidade de curvas: {feature_count}")
            metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                   "contour_feature_count", feature_count, "count")
        except Exception as exc:
            log_error(args.log_file, args.dataset, args.gpu, args.module,
                      f"Falha ao contar feições no GPKG: {exc}")
            metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                   "contour_feature_count", -1, "count", "WARNING", str(exc))
    else:
        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "contour_feature_count", -1, "count", "SKIPPED",
               f"Contagem automática implementada apenas para GPKG; formato={args.format}")

    return 0


if __name__ == "__main__":
    sys.exit(main())