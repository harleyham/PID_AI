#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import laspy
import rasterio

from p1_logging import log_info, log_warn, log_error, metric


def run_cmd(cmd, log_file, dataset, gpu, module, desc):
    log_info(log_file, dataset, gpu, module, f"Executando: {desc}")
    log_info(log_file, dataset, gpu, module, " ".join(cmd))

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )

    if proc.stdout:
        for line in proc.stdout.splitlines():
            log_info(log_file, dataset, gpu, module, line, echo=False)

    if proc.returncode != 0:
        log_error(log_file, dataset, gpu, module, f"Falha em: {desc} (exit={proc.returncode})")
        raise RuntimeError(f"Comando falhou: {' '.join(cmd)}")

    return proc.stdout


def detect_epsg(dense_las: Path, enu_meta_json: Path):
    # Fonte primária: CRS embutido no LAS
    try:
        with laspy.open(dense_las) as reader:
            crs = reader.header.parse_crs()
            if crs is not None:
                epsg = crs.to_epsg()
                if epsg is not None:
                    return int(epsg), "LAS"
    except Exception:
        pass

    # Fallback: enu_origin.json
    try:
        if enu_meta_json.exists():
            meta = json.loads(enu_meta_json.read_text(encoding="utf-8"))
            epsg = meta.get("epsg")
            if epsg is not None:
                return int(epsg), "ENU_META_JSON"
    except Exception:
        pass

    raise RuntimeError("Não foi possível determinar o EPSG a partir do LAS nem do enu_origin.json")


def las_bounds(dense_las: Path):
    with laspy.open(dense_las) as reader:
        mins = reader.header.mins
        maxs = reader.header.maxs
        return (
            float(mins[0]), float(maxs[0]),
            float(mins[1]), float(maxs[1]),
            float(mins[2]), float(maxs[2]),
        )


def raster_stats(path: Path):
    with rasterio.open(path) as ds:
        arr = ds.read(1, masked=True)
        nodata = ds.nodata
        if arr.count() == 0:
            return {
                "width": ds.width,
                "height": ds.height,
                "nodata": nodata,
                "min": None,
                "max": None,
                "crs": ds.crs.to_string() if ds.crs else None,
                "res_x": ds.res[0],
                "res_y": ds.res[1],
            }

        return {
            "width": ds.width,
            "height": ds.height,
            "nodata": nodata,
            "min": float(arr.min()),
            "max": float(arr.max()),
            "crs": ds.crs.to_string() if ds.crs else None,
            "res_x": ds.res[0],
            "res_y": ds.res[1],
        }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gera DSM, DTM, hillshade e CHM a partir de dense_utm_color.las"
    )
    parser.add_argument("--dense-las", required=True, help="Arquivo LAS de entrada")
    parser.add_argument("--output-dir", required=True, help="Diretório de saída")
    parser.add_argument("--enu-meta-json", required=True, help="Arquivo enu_origin.json")
    parser.add_argument("--log-file", required=False, help="Arquivo de log textual")
    parser.add_argument("--metrics-csv", required=False, help="CSV de métricas")
    parser.add_argument("--dataset", required=True, help="Nome do dataset")
    parser.add_argument("--gpu", required=True, help="GPU usada no processamento")
    parser.add_argument("--module", required=True, help="Nome do módulo")

    parser.add_argument("--resolution", type=float, default=0.50, help="Resolução do raster em metros")
    parser.add_argument("--nodata", type=float, default=-9999.0, help="Valor NoData")
    parser.add_argument("--smrf-scalar", type=float, default=1.25)
    parser.add_argument("--smrf-slope", type=float, default=0.15)
    parser.add_argument("--smrf-threshold", type=float, default=0.50)
    parser.add_argument("--smrf-window", type=float, default=16.0)

    args = parser.parse_args()

    log_file = args.log_file
    metrics_csv = args.metrics_csv
    dataset = args.dataset
    gpu = args.gpu
    module = args.module

    dense_las = Path(args.dense_las)
    output_dir = Path(args.output_dir)
    enu_meta_json = Path(args.enu_meta_json)

    if not dense_las.exists():
        log_error(log_file, dataset, gpu, module, f"LAS não encontrado: {dense_las}")
        metric(metrics_csv, dataset, gpu, module, "dense_las_exists", 0, "bool", "FAILED", str(dense_las))
        return 1

    if not enu_meta_json.exists():
        log_error(log_file, dataset, gpu, module, f"enu_origin.json não encontrado: {enu_meta_json}")
        metric(metrics_csv, dataset, gpu, module, "enu_meta_exists", 0, "bool", "FAILED", str(enu_meta_json))
        return 2

    output_dir.mkdir(parents=True, exist_ok=True)

    ground_laz = output_dir / "dense_ground.laz"
    dtm_tif = output_dir / "DTM.tif"
    dsm_tif = output_dir / "DSM.tif"
    chm_tif = output_dir / "CHM.tif"
    dtm_hs = output_dir / "DTM_hillshade.tif"
    dsm_hs = output_dir / "DSM_hillshade.tif"

    epsg_code, epsg_source = detect_epsg(dense_las, enu_meta_json)
    srs = f"EPSG:{epsg_code}"

    minx, maxx, miny, maxy, minz, maxz = las_bounds(dense_las)
    pdal_bounds = f"([{minx},{maxx}],[{miny},{maxy}])"

    log_info(log_file, dataset, gpu, module, f"SRS detectado: {srs} (origem: {epsg_source})")
    log_info(log_file, dataset, gpu, module, f"Bounds LAS X: {minx} -> {maxx}")
    log_info(log_file, dataset, gpu, module, f"Bounds LAS Y: {miny} -> {maxy}")
    log_info(log_file, dataset, gpu, module, f"Bounds LAS Z: {minz} -> {maxz}")

    metric(metrics_csv, dataset, gpu, module, "epsg", epsg_code, "code")
    metric(metrics_csv, dataset, gpu, module, "epsg_source", epsg_source, "source")
    metric(metrics_csv, dataset, gpu, module, "resolution", args.resolution, "m")
    metric(metrics_csv, dataset, gpu, module, "nodata", args.nodata, "value")
    metric(metrics_csv, dataset, gpu, module, "las_minx", minx, "m")
    metric(metrics_csv, dataset, gpu, module, "las_maxx", maxx, "m")
    metric(metrics_csv, dataset, gpu, module, "las_miny", miny, "m")
    metric(metrics_csv, dataset, gpu, module, "las_maxy", maxy, "m")
    metric(metrics_csv, dataset, gpu, module, "las_minz", minz, "m")
    metric(metrics_csv, dataset, gpu, module, "las_maxz", maxz, "m")

    with tempfile.TemporaryDirectory(prefix="m06_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        dtm_raw = tmp_dir / "DTM_raw.tif"
        dsm_raw = tmp_dir / "DSM_raw.tif"

        # 1) Classificação de solo + exportação dense_ground.laz
        ground_pipeline = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": str(dense_las),
                },
                {
                    "type": "filters.smrf",
                    "scalar": args.smrf_scalar,
                    "slope": args.smrf_slope,
                    "threshold": args.smrf_threshold,
                    "window": args.smrf_window,
                },
                {
                    "type": "filters.expression",
                    "expression": "Classification == 2"
                },
                {
                    "type": "writers.las",
                    "filename": str(ground_laz),
                    "minor_version": 4,
                    "dataformat_id": 3,
                    "compression": "laszip",
                    "forward": "all"
                }
            ]
        }
        log_info(log_file, dataset, gpu, module, "Executando: PDAL SMRF + exportação ground LAZ")
        log_info(log_file, dataset, gpu, module, "pdal pipeline --stdin")
        proc = subprocess.run(
            ["pdal", "pipeline", "--stdin"],
            input=json.dumps(ground_pipeline),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proc.stdout:
            for line in proc.stdout.splitlines():
                log_info(log_file, dataset, gpu, module, line, echo=False)
        if proc.returncode != 0:
            log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL de classificação de solo")
            return 3

        # 2) DTM
        dtm_pipeline = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": str(ground_laz),
                },
                {
                    "type": "writers.gdal",
                    "filename": str(dtm_raw),
                    "resolution": args.resolution,
                    "bounds": pdal_bounds,
                    "output_type": "idw",
                    "data_type": "float32",
                    "nodata": args.nodata,
                    "window_size": 4,
                    "gdaldriver": "GTiff",
                    "override_srs": srs,
                }
            ]
        }

        proc = subprocess.run(
            ["pdal", "pipeline", "--stdin"],
            input=json.dumps(dtm_pipeline),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proc.stdout:
            for line in proc.stdout.splitlines():
                log_info(log_file, dataset, gpu, module, line, echo=False)
        if proc.returncode != 0:
            log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL do DTM")
            return 4

        # 3) DSM
        dsm_pipeline = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": str(dense_las),
                },
                {
                    "type": "writers.gdal",
                    "filename": str(dsm_raw),
                    "resolution": args.resolution,
                    "bounds": pdal_bounds,
                    "output_type": "max",
                    "data_type": "float32",
                    "nodata": args.nodata,
                    "window_size": 0,
                    "gdaldriver": "GTiff",
                    "override_srs": srs,
                }
            ]
        }

        proc = subprocess.run(
            ["pdal", "pipeline", "--stdin"],
            input=json.dumps(dsm_pipeline),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proc.stdout:
            for line in proc.stdout.splitlines():
                log_info(log_file, dataset, gpu, module, line, echo=False)
        if proc.returncode != 0:
            log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL do DSM")
            return 5

        # 4) Compressão/tiling
        run_cmd(
            [
                "gdal_translate", str(dtm_raw), str(dtm_tif),
                "-co", "TILED=YES",
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=3",
                "-co", "BIGTIFF=IF_SAFER",
            ],
            log_file, dataset, gpu, module, "gdal_translate DTM"
        )

        run_cmd(
            [
                "gdal_translate", str(dsm_raw), str(dsm_tif),
                "-co", "TILED=YES",
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=3",
                "-co", "BIGTIFF=IF_SAFER",
            ],
            log_file, dataset, gpu, module, "gdal_translate DSM"
        )

        # 5) Hillshade
        run_cmd(
            [
                "gdaldem", "hillshade", str(dtm_tif), str(dtm_hs),
                "-multidirectional", "-compute_edges", "-of", "GTiff"
            ],
            log_file, dataset, gpu, module, "gdaldem hillshade DTM"
        )

        run_cmd(
            [
                "gdaldem", "hillshade", str(dsm_tif), str(dsm_hs),
                "-multidirectional", "-compute_edges", "-of", "GTiff"
            ],
            log_file, dataset, gpu, module, "gdaldem hillshade DSM"
        )

        # 6) CHM
        run_cmd(
            [
                "gdal_calc.py",
                "-A", str(dsm_tif),
                "-B", str(dtm_tif),
                f"--outfile={str(chm_tif)}",
                f"--calc=where((A=={args.nodata})|(B=={args.nodata}), {args.nodata}, maximum(A-B,0))",
                f"--NoDataValue={args.nodata}",
                "--type=Float32",
                "--overwrite",
                "--co=TILED=YES",
                "--co=COMPRESS=DEFLATE",
                "--co=PREDICTOR=3",
                "--co=BIGTIFF=IF_SAFER",
                "--quiet",
            ],
            log_file, dataset, gpu, module, "gdal_calc CHM"
        )

    # Estatísticas dos rasters
    for raster_path, prefix in [
        (dtm_tif, "dtm"),
        (dsm_tif, "dsm"),
        (chm_tif, "chm"),
    ]:
        if not raster_path.exists():
            log_error(log_file, dataset, gpu, module, f"Raster não encontrado: {raster_path}")
            metric(metrics_csv, dataset, gpu, module, f"{prefix}_exists", 0, "bool", "FAILED", str(raster_path))
            return 6

        stats = raster_stats(raster_path)

        log_info(log_file, dataset, gpu, module, f"{prefix.upper()} raster: {raster_path}")
        log_info(log_file, dataset, gpu, module, f"{prefix.upper()} CRS: {stats['crs']}")
        log_info(log_file, dataset, gpu, module, f"{prefix.upper()} Size: {stats['width']} x {stats['height']}")
        log_info(log_file, dataset, gpu, module, f"{prefix.upper()} Resolution: {stats['res_x']} x {stats['res_y']}")
        log_info(log_file, dataset, gpu, module, f"{prefix.upper()} Min/Max: {stats['min']} / {stats['max']}")

        metric(metrics_csv, dataset, gpu, module, f"{prefix}_width", stats["width"], "pixels")
        metric(metrics_csv, dataset, gpu, module, f"{prefix}_height", stats["height"], "pixels")
        metric(metrics_csv, dataset, gpu, module, f"{prefix}_res_x", stats["res_x"], "m")
        metric(metrics_csv, dataset, gpu, module, f"{prefix}_res_y", stats["res_y"], "m")
        metric(metrics_csv, dataset, gpu, module, f"{prefix}_crs", stats["crs"], "srs")
        if stats["min"] is not None:
            metric(metrics_csv, dataset, gpu, module, f"{prefix}_min", stats["min"], "m")
        if stats["max"] is not None:
            metric(metrics_csv, dataset, gpu, module, f"{prefix}_max", stats["max"], "m")

    log_info(log_file, dataset, gpu, module, f"Ground LAZ: {ground_laz}")
    log_info(log_file, dataset, gpu, module, f"DTM: {dtm_tif}")
    log_info(log_file, dataset, gpu, module, f"DSM: {dsm_tif}")
    log_info(log_file, dataset, gpu, module, f"CHM: {chm_tif}")
    log_info(log_file, dataset, gpu, module, f"DTM Hillshade: {dtm_hs}")
    log_info(log_file, dataset, gpu, module, f"DSM Hillshade: {dsm_hs}")

    metric(metrics_csv, dataset, gpu, module, "ground_laz_exists", int(ground_laz.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dtm_exists", int(dtm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dsm_exists", int(dsm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "chm_exists", int(chm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dtm_hillshade_exists", int(dtm_hs.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dsm_hillshade_exists", int(dsm_hs.exists()), "bool")

    return 0


if __name__ == "__main__":
    sys.exit(main())
    