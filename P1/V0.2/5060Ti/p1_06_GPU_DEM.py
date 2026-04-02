#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import rasterio


def now_str():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_info(log_file, dataset, gpu, module, msg, echo=True):
    line = f"[{now_str()}] [INFO] [{dataset}] [{gpu}] [{module}] {msg}"
    if echo:
        print(line, flush=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_error(log_file, dataset, gpu, module, msg):
    line = f"[{now_str()}] [ERROR] [{dataset}] [{gpu}] [{module}] {msg}"
    print(line, flush=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def metric(metrics_csv, dataset, gpu, module, metric_name, value, unit="", status="SUCCESS", notes=""):
    with open(metrics_csv, "a", encoding="utf-8") as f:
        f.write(f"{now_str()};{dataset};{gpu};{module};{metric_name};{value};{unit};{status};{notes}\n")


def log_subprocess_tail(log_file, dataset, gpu, module, stdout_text, max_lines=20):
    if not stdout_text:
        return
    lines = [line for line in stdout_text.splitlines() if line.strip()]
    if not lines:
        return
    tail = lines[-max_lines:]
    log_info(
        log_file, dataset, gpu, module,
        f"Resumo do subprocesso ({len(lines)} linhas; mostrando ultimas {len(tail)}):",
        echo=False,
    )
    for line in tail:
        log_info(log_file, dataset, gpu, module, line, echo=False)


def run_cmd(cmd, log_file, dataset, gpu, module, label):
    log_info(log_file, dataset, gpu, module, f"Executando: {label}")
    log_info(log_file, dataset, gpu, module, " ".join(str(x) for x in cmd))
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False
    )
    if proc.returncode != 0:
        log_error(log_file, dataset, gpu, module, f"Falha em: {label} (exit_code={proc.returncode})")
        log_subprocess_tail(log_file, dataset, gpu, module, proc.stdout)
        return False
    log_info(log_file, dataset, gpu, module, f"{label} concluido com sucesso", echo=False)
    return True


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def pdal_summary(path):
    proc = subprocess.run(
        ["pdal", "info", str(path), "--summary"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stdout)
    return json.loads(proc.stdout)


def raster_stats(path):
    with rasterio.open(path) as ds:
        band = ds.read(1, masked=True)
        if band.count() == 0:
            min_val = None
            max_val = None
        else:
            min_val = float(band.min())
            max_val = float(band.max())

        crs_str = ds.crs.to_string() if ds.crs else None
        res_x, res_y = ds.res
        return {
            "crs": crs_str,
            "width": ds.width,
            "height": ds.height,
            "res_x": float(res_x),
            "res_y": float(abs(res_y)),
            "min": min_val,
            "max": max_val,
        }


def main():
    parser = argparse.ArgumentParser(description="Pipeline P1 - M06 DEM/DSM/CHM")
    parser.add_argument("--dense-las", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--enu-meta-json", required=True)
    parser.add_argument("--resolution", type=float, default=0.05)
    parser.add_argument("--nodata", type=float, default=-9999.0)
    parser.add_argument("--smrf-scalar", type=float, default=1.25)
    parser.add_argument("--smrf-slope", type=float, default=0.15)
    parser.add_argument("--smrf-threshold", type=float, default=0.50)
    parser.add_argument("--smrf-window", type=float, default=16.0)
    parser.add_argument("--dtm-output-type", default="idw")
    parser.add_argument("--dtm-window-size", type=int, default=1)
    parser.add_argument("--dsm-output-type", default="max")
    parser.add_argument("--dsm-window-size", type=int, default=1)
    parser.add_argument("--fillnodata-max-distance", type=int, default=20)
    parser.add_argument("--fillnodata-smoothing-iterations", type=int, default=1)

    parser.add_argument("--log-file", required=True)
    parser.add_argument("--metrics-csv", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--gpu", required=True)
    parser.add_argument("--module", required=True)

    args = parser.parse_args()

    dense_las = Path(args.dense_las)
    output_dir = Path(args.output_dir)
    enu_meta_json = Path(args.enu_meta_json)

    log_file = args.log_file
    metrics_csv = args.metrics_csv
    dataset = args.dataset
    gpu = args.gpu
    module = args.module

    output_dir.mkdir(parents=True, exist_ok=True)

    dtm_tif = output_dir / "DTM.tif"
    dsm_tif = output_dir / "DSM.tif"
    dtm_closed_tif = output_dir / "DTM_closed.tif"
    dsm_closed_tif = output_dir / "DSM_closed.tif"
    chm_tif = output_dir / "CHM.tif"
    dtm_hs = output_dir / "DTM_hillshade.tif"
    dsm_hs = output_dir / "DSM_hillshade.tif"
    ground_laz = output_dir / "dense_ground.laz"

    if not dense_las.exists():
        log_error(log_file, dataset, gpu, module, f"LAS não encontrada: {dense_las}")
        return 2
    if not enu_meta_json.exists():
        log_error(log_file, dataset, gpu, module, f"ENU meta não encontrado: {enu_meta_json}")
        return 2

    meta = read_json(enu_meta_json)
    epsg = int(meta["epsg"])
    srs = f"EPSG:{epsg}"

    summary = pdal_summary(dense_las)
    bounds = summary["summary"]["bounds"]
    minx = bounds["minx"]
    maxx = bounds["maxx"]
    miny = bounds["miny"]
    maxy = bounds["maxy"]
    minz = bounds["minz"]
    maxz = bounds["maxz"]

    pdal_bounds = f"([{minx},{maxx}],[{miny},{maxy}])"

    log_info(log_file, dataset, gpu, module, f"SRS detectado: {srs} (origem: ENU meta)")
    log_info(log_file, dataset, gpu, module, f"Bounds LAS X: {minx} -> {maxx}")
    log_info(log_file, dataset, gpu, module, f"Bounds LAS Y: {miny} -> {maxy}")
    log_info(log_file, dataset, gpu, module, f"Bounds LAS Z: {minz} -> {maxz}")

    metric(metrics_csv, dataset, gpu, module, "epsg", epsg, "code")
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
        proc = subprocess.run(
            ["pdal", "pipeline", "--stdin"],
            input=json.dumps(ground_pipeline),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL de classificação de solo")
            log_subprocess_tail(log_file, dataset, gpu, module, proc.stdout)
            return 3
        log_info(log_file, dataset, gpu, module, "PDAL SMRF + exportação ground LAZ concluido", echo=False)

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
                    "output_type": args.dtm_output_type,
                    "data_type": "float32",
                    "nodata": args.nodata,
                    "window_size": args.dtm_window_size,
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
        if proc.returncode != 0:
            log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL do DTM")
            log_subprocess_tail(log_file, dataset, gpu, module, proc.stdout)
            return 4
        log_info(log_file, dataset, gpu, module, "Pipeline PDAL do DTM concluido", echo=False)

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
                    "output_type": args.dsm_output_type,
                    "data_type": "float32",
                    "nodata": args.nodata,
                    "window_size": args.dsm_window_size,
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
        if proc.returncode != 0:
            log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL do DSM")
            log_subprocess_tail(log_file, dataset, gpu, module, proc.stdout)
            return 5
        log_info(log_file, dataset, gpu, module, "Pipeline PDAL do DSM concluido", echo=False)

        # 4) Compressão/tiling
        if not run_cmd(
            [
                "gdal_translate", str(dtm_raw), str(dtm_tif),
                "-co", "TILED=YES",
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=3",
                "-co", "BIGTIFF=IF_SAFER",
            ],
            log_file, dataset, gpu, module, "gdal_translate DTM"
        ):
            return 6

        if not run_cmd(
            [
                "gdal_translate", str(dsm_raw), str(dsm_tif),
                "-co", "TILED=YES",
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=3",
                "-co", "BIGTIFF=IF_SAFER",
            ],
            log_file, dataset, gpu, module, "gdal_translate DSM"
        ):
            return 6

        # 4b) Fechamento controlado de buracos para o M07
        if not run_cmd(
            [
                "gdal_fillnodata.py",
                "-md", str(args.fillnodata_max_distance),
                "-si", str(args.fillnodata_smoothing_iterations),
                str(dtm_tif),
                str(dtm_closed_tif),
            ],
            log_file, dataset, gpu, module, "gdal_fillnodata DTM_closed"
        ):
            return 6

        if not run_cmd(
            [
                "gdal_fillnodata.py",
                "-md", str(args.fillnodata_max_distance),
                "-si", str(args.fillnodata_smoothing_iterations),
                str(dsm_tif),
                str(dsm_closed_tif),
            ],
            log_file, dataset, gpu, module, "gdal_fillnodata DSM_closed"
        ):
            return 6

        # 5) Hillshade
        if not run_cmd(
            [
                "gdaldem", "hillshade", str(dtm_tif), str(dtm_hs),
                "-multidirectional", "-compute_edges", "-of", "GTiff"
            ],
            log_file, dataset, gpu, module, "gdaldem hillshade DTM"
        ):
            return 6

        if not run_cmd(
            [
                "gdaldem", "hillshade", str(dsm_tif), str(dsm_hs),
                "-multidirectional", "-compute_edges", "-of", "GTiff"
            ],
            log_file, dataset, gpu, module, "gdaldem hillshade DSM"
        ):
            return 6

        # 6) CHM (mantido sobre os rasters originais)
        if not run_cmd(
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
        ):
            return 6

    # Estatísticas dos rasters
    for raster_path, prefix in [
        (dtm_tif, "dtm"),
        (dsm_tif, "dsm"),
        (dtm_closed_tif, "dtm_closed"),
        (dsm_closed_tif, "dsm_closed"),
        (chm_tif, "chm"),
    ]:
        if not raster_path.exists():
            log_error(log_file, dataset, gpu, module, f"Raster não encontrado: {raster_path}")
            metric(metrics_csv, dataset, gpu, module, f"{prefix}_exists", 0, "bool", "FAILED", str(raster_path))
            return 7

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
    log_info(log_file, dataset, gpu, module, f"DTM Closed: {dtm_closed_tif}")
    log_info(log_file, dataset, gpu, module, f"DSM Closed: {dsm_closed_tif}")
    log_info(log_file, dataset, gpu, module, f"CHM: {chm_tif}")
    log_info(log_file, dataset, gpu, module, f"DTM Hillshade: {dtm_hs}")
    log_info(log_file, dataset, gpu, module, f"DSM Hillshade: {dsm_hs}")

    metric(metrics_csv, dataset, gpu, module, "ground_laz_exists", int(ground_laz.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dtm_exists", int(dtm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dsm_exists", int(dsm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dtm_closed_exists", int(dtm_closed_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dsm_closed_exists", int(dsm_closed_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "chm_exists", int(chm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dtm_hillshade_exists", int(dtm_hs.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dsm_hillshade_exists", int(dsm_hs.exists()), "bool")

    return 0


if __name__ == "__main__":
    sys.exit(main())
    
