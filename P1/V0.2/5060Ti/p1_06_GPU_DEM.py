#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import laspy
import numpy as np
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

    # produtos analíticos
    parser.add_argument("--dtm-output-type", default="idw")
    parser.add_argument("--dtm-window-size", type=int, default=2)
    parser.add_argument("--dsm-output-type", default="max")
    parser.add_argument("--dsm-window-size", type=int, default=1)

    # produtos fechados
    parser.add_argument("--dtm-closed-output-type", default="idw")
    parser.add_argument("--dtm-closed-window-size", type=int, default=8)
    parser.add_argument("--dsm-closed-output-type", default="idw")
    parser.add_argument("--dsm-closed-window-size", type=int, default=4)

    parser.add_argument("--dtm-fillnodata-max-distance", type=int, default=60)
    parser.add_argument("--dsm-fillnodata-max-distance", type=int, default=30)
    parser.add_argument("--fillnodata-smoothing-iterations", type=int, default=1)
    parser.add_argument("--ortho-surface-mode", default="DSM_THEN_DTM")

    parser.add_argument("--log-file", required=True)
    parser.add_argument("--metrics-csv", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--dataset-slug", required=True)
    parser.add_argument("--gpu", required=True)
    parser.add_argument("--module", required=True)

    parser.add_argument(
        "--low-outlier-enable",
        type=int,
        default=int(os.environ.get("LOW_OUTLIER_ENABLE", "1"))
    )
    parser.add_argument(
        "--low-outlier-percentile",
        type=float,
        default=float(os.environ.get("LOW_OUTLIER_PERCENTILE", "1.0"))
    )
    parser.add_argument(
        "--low-outlier-margin",
        type=float,
        default=float(os.environ.get("LOW_OUTLIER_MARGIN", "5.0"))
    )
    parser.add_argument(
        "--outlier-mean-k",
        type=int,
        default=int(os.environ.get("OUTLIER_MEAN_K", "12"))
    )
    parser.add_argument(
        "--outlier-multiplier",
        type=float,
        default=float(os.environ.get("OUTLIER_MULTIPLIER", "2.5"))
    )

    args = parser.parse_args()

    dense_las = Path(args.dense_las)
    output_dir = Path(args.output_dir)
    enu_meta_json = Path(args.enu_meta_json)

    log_file = args.log_file
    metrics_csv = args.metrics_csv
    dataset = args.dataset
    dataset_slug = args.dataset_slug
    gpu = args.gpu
    module = args.module

    output_dir.mkdir(parents=True, exist_ok=True)

    dtm_tif = output_dir / f"DTM_{dataset_slug}.tif"
    dsm_tif = output_dir / f"DSM_{dataset_slug}.tif"
    dtm_closed_tif = output_dir / f"DTM_closed_{args.dataset_slug}.tif"
    dsm_closed_tif = output_dir / f"DSM_closed_{args.dataset_slug}.tif"
    ortho_surface_tif = output_dir / f"ORTHO_SURFACE_{args.dataset_slug}.tif"
    chm_tif = output_dir / f"CHM_{args.dataset_slug}.tif"
    dtm_hs = output_dir / f"DTM_hillshade_{args.dataset_slug}.tif"
    dsm_hs = output_dir / f"DSM_hillshade_{args.dataset_slug}.tif"
    ground_laz = output_dir / f"dense_ground_{args.dataset_slug}.laz"

    dense_prefiltered_laz = output_dir / "dense_prefiltered.laz"

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
        dtm_closed_raw = tmp_dir / "DTM_closed_raw.tif"
        dsm_closed_raw = tmp_dir / "DSM_closed_raw.tif"


        # ----------------------------------------------------
        # 0) Pré-filtro de outliers baixos
        # ----------------------------------------------------
        source_for_smrf = dense_las
        source_for_dsm = dense_las

        if args.low_outlier_enable == 1:
            log_info(log_file, dataset, gpu, module, "Calculando piso robusto para remoção de outliers baixos")

            las = laspy.read(dense_las)
            z_values = np.asarray(las.z, dtype=np.float64)

            z_percentile = float(np.percentile(z_values, args.low_outlier_percentile))
            z_floor = z_percentile - float(args.low_outlier_margin)

            log_info(
                log_file, dataset, gpu, module,
                f"LOW_OUTLIER_PERCENTILE={args.low_outlier_percentile} -> z_percentile={z_percentile:.3f} m"
            )
            log_info(
                log_file, dataset, gpu, module,
                f"LOW_OUTLIER_MARGIN={args.low_outlier_margin} -> z_floor={z_floor:.3f} m"
            )
            log_info(
                log_file, dataset, gpu, module,
                f"OUTLIER_MEAN_K={args.outlier_mean_k}, OUTLIER_MULTIPLIER={args.outlier_multiplier}"
            )

            metric(metrics_csv, dataset, gpu, module, "low_outlier_percentile", args.low_outlier_percentile, "percent")
            metric(metrics_csv, dataset, gpu, module, "low_outlier_margin", args.low_outlier_margin, "m")
            metric(metrics_csv, dataset, gpu, module, "z_percentile_value", z_percentile, "m")
            metric(metrics_csv, dataset, gpu, module, "z_floor", z_floor, "m")
            metric(metrics_csv, dataset, gpu, module, "outlier_mean_k", args.outlier_mean_k, "count")
            metric(metrics_csv, dataset, gpu, module, "outlier_multiplier", args.outlier_multiplier, "value")

            prefilter_pipeline = {
                "pipeline": [
                    {
                        "type": "readers.las",
                        "filename": str(dense_las),
                    },
                    {
                        "type": "filters.outlier",
                        "method": "statistical",
                        "mean_k": args.outlier_mean_k,
                        "multiplier": args.outlier_multiplier,
                    },
                    {
                        "type": "filters.expression",
                        "expression": f"Z >= {z_floor}"
                    },
                    {
                        "type": "writers.las",
                        "filename": str(dense_prefiltered_laz),
                        "minor_version": 4,
                        "dataformat_id": 3,
                        "compression": "laszip",
                        "forward": "all"
                    }
                ]
            }

            log_info(log_file, dataset, gpu, module, "Executando: PDAL prefilter de outliers baixos")
            proc = subprocess.run(
                ["pdal", "pipeline", "--stdin"],
                input=json.dumps(prefilter_pipeline),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL de pré-filtro dos outliers baixos")
                log_subprocess_tail(log_file, dataset, gpu, module, proc.stdout)
                return 3

            log_info(log_file, dataset, gpu, module, "PDAL prefilter de outliers baixos concluido", echo=False)

            if dense_prefiltered_laz.exists():
                try:
                    pref_summary = pdal_summary(dense_prefiltered_laz)
                    pref_points = pref_summary["summary"]["num_points"]
                    log_info(log_file, dataset, gpu, module, f"Pontos após pré-filtro: {pref_points}")
                    metric(metrics_csv, dataset, gpu, module, "prefiltered_points", pref_points, "count")
                except Exception as exc:
                    log_info(
                        log_file, dataset, gpu, module,
                        f"Não foi possível contar os pontos do LAZ pré-filtrado: {exc}",
                    )
                    metric(
                        metrics_csv, dataset, gpu, module,
                        "prefiltered_points", "N/A", "count", "WARNING", str(exc)
                    )
                metric(metrics_csv, dataset, gpu, module, "dense_prefiltered_exists", 1, "bool")
            else:
                metric(metrics_csv, dataset, gpu, module, "dense_prefiltered_exists", 0, "bool", "FAILED")

            source_for_smrf = dense_prefiltered_laz
            source_for_dsm = dense_prefiltered_laz
        else:
            log_info(log_file, dataset, gpu, module, "Pré-filtro de outliers baixos desabilitado")
            metric(metrics_csv, dataset, gpu, module, "low_outlier_enable", 0, "bool")

        # ----------------------------------------------------
        # 1) Classificação de solo
        # ----------------------------------------------------
        ground_pipeline = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": str(source_for_smrf),
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

        # ----------------------------------------------------
        # 2) DTM analítico
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # 3) DSM analítico
        # ----------------------------------------------------
        dsm_pipeline = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": str(source_for_dsm),
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

        # ----------------------------------------------------
        # 4) DTM fechado (dedicado a continuidade)
        # ----------------------------------------------------
        dtm_closed_pipeline = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": str(ground_laz),
                },
                {
                    "type": "writers.gdal",
                    "filename": str(dtm_closed_raw),
                    "resolution": args.resolution,
                    "bounds": pdal_bounds,
                    "output_type": args.dtm_closed_output_type,
                    "data_type": "float32",
                    "nodata": args.nodata,
                    "window_size": args.dtm_closed_window_size,
                    "gdaldriver": "GTiff",
                    "override_srs": srs,
                }
            ]
        }

        proc = subprocess.run(
            ["pdal", "pipeline", "--stdin"],
            input=json.dumps(dtm_closed_pipeline),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL do DTM fechado")
            log_subprocess_tail(log_file, dataset, gpu, module, proc.stdout)
            return 5
        log_info(log_file, dataset, gpu, module, "Pipeline PDAL do DTM fechado concluido", echo=False)

        # ----------------------------------------------------
        # 5) DSM fechado (dedicado a continuidade)
        # ----------------------------------------------------
        dsm_closed_pipeline = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": str(source_for_dsm),
                },
                {
                    "type": "writers.gdal",
                    "filename": str(dsm_closed_raw),
                    "resolution": args.resolution,
                    "bounds": pdal_bounds,
                    "output_type": args.dsm_closed_output_type,
                    "data_type": "float32",
                    "nodata": args.nodata,
                    "window_size": args.dsm_closed_window_size,
                    "gdaldriver": "GTiff",
                    "override_srs": srs,
                }
            ]
        }

        proc = subprocess.run(
            ["pdal", "pipeline", "--stdin"],
            input=json.dumps(dsm_closed_pipeline),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            log_error(log_file, dataset, gpu, module, "Falha no pipeline PDAL do DSM fechado")
            log_subprocess_tail(log_file, dataset, gpu, module, proc.stdout)
            return 5
        log_info(log_file, dataset, gpu, module, "Pipeline PDAL do DSM fechado concluido", echo=False)

        # ----------------------------------------------------
        # 6) Compactar rasters analíticos
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # 7) Fechar DTM fechado
        # ----------------------------------------------------
        if not run_cmd(
            [
                "gdal_fillnodata.py",
                "-md", str(args.dtm_fillnodata_max_distance),
                "-si", str(args.fillnodata_smoothing_iterations),
                str(dtm_closed_raw),
                str(dtm_closed_tif),
            ],
            log_file, dataset, gpu, module, "gdal_fillnodata DTM_closed"
        ):
            return 6

        # ----------------------------------------------------
        # 8) Fechar DSM fechado
        # ----------------------------------------------------
        if not run_cmd(
            [
                "gdal_fillnodata.py",
                "-md", str(args.dsm_fillnodata_max_distance),
                "-si", str(args.fillnodata_smoothing_iterations),
                str(dsm_closed_raw),
                str(dsm_closed_tif),
            ],
            log_file, dataset, gpu, module, "gdal_fillnodata DSM_closed"
        ):
            return 6
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        # ----------------------------------------------------
        # 9) ORTHO_SURFACE híbrido
        # ----------------------------------------------------
        # if args.ortho_surface_mode == "DSM_THEN_DTM":
        #     calc_expr = f"where(A=={args.nodata}, B, A)"
        # else:
        #     calc_expr = f"where(B=={args.nodata}, A, B)"

        # if not run_cmd(
        #     [
        #         "gdal_calc.py",
        #         "-A", str(dsm_closed_tif),
        #         "-B", str(dtm_closed_tif),
        #         f"--outfile={str(ortho_surface_tif)}",
        #         f"--calc={calc_expr}",
        #         f"--NoDataValue={args.nodata}",
        #         "--type=Float32",
        #         "--overwrite",
        #         "--co=TILED=YES",
        #         "--co=COMPRESS=DEFLATE",
        #         "--co=PREDICTOR=3",
        #         "--co=BIGTIFF=IF_SAFER",
        #         "--quiet",
        #     ],
        #     log_file, dataset, gpu, module, "gdal_calc ORTHO_SURFACE"
        # ):
        #     return 6

        # ----------------------------------------------------
        # 9) ORTHO_SURFACE robusto
        # Regra: ORTHO_SURFACE nunca pode ficar pior que DSM_closed.
        # Base principal = DSM_closed
        # Fallback        = DTM_closed apenas onde DSM_closed estiver inválido
        # ----------------------------------------------------
        log_info(
            log_file, dataset, gpu, module,
            "Montando ORTHO_SURFACE robusto a partir de DSM_closed com fallback em DTM_closed"
        )

        with rasterio.open(dsm_closed_tif) as ds_dsm:
            dsm_arr = ds_dsm.read(1).astype(np.float32)
            dsm_profile = ds_dsm.profile.copy()
            dsm_nodata = ds_dsm.nodata

        with rasterio.open(dtm_closed_tif) as ds_dtm:
            dtm_arr = ds_dtm.read(1).astype(np.float32)
            dtm_nodata = ds_dtm.nodata

        def valid_mask(arr, nodata_value):
            mask = np.isfinite(arr)
            if nodata_value is not None:
                mask &= (arr != nodata_value)
            mask &= (arr != args.nodata)
            return mask

        dsm_valid = valid_mask(dsm_arr, dsm_nodata)
        dtm_valid = valid_mask(dtm_arr, dtm_nodata)

        ortho_arr = np.full(dsm_arr.shape, args.nodata, dtype=np.float32)
        ortho_arr[dsm_valid] = dsm_arr[dsm_valid]

        fill_mask = (~dsm_valid) & dtm_valid
        ortho_arr[fill_mask] = dtm_arr[fill_mask]

        # Garantia explícita:
        # onde DSM_closed for válido, ORTHO_SURFACE deve coincidir com DSM_closed
        ortho_arr[dsm_valid] = dsm_arr[dsm_valid]

        ortho_valid = np.isfinite(ortho_arr) & (ortho_arr != args.nodata)

        # Limpa chaves herdadas potencialmente problemáticas
        dsm_profile.pop("blockxsize", None)
        dsm_profile.pop("blockysize", None)
        dsm_profile.pop("tiled", None)

        # Escrita robusta do raster final com blocos válidos para GTiff
        dsm_profile.update(
            driver="GTiff",
            dtype="float32",
            nodata=args.nodata,
            compress="DEFLATE",
            predictor=3,
            tiled=True,
            blockxsize=256,
            blockysize=256,
            BIGTIFF="IF_SAFER"
        )

        with rasterio.open(ortho_surface_tif, "w", **dsm_profile) as dst:
            dst.write(ortho_arr.astype(np.float32), 1)

        dsm_holes = int((~dsm_valid).sum())
        dtm_holes = int((~dtm_valid).sum())
        ortho_holes = int((~ortho_valid).sum())
        filled_from_dtm = int(fill_mask.sum())

        log_info(log_file, dataset, gpu, module, f"DSM_closed pixels inválidos: {dsm_holes}")
        log_info(log_file, dataset, gpu, module, f"DTM_closed pixels inválidos: {dtm_holes}")
        log_info(log_file, dataset, gpu, module, f"ORTHO_SURFACE pixels inválidos: {ortho_holes}")
        log_info(log_file, dataset, gpu, module, f"Pixels preenchidos com fallback do DTM_closed: {filled_from_dtm}")

        metric(metrics_csv, dataset, gpu, module, "dsm_closed_invalid_pixels", dsm_holes, "count")
        metric(metrics_csv, dataset, gpu, module, "dtm_closed_invalid_pixels", dtm_holes, "count")
        metric(metrics_csv, dataset, gpu, module, "ortho_surface_invalid_pixels", ortho_holes, "count")
        metric(metrics_csv, dataset, gpu, module, "ortho_surface_filled_from_dtm", filled_from_dtm, "count")

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

        # ----------------------------------------------------
        # 10) Hillshade
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # 11) CHM sobre rasters analíticos
        # ----------------------------------------------------
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
        (ortho_surface_tif, "ortho_surface"),
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
    log_info(log_file, dataset, gpu, module, f"ORTHO_SURFACE: {ortho_surface_tif}")
    log_info(log_file, dataset, gpu, module, f"CHM: {chm_tif}")
    log_info(log_file, dataset, gpu, module, f"DTM Hillshade: {dtm_hs}")
    log_info(log_file, dataset, gpu, module, f"DSM Hillshade: {dsm_hs}")

    metric(metrics_csv, dataset, gpu, module, "ground_laz_exists", int(ground_laz.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dtm_exists", int(dtm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dsm_exists", int(dsm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dtm_closed_exists", int(dtm_closed_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dsm_closed_exists", int(dsm_closed_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "ortho_surface_exists", int(ortho_surface_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "chm_exists", int(chm_tif.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dtm_hillshade_exists", int(dtm_hs.exists()), "bool")
    metric(metrics_csv, dataset, gpu, module, "dsm_hillshade_exists", int(dsm_hs.exists()), "bool")

    return 0


if __name__ == "__main__":
    sys.exit(main())
