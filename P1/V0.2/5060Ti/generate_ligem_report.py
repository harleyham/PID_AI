#!/usr/bin/env python3
import csv
import json
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import rasterio


SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "p1_config.sh"
COLMAP_BIN = os.environ.get("COLMAP_BIN", "colmap")


def load_config() -> dict[str, str]:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {CONFIG_FILE}")

    export_keys = [
        "PROJECT_ROOT",
        "PIPELINE_NAME",
        "DATASET",
        "DATASET_SLUG",
        "GPU",
        "WORKSPACE",
        "DATABASE",
        "ENU_PATH",
        "INPUT_PLY",
        "ENU_META_JSON",
        "OUTPUT_PATH",
        "LOG_DIR",
        "METRICS_CSV",
        "PIPELINE_LOG",
        "DENSE_LAS",
        "ORTHO_TIF",
    ]

    shell_snippet = " && ".join(
        [f'printf "%s=%s\\n" "{key}" "${key}"' for key in export_keys]
    )

    result = subprocess.run(
        ["bash", "-lc", f'source "{CONFIG_FILE}" && {shell_snippet}'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Falha ao carregar p1_config.sh")

    config: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        config[key] = value

    return config


def read_metrics_csv(metrics_csv: str) -> list[dict]:
    if not os.path.exists(metrics_csv):
        return []

    rows = []
    with open(metrics_csv, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if len(row) < 8:
                continue
            rows.append({
                "timestamp": row[0],
                "dataset": row[1],
                "gpu": row[2],
                "module": row[3],
                "metric": row[4],
                "value": row[5],
                "unit": row[6],
                "status": row[7],
                "notes": row[8] if len(row) > 8 else "",
            })
    return rows


def latest_metric(rows: list[dict], module: str, metric_name: str) -> str | None:
    matches = [r for r in rows if r["module"] == module and r["metric"] == metric_name]
    if not matches:
        return None
    return matches[-1]["value"]


def latest_metric_any(rows: list[dict], module: str, metric_names: list[str]) -> str | None:
    for metric_name in metric_names:
        val = latest_metric(rows, module, metric_name)
        if val is not None:
            return val
    return None


def get_images_count(database: str) -> int:
    if not os.path.exists(database):
        return 0

    try:
        conn = sqlite3.connect(database)
        try:
            return int(conn.execute("SELECT count(*) FROM images").fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return 0


def get_dense_stats(dense_ply: str) -> str:
    if not os.path.exists(dense_ply):
        return "Arquivo não encontrado"

    try:
        result = subprocess.run(
            ["pdal", "info", "--summary", dense_ply],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        stats = json.loads(result.stdout)
        num_points = stats["summary"]["num_points"]
        return f"{num_points:,}"
    except Exception:
        return "Erro PDAL"


def get_colmap_stats(sparse_dir: str) -> tuple[str, str, str]:
    if not os.path.exists(sparse_dir):
        return "N/A", "N/A", "N/A"

    try:
        result = subprocess.run(
            [COLMAP_BIN, "model_analyzer", "--path", sparse_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        out = result.stdout

        registered_images = "N/A"
        points = "0"
        error = "0.0"

        reg_match = re.search(r"Registered images:\s+(\d+)", out)
        p_match = re.search(r"Points:\s+(\d+)", out)
        e_match = re.search(r"Mean reprojection error:\s+([\d.]+)px", out)

        if reg_match:
            registered_images = reg_match.group(1)
        if p_match:
            points = p_match.group(1)
        if e_match:
            error = e_match.group(1)

        return registered_images, points, error
    except Exception as exc:
        err = f"Erro: {exc}"
        return err, err, err


def get_alignment_info(enu_meta_json: str) -> tuple[str, str]:
    if not os.path.exists(enu_meta_json):
        return "ENU", "N/A"

    try:
        meta = json.loads(Path(enu_meta_json).read_text(encoding="utf-8"))
        epsg = str(meta.get("epsg", "N/A"))
        return "ENU", epsg
    except Exception:
        return "ENU", "N/A"


def get_las_stats(las_path: str) -> dict:
    if not os.path.exists(las_path):
        return {"exists": False}

    try:
        result = subprocess.run(
            ["pdal", "info", "--summary", las_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        stats = json.loads(result.stdout)
        summary = stats["summary"]
        bounds = summary["bounds"]
        return {
            "exists": True,
            "num_points": summary.get("num_points", "N/A"),
            "minx": bounds.get("minx", "N/A"),
            "maxx": bounds.get("maxx", "N/A"),
            "miny": bounds.get("miny", "N/A"),
            "maxy": bounds.get("maxy", "N/A"),
            "minz": bounds.get("minz", "N/A"),
            "maxz": bounds.get("maxz", "N/A"),
        }
    except Exception as exc:
        return {"exists": True, "error": str(exc)}


def get_raster_stats(path: str) -> dict:
    if not os.path.exists(path):
        return {"exists": False}

    try:
        with rasterio.open(path) as ds:
            band = ds.read(1, masked=True)
            if band.count() == 0:
                min_val = None
                max_val = None
            else:
                min_val = float(band.min())
                max_val = float(band.max())

            crs_str = ds.crs.to_string() if ds.crs else "N/A"
            res_x, res_y = ds.res
            return {
                "exists": True,
                "width": ds.width,
                "height": ds.height,
                "res_x": float(res_x),
                "res_y": float(abs(res_y)),
                "crs": crs_str,
                "min": min_val,
                "max": max_val,
                "count": ds.count,
            }
    except Exception as exc:
        return {"exists": True, "error": str(exc)}


def fmt_file_state(stats: dict) -> str:
    if not stats.get("exists", False):
        return "Arquivo não encontrado"
    if "error" in stats:
        return f"Erro: {stats['error']}"
    return "OK"


def format_seconds(value: str | None) -> str:
    if value is None or value == "N/A":
        return "N/A"
    try:
        total = int(float(value))
        hh = total // 3600
        mm = (total % 3600) // 60
        ss = total % 60
        return f"{total} s ({hh:02d}:{mm:02d}:{ss:02d})"
    except Exception:
        return str(value)


def generate() -> int:
    config = load_config()

    database = config["DATABASE"]
    sparse_dir = config["ENU_PATH"]
    dense_ply = config["INPUT_PLY"]
    enu_meta_json = config["ENU_META_JSON"]
    metrics_csv = config["METRICS_CSV"]
    output_path = config["OUTPUT_PATH"]
    dense_las = config["DENSE_LAS"]
    ortho_tif = config.get("ORTHO_TIF", f"{output_path}/ORTHO.tif")

    metrics_rows = read_metrics_csv(metrics_csv)

    dense_pts = get_dense_stats(dense_ply)
    registered_images, sparse_pts, reproj_err = get_colmap_stats(sparse_dir)
    total_images = get_images_count(database)
    alignment_ref, epsg_code = get_alignment_info(enu_meta_json)

    las_stats = get_las_stats(dense_las)

    dtm_stats = get_raster_stats(str(Path(output_path) / "DTM.tif"))
    dsm_stats = get_raster_stats(str(Path(output_path) / "DSM.tif"))
    dtm_closed_stats = get_raster_stats(str(Path(output_path) / "DTM_closed.tif"))
    dsm_closed_stats = get_raster_stats(str(Path(output_path) / "DSM_closed.tif"))
    ortho_surface_stats = get_raster_stats(str(Path(output_path) / "ORTHO_SURFACE.tif"))
    chm_stats = get_raster_stats(str(Path(output_path) / "CHM.tif"))
    ortho_stats = get_raster_stats(ortho_tif)

    total_runtime = latest_metric(metrics_rows, "PIPELINE", "total_runtime")
    total_runtime_fail = latest_metric(metrics_rows, "PIPELINE", "total_runtime_until_failure")

    step_00a = latest_metric(metrics_rows, "PIPELINE", "step_00_check_env_runtime")
    step_00b = latest_metric(metrics_rows, "PIPELINE", "step_00_env_snapshot_runtime")
    step_01 = latest_metric(metrics_rows, "PIPELINE", "step_01_feature_extraction_runtime")
    step_02 = latest_metric(metrics_rows, "PIPELINE", "step_02_exhaustive_matching_runtime")
    step_03 = latest_metric(metrics_rows, "PIPELINE", "step_03_sparse_mapper_runtime")
    step_04 = latest_metric(metrics_rows, "PIPELINE", "step_04_dense_reconstruction_runtime")
    step_05 = latest_metric(metrics_rows, "PIPELINE", "step_05_export_dense_runtime")
    step_06 = latest_metric(metrics_rows, "PIPELINE", "step_06_dem_runtime")
    step_07 = latest_metric(metrics_rows, "PIPELINE", "step_07_orthomosaic_runtime")
    step_08 = latest_metric(metrics_rows, "PIPELINE", "step_08_report_runtime")

    m01_time = latest_metric_any(metrics_rows, "M01", ["duration", "module_duration"])
    m02_time = latest_metric_any(metrics_rows, "M02", ["duration", "module_duration"])
    m03_time = latest_metric_any(metrics_rows, "M03", ["duration", "module_duration"])
    m04_time = latest_metric_any(metrics_rows, "M04", ["duration", "module_duration"])
    m05_time = latest_metric_any(metrics_rows, "M05", ["duration", "module_duration"])
    m06_time = latest_metric_any(metrics_rows, "M06", ["duration", "module_duration"])
    m07_time = latest_metric_any(metrics_rows, "M07", ["duration", "module_duration"])

    report = f"""
=====================================================
        LIGEM QUALITY REPORT - PIPELINE P1
=====================================================
DATA: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
PIPELINE: {config["PIPELINE_NAME"]}
DATASET: {config["DATASET"]}
GPU: {config["GPU"]}

[1. ESTATISTICAS DE RECONSTRUCAO]
a) Pontos da Nuvem Densa (PLY): {dense_pts} pontos
b) Pontos da Nuvem Esparsa: {sparse_pts} pontos
c) Erro de Reprojecao: {reproj_err} px

[2. GEORREFERENCIAMENTO & QUALIDADE P1]
d) Referencia de Alinhamento: {alignment_ref}
e) EPSG Detectado: {epsg_code}

[3. INFORMACOES DO PROJETO]
Imagens no Banco COLMAP: {total_images}
Imagens Registradas no Modelo: {registered_images}
Workspace: {config["WORKSPACE"]}
Dense PLY: {dense_ply}
Dense LAS: {dense_las}
Modelo ENU: {sparse_dir}

[4. TEMPOS DO PIPELINE]
Tempo total do pipeline (até o fim do M07): {format_seconds(total_runtime)}
Tempo até falha (se aplicável): {format_seconds(total_runtime_fail)}

[4.1] Tempos por passo do run_pipeline.sh
PASSO 0A - check_env: {format_seconds(step_00a)}
PASSO 0B - env_snapshot: {format_seconds(step_00b)}
PASSO 1  - feature_extraction: {format_seconds(step_01)}
PASSO 2  - exhaustive_matching: {format_seconds(step_02)}
PASSO 3  - sparse_mapper: {format_seconds(step_03)}
PASSO 4  - dense_reconstruction: {format_seconds(step_04)}
PASSO 5  - export_dense: {format_seconds(step_05)}
PASSO 6  - dem: {format_seconds(step_06)}
PASSO 7  - orthomosaic: {format_seconds(step_07)}
PASSO 8  - report: {format_seconds(step_08)}

[4.2] Tempos por módulo (METRICS_CSV)
M01: {format_seconds(m01_time)}
M02: {format_seconds(m02_time)}
M03: {format_seconds(m03_time)}
M04: {format_seconds(m04_time)}
M05: {format_seconds(m05_time)}
M06: {format_seconds(m06_time)}
M07: {format_seconds(m07_time)}

[5. ESTATISTICAS DO LAS EXPORTADO]
LAS existe: {"SIM" if las_stats.get("exists") else "NAO"}
Pontos LAS: {las_stats.get("num_points", "N/A")}
Bounds E: {las_stats.get("minx", "N/A")} -> {las_stats.get("maxx", "N/A")}
Bounds N: {las_stats.get("miny", "N/A")} -> {las_stats.get("maxy", "N/A")}
Bounds H: {las_stats.get("minz", "N/A")} -> {las_stats.get("maxz", "N/A")}

[6. PRODUTOS RASTER - ANALITICOS]
DTM: {fmt_file_state(dtm_stats)}
  CRS: {dtm_stats.get("crs", "N/A")}
  Size: {dtm_stats.get("width", "N/A")} x {dtm_stats.get("height", "N/A")}
  Res: {dtm_stats.get("res_x", "N/A")} x {dtm_stats.get("res_y", "N/A")}
  Min/Max: {dtm_stats.get("min", "N/A")} / {dtm_stats.get("max", "N/A")}

DSM: {fmt_file_state(dsm_stats)}
  CRS: {dsm_stats.get("crs", "N/A")}
  Size: {dsm_stats.get("width", "N/A")} x {dsm_stats.get("height", "N/A")}
  Res: {dsm_stats.get("res_x", "N/A")} x {dsm_stats.get("res_y", "N/A")}
  Min/Max: {dsm_stats.get("min", "N/A")} / {dsm_stats.get("max", "N/A")}

CHM: {fmt_file_state(chm_stats)}
  CRS: {chm_stats.get("crs", "N/A")}
  Size: {chm_stats.get("width", "N/A")} x {chm_stats.get("height", "N/A")}
  Res: {chm_stats.get("res_x", "N/A")} x {chm_stats.get("res_y", "N/A")}
  Min/Max: {chm_stats.get("min", "N/A")} / {chm_stats.get("max", "N/A")}

[7. PRODUTOS RASTER - SUPERFICIES FECHADAS]
DTM_closed: {fmt_file_state(dtm_closed_stats)}
  CRS: {dtm_closed_stats.get("crs", "N/A")}
  Size: {dtm_closed_stats.get("width", "N/A")} x {dtm_closed_stats.get("height", "N/A")}
  Res: {dtm_closed_stats.get("res_x", "N/A")} x {dtm_closed_stats.get("res_y", "N/A")}
  Min/Max: {dtm_closed_stats.get("min", "N/A")} / {dtm_closed_stats.get("max", "N/A")}

DSM_closed: {fmt_file_state(dsm_closed_stats)}
  CRS: {dsm_closed_stats.get("crs", "N/A")}
  Size: {dsm_closed_stats.get("width", "N/A")} x {dsm_closed_stats.get("height", "N/A")}
  Res: {dsm_closed_stats.get("res_x", "N/A")} x {dsm_closed_stats.get("res_y", "N/A")}
  Min/Max: {dsm_closed_stats.get("min", "N/A")} / {dsm_closed_stats.get("max", "N/A")}

ORTHO_SURFACE: {fmt_file_state(ortho_surface_stats)}
  CRS: {ortho_surface_stats.get("crs", "N/A")}
  Size: {ortho_surface_stats.get("width", "N/A")} x {ortho_surface_stats.get("height", "N/A")}
  Res: {ortho_surface_stats.get("res_x", "N/A")} x {ortho_surface_stats.get("res_y", "N/A")}
  Min/Max: {ortho_surface_stats.get("min", "N/A")} / {ortho_surface_stats.get("max", "N/A")}

[8. ORTOMOSAICO]
ORTHO: {fmt_file_state(ortho_stats)}
  CRS: {ortho_stats.get("crs", "N/A")}
  Size: {ortho_stats.get("width", "N/A")} x {ortho_stats.get("height", "N/A")}
  Res: {ortho_stats.get("res_x", "N/A")} x {ortho_stats.get("res_y", "N/A")}
  Bandas: {ortho_stats.get("count", "N/A")}

=====================================================
"""

    print(report)
    report_path = Path(config["LOG_DIR"]) / f'relatorio_p1_final_{config["DATASET"]}.txt'
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(generate())
