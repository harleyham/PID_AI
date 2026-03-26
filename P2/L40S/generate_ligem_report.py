#!/usr/bin/env python3
import json
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "p1_config.sh"


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
            ["colmap", "model_analyzer", "--path", sparse_dir],
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


def generate() -> int:
    config = load_config()

    database = config["DATABASE"]
    sparse_dir = config["ENU_PATH"]
    dense_ply = config["INPUT_PLY"]
    enu_meta_json = config["ENU_META_JSON"]

    dense_pts = get_dense_stats(dense_ply)
    registered_images, sparse_pts, reproj_err = get_colmap_stats(sparse_dir)
    total_images = get_images_count(database)
    alignment_ref, epsg_code = get_alignment_info(enu_meta_json)

    report = f"""
=====================================================
        LIGEM QUALITY REPORT - PIPELINE P1
=====================================================
DATA: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
PIPELINE: {config["PIPELINE_NAME"]}
DATASET: {config["DATASET"]}
GPU: {config["GPU"]}

[1. ESTATISTICAS DE RECONSTRUCAO]
a) Pontos da Nuvem Densa: {dense_pts} pontos
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
Modelo ENU: {sparse_dir}
=====================================================
"""

    print(report)
    report_path = Path(config["LOG_DIR"]) / "relatorio_p1_final.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(generate())
