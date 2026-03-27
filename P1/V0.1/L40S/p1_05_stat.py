#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import laspy

from p1_logging import log_info, log_error, metric


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Calcula estatísticas do LAS exportado no Módulo 05."
    )
    parser.add_argument("--input-las", required=True, help="Arquivo LAS de entrada.")
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

    las_path = Path(args.input_las)

    if not las_path.exists():
        log_error(log_file, dataset, gpu, module, f"LAS não encontrado: {las_path}")
        metric(metrics_csv, dataset, gpu, module, "input_las_exists", 0, "bool", "FAILED", str(las_path))
        return 1

    las = laspy.read(las_path)

    x = las.x
    y = las.y
    z = las.z

    points = len(x)
    e_min, e_max = float(x.min()), float(x.max())
    n_min, n_max = float(y.min()), float(y.max())
    h_min, h_max = float(z.min()), float(z.max())

    log_info(log_file, dataset, gpu, module, f"Estatísticas do LAS: {las_path}")
    log_info(log_file, dataset, gpu, module, f"Pontos: {points}")
    log_info(log_file, dataset, gpu, module, f"Bounds E: {e_min} {e_max}")
    log_info(log_file, dataset, gpu, module, f"Bounds N: {n_min} {n_max}")
    log_info(log_file, dataset, gpu, module, f"Bounds H: {h_min} {h_max}")

    metric(metrics_csv, dataset, gpu, module, "stat_points", points, "count")
    metric(metrics_csv, dataset, gpu, module, "stat_E_min", e_min, "m")
    metric(metrics_csv, dataset, gpu, module, "stat_E_max", e_max, "m")
    metric(metrics_csv, dataset, gpu, module, "stat_N_min", n_min, "m")
    metric(metrics_csv, dataset, gpu, module, "stat_N_max", n_max, "m")
    metric(metrics_csv, dataset, gpu, module, "stat_H_min", h_min, "m")
    metric(metrics_csv, dataset, gpu, module, "stat_H_max", h_max, "m")

    header = las.header
    point_format = header.point_format.id
    version = f"{header.version.major}.{header.version.minor}"

    log_info(log_file, dataset, gpu, module, f"LAS point_format: {point_format}")
    log_info(log_file, dataset, gpu, module, f"LAS version: {version}")

    metric(metrics_csv, dataset, gpu, module, "stat_point_format", point_format, "id")
    metric(metrics_csv, dataset, gpu, module, "stat_las_version", version, "version")

    return 0


if __name__ == "__main__":
    sys.exit(main())
    