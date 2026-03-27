import csv
import os
from datetime import datetime


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write_line(log_file: str, line: str) -> None:
    if not log_file:
        return
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_line(
    log_file: str,
    level: str,
    dataset: str,
    gpu: str,
    module: str,
    message: str,
    echo: bool = True,
) -> None:
    line = f"[{now_str()}] [{level}] [{dataset}] [{gpu}] [{module}] {message}"
    if echo:
        print(line)
    _write_line(log_file, line)


def log_info(
    log_file: str,
    dataset: str,
    gpu: str,
    module: str,
    message: str,
    echo: bool = True,
) -> None:
    log_line(log_file, "INFO", dataset, gpu, module, message, echo)


def log_warn(
    log_file: str,
    dataset: str,
    gpu: str,
    module: str,
    message: str,
    echo: bool = True,
) -> None:
    log_line(log_file, "WARN", dataset, gpu, module, message, echo)


def log_error(
    log_file: str,
    dataset: str,
    gpu: str,
    module: str,
    message: str,
    echo: bool = True,
) -> None:
    log_line(log_file, "ERROR", dataset, gpu, module, message, echo)


def ensure_metrics_header(metrics_csv: str) -> None:
    if not metrics_csv:
        return

    directory = os.path.dirname(metrics_csv)
    if directory:
        os.makedirs(directory, exist_ok=True)

    if not os.path.exists(metrics_csv) or os.path.getsize(metrics_csv) == 0:
        with open(metrics_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(
                [
                    "timestamp",
                    "dataset",
                    "gpu",
                    "module",
                    "metric",
                    "value",
                    "unit",
                    "status",
                    "notes",
                ]
            )


def metric(
    metrics_csv: str,
    dataset: str,
    gpu: str,
    module: str,
    metric_name: str,
    value,
    unit: str = "",
    status: str = "SUCCESS",
    notes: str = "",
) -> None:
    if not metrics_csv:
        return

    ensure_metrics_header(metrics_csv)

    with open(metrics_csv, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(
            [
                now_str(),
                dataset,
                gpu,
                module,
                metric_name,
                value,
                unit,
                status,
                notes,
            ]
        )


def log_and_metric(
    log_file: str,
    metrics_csv: str,
    dataset: str,
    gpu: str,
    module: str,
    message: str,
    metric_name: str,
    value,
    unit: str = "",
    status: str = "SUCCESS",
    notes: str = "",
    level: str = "INFO",
    echo: bool = True,
) -> None:
    log_line(log_file, level, dataset, gpu, module, message, echo)
    metric(metrics_csv, dataset, gpu, module, metric_name, value, unit, status, notes)
    