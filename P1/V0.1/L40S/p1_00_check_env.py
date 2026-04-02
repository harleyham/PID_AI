#!/usr/bin/env python3
import importlib
import shutil
import sys

from p1_logging import log_info, log_error, log_warn, metric


REQUIRED_PYTHON_MODULES = [
    ("numpy", True),
    ("laspy", True),
    ("rasterio", True),
    ("json", True),
]

REQUIRED_BINARIES = [
    ("colmap", True),
    ("pdal", True),
    ("gdalinfo", True),
    ("python", True),
]


def check_python_module(name):
    try:
        mod = importlib.import_module(name)
        version = getattr(mod, "__version__", "unknown")
        return True, version
    except Exception:
        return False, None


def check_binary(name):
    path = shutil.which(name)
    return path is not None, path


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Verificação de ambiente do pipeline P2")
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--metrics-csv", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--gpu", required=True)
    parser.add_argument("--module", required=True)
    args = parser.parse_args()

    log_info(args.log_file, args.dataset, args.gpu, args.module, "Iniciando verificação de ambiente")

    failures = 0

    # -------------------------
    # Python modules
    # -------------------------
    log_info(args.log_file, args.dataset, args.gpu, args.module, "Verificando módulos Python")

    for name, required in REQUIRED_PYTHON_MODULES:
        ok, version = check_python_module(name)

        if ok:
            log_info(args.log_file, args.dataset, args.gpu, args.module,
                     f"OK Python module: {name} ({version})")
            metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                   f"python_module_{name}", 1, "bool")
        else:
            msg = f"FALTA Python module: {name}"
            if required:
                log_error(args.log_file, args.dataset, args.gpu, args.module, msg)
                metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                       f"python_module_{name}", 0, "bool", "FAILED")
                failures += 1
            else:
                log_warn(args.log_file, args.dataset, args.gpu, args.module, msg)
                metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                       f"python_module_{name}", 0, "bool", "WARNING")

    # -------------------------
    # System binaries
    # -------------------------
    log_info(args.log_file, args.dataset, args.gpu, args.module, "Verificando binários do sistema")

    for name, required in REQUIRED_BINARIES:
        ok, path = check_binary(name)

        if ok:
            log_info(args.log_file, args.dataset, args.gpu, args.module,
                     f"OK binary: {name} ({path})")
            metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                   f"binary_{name}", 1, "bool")
        else:
            msg = f"FALTA binary: {name}"
            if required:
                log_error(args.log_file, args.dataset, args.gpu, args.module, msg)
                metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                       f"binary_{name}", 0, "bool", "FAILED")
                failures += 1
            else:
                log_warn(args.log_file, args.dataset, args.gpu, args.module, msg)
                metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                       f"binary_{name}", 0, "bool", "WARNING")

    # -------------------------
    # Python runtime
    # -------------------------
    python_exec = sys.executable
    log_info(args.log_file, args.dataset, args.gpu, args.module,
             f"Python executável: {python_exec}")

    metric(args.metrics_csv, args.dataset, args.gpu, args.module,
           "python_executable", python_exec, "path")

    # -------------------------
    # Resultado final
    # -------------------------
    if failures > 0:
        log_error(args.log_file, args.dataset, args.gpu, args.module,
                  f"Falha na verificação de ambiente: {failures} dependências ausentes")
        sys.exit(1)

    log_info(args.log_file, args.dataset, args.gpu, args.module,
             "Ambiente OK")

    return 0


if __name__ == "__main__":
    sys.exit(main())