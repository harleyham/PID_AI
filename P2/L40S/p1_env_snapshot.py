#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import re
import shutil
import socket
import subprocess
from typing import Tuple


DELIMITER = ";"


# =========================
# Configuração de mínimos
# =========================
MIN_GDAL_CLI = "3.8.0"
MIN_PDAL = "2.5.0"
MIN_COLMAP = "3.14.0"
MIN_TORCH = "2.5.0"


# =========================
# Utilidades
# =========================
def run(cmd):
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return out.strip()
    except Exception:
        return None


def csv_escape(value):
    s = str(value)
    if any(ch in s for ch in [DELIMITER, '"', "\n"]):
        s = '"' + s.replace('"', '""') + '"'
    return s


def normalize_version_tuple(version_str: str) -> Tuple[int, ...]:
    if not version_str:
        return tuple()
    nums = re.findall(r"\d+", version_str)
    return tuple(int(x) for x in nums)


def version_ge(found: str, minimum: str) -> bool:
    a = normalize_version_tuple(found)
    b = normalize_version_tuple(minimum)
    if not a or not b:
        return False

    max_len = max(len(a), len(b))
    a = a + (0,) * (max_len - len(a))
    b = b + (0,) * (max_len - len(b))
    return a >= b


def status_from_check(ok: bool) -> str:
    return "OK" if ok else "FAIL"


def parse_cuda_string_to_major_minor(cuda_str: str) -> str:
    if not cuda_str:
        return ""

    s = str(cuda_str).strip()

    if re.fullmatch(r"\d+\.\d+", s):
        return s

    if re.fullmatch(r"\d{3}", s):
        return f"{s[0:2]}.{s[2]}"

    if re.fullmatch(r"\d{2}", s):
        return f"{s[0]}.{s[1]}"

    return s


def driver_supports_cuda(driver_version: str, cuda_version: str) -> str:
    if not driver_version or not cuda_version:
        return "UNKNOWN"

    drv = normalize_version_tuple(driver_version)
    cuda = parse_cuda_string_to_major_minor(cuda_version)

    if not drv or not cuda:
        return "UNKNOWN"

    major_driver = drv[0]

    try:
        cuda_major = int(cuda.split(".")[0])
    except Exception:
        return "UNKNOWN"

    if cuda_major >= 12:
        return "YES" if major_driver >= 525 else "NO"

    if cuda_major >= 11:
        return "YES" if major_driver >= 450 else "NO"

    return "UNKNOWN"


# =========================
# GPU / Driver / CUDA
# =========================
def get_gpu_info():
    if not shutil.which("nvidia-smi"):
        return {
            "gpu": "NO_GPU",
            "vram_total_mb": "",
            "vram_free_mb": "",
            "driver": "",
        }

    query = run([
        "nvidia-smi",
        "--query-gpu=name,memory.total,memory.free,driver_version",
        "--format=csv,noheader,nounits"
    ])

    if not query:
        return {
            "gpu": "UNKNOWN",
            "vram_total_mb": "",
            "vram_free_mb": "",
            "driver": "",
        }

    first = query.splitlines()[0]
    parts = [x.strip() for x in first.split(",")]

    if len(parts) < 4:
        return {
            "gpu": "UNKNOWN",
            "vram_total_mb": "",
            "vram_free_mb": "",
            "driver": "",
        }

    name, total_mb, free_mb, driver = parts
    return {
        "gpu": name,
        "vram_total_mb": total_mb,
        "vram_free_mb": free_mb,
        "driver": driver,
    }


def get_nvcc_version():
    if not shutil.which("nvcc"):
        return "NOT_FOUND"

    out = run(["nvcc", "--version"])
    if not out:
        return "UNKNOWN"

    for line in out.splitlines():
        line = line.strip()
        if "release" in line.lower():
            parts = line.split("release", 1)
            if len(parts) == 2:
                version = parts[1].split(",")[0].strip()
                return version

    return "UNKNOWN"


# =========================
# GDAL
# =========================
def get_gdal_cli_version():
    out = run(["gdalinfo", "--version"])
    if not out:
        return "NOT_FOUND"

    m = re.search(r"GDAL\s+([0-9][^,\s]*)", out)
    if m:
        return m.group(1).strip()
    return "UNKNOWN"


def get_gdal_python_version():
    try:
        from osgeo import gdal
        return str(gdal.VersionInfo("RELEASE_NAME"))
    except Exception:
        return "NOT_AVAILABLE"


# =========================
# PDAL
# =========================
def get_pdal_version():
    out = run(["pdal", "--version"])
    if not out:
        return "NOT_FOUND"

    for line in out.splitlines():
        line = line.strip()
        if line.lower().startswith("pdal "):
            return line

    return "UNKNOWN"


def get_pdal_version_number():
    line = get_pdal_version()
    if not line or line in ("NOT_FOUND", "UNKNOWN"):
        return line

    m = re.search(r"pdal\s+([0-9][^\s]*)", line, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return "UNKNOWN"


def get_pdal_info():
    out = run(["pdal", "--drivers"])
    if not out:
        return {
            "cuda": "NOT_FOUND",
            "has_filters_cuda": "NO",
            "has_writers_gdal": "NO",
            "has_readers_las": "NO",
            "has_filters_smrf": "NO",
            "has_filters_pmf": "NO",
            "driver_count": "0",
        }

    lines = [
        ln.strip()
        for ln in out.splitlines()
        if ln.strip() and not ln.strip().startswith("=")
    ]

    def has_driver(name):
        for line in lines:
            if line.startswith(name):
                return "YES"
        return "NO"

    return {
        "cuda": "YES" if has_driver("filters.cuda") == "YES" else "NO",
        "has_filters_cuda": has_driver("filters.cuda"),
        "has_writers_gdal": has_driver("writers.gdal"),
        "has_readers_las": has_driver("readers.las"),
        "has_filters_smrf": has_driver("filters.smrf"),
        "has_filters_pmf": has_driver("filters.pmf"),
        "driver_count": str(len(lines)),
    }


# =========================
# COLMAP
# =========================
def get_colmap_version():
    if not shutil.which("colmap"):
        return "NOT_FOUND"

    candidates = [
        ["colmap", "version"],
        ["colmap", "-h"],
        ["colmap", "help"],
    ]

    for cmd in candidates:
        out = run(cmd)
        if not out:
            continue

        for line in out.splitlines():
            txt = line.strip()

            m = re.search(r"([0-9]+\.[0-9]+\.[0-9]+(?:\.dev[0-9]+|dev[0-9]+)?)", txt)
            if m:
                return m.group(1)

            if "colmap" in txt.lower() and "version" in txt.lower():
                return txt

    return "UNKNOWN"


def get_colmap_version_number():
    line = get_colmap_version()
    if not line or line in ("NOT_FOUND", "UNKNOWN"):
        return line

    m = re.search(r"([0-9]+\.[0-9]+\.[0-9]+(?:\.dev[0-9]+|dev[0-9]+)?)", line)
    if m:
        return m.group(1).strip()

    return line


# =========================
# PyTorch
# =========================
def get_torch_info():
    try:
        import torch

        version = str(torch.__version__)
        cuda_available = str(torch.cuda.is_available())

        cuda_version = torch.version.cuda

        if not cuda_version:
            m = re.search(r"\+cu(\d+)", version)
            if m:
                cuda_version = parse_cuda_string_to_major_minor(m.group(1))

        if not cuda_version:
            cuda_version = get_nvcc_version()
            if cuda_version in ("NOT_FOUND", "UNKNOWN"):
                cuda_version = ""

        torch_device_name = ""
        try:
            if torch.cuda.is_available():
                torch_device_name = torch.cuda.get_device_name(0)
        except Exception:
            torch_device_name = ""

        return {
            "version": version,
            "cuda_available": cuda_available,
            "cuda_version": str(cuda_version) if cuda_version else "",
            "device_name": torch_device_name,
        }

    except Exception:
        return {
            "version": "Not_Installed",
            "cuda_available": "",
            "cuda_version": "",
            "device_name": "",
        }


# =========================
# Consistência e validação
# =========================
def validate_min_versions(gdal_cli, pdal_num, colmap_num, torch_ver):
    gdal_ok = version_ge(gdal_cli, MIN_GDAL_CLI) if gdal_cli not in ("NOT_FOUND", "UNKNOWN") else False
    pdal_ok = version_ge(pdal_num, MIN_PDAL) if pdal_num not in ("NOT_FOUND", "UNKNOWN") else False
    colmap_ok = version_ge(colmap_num, MIN_COLMAP) if colmap_num not in ("NOT_FOUND", "UNKNOWN") else False

    torch_clean = torch_ver.split("+")[0] if torch_ver not in ("Not_Installed", "", None) else ""
    torch_ok = version_ge(torch_clean, MIN_TORCH) if torch_clean else False

    return {
        "gdal_cli_min_ok": status_from_check(gdal_ok),
        "pdal_min_ok": status_from_check(pdal_ok),
        "colmap_min_ok": status_from_check(colmap_ok),
        "torch_min_ok": status_from_check(torch_ok),
    }


def detect_consistency(gpu_info, nvcc_version, torch_info, pdal_cuda):
    driver = gpu_info["driver"]
    torch_cuda_available = torch_info["cuda_available"]
    torch_cuda_version = parse_cuda_string_to_major_minor(torch_info["cuda_version"])
    nvcc_cuda_version = parse_cuda_string_to_major_minor(nvcc_version) if nvcc_version not in ("NOT_FOUND", "UNKNOWN") else ""

    has_gpu = gpu_info["gpu"] not in ("NO_GPU", "UNKNOWN", "")
    pdal_cuda_ok = "YES" if pdal_cuda == "YES" else "NO"

    torch_cuda_vs_nvcc = "UNKNOWN"
    if torch_cuda_version and nvcc_cuda_version:
        torch_cuda_vs_nvcc = "MATCH" if torch_cuda_version == nvcc_cuda_version else "DIFF"

    driver_supports_torch_cuda = driver_supports_cuda(driver, torch_cuda_version)
    driver_supports_nvcc_cuda = driver_supports_cuda(driver, nvcc_cuda_version)

    overall = "OK"

    if has_gpu and torch_cuda_available == "True" and not driver:
        overall = "WARN"

    if torch_cuda_available == "True" and driver_supports_torch_cuda == "NO":
        overall = "FAIL"

    if nvcc_cuda_version and driver_supports_nvcc_cuda == "NO":
        overall = "FAIL"

    if has_gpu and torch_cuda_available != "True":
        overall = "WARN"

    return {
        "has_gpu": "YES" if has_gpu else "NO",
        "pdal_cuda_plugin": pdal_cuda_ok,
        "torch_cuda_vs_nvcc": torch_cuda_vs_nvcc,
        "driver_supports_torch_cuda": driver_supports_torch_cuda,
        "driver_supports_nvcc_cuda": driver_supports_nvcc_cuda,
        "consistency_status": overall,
    }


# =========================
# MAIN
# =========================
def main():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hostname = socket.gethostname()

    gpu_info = get_gpu_info()
    nvcc_version = get_nvcc_version()

    gdal_cli = get_gdal_cli_version()
    gdal_py = get_gdal_python_version()

    pdal_info = get_pdal_info()
    pdal_cuda = pdal_info["cuda"]
    pdal_version = get_pdal_version()
    pdal_version_num = get_pdal_version_number()

    colmap_version = get_colmap_version()
    colmap_version_num = get_colmap_version_number()

    torch_info = get_torch_info()

    min_versions = validate_min_versions(
        gdal_cli=gdal_cli,
        pdal_num=pdal_version_num,
        colmap_num=colmap_version_num,
        torch_ver=torch_info["version"],
    )

    consistency = detect_consistency(
        gpu_info=gpu_info,
        nvcc_version=nvcc_version,
        torch_info=torch_info,
        pdal_cuda=pdal_cuda,
    )

    row = [
        timestamp,
        hostname,
        gpu_info["gpu"],
        gpu_info["vram_total_mb"],
        gpu_info["vram_free_mb"],
        gpu_info["driver"],
        nvcc_version,
        gdal_cli,
        gdal_py,
        pdal_cuda,
        pdal_version,
        pdal_info["has_filters_cuda"],
        pdal_info["has_writers_gdal"],
        pdal_info["has_readers_las"],
        pdal_info["has_filters_smrf"],
        pdal_info["has_filters_pmf"],
        pdal_info["driver_count"],
        colmap_version,
        torch_info["version"],
        torch_info["cuda_available"],
        torch_info["cuda_version"],
        torch_info["device_name"],
        min_versions["gdal_cli_min_ok"],
        min_versions["pdal_min_ok"],
        min_versions["colmap_min_ok"],
        min_versions["torch_min_ok"],
        consistency["has_gpu"],
        consistency["pdal_cuda_plugin"],
        consistency["torch_cuda_vs_nvcc"],
        consistency["driver_supports_torch_cuda"],
        consistency["driver_supports_nvcc_cuda"],
        consistency["consistency_status"],
    ]

    print(DELIMITER.join(csv_escape(x) for x in row))


if __name__ == "__main__":
    main()