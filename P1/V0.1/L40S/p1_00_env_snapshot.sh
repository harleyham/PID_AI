#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/p1_config.sh"

if [[ ! -f "$CONFIG_FILE" ]]; then
    printf 'ERRO: arquivo de configuração não encontrado: %s\n' "$CONFIG_FILE" >&2
    exit 1
fi

source "$CONFIG_FILE"

: "${PYTHON_BIN:=python3}"
: "${ENV_SNAPSHOT_CSV:=$PROJECT_ROOT/env_history.csv}"

HEADER="timestamp;hostname;gpu;vram_total_mb;vram_free_mb;driver;nvcc_cuda;gdal_cli;gdal_python;pdal_cuda;pdal_version;pdal_has_filters_cuda;pdal_has_writers_gdal;pdal_has_readers_las;pdal_has_filters_smrf;pdal_has_filters_pmf;pdal_driver_count;colmap_ver;pytorch;pytorch_cuda_available;pytorch_cuda_version;pytorch_device_name;gdal_cli_min_ok;pdal_min_ok;colmap_min_ok;torch_min_ok;has_gpu;pdal_cuda_plugin;torch_cuda_vs_nvcc;driver_supports_torch_cuda;driver_supports_nvcc_cuda;consistency_status"

if [[ ! -f "$ENV_SNAPSHOT_CSV" ]]; then
    echo "$HEADER" > "$ENV_SNAPSHOT_CSV"
fi

LINE="$("$PYTHON_BIN" "$SCRIPT_DIR/p1_env_snapshot.py")"

echo "$LINE" >> "$ENV_SNAPSHOT_CSV"

echo "[INFO] Snapshot salvo em: $ENV_SNAPSHOT_CSV"
echo "[INFO] $LINE"