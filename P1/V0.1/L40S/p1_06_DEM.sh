#!/bin/bash
set -euo pipefail

# Pipeline V0.2 - Módulo 06: Geração de DSM, DTM, hillshade e CHM
# Compatível com:
# - p1_config.sh organizado por módulos M00..M06
# - p1_logging.sh consolidado
# - p1_06_GPU_DEM.py

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/p1_config.sh"
LOGGING_FILE="$SCRIPT_DIR/p1_logging.sh"

if [[ ! -f "$CONFIG_FILE" ]]; then
    printf 'ERRO: arquivo de configuração não encontrado: %s\n' "$CONFIG_FILE" >&2
    exit 1
fi

if [[ ! -f "$LOGGING_FILE" ]]; then
    printf 'ERRO: arquivo de logging não encontrado: %s\n' "$LOGGING_FILE" >&2
    exit 1
fi

source "$CONFIG_FILE"
source "$LOGGING_FILE"

p1_ensure_dirs
p1_init_logs

MODULE="M06"

p1_module_start "$MODULE" START_TS

# ------------------------------------------------------------
# Defaults / compatibilidade com config novo
# ------------------------------------------------------------
: "${PYTHON_BIN:=python}"
: "${LAS_OUTPUT:=$OUTPUT_PATH/dense_utm_color.las}"
: "${OUTPUT_PATH:=$OUTPUT_DIR}"

: "${DTM_TIF:=$OUTPUT_PATH/DTM.tif}"
: "${DSM_TIF:=$OUTPUT_PATH/DSM.tif}"
: "${CHM_TIF:=$OUTPUT_PATH/CHM.tif}"
: "${DTM_HILLSHADE_TIF:=$OUTPUT_PATH/DTM_hillshade.tif}"
: "${DSM_HILLSHADE_TIF:=$OUTPUT_PATH/DSM_hillshade.tif}"
: "${DENSE_GROUND_LAZ:=$OUTPUT_PATH/dense_ground.laz}"

mkdir -p "$OUTPUT_PATH"

p1_assert_file_exists "$MODULE" "$LAS_OUTPUT"
p1_assert_nonempty_file "$MODULE" "$LAS_OUTPUT"
p1_assert_file_exists "$MODULE" "$ENU_META_JSON"
p1_assert_nonempty_file "$MODULE" "$ENU_META_JSON"

# Limpeza das saídas
rm -f \
    "$DTM_TIF" \
    "$DSM_TIF" \
    "$CHM_TIF" \
    "$DTM_HILLSHADE_TIF" \
    "$DSM_HILLSHADE_TIF" \
    "$DENSE_GROUND_LAZ"

# ------------------------------------------------------------
# Métricas de entrada / parâmetros
# ------------------------------------------------------------
p1_metric "$MODULE" "python_bin" "$PYTHON_BIN" "path"
p1_metric "$MODULE" "dense_las" "$LAS_OUTPUT" "path"
p1_metric "$MODULE" "enu_meta_json" "$ENU_META_JSON" "path"
p1_metric "$MODULE" "output_path" "$OUTPUT_PATH" "path"

[[ -n "${DEM_RESOLUTION:-}" ]] && p1_metric "$MODULE" "dem_resolution" "$DEM_RESOLUTION" "meters"
[[ -n "${DEM_NUM_THREADS:-}" ]] && p1_metric "$MODULE" "dem_num_threads" "$DEM_NUM_THREADS" "count"
[[ -n "${GROUND_CLASSIFICATION_METHOD:-}" ]] && p1_metric "$MODULE" "ground_classification_method" "$GROUND_CLASSIFICATION_METHOD" "mode"

# ------------------------------------------------------------
# Diagnóstico da LAS de entrada
# ------------------------------------------------------------
p1_log_info "$MODULE" "Executando pdal info --summary na nuvem LAS de entrada"
p1_run_cmd "$MODULE" "pdal info --summary" \
    pdal info "$LAS_OUTPUT" --summary

# ------------------------------------------------------------
# Execução do M06 em Python
# ------------------------------------------------------------
p1_log_info "$MODULE" "Gerando DSM, DTM, hillshade e CHM"

PY_CMD=(
    "$PYTHON_BIN" "$SCRIPT_DIR/p1_06_GPU_DEM.py"
    --dense-las "$LAS_OUTPUT"
    --output-dir "$OUTPUT_PATH"
    --enu-meta-json "$ENU_META_JSON"
    --log-file "$PIPELINE_LOG"
    --metrics-csv "$METRICS_CSV"
    --dataset "$DATASET"
    --gpu "$GPU"
    --module "$MODULE"
    --resolution "${DEM_RESOLUTION:-0.10}"
    --nodata "${DEM_NODATA:--9999}"
    --smrf-scalar "${SMRF_SCALAR:-1.25}"
    --smrf-slope "${SMRF_SLOPE:-0.15}"
    --smrf-threshold "${SMRF_THRESHOLD:-0.50}"
    --smrf-window "${SMRF_WINDOW:-16.0}"
    --dtm-output-type "${DTM_OUTPUT_TYPE:-idw}"
    --dtm-window-size "${DTM_WINDOW_SIZE:-2}"
    --dsm-output-type "${DSM_OUTPUT_TYPE:-max}"
    --dsm-window-size "${DSM_WINDOW_SIZE:-1}"
)

p1_run_cmd "$MODULE" "p1_06_GPU_DEM.py" "${PY_CMD[@]}"

# ------------------------------------------------------------
# Validação das saídas obrigatórias
# ------------------------------------------------------------
p1_assert_file_exists "$MODULE" "$DTM_TIF"
p1_assert_nonempty_file "$MODULE" "$DTM_TIF"

p1_assert_file_exists "$MODULE" "$DSM_TIF"
p1_assert_nonempty_file "$MODULE" "$DSM_TIF"

p1_assert_file_exists "$MODULE" "$CHM_TIF"
p1_assert_nonempty_file "$MODULE" "$CHM_TIF"

# ------------------------------------------------------------
# Validação das saídas opcionais
# ------------------------------------------------------------
if [[ -f "$DTM_HILLSHADE_TIF" ]]; then
    p1_assert_nonempty_file "$MODULE" "$DTM_HILLSHADE_TIF"
fi

if [[ -f "$DSM_HILLSHADE_TIF" ]]; then
    p1_assert_nonempty_file "$MODULE" "$DSM_HILLSHADE_TIF"
fi

if [[ -f "$DENSE_GROUND_LAZ" ]]; then
    p1_assert_nonempty_file "$MODULE" "$DENSE_GROUND_LAZ"
fi

# ------------------------------------------------------------
# Métricas de tamanho dos arquivos
# ------------------------------------------------------------
DTM_SIZE_BYTES="$(stat -c%s "$DTM_TIF")"
DSM_SIZE_BYTES="$(stat -c%s "$DSM_TIF")"
CHM_SIZE_BYTES="$(stat -c%s "$CHM_TIF")"

p1_metric "$MODULE" "dtm_size" "$DTM_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "dsm_size" "$DSM_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "chm_size" "$CHM_SIZE_BYTES" "bytes"

if [[ -f "$DTM_HILLSHADE_TIF" ]]; then
    DTM_HS_SIZE_BYTES="$(stat -c%s "$DTM_HILLSHADE_TIF")"
    p1_metric "$MODULE" "dtm_hillshade_size" "$DTM_HS_SIZE_BYTES" "bytes"
fi

if [[ -f "$DSM_HILLSHADE_TIF" ]]; then
    DSM_HS_SIZE_BYTES="$(stat -c%s "$DSM_HILLSHADE_TIF")"
    p1_metric "$MODULE" "dsm_hillshade_size" "$DSM_HS_SIZE_BYTES" "bytes"
fi

if [[ -f "$DENSE_GROUND_LAZ" ]]; then
    GROUND_LAZ_SIZE_BYTES="$(stat -c%s "$DENSE_GROUND_LAZ")"
    p1_metric "$MODULE" "ground_laz_size" "$GROUND_LAZ_SIZE_BYTES" "bytes"
fi

# ------------------------------------------------------------
# Resumo final
# ------------------------------------------------------------
p1_log_info "$MODULE" "Saídas geradas:"
p1_log_info "$MODULE" "DTM: $DTM_TIF"
p1_log_info "$MODULE" "DSM: $DSM_TIF"
p1_log_info "$MODULE" "CHM: $CHM_TIF"

if [[ -f "$DTM_HILLSHADE_TIF" ]]; then
    p1_log_info "$MODULE" "DTM Hillshade: $DTM_HILLSHADE_TIF"
fi

if [[ -f "$DSM_HILLSHADE_TIF" ]]; then
    p1_log_info "$MODULE" "DSM Hillshade: $DSM_HILLSHADE_TIF"
fi

if [[ -f "$DENSE_GROUND_LAZ" ]]; then
    p1_log_info "$MODULE" "Ground LAZ: $DENSE_GROUND_LAZ"
fi

p1_module_end "$MODULE" "$START_TS" "SUCCESS"
