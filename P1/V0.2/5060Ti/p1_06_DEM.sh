#!/bin/bash
set -euo pipefail

# Pipeline V0.2 - Módulo 06: Geração de DSM, DTM, hillshade e CHM
# + superfícies fechadas para uso no M07
#
# Saídas novas:
# - DTM_closed.tif
# - DSM_closed.tif

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
# Defaults / compatibilidade
# ------------------------------------------------------------
: "${PYTHON_BIN:=python3}"
: "${LAS_OUTPUT:=$OUTPUT_PATH/dense_utm_color.las}"
: "${OUTPUT_PATH:=$OUTPUT_DIR}"

: "${DTM_TIF:=$OUTPUT_PATH/DTM.tif}"
: "${DSM_TIF:=$OUTPUT_PATH/DSM.tif}"
: "${DTM_CLOSED_TIF:=$OUTPUT_PATH/DTM_closed.tif}"
: "${DSM_CLOSED_TIF:=$OUTPUT_PATH/DSM_closed.tif}"
: "${CHM_TIF:=$OUTPUT_PATH/CHM.tif}"
: "${DTM_HILLSHADE_TIF:=$OUTPUT_PATH/DTM_hillshade.tif}"
: "${DSM_HILLSHADE_TIF:=$OUTPUT_PATH/DSM_hillshade.tif}"
: "${DENSE_GROUND_LAZ:=$OUTPUT_PATH/dense_ground.laz}"

: "${DEM_RESOLUTION:=0.05}"
: "${DEM_NODATA:=-9999}"
: "${SMRF_SCALAR:=1.25}"
: "${SMRF_SLOPE:=0.15}"
: "${SMRF_THRESHOLD:=0.50}"
: "${SMRF_WINDOW:=16.0}"
: "${DTM_OUTPUT_TYPE:=idw}"
: "${DTM_WINDOW_SIZE:=1}"
: "${DSM_OUTPUT_TYPE:=max}"
: "${DSM_WINDOW_SIZE:=1}"

# fechamento controlado para ortho
: "${FILLNODATA_MAX_DISTANCE:=20}"
: "${FILLNODATA_SMOOTHING_ITERATIONS:=1}"

mkdir -p "$OUTPUT_PATH"

p1_assert_file_exists "$MODULE" "$LAS_OUTPUT"
p1_assert_nonempty_file "$MODULE" "$LAS_OUTPUT"
p1_assert_file_exists "$MODULE" "$ENU_META_JSON"
p1_assert_nonempty_file "$MODULE" "$ENU_META_JSON"

# ------------------------------------------------------------
# Limpeza das saídas
# ------------------------------------------------------------
rm -f \
    "$DTM_TIF" \
    "$DSM_TIF" \
    "$DTM_CLOSED_TIF" \
    "$DSM_CLOSED_TIF" \
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

p1_metric "$MODULE" "dem_resolution" "$DEM_RESOLUTION" "meters"
p1_metric "$MODULE" "dem_nodata" "$DEM_NODATA" "value"
p1_metric "$MODULE" "smrf_scalar" "$SMRF_SCALAR" "value"
p1_metric "$MODULE" "smrf_slope" "$SMRF_SLOPE" "value"
p1_metric "$MODULE" "smrf_threshold" "$SMRF_THRESHOLD" "value"
p1_metric "$MODULE" "smrf_window" "$SMRF_WINDOW" "value"
p1_metric "$MODULE" "dtm_output_type" "$DTM_OUTPUT_TYPE" "mode"
p1_metric "$MODULE" "dtm_window_size" "$DTM_WINDOW_SIZE" "pixels"
p1_metric "$MODULE" "dsm_output_type" "$DSM_OUTPUT_TYPE" "mode"
p1_metric "$MODULE" "dsm_window_size" "$DSM_WINDOW_SIZE" "pixels"
p1_metric "$MODULE" "fillnodata_max_distance" "$FILLNODATA_MAX_DISTANCE" "pixels"
p1_metric "$MODULE" "fillnodata_smoothing_iterations" "$FILLNODATA_SMOOTHING_ITERATIONS" "count"

# ------------------------------------------------------------
# Diagnóstico da LAS de entrada
# ------------------------------------------------------------
p1_log_info "$MODULE" "Executando pdal info --summary na nuvem LAS de entrada"
p1_run_cmd "$MODULE" "pdal info --summary" \
    pdal info "$LAS_OUTPUT" --summary

# ------------------------------------------------------------
# Execução do M06 em Python
# ------------------------------------------------------------
p1_log_info "$MODULE" "Gerando DSM, DTM, superfícies fechadas, hillshade e CHM"

PY_CMD=(
    "$PYTHON_BIN" "$SCRIPT_DIR/p1_06_GPU_DEM.py"
    --dense-las "$LAS_OUTPUT"
    --output-dir "$OUTPUT_PATH"
    --enu-meta-json "$ENU_META_JSON"
    --resolution "$DEM_RESOLUTION"
    --nodata "$DEM_NODATA"
    --smrf-scalar "$SMRF_SCALAR"
    --smrf-slope "$SMRF_SLOPE"
    --smrf-threshold "$SMRF_THRESHOLD"
    --smrf-window "$SMRF_WINDOW"
    --dtm-output-type "$DTM_OUTPUT_TYPE"
    --dtm-window-size "$DTM_WINDOW_SIZE"
    --dsm-output-type "$DSM_OUTPUT_TYPE"
    --dsm-window-size "$DSM_WINDOW_SIZE"
    --fillnodata-max-distance "$FILLNODATA_MAX_DISTANCE"
    --fillnodata-smoothing-iterations "$FILLNODATA_SMOOTHING_ITERATIONS"
    --log-file "$PIPELINE_LOG"
    --metrics-csv "$METRICS_CSV"
    --dataset "$DATASET"
    --gpu "$GPU"
    --module "$MODULE"
)

p1_run_cmd "$MODULE" "p1_06_GPU_DEM.py" "${PY_CMD[@]}"

# ------------------------------------------------------------
# Validação das saídas obrigatórias
# ------------------------------------------------------------
p1_assert_file_exists "$MODULE" "$DTM_TIF"
p1_assert_nonempty_file "$MODULE" "$DTM_TIF"

p1_assert_file_exists "$MODULE" "$DSM_TIF"
p1_assert_nonempty_file "$MODULE" "$DSM_TIF"

p1_assert_file_exists "$MODULE" "$DTM_CLOSED_TIF"
p1_assert_nonempty_file "$MODULE" "$DTM_CLOSED_TIF"

p1_assert_file_exists "$MODULE" "$DSM_CLOSED_TIF"
p1_assert_nonempty_file "$MODULE" "$DSM_CLOSED_TIF"

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
DTM_CLOSED_SIZE_BYTES="$(stat -c%s "$DTM_CLOSED_TIF")"
DSM_CLOSED_SIZE_BYTES="$(stat -c%s "$DSM_CLOSED_TIF")"
CHM_SIZE_BYTES="$(stat -c%s "$CHM_TIF")"

p1_metric "$MODULE" "dtm_size" "$DTM_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "dsm_size" "$DSM_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "dtm_closed_size" "$DTM_CLOSED_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "dsm_closed_size" "$DSM_CLOSED_SIZE_BYTES" "bytes"
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
p1_log_info "$MODULE" "DTM Closed: $DTM_CLOSED_TIF"
p1_log_info "$MODULE" "DSM Closed: $DSM_CLOSED_TIF"
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
