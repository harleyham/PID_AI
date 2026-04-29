#!/bin/bash
set -euo pipefail

# Pipeline V0.2 - Módulo 06: Geração de DSM, DTM, hillshade e CHM
# Prioridade atual:
# - melhorar terra nua
# - reduzir vazios
# - separar raster analítico de raster voltado a continuidade

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

: "${PYTHON_BIN:=python3}"
: "${LAS_OUTPUT:=$OUTPUT_PATH/dense_utm_color.las}"
: "${OUTPUT_PATH:=$OUTPUT_DIR}"

: "${DTM_TIF:=$OUTPUT_PATH/DTM_${DATASET_SLUG}.tif}"
: "${DSM_TIF:=$OUTPUT_PATH/DSM_${DATASET_SLUG}.tif}"
: "${DTM_CLOSED_TIF:=$OUTPUT_PATH/DTM_closed_${DATASET_SLUG}.tif}"
: "${DSM_CLOSED_TIF:=$OUTPUT_PATH/DSM_closed_${DATASET_SLUG}.tif}"
: "${ORTHO_SURFACE_TIF:=$OUTPUT_PATH/ORTHO_SURFACE_${DATASET_SLUG}.tif}"
: "${CHM_TIF:=$OUTPUT_PATH/CHM_${DATASET_SLUG}.tif}"
: "${DTM_HILLSHADE_TIF:=$OUTPUT_PATH/DTM_hillshade_${DATASET_SLUG}.tif}"
: "${DSM_HILLSHADE_TIF:=$OUTPUT_PATH/DSM_hillshade_${DATASET_SLUG}.tif}"
: "${DENSE_GROUND_LAZ:=$OUTPUT_PATH/dense_ground_${DATASET_SLUG}.laz}"

mkdir -p "$OUTPUT_PATH"

p1_assert_file_exists "$MODULE" "$LAS_OUTPUT"
p1_assert_nonempty_file "$MODULE" "$LAS_OUTPUT"
p1_assert_file_exists "$MODULE" "$ENU_META_JSON"
p1_assert_nonempty_file "$MODULE" "$ENU_META_JSON"

rm -f \
    "$DTM_TIF" \
    "$DSM_TIF" \
    "$DTM_CLOSED_TIF" \
    "$DSM_CLOSED_TIF" \
    "$ORTHO_SURFACE_TIF" \
    "$CHM_TIF" \
    "$DTM_HILLSHADE_TIF" \
    "$DSM_HILLSHADE_TIF" \
    "$DENSE_GROUND_LAZ"

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

p1_metric "$MODULE" "dtm_closed_output_type" "$DTM_CLOSED_OUTPUT_TYPE" "mode"
p1_metric "$MODULE" "dtm_closed_window_size" "$DTM_CLOSED_WINDOW_SIZE" "pixels"
p1_metric "$MODULE" "dsm_closed_output_type" "$DSM_CLOSED_OUTPUT_TYPE" "mode"
p1_metric "$MODULE" "dsm_closed_window_size" "$DSM_CLOSED_WINDOW_SIZE" "pixels"

p1_metric "$MODULE" "dtm_fillnodata_max_distance" "$DTM_FILLNODATA_MAX_DISTANCE" "pixels"
p1_metric "$MODULE" "dsm_fillnodata_max_distance" "$DSM_FILLNODATA_MAX_DISTANCE" "pixels"
p1_metric "$MODULE" "fillnodata_smoothing_iterations" "$FILLNODATA_SMOOTHING_ITERATIONS" "count"
p1_metric "$MODULE" "ortho_surface_mode" "$ORTHO_SURFACE_MODE" "mode"

p1_log_info "$MODULE" "Executando pdal info --summary na nuvem LAS de entrada"
p1_run_cmd "$MODULE" "pdal info --summary" \
    pdal info "$LAS_OUTPUT" --summary

p1_log_info "$MODULE" "Gerando DTM/DSM analíticos, superfícies fechadas e CHM"

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
    --dtm-closed-output-type "$DTM_CLOSED_OUTPUT_TYPE"
    --dtm-closed-window-size "$DTM_CLOSED_WINDOW_SIZE"
    --dsm-closed-output-type "$DSM_CLOSED_OUTPUT_TYPE"
    --dsm-closed-window-size "$DSM_CLOSED_WINDOW_SIZE"
    --dtm-fillnodata-max-distance "$DTM_FILLNODATA_MAX_DISTANCE"
    --dsm-fillnodata-max-distance "$DSM_FILLNODATA_MAX_DISTANCE"
    --fillnodata-smoothing-iterations "$FILLNODATA_SMOOTHING_ITERATIONS"
    --ortho-surface-mode "$ORTHO_SURFACE_MODE"
    --log-file "$PIPELINE_LOG"
    --metrics-csv "$METRICS_CSV"
    --dataset "$DATASET"
    --dataset-slug "$DATASET_SLUG"
    --gpu "$GPU"
    --module "$MODULE"
)

p1_run_cmd "$MODULE" "p1_06_GPU_DEM.py" "${PY_CMD[@]}"

p1_assert_file_exists "$MODULE" "$DTM_TIF"
p1_assert_nonempty_file "$MODULE" "$DTM_TIF"

p1_assert_file_exists "$MODULE" "$DSM_TIF"
p1_assert_nonempty_file "$MODULE" "$DSM_TIF"

p1_assert_file_exists "$MODULE" "$DTM_CLOSED_TIF"
p1_assert_nonempty_file "$MODULE" "$DTM_CLOSED_TIF"

p1_assert_file_exists "$MODULE" "$DSM_CLOSED_TIF"
p1_assert_nonempty_file "$MODULE" "$DSM_CLOSED_TIF"

p1_assert_file_exists "$MODULE" "$ORTHO_SURFACE_TIF"
p1_assert_nonempty_file "$MODULE" "$ORTHO_SURFACE_TIF"

p1_assert_file_exists "$MODULE" "$CHM_TIF"
p1_assert_nonempty_file "$MODULE" "$CHM_TIF"

if [[ -f "$DTM_HILLSHADE_TIF" ]]; then
    p1_assert_nonempty_file "$MODULE" "$DTM_HILLSHADE_TIF"
fi

if [[ -f "$DSM_HILLSHADE_TIF" ]]; then
    p1_assert_nonempty_file "$MODULE" "$DSM_HILLSHADE_TIF"
fi

if [[ -f "$DENSE_GROUND_LAZ" ]]; then
    p1_assert_nonempty_file "$MODULE" "$DENSE_GROUND_LAZ"
fi

DTM_SIZE_BYTES="$(stat -c%s "$DTM_TIF")"
DSM_SIZE_BYTES="$(stat -c%s "$DSM_TIF")"
DTM_CLOSED_SIZE_BYTES="$(stat -c%s "$DTM_CLOSED_TIF")"
DSM_CLOSED_SIZE_BYTES="$(stat -c%s "$DSM_CLOSED_TIF")"
ORTHO_SURFACE_SIZE_BYTES="$(stat -c%s "$ORTHO_SURFACE_TIF")"
CHM_SIZE_BYTES="$(stat -c%s "$CHM_TIF")"

p1_metric "$MODULE" "dtm_size" "$DTM_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "dsm_size" "$DSM_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "dtm_closed_size" "$DTM_CLOSED_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "dsm_closed_size" "$DSM_CLOSED_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "ortho_surface_size" "$ORTHO_SURFACE_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "chm_size" "$CHM_SIZE_BYTES" "bytes"

p1_log_info "$MODULE" "Saídas geradas:"
p1_log_info "$MODULE" "DTM: $DTM_TIF"
p1_log_info "$MODULE" "DSM: $DSM_TIF"
p1_log_info "$MODULE" "DTM Closed: $DTM_CLOSED_TIF"
p1_log_info "$MODULE" "DSM Closed: $DSM_CLOSED_TIF"
p1_log_info "$MODULE" "ORTHO_SURFACE: $ORTHO_SURFACE_TIF"
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