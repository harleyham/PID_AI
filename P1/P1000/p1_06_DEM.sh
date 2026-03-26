#!/bin/bash
set -euo pipefail

# Pipeline P1 - Módulo 06: Geração de DSM, DTM, hillshade e CHM
# Padrão consolidado:
# - usa p1_config.sh + p1_logging.sh
# - orquestra o processamento via p1_06_GPU_DEM.py
# - valida saídas
# - registra mensagens em PIPELINE_LOG
# - registra métricas em METRICS_CSV

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

p1_assert_file_exists "$MODULE" "$DENSE_LAS"
p1_assert_nonempty_file "$MODULE" "$DENSE_LAS"
p1_assert_file_exists "$MODULE" "$ENU_META_JSON"
p1_assert_nonempty_file "$MODULE" "$ENU_META_JSON"

mkdir -p "$OUTPUT_DIR"

DTM_TIF="$OUTPUT_DIR/DTM.tif"
DSM_TIF="$OUTPUT_DIR/DSM.tif"
CHM_TIF="$OUTPUT_DIR/CHM.tif"
DTM_HS="$OUTPUT_DIR/DTM_hillshade.tif"
DSM_HS="$OUTPUT_DIR/DSM_hillshade.tif"
GROUND_LAZ="$OUTPUT_DIR/dense_ground.laz"

rm -f "$DTM_TIF" "$DSM_TIF" "$CHM_TIF" "$DTM_HS" "$DSM_HS" "$GROUND_LAZ"

p1_metric "$MODULE" "dense_las" "$DENSE_LAS" "path"
p1_metric "$MODULE" "enu_meta_json" "$ENU_META_JSON" "path"
p1_metric "$MODULE" "output_dir" "$OUTPUT_DIR" "path"

p1_log_info "$MODULE" "Executando pdal info --summary na nuvem LAS de entrada"
p1_run_cmd "$MODULE" "PDAL Info" pdal info "$DENSE_LAS" --summary

p1_log_info "$MODULE" "Gerando DSM, DTM, hillshade e CHM"
python3 "$SCRIPT_DIR/p1_06_GPU_DEM.py" \
    --dense-las "$DENSE_LAS" \
    --output-dir "$OUTPUT_DIR" \
    --enu-meta-json "$ENU_META_JSON" \
    --log-file "$PIPELINE_LOG" \
    --metrics-csv "$METRICS_CSV" \
    --dataset "$DATASET" \
    --gpu "$GPU" \
    --module "$MODULE"

p1_assert_file_exists "$MODULE" "$DTM_TIF"
p1_assert_nonempty_file "$MODULE" "$DTM_TIF"

p1_assert_file_exists "$MODULE" "$DSM_TIF"
p1_assert_nonempty_file "$MODULE" "$DSM_TIF"

p1_assert_file_exists "$MODULE" "$CHM_TIF"
p1_assert_nonempty_file "$MODULE" "$CHM_TIF"

if [[ -f "$DTM_HS" ]]; then
    p1_assert_nonempty_file "$MODULE" "$DTM_HS"
fi

if [[ -f "$DSM_HS" ]]; then
    p1_assert_nonempty_file "$MODULE" "$DSM_HS"
fi

if [[ -f "$GROUND_LAZ" ]]; then
    p1_assert_nonempty_file "$MODULE" "$GROUND_LAZ"
fi

DTM_SIZE_BYTES="$(stat -c%s "$DTM_TIF")"
DSM_SIZE_BYTES="$(stat -c%s "$DSM_TIF")"
CHM_SIZE_BYTES="$(stat -c%s "$CHM_TIF")"

p1_metric "$MODULE" "dtm_size" "$DTM_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "dsm_size" "$DSM_SIZE_BYTES" "bytes"
p1_metric "$MODULE" "chm_size" "$CHM_SIZE_BYTES" "bytes"

if [[ -f "$DTM_HS" ]]; then
    DTM_HS_SIZE_BYTES="$(stat -c%s "$DTM_HS")"
    p1_metric "$MODULE" "dtm_hillshade_size" "$DTM_HS_SIZE_BYTES" "bytes"
fi

if [[ -f "$DSM_HS" ]]; then
    DSM_HS_SIZE_BYTES="$(stat -c%s "$DSM_HS")"
    p1_metric "$MODULE" "dsm_hillshade_size" "$DSM_HS_SIZE_BYTES" "bytes"
fi

if [[ -f "$GROUND_LAZ" ]]; then
    GROUND_LAZ_SIZE_BYTES="$(stat -c%s "$GROUND_LAZ")"
    p1_metric "$MODULE" "ground_laz_size" "$GROUND_LAZ_SIZE_BYTES" "bytes"
fi

p1_log_info "$MODULE" "Saídas geradas:"
p1_log_info "$MODULE" "DTM: $DTM_TIF"
p1_log_info "$MODULE" "DSM: $DSM_TIF"
p1_log_info "$MODULE" "CHM: $CHM_TIF"

if [[ -f "$DTM_HS" ]]; then
    p1_log_info "$MODULE" "DTM Hillshade: $DTM_HS"
fi

if [[ -f "$DSM_HS" ]]; then
    p1_log_info "$MODULE" "DSM Hillshade: $DSM_HS"
fi

if [[ -f "$GROUND_LAZ" ]]; then
    p1_log_info "$MODULE" "Ground LAZ: $GROUND_LAZ"
fi

p1_module_end "$MODULE" "$START_TS" "SUCCESS"
