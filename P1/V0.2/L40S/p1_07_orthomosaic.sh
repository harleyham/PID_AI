#!/bin/bash
set -euo pipefail

# Pipeline P1 - Módulo 07: Geração de ortomosaico
# Padrão consolidado:
# - usa p1_config.sh + p1_logging.sh
# - consome DSM/DTM do M06 + imagens undistorted do M04 + modelo dense/sparse
# - converte modelo COLMAP para TXT
# - gera ORTHO.tif via p1_07_orthomosaic.py
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

MODULE="M07"
p1_module_start "$MODULE" START_TS

: "${PYTHON_BIN:=python3}"
: "${OUTPUT_DIR:=$OUTPUT_PATH}"
: "${ORTHO_ENABLED:=1}"
: "${ORTHO_RESOLUTION:=0.03}"
: "${ORTHO_USE_DSM:=1}"
: "${ORTHO_MAX_CANDIDATES:=8}"
: "${ORTHO_TILE_SIZE:=1024}"
: "${ORTHO_BLEND_MODE:=best_angle}"
: "${ORTHO_COMPRESS:=DEFLATE}"
: "${ORTHO_JPEG_QUALITY:=90}"
: "${ORTHO_TIF:=$OUTPUT_PATH/ORTHO_${DATASET_SLUG}.tif}"
: "${ORTHO_VRT:=$OUTPUT_PATH/ORTHO_${DATASET_SLUG}.vrt}"
: "${ORTHO_PREVIEW_JPG:=$OUTPUT_PATH/ORTHO_preview_${DATASET_SLUG}.jpg}"

if [[ "$ORTHO_ENABLED" != "1" ]]; then
    p1_log_warn "$MODULE" "ORTHO_ENABLED != 1; módulo será ignorado"
    p1_metric "$MODULE" "skipped" "1" "bool" "SKIPPED" "ORTHO_ENABLED=$ORTHO_ENABLED"
    p1_module_end "$MODULE" "$START_TS" "SKIPPED"
    exit 0
fi

mkdir -p "$OUTPUT_PATH"

DSM_TIF="$OUTPUT_DIR/DSM_${DATASET_SLUG}.tif"
DTM_TIF="$OUTPUT_DIR/DTM_${DATASET_SLUG}.tif"
DSM_CLOSED_TIF="$OUTPUT_DIR/DSM_closed_${DATASET_SLUG}.tif"
DTM_CLOSED_TIF="$OUTPUT_DIR/DTM_closed_${DATASET_SLUG}.tif"
ORTHO_SURFACE_TIF="$OUTPUT_DIR/ORTHO_SURFACE_${DATASET_SLUG}.tif"

ORTHO_SURFACE="$DSM_TIF"

if [[ "$ORTHO_USE_DSM" != "1" ]]; then
    ORTHO_SURFACE="$DTM_TIF"
fi

if [[ -f "$ORTHO_SURFACE_TIF" ]]; then
    ORTHO_SURFACE="$ORTHO_SURFACE_TIF"
fi

# Preferir superfície fechada se existir
if [[ ! -f "$ORTHO_SURFACE_TIF" && "$ORTHO_USE_DSM" == "1" && -f "$DSM_CLOSED_TIF" ]]; then
    ORTHO_SURFACE="$DSM_CLOSED_TIF"
fi
if [[ ! -f "$ORTHO_SURFACE_TIF" && "$ORTHO_USE_DSM" != "1" && -f "$DTM_CLOSED_TIF" ]]; then
    ORTHO_SURFACE="$DTM_CLOSED_TIF"
fi

DENSE_SPARSE_DIR="$DENSE_PATH/sparse"
DENSE_SPARSE_TXT_DIR="$DENSE_PATH/sparse_txt"
DENSE_IMAGES_DIR="$DENSE_PATH/images"

p1_assert_file_exists "$MODULE" "$ORTHO_SURFACE"
p1_assert_nonempty_file "$MODULE" "$ORTHO_SURFACE"

p1_assert_dir_exists "$MODULE" "$DENSE_SPARSE_DIR"
p1_assert_dir_exists "$MODULE" "$DENSE_IMAGES_DIR"

p1_assert_file_exists "$MODULE" "$ENU_META_JSON"
p1_assert_nonempty_file "$MODULE" "$ENU_META_JSON"

mkdir -p "$DENSE_SPARSE_TXT_DIR"

rm -f "$ORTHO_TIF" "$ORTHO_VRT" "$ORTHO_PREVIEW_JPG"

p1_metric "$MODULE" "ortho_surface" "$ORTHO_SURFACE" "path"
p1_metric "$MODULE" "ortho_resolution" "$ORTHO_RESOLUTION" "m"
p1_metric "$MODULE" "ortho_use_dsm" "$ORTHO_USE_DSM" "bool"
p1_metric "$MODULE" "ortho_max_candidates" "$ORTHO_MAX_CANDIDATES" "count"
p1_metric "$MODULE" "ortho_tile_size" "$ORTHO_TILE_SIZE" "px"
p1_metric "$MODULE" "ortho_blend_mode" "$ORTHO_BLEND_MODE" "mode"
p1_metric "$MODULE" "ortho_compress" "$ORTHO_COMPRESS" "mode"
p1_metric "$MODULE" "ortho_jpeg_quality" "$ORTHO_JPEG_QUALITY" "value"
p1_metric "$MODULE" "dense_sparse_dir" "$DENSE_SPARSE_DIR" "path"
p1_metric "$MODULE" "dense_images_dir" "$DENSE_IMAGES_DIR" "path"

p1_log_info "$MODULE" "Convertendo modelo COLMAP de binário para TXT"
p1_run_cmd "$MODULE" "COLMAP model_converter" \
    "$COLMAP_BIN" model_converter \
        --input_path "$DENSE_SPARSE_DIR" \
        --output_path "$DENSE_SPARSE_TXT_DIR" \
        --output_type TXT

p1_assert_file_exists "$MODULE" "$DENSE_SPARSE_TXT_DIR/cameras.txt"
p1_assert_nonempty_file "$MODULE" "$DENSE_SPARSE_TXT_DIR/cameras.txt"
p1_assert_file_exists "$MODULE" "$DENSE_SPARSE_TXT_DIR/images.txt"
p1_assert_nonempty_file "$MODULE" "$DENSE_SPARSE_TXT_DIR/images.txt"

p1_log_info "$MODULE" "Gerando ortomosaico"
p1_run_cmd "$MODULE" "p1_07_orthomosaic.py" \
    "$PYTHON_BIN" "$SCRIPT_DIR/p1_07_orthomosaic.py" \
        --surface "$ORTHO_SURFACE" \
        --images-dir "$DENSE_IMAGES_DIR" \
        --model-dir "$DENSE_SPARSE_TXT_DIR" \
        --enu-meta-json "$ENU_META_JSON" \
        --output-tif "$ORTHO_TIF" \
        --output-vrt "$ORTHO_VRT" \
        --output-preview "$ORTHO_PREVIEW_JPG" \
        --resolution "$ORTHO_RESOLUTION" \
        --tile-size "$ORTHO_TILE_SIZE" \
        --blend-mode "$ORTHO_BLEND_MODE" \
        --max-candidates "$ORTHO_MAX_CANDIDATES" \
        --compress "$ORTHO_COMPRESS" \
        --jpeg-quality "$ORTHO_JPEG_QUALITY" \
        --log-file "$PIPELINE_LOG" \
        --metrics-csv "$METRICS_CSV" \
        --dataset "$DATASET" \
        --gpu "$GPU" \
        --module "$MODULE"

p1_assert_file_exists "$MODULE" "$ORTHO_TIF"
p1_assert_nonempty_file "$MODULE" "$ORTHO_TIF"

ORTHO_SIZE_BYTES="$(stat -c%s "$ORTHO_TIF")"
p1_metric "$MODULE" "ortho_size" "$ORTHO_SIZE_BYTES" "bytes"

if [[ -f "$ORTHO_VRT" ]]; then
    p1_assert_nonempty_file "$MODULE" "$ORTHO_VRT"
    VRT_SIZE_BYTES="$(stat -c%s "$ORTHO_VRT")"
    p1_metric "$MODULE" "ortho_vrt_size" "$VRT_SIZE_BYTES" "bytes"
fi

if [[ -f "$ORTHO_PREVIEW_JPG" ]]; then
    p1_assert_nonempty_file "$MODULE" "$ORTHO_PREVIEW_JPG"
    PREVIEW_SIZE_BYTES="$(stat -c%s "$ORTHO_PREVIEW_JPG")"
    p1_metric "$MODULE" "ortho_preview_size" "$PREVIEW_SIZE_BYTES" "bytes"
fi

p1_log_info "$MODULE" "Arquivo gerado em: $ORTHO_TIF"
p1_module_end "$MODULE" "$START_TS" "SUCCESS"
