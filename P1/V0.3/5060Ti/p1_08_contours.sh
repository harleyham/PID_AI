#!/bin/bash
set -euo pipefail

# Pipeline P1 - Módulo 08: Geração de curvas de nível
# Padrão consolidado:
# - usa p1_config.sh + p1_logging.sh
# - consome raster de elevação gerado no M06
# - gera curvas por gdal_contour
# - valida saída e registra métricas

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

MODULE="M08"
p1_module_start "$MODULE" START_TS

: "${PYTHON_BIN:=python3}"
: "${OUTPUT_DIR:=$OUTPUT_PATH}"

: "${CONTOUR_ENABLED:=1}"
: "${CONTOUR_INPUT_MODE:=DTM_CLOSED}"
: "${CONTOUR_INTERVAL:=1.0}"
: "${CONTOUR_BASE:=0.0}"
: "${CONTOUR_FIELD_NAME:=elev}"
: "${CONTOUR_FORMAT:=GPKG}"
: "${CONTOUR_LAYER_NAME:=contours}"
: "${CONTOUR_OUTPUT:=$OUTPUT_PATH/contours_1m_${DATASET_SLUG}.gpkg}"
: "${CONTOUR_BAND:=1}"
: "${CONTOUR_IGNORE_NODATA:=1}"
: "${CONTOUR_3D:=0}"

if [[ "$CONTOUR_ENABLED" != "1" ]]; then
    p1_log_warn "$MODULE" "CONTOUR_ENABLED != 1; módulo será ignorado"
    p1_metric "$MODULE" "skipped" "1" "bool" "SKIPPED" "CONTOUR_ENABLED=$CONTOUR_ENABLED"
    p1_module_end "$MODULE" "$START_TS" "SKIPPED"
    exit 0
fi

mkdir -p "$OUTPUT_PATH"

DTM_TIF="$OUTPUT_DIR/DTM_${DATASET_SLUG}.tif"
DSM_TIF="$OUTPUT_DIR/DSM_${DATASET_SLUG}.tif"
DTM_CLOSED_TIF="$OUTPUT_DIR/DTM_closed_${DATASET_SLUG}.tif"
DSM_CLOSED_TIF="$OUTPUT_DIR/DSM_closed_${DATASET_SLUG}.tif"
ORTHO_SURFACE_TIF="$OUTPUT_DIR/ORTHO_SURFACE_${DATASET_SLUG}.tif"

case "$CONTOUR_INPUT_MODE" in
    DTM)
        CONTOUR_SOURCE="$DTM_TIF"
        ;;
    DSM)
        CONTOUR_SOURCE="$DSM_TIF"
        ;;
    DTM_CLOSED)
        CONTOUR_SOURCE="$DTM_CLOSED_TIF"
        ;;
    DSM_CLOSED)
        CONTOUR_SOURCE="$DSM_CLOSED_TIF"
        ;;
    ORTHO_SURFACE)
        CONTOUR_SOURCE="$ORTHO_SURFACE_TIF"
        ;;
    *)
        p1_fail_module "$MODULE" "CONTOUR_INPUT_MODE inválido: $CONTOUR_INPUT_MODE"
        ;;
esac

p1_assert_file_exists "$MODULE" "$CONTOUR_SOURCE"
p1_assert_nonempty_file "$MODULE" "$CONTOUR_SOURCE"

rm -f "$CONTOUR_OUTPUT"

p1_metric "$MODULE" "contour_source" "$CONTOUR_SOURCE" "path"
p1_metric "$MODULE" "contour_interval" "$CONTOUR_INTERVAL" "m"
p1_metric "$MODULE" "contour_base" "$CONTOUR_BASE" "m"
p1_metric "$MODULE" "contour_field_name" "$CONTOUR_FIELD_NAME" "field"
p1_metric "$MODULE" "contour_format" "$CONTOUR_FORMAT" "format"
p1_metric "$MODULE" "contour_layer_name" "$CONTOUR_LAYER_NAME" "layer"
p1_metric "$MODULE" "contour_output" "$CONTOUR_OUTPUT" "path"
p1_metric "$MODULE" "contour_band" "$CONTOUR_BAND" "id"
p1_metric "$MODULE" "contour_ignore_nodata" "$CONTOUR_IGNORE_NODATA" "bool"
p1_metric "$MODULE" "contour_3d" "$CONTOUR_3D" "bool"

p1_log_info "$MODULE" "Inspecionando raster de entrada"
p1_run_cmd "$MODULE" "gdalinfo raster de curvas" \
    gdalinfo "$CONTOUR_SOURCE"

GDAL_CMD=(
    gdal_contour
    -b "$CONTOUR_BAND"
    -a "$CONTOUR_FIELD_NAME"
    -i "$CONTOUR_INTERVAL"
    -off "$CONTOUR_BASE"
    -of "$CONTOUR_FORMAT"
)

if [[ "$CONTOUR_IGNORE_NODATA" == "1" ]]; then
    GDAL_CMD+=(-inodata)
fi

if [[ "$CONTOUR_3D" == "1" ]]; then
    GDAL_CMD+=(-3d)
fi

GDAL_CMD+=(
    "$CONTOUR_SOURCE"
    "$CONTOUR_OUTPUT"
    -lco "OVERWRITE=YES"
    -nln "$CONTOUR_LAYER_NAME"
)

p1_log_info "$MODULE" "Gerando curvas de nível"
p1_run_cmd "$MODULE" "gdal_contour" "${GDAL_CMD[@]}"

p1_assert_file_exists "$MODULE" "$CONTOUR_OUTPUT"
p1_assert_nonempty_file "$MODULE" "$CONTOUR_OUTPUT"

CONTOUR_SIZE_BYTES="$(stat -c%s "$CONTOUR_OUTPUT")"
p1_metric "$MODULE" "contour_output_size" "$CONTOUR_SIZE_BYTES" "bytes"

p1_log_info "$MODULE" "Calculando estatísticas das curvas"
p1_run_cmd "$MODULE" "p1_08_contours_stat.py" \
    "$PYTHON_BIN" "$SCRIPT_DIR/p1_08_contours_stat.py" \
        --input-vector "$CONTOUR_OUTPUT" \
        --input-raster "$CONTOUR_SOURCE" \
        --layer-name "$CONTOUR_LAYER_NAME" \
        --field-name "$CONTOUR_FIELD_NAME" \
        --format "$CONTOUR_FORMAT" \
        --log-file "$PIPELINE_LOG" \
        --metrics-csv "$METRICS_CSV" \
        --dataset "$DATASET" \
        --gpu "$GPU" \
        --module "$MODULE"

p1_log_info "$MODULE" "Curvas geradas em: $CONTOUR_OUTPUT"

p1_module_end "$MODULE" "$START_TS" "SUCCESS"