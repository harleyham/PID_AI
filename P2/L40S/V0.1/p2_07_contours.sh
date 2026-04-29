#!/bin/bash
set -euo pipefail

PROJECT_ROOT="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO"
P2_CODE_DIR="$PROJECT_ROOT/03_Scripts_Common/P2/L40S/V0.1"
source "$P2_CODE_DIR/p2_config.sh"
source "$P2_CODE_DIR/p2_logging.sh"

p2_ensure_dirs
p2_init_logs

MODULE="M07"
p2_module_start "$MODULE" START_TS

case "$CONTOUR_INPUT_MODE" in
    DTM_RAW)
        CONTOUR_SOURCE="$DTM_RAW_TIF"
        ;;
    DTM_REFINED)
        CONTOUR_SOURCE="$DTM_REFINED_TIF"
        ;;
    DSM)
        CONTOUR_SOURCE="$DSM_TIF"
        ;;
    *)
        p2_log_error "$MODULE" "CONTOUR_INPUT_MODE invalido: $CONTOUR_INPUT_MODE"
        exit 1
        ;;
esac

p2_metric "$MODULE" "contour_source" "$CONTOUR_SOURCE" "path"
p2_metric "$MODULE" "contour_output" "$CONTOUR_OUTPUT" "path"

if [[ "$P2_EXECUTION_MODE" == "stub" ]]; then
    p2_log_warn "$MODULE" "Modo stub ativo; registrando contrato sem gerar curvas fisicas"
    p2_run_cmd "$MODULE" "manifesto de curvas em stub" \
        "$PYTHON_BIN" "$P2_CODE_DIR/p2_07_contours_stat.py" \
        --mode stub \
        --input-raster "$CONTOUR_SOURCE" \
        --input-vector "$CONTOUR_OUTPUT" \
        --layer-name "$CONTOUR_LAYER_NAME" \
        --field-name "$CONTOUR_FIELD_NAME" \
        --format "$CONTOUR_FORMAT" \
        --manifest "$CONTOUR_MANIFEST_JSON"
    p2_module_end "$MODULE" "$START_TS" "SUCCESS"
    exit 0
fi

p2_assert_file_exists "$MODULE" "$CONTOUR_SOURCE"
p2_assert_nonempty_file "$MODULE" "$CONTOUR_SOURCE"
rm -f "$CONTOUR_OUTPUT"

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

p2_run_cmd "$MODULE" "gdal_contour" "${GDAL_CMD[@]}"
p2_assert_file_exists "$MODULE" "$CONTOUR_OUTPUT"
p2_assert_nonempty_file "$MODULE" "$CONTOUR_OUTPUT"

p2_run_cmd "$MODULE" "estatisticas de curvas" \
    "$PYTHON_BIN" "$P2_CODE_DIR/p2_07_contours_stat.py" \
    --mode live \
    --input-raster "$CONTOUR_SOURCE" \
    --input-vector "$CONTOUR_OUTPUT" \
    --layer-name "$CONTOUR_LAYER_NAME" \
    --field-name "$CONTOUR_FIELD_NAME" \
    --format "$CONTOUR_FORMAT" \
    --manifest "$CONTOUR_MANIFEST_JSON"

p2_module_end "$MODULE" "$START_TS" "SUCCESS"
