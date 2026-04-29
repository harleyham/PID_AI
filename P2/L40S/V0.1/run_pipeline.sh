#!/bin/bash
set -euo pipefail

PROJECT_ROOT="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO"
P2_CODE_DIR="$PROJECT_ROOT/03_Scripts_Common/P2/L40S/V0.1"
source "$P2_CODE_DIR/p2_config.sh"
source "$P2_CODE_DIR/p2_logging.sh"

p2_ensure_dirs
p2_init_logs

PIPELINE_START_TS="$(date +%s)"

format_duration() {
    local total_seconds="$1"
    local hh mm ss
    hh=$(( total_seconds / 3600 ))
    mm=$(( (total_seconds % 3600) / 60 ))
    ss=$(( total_seconds % 60 ))
    printf "%02d:%02d:%02d" "$hh" "$mm" "$ss"
}

log_pipeline_header() {
    p2_log_info "PIPELINE" "Iniciando processamento P2 $P2_PIPELINE_KIND/$P2_PIPELINE_VERSION em modo $P2_EXECUTION_MODE"
    p2_metric "PIPELINE" "pipeline_kind" "$P2_PIPELINE_KIND" "kind"
    p2_metric "PIPELINE" "pipeline_version" "$P2_PIPELINE_VERSION" "version"
    p2_metric "PIPELINE" "model_version" "$P2_MODEL_VERSION" "version"
    p2_metric "PIPELINE" "execution_mode" "$P2_EXECUTION_MODE" "mode"
}

log_pipeline_footer_success() {
    local total_seconds="$1"
    local total_hms="$2"
    p2_log_info "PIPELINE" "Pipeline concluido em ${total_seconds}s (${total_hms})"
    p2_metric "PIPELINE" "total_runtime" "$total_seconds" "seconds" "SUCCESS" "hhmmss=$total_hms"
}

log_pipeline_footer_error() {
    local exit_code="$1"
    local end_ts elapsed elapsed_hms
    end_ts="$(date +%s)"
    elapsed=$(( end_ts - PIPELINE_START_TS ))
    elapsed_hms="$(format_duration "$elapsed")"
    p2_log_error "PIPELINE" "Pipeline interrompido com exit_code=${exit_code} apos ${elapsed}s (${elapsed_hms})"
    p2_metric "PIPELINE" "total_runtime_until_failure" "$elapsed" "seconds" "FAILED" "exit_code=$exit_code;hhmmss=$elapsed_hms"
}

run_step() {
    local enabled_flag="$1"
    local step_key="$2"
    local message="$3"
    shift 3

    if [[ "$enabled_flag" != "1" ]]; then
        p2_log_warn "PIPELINE" "${step_key} desabilitado; passo ignorado"
        p2_metric "PIPELINE" "step_${step_key}_skipped" "1" "bool" "SKIPPED"
        return 0
    fi

    local start_ts end_ts elapsed elapsed_hms
    start_ts="$(date +%s)"
    p2_log_info "PIPELINE" "$message"
    "$@"
    end_ts="$(date +%s)"
    elapsed=$(( end_ts - start_ts ))
    elapsed_hms="$(format_duration "$elapsed")"
    p2_metric "PIPELINE" "step_${step_key}_runtime" "$elapsed" "seconds" "SUCCESS" "hhmmss=$elapsed_hms"
}

trap 'log_pipeline_footer_error $?' ERR

log_pipeline_header

run_step "$P2_01_ENABLED" "01_ingest" "[PASSO 1] Ingestao e normalizacao da nuvem..." \
    "$PYTHON_BIN" "$P2_CODE_DIR/p2_01_ingest.py" \
    --input-point-cloud "$P2_INPUT_POINT_CLOUD" \
    --normalize-mode "$P2_NORMALIZE_MODE" \
    --normalized-point-cloud "$NORMALIZED_POINT_CLOUD" \
    --output-manifest "$INGEST_MANIFEST_JSON"

run_step "$P2_02_ENABLED" "02_diagnostico" "[PASSO 2] Diagnostico da nuvem de pontos..." \
    "$PYTHON_BIN" "$P2_CODE_DIR/p2_02_diagnostico.py" \
    --input-point-cloud "$NORMALIZED_POINT_CLOUD" \
    --output-report "$DIAGNOSTIC_JSON"

run_step "$P2_03_ENABLED" "03_preprocess_dataset" "[PASSO 3] Preparando dataset 2D para segmentacao semantica..." \
    "$PYTHON_BIN" "$P2_CODE_DIR/p2_03_preprocess.py" \
    --input-point-cloud "$NORMALIZED_POINT_CLOUD" \
    --tile-size-meters "$TILE_SIZE_METERS" \
    --tile-overlap-meters "$TILE_OVERLAP_METERS" \
    --voxel-size-meters "$PREPROCESS_VOXEL_SIZE_METERS" \
    --min-tile-area-percent "$PREPROCESS_MIN_TILE_AREA_PERCENT" \
    --label-source "$PREPROCESS_LABEL_SOURCE" \
    --asprs-ground-classes "$PREPROCESS_ASPRS_GROUND_CLASSES" \
    --asprs-vegetation-classes "$PREPROCESS_ASPRS_VEGETATION_CLASSES" \
    --asprs-building-classes "$PREPROCESS_ASPRS_BUILDING_CLASSES" \
    --asprs-paved-surface-classes "$PREPROCESS_ASPRS_PAVED_SURFACE_CLASSES" \
    --asprs-water-classes "$PREPROCESS_ASPRS_WATER_CLASSES" \
    --min-labeled-point-percent "$PREPROCESS_MIN_LABELED_POINT_PERCENT" \
    --tiles-dir "$TILES_DIR" \
    --output-manifest "$PREPROCESS_MANIFEST_JSON"

run_step "$P2_04_ENABLED" "04_ground_ai" "[PASSO 4] Executando SegFormer para treino/inferencia semantica..." \
    "$PYTHON_BIN" "$P2_CODE_DIR/p2_04_ground_ai.py" \
    --input-point-cloud "$NORMALIZED_POINT_CLOUD" \
    --output-point-cloud "$CLASSIFIED_POINT_CLOUD" \
    --model-name "$GROUND_MODEL_NAME" \
    --batch-size "$GROUND_BATCH_SIZE" \
    --device "$GROUND_DEVICE" \
    --execution-mode "$P2_EXECUTION_MODE" \
    --pipeline-kind "$P2_PIPELINE_KIND" \
    --pipeline-version "$P2_PIPELINE_VERSION" \
    --model-version "$P2_MODEL_VERSION" \
    --model-registry-root "$MODEL_REGISTRY_ROOT" \
    --data-host-root "$TAO_DATA_HOST" \
    --scripts-host-root "$SCRIPTS_HOST" \
    --results-host-root "$RESULTS_HOST" \
    --container-data-root "$TAO_CONTAINER_DATA_ROOT" \
    --container-scripts-root "$TAO_CONTAINER_SCRIPTS_ROOT" \
    --container-results-root "$TAO_CONTAINER_RESULTS_ROOT" \
    --docker-compose-file "$TAO_DOCKER_COMPOSE_FILE" \
    --spec-file "$GROUND_TAO_SPEC_FILE" \
    --generated-spec-file "$GROUND_GENERATED_SPEC_FILE" \
    --dataset-root "$GROUND_DATASET_ROOT" \
    --dataset-manifest "$GROUND_DATASET_MANIFEST" \
    --classes-manifest "$GROUND_CLASSES_MANIFEST" \
    --results-dir "$GROUND_TAO_RESULTS_DIR" \
    --tao-subcommand "$GROUND_TAO_SUBCOMMAND" \
    --tao-extra-args "$GROUND_TAO_EXTRA_ARGS" \
    --train-epochs "$GROUND_TRAIN_EPOCHS" \
    --train-workers "$GROUND_TRAIN_WORKERS" \
    --output-manifest "$GROUND_AI_MANIFEST_JSON"

run_step "$P2_05_ENABLED" "05_dtm_filter" "[PASSO 5] Filtrando solo e preparando geracao de DTM/DSM..." \
    "$PYTHON_BIN" "$P2_CODE_DIR/p2_05_dtm.py" \
    --input-point-cloud "$CLASSIFIED_POINT_CLOUD" \
    --dtm-raster "$DTM_RAW_TIF" \
    --dsm-raster "$DSM_TIF" \
    --void-mask-raster "$VOID_MASK_TIF" \
    --confidence-raster "$CONFIDENCE_TIF" \
    --resolution "$DTM_RESOLUTION" \
    --interpolation "$DTM_INTERPOLATION" \
    --power "$DTM_POWER" \
    --search-radius "$DTM_SEARCH_RADIUS" \
    --output-manifest "$DTM_MANIFEST_JSON"

run_step "$P2_06_ENABLED" "06_refine_dtm" "[PASSO 6] Planejando refinamento do DTM..." \
    "$PYTHON_BIN" "$P2_CODE_DIR/p2_06_refine_dtm.py" \
    --input-dtm "$DTM_RAW_TIF" \
    --output-dtm "$DTM_REFINED_TIF" \
    --fill-distance "$REFINE_FILL_DISTANCE" \
    --smoothing-passes "$REFINE_SMOOTHING_PASSES" \
    --preserve-breaklines "$REFINE_PRESERVE_BREAKLINES" \
    --output-manifest "$REFINE_MANIFEST_JSON"

run_step "$P2_07_ENABLED" "07_contours" "[PASSO 7] Gerando curvas de nivel..." \
    "$P2_CODE_DIR/p2_07_contours.sh"

run_step "$P2_08_ENABLED" "08_quality" "[PASSO 8] Consolidando qualidade e rastreabilidade..." \
    "$PYTHON_BIN" "$P2_CODE_DIR/p2_08_quality.py" \
    --diagnostic-report "$DIAGNOSTIC_JSON" \
    --dtm-manifest "$DTM_MANIFEST_JSON" \
    --refine-manifest "$REFINE_MANIFEST_JSON" \
    --contour-manifest "$CONTOUR_MANIFEST_JSON" \
    --quality-report "$QUALITY_REPORT_JSON"

PIPELINE_END_TS="$(date +%s)"
PIPELINE_TOTAL_SECONDS=$(( PIPELINE_END_TS - PIPELINE_START_TS ))
PIPELINE_TOTAL_HMS="$(format_duration "$PIPELINE_TOTAL_SECONDS")"

log_pipeline_footer_success "$PIPELINE_TOTAL_SECONDS" "$PIPELINE_TOTAL_HMS"
