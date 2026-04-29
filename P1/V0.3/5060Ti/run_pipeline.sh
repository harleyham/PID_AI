#!/bin/bash
# Pipeline P1 / P1B

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/p1_config.sh"

source "$CONFIG_FILE"

mkdir -p "$LOG_DIR"
touch "$PIPELINE_LOG"
touch "$METRICS_CSV"

PIPELINE_START_TS="$(date +%s)"

format_duration() {
    local total_seconds="$1"
    local hh mm ss
    hh=$(( total_seconds / 3600 ))
    mm=$(( (total_seconds % 3600) / 60 ))
    ss=$(( total_seconds % 60 ))
    printf "%02d:%02d:%02d" "$hh" "$mm" "$ss"
}

write_metric() {
    local module="$1"
    local metric_name="$2"
    local value="$3"
    local unit="$4"
    local status="${5:-SUCCESS}"
    local notes="${6:-}"
    echo "$(date '+%Y-%m-%d %H:%M:%S');$DATASET;$GPU;$module;$metric_name;$value;$unit;$status;$notes" >> "$METRICS_CSV"
}

log_pipeline_header() {
    echo "=====================================================" | tee -a "$PIPELINE_LOG"
    echo "INICIANDO PROCESSAMENTO - $(date) - PIPELINE: ${PIPELINE_NAME:-P1} - GPU: $GPU - DATASET: $DATASET - RUN_MODE: ${PIPELINE_RUN_MODE:-full}" | tee -a "$PIPELINE_LOG"
    echo "=====================================================" | tee -a "$PIPELINE_LOG"
}

log_pipeline_footer_success() {
    local total_seconds="$1"
    local total_hms="$2"
    echo "=====================================================" | tee -a "$PIPELINE_LOG"
    echo "PROCESSAMENTO CONCLUÍDO - $(date) - PIPELINE: ${PIPELINE_NAME:-P1} - GPU: $GPU - DATASET: $DATASET - RUN_MODE: ${PIPELINE_RUN_MODE:-full}" | tee -a "$PIPELINE_LOG"
    echo "TEMPO TOTAL DE EXECUÇÃO: ${total_seconds}s (${total_hms})" | tee -a "$PIPELINE_LOG"
    echo "=====================================================" | tee -a "$PIPELINE_LOG"
}

log_pipeline_footer_error() {
    local exit_code="$1"
    local end_ts elapsed elapsed_hms

    end_ts="$(date +%s)"
    elapsed=$(( end_ts - PIPELINE_START_TS ))
    elapsed_hms="$(format_duration "$elapsed")"

    echo "=====================================================" | tee -a "$PIPELINE_LOG"
    echo "PROCESSAMENTO INTERROMPIDO - $(date) - PIPELINE: ${PIPELINE_NAME:-P1} - GPU: $GPU - DATASET: $DATASET - RUN_MODE: ${PIPELINE_RUN_MODE:-full} - EXIT_CODE: $exit_code" | tee -a "$PIPELINE_LOG"
    echo "TEMPO TOTAL ATÉ A FALHA: ${elapsed}s (${elapsed_hms})" | tee -a "$PIPELINE_LOG"
    echo "=====================================================" | tee -a "$PIPELINE_LOG"

    write_metric "PIPELINE" "total_runtime_until_failure" "$elapsed" "seconds" "FAILED" "exit_code=$exit_code"
}

run_step() {
    local step_key="$1"
    local message="$2"
    shift 2

    local start_ts end_ts elapsed elapsed_hms
    start_ts="$(date +%s)"

    echo "$message" | tee -a "$PIPELINE_LOG"
    "$@"

    end_ts="$(date +%s)"
    elapsed=$(( end_ts - start_ts ))
    elapsed_hms="$(format_duration "$elapsed")"

    echo "[${step_key}] concluído em ${elapsed}s (${elapsed_hms})" | tee -a "$PIPELINE_LOG"
    write_metric "PIPELINE" "step_${step_key}_runtime" "$elapsed" "seconds" "SUCCESS" "hhmmss=$elapsed_hms"
}

skip_step() {
    local step_key="$1"
    local message="$2"
    local notes="${3:-}"

    echo "$message" | tee -a "$PIPELINE_LOG"
    echo "[${step_key}] ignorado" | tee -a "$PIPELINE_LOG"
    write_metric "PIPELINE" "step_${step_key}_runtime" "0" "seconds" "SKIPPED" "$notes"
}

validate_partial_inputs() {
    : "${DENSE_LAS:?ERRO: DENSE_LAS não definido no modo from_m06}"

    if [[ ! -f "$DENSE_LAS" ]]; then
        echo "ERRO: arquivo LAS externo não encontrado: $DENSE_LAS" | tee -a "$PIPELINE_LOG"
        write_metric "PIPELINE" "external_dense_las_exists" "0" "bool" "FAILED" "$DENSE_LAS"
        exit 1
    fi

    if [[ ! -s "$DENSE_LAS" ]]; then
        echo "ERRO: arquivo LAS externo vazio: $DENSE_LAS" | tee -a "$PIPELINE_LOG"
        write_metric "PIPELINE" "external_dense_las_nonempty" "0" "bool" "FAILED" "$DENSE_LAS"
        exit 1
    fi

    if [[ -n "${ENU_META_JSON:-}" && -f "$ENU_META_JSON" && -s "$ENU_META_JSON" ]]; then
        write_metric "PIPELINE" "enu_meta_exists" "1" "bool" "SUCCESS" "$ENU_META_JSON"
    else
        echo "[WARN] ENU_META_JSON ausente; M06 tentará inferir o EPSG do LAS/LAZ de entrada" | tee -a "$PIPELINE_LOG"
        write_metric "PIPELINE" "enu_meta_exists" "0" "bool" "WARNING" "M06_infer_epsg_from_las"
    fi

    write_metric "PIPELINE" "external_dense_las_exists" "1" "bool" "SUCCESS" "$DENSE_LAS"
}

run_full_pipeline() {
    run_step "00_check_env" "[PASSO 0A] Verificando ambiente..." "$SCRIPT_DIR/p1_00_check_env.sh"
    run_step "00_env_snapshot" "[PASSO 0B] Registrando snapshot do ambiente..." "$SCRIPT_DIR/p1_00_env_snapshot.sh"

    run_step "01_feature_extraction" "[PASSO 1] Extraindo coordenadas..." "$SCRIPT_DIR/p1_01_feature_extraction.sh"
    run_step "02_exhaustive_matching" "[PASSO 2] Iniciando Feature Matching ..." "$SCRIPT_DIR/p1_02_exhaustive_matching.sh"
    run_step "03_sparse_mapper" "[PASSO 3] Gerando Nuvem Esparsa e Alinhando ao GPS..." "$SCRIPT_DIR/p1_03_sparse_mapper.sh"
    run_step "04_dense_reconstruction" "[PASSO 4] Iniciando Densificação (Patch Match Stereo)..." "$SCRIPT_DIR/p1_04_dense_reconstruction.sh"
    run_step "05_export_dense" "[PASSO 5] Exportando nuvem densa ENU para LAS UTM..." "$SCRIPT_DIR/p1_05_export_dense_robusto.sh"
    run_step "06_dem" "[PASSO 6] Gerando DTM / DSM / CHM / superfícies fechadas..." "$SCRIPT_DIR/p1_06_DEM.sh"
    run_step "07_orthomosaic" "[PASSO 7] Gerando ortomosaico..." "$SCRIPT_DIR/p1_07_orthomosaic.sh"
    run_step "08_contours" "[PASSO 8] Gerando curvas de nível..." "$SCRIPT_DIR/p1_08_contours.sh"
}

run_from_m06_pipeline() {
    validate_partial_inputs

    echo "[INFO] Modo parcial detectado: execução iniciará no M06 usando LAS externo" | tee -a "$PIPELINE_LOG"
    echo "[INFO] Origem da nuvem densa: ${DENSE_SOURCE_NAME:-EXTERNA}" | tee -a "$PIPELINE_LOG"
    echo "[INFO] LAS de entrada: $DENSE_LAS" | tee -a "$PIPELINE_LOG"

    write_metric "PIPELINE" "run_mode" "${PIPELINE_RUN_MODE:-from_m06}" "mode"
    write_metric "PIPELINE" "dense_source_name" "${DENSE_SOURCE_NAME:-EXTERNA}" "source"
    write_metric "PIPELINE" "external_dense_las" "$DENSE_LAS" "path"
    write_metric "PIPELINE" "partial_processing_start_module" "M06" "module"

    skip_step "00_check_env" "[PASSO 0A] Verificação de ambiente ignorada no modo parcial." "PIPELINE_RUN_MODE=from_m06"
    skip_step "00_env_snapshot" "[PASSO 0B] Snapshot de ambiente ignorado no modo parcial." "PIPELINE_RUN_MODE=from_m06"

    skip_step "01_feature_extraction" "[PASSO 1] Feature extraction ignorado no modo parcial." "PIPELINE_RUN_MODE=from_m06"
    skip_step "02_exhaustive_matching" "[PASSO 2] Matching ignorado no modo parcial." "PIPELINE_RUN_MODE=from_m06"
    skip_step "03_sparse_mapper" "[PASSO 3] Sparse mapper ignorado no modo parcial." "PIPELINE_RUN_MODE=from_m06"
    skip_step "04_dense_reconstruction" "[PASSO 4] Reconstrução densa ignorada no modo parcial." "PIPELINE_RUN_MODE=from_m06"
    skip_step "05_export_dense" "[PASSO 5] Exportação da nuvem densa ignorada no modo parcial." "PIPELINE_RUN_MODE=from_m06"

    run_step "06_dem" "[PASSO 6] Gerando DTM / DSM / CHM / superfícies fechadas..." "$SCRIPT_DIR/p1_06_DEM.sh"

    skip_step "07_orthomosaic" "[PASSO 7] Ortomosaico ignorado no modo parcial." "PIPELINE_RUN_MODE=from_m06"

    run_step "08_contours" "[PASSO 8] Gerando curvas de nível..." "$SCRIPT_DIR/p1_08_contours.sh"
}

trap 'log_pipeline_footer_error $?' ERR

log_pipeline_header

write_metric "PIPELINE" "pipeline_name" "${PIPELINE_NAME:-P1}" "name"
write_metric "PIPELINE" "run_mode" "${PIPELINE_RUN_MODE:-full}" "mode"

case "${PIPELINE_RUN_MODE:-full}" in
    full)
        run_full_pipeline
        ;;
    from_m06)
        run_from_m06_pipeline
        ;;
    *)
        echo "ERRO: PIPELINE_RUN_MODE inválido: ${PIPELINE_RUN_MODE}" | tee -a "$PIPELINE_LOG"
        write_metric "PIPELINE" "run_mode_valid" "0" "bool" "FAILED" "${PIPELINE_RUN_MODE}"
        exit 1
        ;;
esac

PIPELINE_END_TS="$(date +%s)"
PIPELINE_TOTAL_SECONDS=$(( PIPELINE_END_TS - PIPELINE_START_TS ))
PIPELINE_TOTAL_HMS="$(format_duration "$PIPELINE_TOTAL_SECONDS")"

write_metric "PIPELINE" "total_runtime" "$PIPELINE_TOTAL_SECONDS" "seconds" "SUCCESS" "hhmmss=$PIPELINE_TOTAL_HMS"

run_step "09_report" "[PASSO 9] Gerando Relatório Dinâmico..." "$PYTHON_BIN" "$SCRIPT_DIR/generate_ligem_report.py"

log_pipeline_footer_success "$PIPELINE_TOTAL_SECONDS" "$PIPELINE_TOTAL_HMS"
