#!/bin/bash
# Pipeline P1

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
    echo "INICIANDO PROCESSAMENTO - $(date) - GPU: $GPU - DATASET: $DATASET" | tee -a "$PIPELINE_LOG"
    echo "=====================================================" | tee -a "$PIPELINE_LOG"
}

log_pipeline_footer_success() {
    local total_seconds="$1"
    local total_hms="$2"
    echo "=====================================================" | tee -a "$PIPELINE_LOG"
    echo "PROCESSAMENTO CONCLUÍDO - $(date) - GPU: $GPU - DATASET: $DATASET" | tee -a "$PIPELINE_LOG"
    echo "TEMPO TOTAL DE EXECUÇÃO (até o fim do M07): ${total_seconds}s (${total_hms})" | tee -a "$PIPELINE_LOG"
    echo "=====================================================" | tee -a "$PIPELINE_LOG"
}

log_pipeline_footer_error() {
    local exit_code="$1"
    local end_ts elapsed elapsed_hms

    end_ts="$(date +%s)"
    elapsed=$(( end_ts - PIPELINE_START_TS ))
    elapsed_hms="$(format_duration "$elapsed")"

    echo "=====================================================" | tee -a "$PIPELINE_LOG"
    echo "PROCESSAMENTO INTERROMPIDO - $(date) - GPU: $GPU - DATASET: $DATASET - EXIT_CODE: $exit_code" | tee -a "$PIPELINE_LOG"
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

trap 'log_pipeline_footer_error $?' ERR

log_pipeline_header

run_step "00_check_env" "[PASSO 0A] Verificando ambiente..." "$SCRIPT_DIR/p1_00_check_env.sh"
run_step "00_env_snapshot" "[PASSO 0B] Registrando snapshot do ambiente..." "$SCRIPT_DIR/p1_00_env_snapshot.sh"

run_step "01_feature_extraction" "[PASSO 1] Extraindo coordenadas..." "$SCRIPT_DIR/p1_01_feature_extraction.sh"
run_step "02_exhaustive_matching" "[PASSO 2] Iniciando Feature Matching ..." "$SCRIPT_DIR/p1_02_exhaustive_matching.sh"
run_step "03_sparse_mapper" "[PASSO 3] Gerando Nuvem Esparsa e Alinhando ao GPS..." "$SCRIPT_DIR/p1_03_sparse_mapper.sh"
run_step "04_dense_reconstruction" "[PASSO 4] Iniciando Densificação (Patch Match Stereo)..." "$SCRIPT_DIR/p1_04_dense_reconstruction.sh"
run_step "05_export_dense" "[PASSO 5] Exportando nuvem densa ENU para LAS UTM..." "$SCRIPT_DIR/p1_05_export_dense_robusto.sh"
run_step "06_dem" "[PASSO 6] Gerando DTM / DSM / CHM / superfícies fechadas..." "$SCRIPT_DIR/p1_06_DEM.sh"
run_step "07_orthomosaic" "[PASSO 7] Gerando ortomosaico..." "$SCRIPT_DIR/p1_07_orthomosaic.sh"

PIPELINE_END_TS="$(date +%s)"
PIPELINE_TOTAL_SECONDS=$(( PIPELINE_END_TS - PIPELINE_START_TS ))
PIPELINE_TOTAL_HMS="$(format_duration "$PIPELINE_TOTAL_SECONDS")"

write_metric "PIPELINE" "total_runtime" "$PIPELINE_TOTAL_SECONDS" "seconds" "SUCCESS" "hhmmss=$PIPELINE_TOTAL_HMS"

run_step "08_report" "[PASSO 8] Gerando Relatório Dinâmico..." python3 "$SCRIPT_DIR/generate_ligem_report.py"

log_pipeline_footer_success "$PIPELINE_TOTAL_SECONDS" "$PIPELINE_TOTAL_HMS"