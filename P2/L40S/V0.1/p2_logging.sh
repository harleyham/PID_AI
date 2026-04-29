#!/bin/bash
set -euo pipefail

p2_now() {
    date "+%Y-%m-%d %H:%M:%S"
}

p2_require_logging_vars() {
    : "${LOG_DIR:?ERRO: LOG_DIR nao definido}"
    : "${PIPELINE_LOG:?ERRO: PIPELINE_LOG nao definido}"
    : "${METRICS_CSV:?ERRO: METRICS_CSV nao definido}"
    : "${DATASET:?ERRO: DATASET nao definido}"
    : "${GPU:?ERRO: GPU nao definido}"
}

p2_init_logs() {
    p2_require_logging_vars
    mkdir -p "$LOG_DIR"
    touch "$PIPELINE_LOG"

    if [[ ! -f "$METRICS_CSV" || ! -s "$METRICS_CSV" ]]; then
        printf 'timestamp;dataset;gpu;module;metric;value;unit;status;notes\n' > "$METRICS_CSV"
    fi
}

p2_log() {
    local level="$1"
    local module="$2"
    local message="$3"
    local ts
    ts="$(p2_now)"
    printf '[%s] [%s] [%s] [%s] [%s] %s\n' "$ts" "$level" "$DATASET" "$GPU" "$module" "$message" | tee -a "$PIPELINE_LOG"
}

p2_log_info() {
    p2_log "INFO" "$1" "$2"
}

p2_log_warn() {
    p2_log "WARN" "$1" "$2"
}

p2_log_error() {
    p2_log "ERROR" "$1" "$2"
}

p2_metric() {
    local module="$1"
    local metric_name="$2"
    local value="$3"
    local unit="${4:-}"
    local status="${5:-SUCCESS}"
    local notes="${6:-}"
    local ts
    ts="$(p2_now)"

    printf '%s;%s;%s;%s;%s;%s;%s;%s;%s\n' \
        "$ts" "$DATASET" "$GPU" "$module" "$metric_name" "$value" "$unit" "$status" "$notes" \
        >> "$METRICS_CSV"
}

p2_module_start() {
    local module="$1"
    local __resultvar="$2"
    local ts_epoch
    ts_epoch="$(date +%s)"
    printf -v "$__resultvar" '%s' "$ts_epoch"
    p2_log_info "$module" "Iniciando modulo"
    p2_metric "$module" "module_start" "$(p2_now)" "datetime" "STARTED"
}

p2_module_end() {
    local module="$1"
    local start_ts="$2"
    local status="${3:-SUCCESS}"
    local end_ts duration
    end_ts="$(date +%s)"
    duration=$((end_ts - start_ts))
    p2_log_info "$module" "Modulo concluido em ${duration}s"
    p2_metric "$module" "duration" "$duration" "seconds" "$status"
    p2_metric "$module" "module_end" "$(p2_now)" "datetime" "$status"
}

p2_run_cmd() {
    local module="$1"
    local desc="$2"
    shift 2
    p2_log_info "$module" "Executando: $desc"

    if "$@"; then
        p2_log_info "$module" "Concluido: $desc"
    else
        local exit_code=$?
        p2_log_error "$module" "Falha em: $desc (exit_code=${exit_code})"
        return "$exit_code"
    fi
}

p2_assert_file_exists() {
    local module="$1"
    local filepath="$2"

    if [[ ! -f "$filepath" ]]; then
        p2_log_error "$module" "Arquivo nao encontrado: $filepath"
        p2_metric "$module" "file_exists" "0" "bool" "FAILED" "$filepath"
        exit 1
    fi

    p2_metric "$module" "file_exists" "1" "bool" "SUCCESS" "$filepath"
}

p2_assert_nonempty_file() {
    local module="$1"
    local filepath="$2"

    if [[ ! -s "$filepath" ]]; then
        p2_log_error "$module" "Arquivo vazio ou inexistente: $filepath"
        p2_metric "$module" "file_nonempty" "0" "bool" "FAILED" "$filepath"
        exit 1
    fi

    p2_metric "$module" "file_nonempty" "1" "bool" "SUCCESS" "$filepath"
}
