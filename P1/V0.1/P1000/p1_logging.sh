#!/bin/bash
set -euo pipefail

p1_now() {
    date "+%Y-%m-%d %H:%M:%S"
}

p1_require_logging_vars() {
    : "${LOG_DIR:?ERRO: LOG_DIR não definido}"
    : "${PIPELINE_LOG:?ERRO: PIPELINE_LOG não definido}"
    : "${METRICS_CSV:?ERRO: METRICS_CSV não definido}"
    : "${DATASET:?ERRO: DATASET não definido}"
    : "${GPU:?ERRO: GPU não definido}"
}

p1_init_logs() {
    p1_require_logging_vars
    mkdir -p "$LOG_DIR"

    touch "$PIPELINE_LOG"

    if [[ ! -f "$METRICS_CSV" || ! -s "$METRICS_CSV" ]]; then
        echo "timestamp;dataset;gpu;module;metric;value;unit;status;notes" > "$METRICS_CSV"
    fi
}

p1_log() {
    local level="$1"
    local module="$2"
    local message="$3"

    local ts
    ts="$(p1_now)"

    echo "[$ts] [$level] [$DATASET] [$GPU] [$module] $message" | tee -a "$PIPELINE_LOG"
}

p1_log_info() {
    p1_log "INFO" "$1" "$2"
}

p1_log_warn() {
    p1_log "WARN" "$1" "$2"
}

p1_log_error() {
    p1_log "ERROR" "$1" "$2"
}

p1_metric() {
    local module="$1"
    local metric_name="$2"
    local value="$3"
    local unit="${4:-}"
    local status="${5:-SUCCESS}"
    local notes="${6:-}"

    local ts
    ts="$(p1_now)"

    printf '%s;%s;%s;%s;%s;%s;%s;%s;%s\n' \
        "$ts" "$DATASET" "$GPU" "$module" "$metric_name" "$value" "$unit" "$status" "$notes" \
        >> "$METRICS_CSV"
}

p1_module_start() {
    local module="$1"
    local __resultvar="$2"

    local ts_epoch ts_human
    ts_epoch="$(date +%s)"
    ts_human="$(p1_now)"

    printf -v "$__resultvar" '%s' "$ts_epoch"

    p1_log_info "$module" "Iniciando módulo"
    p1_metric "$module" "module_start" "$ts_human" "datetime" "STARTED"
}

p1_module_end() {
    local module="$1"
    local start_ts="$2"
    local status="${3:-SUCCESS}"

    local end_ts duration end_human
    end_ts="$(date +%s)"
    end_human="$(p1_now)"
    duration=$((end_ts - start_ts))

    p1_log_info "$module" "Módulo concluído em ${duration}s"
    p1_metric "$module" "duration" "$duration" "seconds" "$status"
    p1_metric "$module" "module_end" "$end_human" "datetime" "$status"
}

p1_run_cmd() {
    local module="$1"
    local desc="$2"
    shift 2

    p1_log_info "$module" "Executando: $desc"

    if "$@"; then
        p1_log_info "$module" "Concluído: $desc"
    else
        local exit_code=$?
        p1_log_error "$module" "Falha em: $desc (exit_code=${exit_code})"
        return "$exit_code"
    fi
}

p1_assert_file_exists() {
    local module="$1"
    local filepath="$2"

    if [[ ! -f "$filepath" ]]; then
        p1_log_error "$module" "Arquivo não encontrado: $filepath"
        p1_metric "$module" "file_exists" "0" "bool" "FAILED" "$filepath"
        exit 1
    fi

    p1_metric "$module" "file_exists" "1" "bool" "SUCCESS" "$filepath"
}

p1_assert_dir_exists() {
    local module="$1"
    local dirpath="$2"

    if [[ ! -d "$dirpath" ]]; then
        p1_log_error "$module" "Diretório não encontrado: $dirpath"
        p1_metric "$module" "dir_exists" "0" "bool" "FAILED" "$dirpath"
        exit 1
    fi

    p1_metric "$module" "dir_exists" "1" "bool" "SUCCESS" "$dirpath"
}

p1_assert_nonempty_file() {
    local module="$1"
    local filepath="$2"

    if [[ ! -s "$filepath" ]]; then
        p1_log_error "$module" "Arquivo vazio ou inexistente: $filepath"
        p1_metric "$module" "file_nonempty" "0" "bool" "FAILED" "$filepath"
        exit 1
    fi

    p1_metric "$module" "file_nonempty" "1" "bool" "SUCCESS" "$filepath"
}

p1_metric_from_kv() {
    local module="$1"
    local metric_name="$2"
    local value="$3"
    local unit="${4:-}"
    local notes="${5:-}"

    p1_metric "$module" "$metric_name" "$value" "$unit" "SUCCESS" "$notes"
}

p1_fail_module() {
    local module="$1"
    local message="$2"

    p1_log_error "$module" "$message"
    p1_metric "$module" "failure" "1" "bool" "FAILED" "$message"
    exit 1
}