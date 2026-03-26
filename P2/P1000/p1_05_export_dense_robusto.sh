#!/bin/bash
set -euo pipefail

# Pipeline P1 - Módulo 05: Exportação da nuvem densa ENU para LAS UTM
# Padrão consolidado:
# - usa p1_config.sh + p1_logging.sh
# - exporta LAS via p1_05_export_dense.py
# - valida persistência do arquivo
# - coleta estatísticas via p1_05_stat.py
# - escreve mensagens no PIPELINE_LOG e métricas no METRICS_CSV

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

MODULE="M05"
OUTPUT_LAS="$OUTPUT_PATH/dense_utm_color.las"

p1_module_start "$MODULE" START_TS

p1_assert_file_exists "$MODULE" "$INPUT_PLY"
p1_assert_nonempty_file "$MODULE" "$INPUT_PLY"
p1_assert_file_exists "$MODULE" "$ENU_META_JSON"
p1_assert_nonempty_file "$MODULE" "$ENU_META_JSON"

mkdir -p "$OUTPUT_PATH"
rm -f "$OUTPUT_LAS"

p1_metric "$MODULE" "input_ply" "$INPUT_PLY" "path"
p1_metric "$MODULE" "enu_meta_json" "$ENU_META_JSON" "path"
p1_metric "$MODULE" "output_las" "$OUTPUT_LAS" "path"

p1_log_info "$MODULE" "Exportando nuvem ENU para LAS UTM"
python3 "$SCRIPT_DIR/p1_05_export_dense.py" \
    --input-ply "$INPUT_PLY" \
    --enu-meta-json "$ENU_META_JSON" \
    --output-las "$OUTPUT_LAS" \
    --log-file "$PIPELINE_LOG" \
    --metrics-csv "$METRICS_CSV" \
    --dataset "$DATASET" \
    --gpu "$GPU" \
    --module "$MODULE"

p1_assert_file_exists "$MODULE" "$OUTPUT_LAS"
p1_assert_nonempty_file "$MODULE" "$OUTPUT_LAS"

LAS_SIZE_BYTES="$(stat -c%s "$OUTPUT_LAS")"
p1_metric "$MODULE" "output_las_size" "$LAS_SIZE_BYTES" "bytes"

p1_log_info "$MODULE" "Calculando estatísticas do LAS exportado"
python3 "$SCRIPT_DIR/p1_05_stat.py" \
    --input-las "$OUTPUT_LAS" \
    --log-file "$PIPELINE_LOG" \
    --metrics-csv "$METRICS_CSV" \
    --dataset "$DATASET" \
    --gpu "$GPU" \
    --module "$MODULE"

p1_log_info "$MODULE" "Arquivo gerado em: $OUTPUT_LAS"

p1_module_end "$MODULE" "$START_TS" "SUCCESS"
