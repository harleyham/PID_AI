#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/p1_config.sh"
LOGGING_FILE="$SCRIPT_DIR/p1_logging.sh"

source "$CONFIG_FILE"
source "$LOGGING_FILE"

p1_ensure_dirs
p1_init_logs

MODULE="M00"

p1_module_start "$MODULE" START_TS

p1_run_cmd "$MODULE" "Verificação de ambiente Python e binários" \
    python "$SCRIPT_DIR/p1_00_check_env.py" \
    --log-file "$PIPELINE_LOG" \
    --metrics-csv "$METRICS_CSV" \
    --dataset "$DATASET" \
    --gpu "$GPU" \
    --module "$MODULE"

p1_module_end "$MODULE" "$START_TS" "SUCCESS"