#!/bin/bash
set -euo pipefail

# Pipeline V0.2 - Módulo 02: Exhaustive Matching + seleção automática do par inicial

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

MODULE="M02"

p1_module_start "$MODULE" START_TS

p1_assert_file_exists "$MODULE" "$DATABASE"
p1_assert_nonempty_file "$MODULE" "$DATABASE"

rm -f "$INIT_PAIR_AUTO_FILE" "$INIT_PAIR_RANKING_CSV"

p1_metric "$MODULE" "database_path" "$DATABASE" "path"
p1_metric "$MODULE" "feature_matching_use_gpu" "1" "bool"
p1_metric "$MODULE" "feature_matching_max_num_matches" "$NUM_MATCHES" "count"
p1_metric "$MODULE" "init_pair_auto_file" "$INIT_PAIR_AUTO_FILE" "path"
p1_metric "$MODULE" "init_pair_ranking_csv" "$INIT_PAIR_RANKING_CSV" "path"
p1_metric "$MODULE" "init_pair_top_k" "$INIT_PAIR_TOP_K" "count"
p1_metric "$MODULE" "init_pair_min_inliers" "$INIT_PAIR_MIN_INLIERS" "count"
p1_metric "$MODULE" "init_pair_min_degree" "$INIT_PAIR_MIN_DEGREE" "count"

p1_run_cmd "$MODULE" "colmap exhaustive_matcher" \
    "$COLMAP_BIN" exhaustive_matcher \
    --database_path "$DATABASE" \
    --FeatureMatching.use_gpu 1 \
    --FeatureMatching.max_num_matches "$NUM_MATCHES"

python3 "$SCRIPT_DIR/p1_dialog_overlap.py" \
    --project-root "$PROJECT_ROOT" \
    --db-path "$DATABASE" \
    --log-file "$PIPELINE_LOG" \
    --metrics-csv "$METRICS_CSV" \
    --dataset "$DATASET" \
    --gpu "$GPU" \
    --module "$MODULE"

python3 "$SCRIPT_DIR/p1_select_init_pair.py" \
    --db-path "$DATABASE" \
    --out-txt "$INIT_PAIR_AUTO_FILE" \
    --out-csv "$INIT_PAIR_RANKING_CSV" \
    --top-k "$INIT_PAIR_TOP_K" \
    --min-inliers "$INIT_PAIR_MIN_INLIERS" \
    --min-degree "$INIT_PAIR_MIN_DEGREE" \
    --log-file "$PIPELINE_LOG" \
    --metrics-csv "$METRICS_CSV" \
    --dataset "$DATASET" \
    --gpu "$GPU" \
    --module "$MODULE"

p1_assert_file_exists "$MODULE" "$INIT_PAIR_AUTO_FILE"
p1_assert_nonempty_file "$MODULE" "$INIT_PAIR_AUTO_FILE"
p1_assert_file_exists "$MODULE" "$INIT_PAIR_RANKING_CSV"
p1_assert_nonempty_file "$MODULE" "$INIT_PAIR_RANKING_CSV"

p1_log_info "$MODULE" "Par inicial automático salvo em: $INIT_PAIR_AUTO_FILE"
p1_log_info "$MODULE" "Ranking de pares salvo em: $INIT_PAIR_RANKING_CSV"

p1_module_end "$MODULE" "$START_TS" "SUCCESS"
