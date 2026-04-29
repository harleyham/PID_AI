#!/bin/bash
set -euo pipefail

# Pipeline V0.2 - Módulo 01: Feature Extraction

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

MODULE="M01"

p1_module_start "$MODULE" START_TS

p1_assert_dir_exists "$MODULE" "$IMAGES_DIR"

mkdir -p "$WORKSPACE"
rm -f "$DATABASE"

p1_metric "$MODULE" "database_path" "$DATABASE" "path"
p1_metric "$MODULE" "images_dir" "$IMAGES_DIR" "path"
p1_metric "$MODULE" "workspace" "$WORKSPACE" "path"
p1_metric "$MODULE" "coord_file_tmp" "$COORD_FILE_TMP" "path"
p1_metric "$MODULE" "coord_file" "$COORD_FILE" "path"

# Execução com sintaxe para COLMAP 3.14.0.dev0
# max_image_size altera o tamanho da foto. Depende da potência da CPU e GPU
# 1400 ok na 5060Ti e L40S.
# Out of memory na p2000. Tirar GPU e baixar max_size para 1000.

p1_log_info "$MODULE" "Consultando help do COLMAP para detectar a flag de max_image_size"
HELP_TEXT="$("$COLMAP_BIN" feature_extractor --help 2>&1)"

if grep -q -- '--FeatureExtraction.max_image_size' <<<"$HELP_TEXT"; then
    MAX_IMG_FLAG="--FeatureExtraction.max_image_size"
elif grep -q -- '--SiftExtraction.max_image_size' <<<"$HELP_TEXT"; then
    MAX_IMG_FLAG="--SiftExtraction.max_image_size"
else
    p1_fail_module "$MODULE" "Nenhuma flag de max_image_size encontrada no COLMAP desta máquina"
fi

p1_log_info "$MODULE" "Flag detectada para max_image_size: $MAX_IMG_FLAG"
p1_metric "$MODULE" "max_image_flag" "$MAX_IMG_FLAG" "flag"
p1_metric "$MODULE" "max_image_size" "$Extraction_max_image_size" "pixels"
p1_metric "$MODULE" "max_num_features" "$MAX_NUM_FEATURES" "count"

p1_run_cmd "$MODULE" "colmap feature_extractor" \
    "$COLMAP_BIN" feature_extractor \
    --database_path "$DATABASE" \
    --image_path "$IMAGES_DIR" \
    --ImageReader.single_camera 1 \
    --FeatureExtraction.use_gpu 1 \
    "$MAX_IMG_FLAG" "$Extraction_max_image_size" \
    --SiftExtraction.max_num_features "$MAX_NUM_FEATURES"

p1_assert_file_exists "$MODULE" "$DATABASE"
p1_assert_nonempty_file "$MODULE" "$DATABASE"

p1_log_info "$MODULE" "Extraindo coordenadas EXIF para arquivo temporário: $COORD_FILE_TMP"
exiftool -filename -gpslatitude -gpslongitude -gpsaltitude -n -T "$IMAGES_DIR" > "$COORD_FILE_TMP"

p1_assert_nonempty_file "$MODULE" "$COORD_FILE_TMP"

TMP_ROWS="$(wc -l < "$COORD_FILE_TMP")"
p1_metric "$MODULE" "coords_tmp_rows" "$TMP_ROWS" "count"

p1_log_info "$MODULE" "Formatando arquivo de coordenadas para o COLMAP"
"$PYTHON_BIN" "$SCRIPT_DIR/p1_fix_coords.py" \
    --project-root "$PROJECT_ROOT" \
    --input-file "$COORD_FILE_TMP" \
    --output-file "$COORD_FILE" \
| tee -a "$PIPELINE_LOG"

p1_assert_nonempty_file "$MODULE" "$COORD_FILE"

NUM_COORDS="$(wc -l < "$COORD_FILE")"
p1_metric "$MODULE" "coords_rows" "$NUM_COORDS" "count"

p1_log_info "$MODULE" "Coordenadas extraídas corretamente"
p1_log_info "$MODULE" "Primeiras linhas do arquivo formatado:"

while IFS= read -r line; do
    p1_log_info "$MODULE" "$line"
done < <(head -n 3 "$COORD_FILE")

p1_module_end "$MODULE" "$START_TS" "SUCCESS"
