#!/bin/bash
set -euo pipefail

# Pipeline V0.2 - Módulo 03: Sparse Mapper + alinhamento ENU
# Padrão consolidado:
# - preserva o modelo local em sparse/0
# - gera modelo alinhado separado em enu/
# - grava enu_origin.json com a origem real do ENU
# - exporta Esparsa_ENU.ply
# - registra métricas em METRICS_CSV e mensagens em PIPELINE_LOG
# - consome par inicial automático gerado no M02
# - permite override manual via p1_config.sh
# - falha cedo se o modelo local sair fraco

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

MODULE="M03"

p1_module_start "$MODULE" START_TS

p1_assert_file_exists "$MODULE" "$DATABASE"
p1_assert_nonempty_file "$MODULE" "$DATABASE"
p1_assert_file_exists "$MODULE" "$COORD_FILE"
p1_assert_nonempty_file "$MODULE" "$COORD_FILE"
p1_assert_dir_exists "$MODULE" "$IMAGES_DIR"

rm -rf "$SPARSE_PATH"/*
rm -rf "$ENU_PATH"/*
rm -f "$TRANSFORM_PATH" "$ENU_META_JSON" "$OUTPUT_PATH/Esparsa_ENU.ply"

p1_metric "$MODULE" "database_path" "$DATABASE" "path"
p1_metric "$MODULE" "images_dir" "$IMAGES_DIR" "path"
p1_metric "$MODULE" "coord_file" "$COORD_FILE" "path"
p1_metric "$MODULE" "sparse_path" "$SPARSE_PATH" "path"
p1_metric "$MODULE" "enu_path" "$ENU_PATH" "path"
p1_metric "$MODULE" "transform_path" "$TRANSFORM_PATH" "path"
p1_metric "$MODULE" "enu_meta_json" "$ENU_META_JSON" "path"

# Defaults robustos caso não tenham sido definidos no p1_config.sh
: "${INIT_PAIR_AUTO_FILE:=$WORKSPACE/init_pair_auto.sh}"
: "${INIT_IMAGE_ID1:=}"
: "${INIT_IMAGE_ID2:=}"
: "${MIN_REGISTERED_IMAGES_LOCAL:=30}"
: "${MAPPER_MIN_NUM_MATCHES:=15}"
: "${MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS:=80}"
: "${MAPPER_INIT_MIN_NUM_INLIERS:=80}"
: "${MAPPER_ABS_POSE_MIN_NUM_INLIERS:=30}"
: "${MAPPER_ABS_POSE_MIN_INLIER_RATIO:=0.20}"
: "${MAPPER_FILTER_MAX_REPROJ_ERROR:=3}"
: "${ALIGNMENT_MAX_ERROR:=10}"
: "${ALIGNMENT_TYPE:=enu}"

p1_metric "$MODULE" "init_pair_auto_file" "$INIT_PAIR_AUTO_FILE" "path"
p1_metric "$MODULE" "mapper_ba_use_gpu" "1" "bool"
p1_metric "$MODULE" "mapper_multiple_models" "0" "bool"
p1_metric "$MODULE" "mapper_min_num_matches" "$MAPPER_MIN_NUM_MATCHES" "count"
p1_metric "$MODULE" "mapper_ba_global_max_num_iterations" "$MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS" "count"
p1_metric "$MODULE" "mapper_extract_colors" "1" "bool"
p1_metric "$MODULE" "mapper_init_min_num_inliers" "$MAPPER_INIT_MIN_NUM_INLIERS" "count"
p1_metric "$MODULE" "mapper_abs_pose_min_num_inliers" "$MAPPER_ABS_POSE_MIN_NUM_INLIERS" "count"
p1_metric "$MODULE" "mapper_abs_pose_min_inlier_ratio" "$MAPPER_ABS_POSE_MIN_INLIER_RATIO" "ratio"
p1_metric "$MODULE" "mapper_filter_max_reproj_error" "$MAPPER_FILTER_MAX_REPROJ_ERROR" "pixels"
p1_metric "$MODULE" "min_registered_images_local" "$MIN_REGISTERED_IMAGES_LOCAL" "count"
p1_metric "$MODULE" "alignment_max_error" "$ALIGNMENT_MAX_ERROR" "meters"
p1_metric "$MODULE" "alignment_type" "$ALIGNMENT_TYPE" "mode"

AUTO_ID1=""
AUTO_ID2=""

if [[ -f "$INIT_PAIR_AUTO_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$INIT_PAIR_AUTO_FILE"
    AUTO_ID1="${INIT_IMAGE_ID1_AUTO:-}"
    AUTO_ID2="${INIT_IMAGE_ID2_AUTO:-}"
    p1_log_info "$MODULE" "Par automático carregado do M02: ${AUTO_ID1:-vazio} / ${AUTO_ID2:-vazio}"
    [[ -n "$AUTO_ID1" ]] && p1_metric "$MODULE" "mapper_init_image_id1_auto" "$AUTO_ID1" "id"
    [[ -n "$AUTO_ID2" ]] && p1_metric "$MODULE" "mapper_init_image_id2_auto" "$AUTO_ID2" "id"
else
    p1_log_warn "$MODULE" "Arquivo de par automático não encontrado: $INIT_PAIR_AUTO_FILE"
    p1_metric "$MODULE" "init_pair_auto_file_exists" "0" "bool" "WARNING" "$INIT_PAIR_AUTO_FILE"
fi

FINAL_INIT_ID1=""
FINAL_INIT_ID2=""
INIT_SOURCE="none"

if [[ -n "$INIT_IMAGE_ID1" && -n "$INIT_IMAGE_ID2" ]]; then
    FINAL_INIT_ID1="$INIT_IMAGE_ID1"
    FINAL_INIT_ID2="$INIT_IMAGE_ID2"
    INIT_SOURCE="manual_config"
elif [[ -n "$AUTO_ID1" && -n "$AUTO_ID2" ]]; then
    FINAL_INIT_ID1="$AUTO_ID1"
    FINAL_INIT_ID2="$AUTO_ID2"
    INIT_SOURCE="auto_m02"
fi

MAPPER_INIT_ARGS=()
if [[ -n "$FINAL_INIT_ID1" && -n "$FINAL_INIT_ID2" ]]; then
    MAPPER_INIT_ARGS=(
        --Mapper.init_image_id1 "$FINAL_INIT_ID1"
        --Mapper.init_image_id2 "$FINAL_INIT_ID2"
    )
    p1_log_info "$MODULE" "Usando par inicial: $FINAL_INIT_ID1 / $FINAL_INIT_ID2 (fonte=$INIT_SOURCE)"
    p1_metric "$MODULE" "mapper_init_source" "$INIT_SOURCE" "mode"
    p1_metric "$MODULE" "mapper_init_image_id1" "$FINAL_INIT_ID1" "id"
    p1_metric "$MODULE" "mapper_init_image_id2" "$FINAL_INIT_ID2" "id"
else
    p1_log_warn "$MODULE" "Nenhum par inicial definido; COLMAP escolherá automaticamente"
    p1_metric "$MODULE" "mapper_init_source" "colmap_auto" "mode" "WARNING"
fi

p1_run_cmd "$MODULE" "COLMAP mapper" \
    colmap mapper \
    --database_path "$DATABASE" \
    --image_path "$IMAGES_DIR" \
    --output_path "$SPARSE_PATH" \
    --Mapper.ba_use_gpu 1 \
    --Mapper.multiple_models 0 \
    --Mapper.min_num_matches "$MAPPER_MIN_NUM_MATCHES" \
    --Mapper.ba_global_max_num_iterations "$MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS" \
    --Mapper.extract_colors 1 \
    --Mapper.init_min_num_inliers "$MAPPER_INIT_MIN_NUM_INLIERS" \
    --Mapper.abs_pose_min_num_inliers "$MAPPER_ABS_POSE_MIN_NUM_INLIERS" \
    --Mapper.abs_pose_min_inlier_ratio "$MAPPER_ABS_POSE_MIN_INLIER_RATIO" \
    --Mapper.filter_max_reproj_error "$MAPPER_FILTER_MAX_REPROJ_ERROR" \
    --Mapper.num_threads 24 \
    "${MAPPER_INIT_ARGS[@]}"

p1_assert_dir_exists "$MODULE" "$SPARSE_RUN"

SPARSE_STATS="$(colmap model_analyzer --path "$SPARSE_RUN" 2>&1 | grep -E "Registered images:|Points:|Mean reprojection error:" || true)"
if [[ -n "$SPARSE_STATS" ]]; then
    p1_log_info "$MODULE" "Estatísticas do modelo esparso local:"
    while IFS= read -r line; do
        p1_log_info "$MODULE" "$line"
    done <<< "$SPARSE_STATS"

    REGISTERED_LOCAL="$(grep -E "Registered images:" <<<"$SPARSE_STATS" | awk -F': ' '{print $2}' | tr -d ' ' || true)"
    POINTS_LOCAL="$(grep -E "^Points:" <<<"$SPARSE_STATS" | awk -F': ' '{print $2}' | tr -d ' ' || true)"
    REPROJ_LOCAL="$(grep -E "Mean reprojection error:" <<<"$SPARSE_STATS" | awk -F': ' '{print $2}' | awk '{print $1}' || true)"

    [[ -n "$REGISTERED_LOCAL" ]] && p1_metric "$MODULE" "registered_images_local" "$REGISTERED_LOCAL" "count"
    [[ -n "$POINTS_LOCAL" ]] && p1_metric "$MODULE" "sparse_points_local" "$POINTS_LOCAL" "count"
    [[ -n "$REPROJ_LOCAL" ]] && p1_metric "$MODULE" "mean_reprojection_error_local" "$REPROJ_LOCAL" "pixels"
else
    p1_fail_module "$MODULE" "Não foi possível extrair estatísticas do modelo esparso local"
fi

if [[ -z "${REGISTERED_LOCAL:-}" ]]; then
    p1_fail_module "$MODULE" "Não foi possível determinar o número de imagens registradas no modelo local"
fi

if (( REGISTERED_LOCAL < MIN_REGISTERED_IMAGES_LOCAL )); then
    p1_metric "$MODULE" "early_fail_registered_images_local" "$REGISTERED_LOCAL" "count" "FAILED" "Abaixo do limiar mínimo"
    p1_fail_module "$MODULE" "Falha precoce: sparse local insuficiente (${REGISTERED_LOCAL} < ${MIN_REGISTERED_IMAGES_LOCAL})"
fi

ALIGN_HELP="$(colmap model_aligner --help 2>&1 || true)"
ALIGN_EXTRA_ARGS=()

if grep -q -- '--transform_path' <<<"$ALIGN_HELP"; then
    ALIGN_EXTRA_ARGS+=(--transform_path "$TRANSFORM_PATH")
    p1_metric "$MODULE" "model_aligner_transform_path_supported" "1" "bool"
else
    p1_metric "$MODULE" "model_aligner_transform_path_supported" "0" "bool" "WARNING"
fi

p1_run_cmd "$MODULE" "COLMAP model_aligner" \
    colmap model_aligner \
    --input_path "$SPARSE_RUN" \
    --output_path "$ENU_PATH" \
    --ref_images_path "$COORD_FILE" \
    --ref_is_gps 1 \
    --alignment_max_error "$ALIGNMENT_MAX_ERROR" \
    --alignment_type "$ALIGNMENT_TYPE" \
    "${ALIGN_EXTRA_ARGS[@]}"

p1_assert_dir_exists "$MODULE" "$ENU_PATH"

p1_log_info "$MODULE" "Gerando metadados ENU em $ENU_META_JSON"

python3 - <<PY
import json
import math
import pathlib

coord_file_py = pathlib.Path(r"$COORD_FILE")
out_json = pathlib.Path(r"$ENU_META_JSON")

rows = []
with coord_file_py.open("r", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) >= 4:
            name, lat, lon, alt = parts[:4]
            rows.append((name, float(lat), float(lon), float(alt)))

if not rows:
    raise SystemExit("Nenhuma coordenada válida encontrada no arquivo de coordenadas")

first_name, ref_lat, ref_lon, ref_alt = rows[0]

mean_lat = sum(r[1] for r in rows) / len(rows)
mean_lon = sum(r[2] for r in rows) / len(rows)
mean_alt = sum(r[3] for r in rows) / len(rows)

zone = int((ref_lon + 180.0) // 6.0) + 1
hemisphere = "south" if ref_lat < 0 else "north"

if hemisphere == "south" and -80.0 <= ref_lon <= -20.0:
    epsg = 31960 + zone
elif hemisphere == "south":
    epsg = 32700 + zone
else:
    epsg = 32600 + zone

lon0 = (zone - 1) * 6 - 180 + 3
gamma = math.atan(
    math.tan(math.radians(ref_lon - lon0)) * math.sin(math.radians(ref_lat))
)

meta = {
    "first_image": first_name,
    "num_images": len(rows),
    "ref_lat": ref_lat,
    "ref_lon": ref_lon,
    "ref_alt": ref_alt,
    "mean_lat": mean_lat,
    "mean_lon": mean_lon,
    "mean_alt": mean_alt,
    "utm_zone": zone,
    "hemisphere": hemisphere,
    "epsg": epsg,
    "meridian_convergence_rad": gamma,
    "meridian_convergence_deg": math.degrees(gamma),
}

out_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")
print(json.dumps(meta, indent=2))
PY

p1_assert_nonempty_file "$MODULE" "$ENU_META_JSON"

readarray -t ENU_META_VALUES < <(
python3 - <<PY
import json
from pathlib import Path

meta = json.loads(Path(r"$ENU_META_JSON").read_text(encoding="utf-8"))
print(meta["epsg"])
print(meta["num_images"])
print(meta["ref_lat"])
print(meta["ref_lon"])
print(meta["ref_alt"])
print(meta["mean_lat"])
print(meta["mean_lon"])
print(meta["mean_alt"])
PY
)

EPSG_CODE="${ENU_META_VALUES[0]}"
NUM_IMAGES_META="${ENU_META_VALUES[1]}"
REF_LAT="${ENU_META_VALUES[2]}"
REF_LON="${ENU_META_VALUES[3]}"
REF_ALT="${ENU_META_VALUES[4]}"
MEAN_LAT="${ENU_META_VALUES[5]}"
MEAN_LON="${ENU_META_VALUES[6]}"
MEAN_ALT="${ENU_META_VALUES[7]}"

p1_metric "$MODULE" "epsg" "$EPSG_CODE" "code"
p1_metric "$MODULE" "num_images_metadata" "$NUM_IMAGES_META" "count"
p1_metric "$MODULE" "ref_lat" "$REF_LAT" "deg"
p1_metric "$MODULE" "ref_lon" "$REF_LON" "deg"
p1_metric "$MODULE" "ref_alt" "$REF_ALT" "m"
p1_metric "$MODULE" "mean_lat" "$MEAN_LAT" "deg"
p1_metric "$MODULE" "mean_lon" "$MEAN_LON" "deg"
p1_metric "$MODULE" "mean_alt" "$MEAN_ALT" "m"

p1_run_cmd "$MODULE" "COLMAP model_converter" \
    colmap model_converter \
    --input_path "$ENU_PATH" \
    --output_path "$OUTPUT_PATH/Esparsa_ENU.ply" \
    --output_type PLY

p1_assert_file_exists "$MODULE" "$OUTPUT_PATH/Esparsa_ENU.ply"
p1_assert_nonempty_file "$MODULE" "$OUTPUT_PATH/Esparsa_ENU.ply"

ENU_STATS="$(colmap model_analyzer --path "$ENU_PATH" 2>&1 | grep -E "Registered images:|Points:|Mean reprojection error:" || true)"
if [[ -n "$ENU_STATS" ]]; then
    p1_log_info "$MODULE" "Estatísticas do modelo alinhado ENU:"
    while IFS= read -r line; do
        p1_log_info "$MODULE" "$line"
    done <<< "$ENU_STATS"

    REGISTERED_ENU="$(grep -E "Registered images:" <<<"$ENU_STATS" | awk -F': ' '{print $2}' | tr -d ' ' || true)"
    POINTS_ENU="$(grep -E "^Points:" <<<"$ENU_STATS" | awk -F': ' '{print $2}' | tr -d ' ' || true)"
    REPROJ_ENU="$(grep -E "Mean reprojection error:" <<<"$ENU_STATS" | awk -F': ' '{print $2}' | awk '{print $1}' || true)"

    [[ -n "$REGISTERED_ENU" ]] && p1_metric "$MODULE" "registered_images_enu" "$REGISTERED_ENU" "count"
    [[ -n "$POINTS_ENU" ]] && p1_metric "$MODULE" "sparse_points_enu" "$POINTS_ENU" "count"
    [[ -n "$REPROJ_ENU" ]] && p1_metric "$MODULE" "mean_reprojection_error_enu" "$REPROJ_ENU" "pixels"
else
    p1_log_warn "$MODULE" "Não foi possível extrair estatísticas do modelo alinhado ENU"
fi

if [[ -f "$TRANSFORM_PATH" ]]; then
    p1_assert_nonempty_file "$MODULE" "$TRANSFORM_PATH"
    p1_log_info "$MODULE" "Transformação local->ENU salva em: $TRANSFORM_PATH"
fi

p1_log_info "$MODULE" "Modelo local preservado em: $SPARSE_RUN"
p1_log_info "$MODULE" "Modelo alinhado ENU em: $ENU_PATH"
p1_log_info "$MODULE" "Metadados ENU em: $ENU_META_JSON"
p1_log_info "$MODULE" "PLY exportado em: $OUTPUT_PATH/Esparsa_ENU.ply"

p1_module_end "$MODULE" "$START_TS" "SUCCESS"