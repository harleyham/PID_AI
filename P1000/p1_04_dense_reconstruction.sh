#!/bin/bash
set -euo pipefail

# Pipeline P1 - Módulo 04: Reconstrução Densa a partir do modelo ENU

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

MODULE="M04"

p1_module_start "$MODULE" START_TS

p1_assert_dir_exists "$MODULE" "$IMAGES_DIR"
p1_assert_dir_exists "$MODULE" "$ALIGN_PATH"

rm -rf "$DENSE_PATH/stereo" "$DENSE_PATH/images" "$DENSE_PATH/sparse"
rm -f "$OUTPUT_PATH/fused_enu.ply"

p1_metric "$MODULE" "images_dir" "$IMAGES_DIR" "path"
p1_metric "$MODULE" "align_path" "$ALIGN_PATH" "path"
p1_metric "$MODULE" "dense_path" "$DENSE_PATH" "path"
p1_metric "$MODULE" "output_ply" "$OUTPUT_PATH/fused_enu.ply" "path"

p1_metric "$MODULE" "undistorter_output_type" "COLMAP" "mode"

# Ajustes de patch match
p1_metric "$MODULE" "patch_match_max_image_size" "2640" "pixels"
p1_metric "$MODULE" "patch_match_window_step" "2" "pixels"
p1_metric "$MODULE" "patch_match_geom_consistency" "false" "bool"
p1_metric "$MODULE" "patch_match_num_iterations" "3" "count"
p1_metric "$MODULE" "patch_match_cache_size" "$RAM" "gb"

# Ajustes de fusion
p1_metric "$MODULE" "fusion_input_type" "photometric" "mode"
p1_metric "$MODULE" "fusion_check_num_images" "3" "count"
p1_metric "$MODULE" "fusion_min_num_pixels" "5" "count"

p1_log_info "$MODULE" "Executando image_undistorter"
p1_run_cmd "$MODULE" "colmap image_undistorter" colmap image_undistorter \
    --image_path "$IMAGES_DIR" \
    --input_path "$ALIGN_PATH" \
    --output_path "$DENSE_PATH" \
    --output_type COLMAP

p1_log_info "$MODULE" "Executando patch_match_stereo"
p1_run_cmd "$MODULE" "colmap patch_match_stereo" colmap patch_match_stereo \
    --workspace_path "$DENSE_PATH" \
    --workspace_format COLMAP \
    --PatchMatchStereo.max_image_size 2640 \
    --PatchMatchStereo.window_step 2 \
    --PatchMatchStereo.geom_consistency false \
    --PatchMatchStereo.num_iterations 3 \
    --PatchMatchStereo.cache_size "$RAM"

p1_log_info "$MODULE" "Executando stereo_fusion"
p1_run_cmd "$MODULE" "colmap stereo_fusion" colmap stereo_fusion \
    --workspace_path "$DENSE_PATH" \
    --workspace_format COLMAP \
    --input_type photometric \
    --output_path "$OUTPUT_PATH/fused_enu.ply" \
    --StereoFusion.check_num_images 3 \
    --StereoFusion.min_num_pixels 5

p1_assert_file_exists "$MODULE" "$OUTPUT_PATH/fused_enu.ply"
p1_assert_nonempty_file "$MODULE" "$OUTPUT_PATH/fused_enu.ply"

PLY_SIZE_BYTES="$(stat -c%s "$OUTPUT_PATH/fused_enu.ply")"
p1_metric "$MODULE" "fused_ply_size" "$PLY_SIZE_BYTES" "bytes"

PLY_POINTS="$(
python3 - <<PY
from plyfile import PlyData
ply = PlyData.read(r"$OUTPUT_PATH/fused_enu.ply")
print(len(ply["vertex"].data))
PY
)"
p1_metric "$MODULE" "fused_ply_points" "$PLY_POINTS" "count"

PDAL_SUMMARY="$(pdal info --summary "$OUTPUT_PATH/fused_enu.ply" 2>&1 || true)"
if [[ -n "$PDAL_SUMMARY" ]]; then
    p1_log_info "$MODULE" "Resumo PDAL da nuvem densa:"
    while IFS= read -r line; do
        p1_log_info "$MODULE" "$line"
    done <<< "$PDAL_SUMMARY"

    MINX="$(grep -oP '"minx"\s*:\s*\K[-0-9.e+]+' <<<"$PDAL_SUMMARY" | head -n1 || true)"
    MAXX="$(grep -oP '"maxx"\s*:\s*\K[-0-9.e+]+' <<<"$PDAL_SUMMARY" | head -n1 || true)"
    MINY="$(grep -oP '"miny"\s*:\s*\K[-0-9.e+]+' <<<"$PDAL_SUMMARY" | head -n1 || true)"
    MAXY="$(grep -oP '"maxy"\s*:\s*\K[-0-9.e+]+' <<<"$PDAL_SUMMARY" | head -n1 || true)"
    MINZ="$(grep -oP '"minz"\s*:\s*\K[-0-9.e+]+' <<<"$PDAL_SUMMARY" | head -n1 || true)"
    MAXZ="$(grep -oP '"maxz"\s*:\s*\K[-0-9.e+]+' <<<"$PDAL_SUMMARY" | head -n1 || true)"

    [[ -n "$MINX" ]] && p1_metric "$MODULE" "minx" "$MINX" "m"
    [[ -n "$MAXX" ]] && p1_metric "$MODULE" "maxx" "$MAXX" "m"
    [[ -n "$MINY" ]] && p1_metric "$MODULE" "miny" "$MINY" "m"
    [[ -n "$MAXY" ]] && p1_metric "$MODULE" "maxy" "$MAXY" "m"
    [[ -n "$MINZ" ]] && p1_metric "$MODULE" "minz" "$MINZ" "m"
    [[ -n "$MAXZ" ]] && p1_metric "$MODULE" "maxz" "$MAXZ" "m"
else
    p1_log_warn "$MODULE" "Não foi possível obter resumo PDAL da nuvem densa"
fi

p1_log_info "$MODULE" "Saída gerada em: $OUTPUT_PATH/fused_enu.ply"

p1_module_end "$MODULE" "$START_TS" "SUCCESS"
