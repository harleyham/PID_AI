#!/bin/bash
set -euo pipefail

# ============================================================
# Pipeline V0.2 - Configuração central
# Perfis automáticos de escalabilidade para qualidade "medium"
# ============================================================

# Tipo de informação                Função correta
# Mensagem humana (debug, status)   p1_log_info
# Aviso                             p1_log_warn
# Erro                              p1_log_error
# Dado mensurável                   p1_metric

# ------------------------------------------------------------
# Parâmetros principais
# ------------------------------------------------------------
export PROJECT_ROOT="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO"
export PIPELINE_NAME="P1_Tradicional"

export DATASET="Dataset_04"
export GPU="5060Ti"


# Binário do COLMAP usado por todos os módulos
# export COLMAP_BIN="${COLMAP_BIN:-/usr/local/bin/colmap}"
# export COLMAP_BIN="$HOME/opt/colmap_new/bin/colmap"
# export COLMAP_BIN="/usr/local/bin/colmap"
export COLMAP_BIN="$HOME/opt/colmap_ceres_cuda/bin/colmap"


# Qualidade nominal escolhida pelo usuário
# Mantemos "medium" como semântica externa.
export PIPELINE_QUALITY="medium"

# Modo de escala:
# - auto   : escolhe medium_small / medium_mid / medium_large pelo nº de imagens
# - manual : usa PIPELINE_PROFILE_NAME definido abaixo
export PIPELINE_SCALE_MODE="auto"

# Use somente se PIPELINE_SCALE_MODE="manual"
export PIPELINE_PROFILE_NAME="medium_mid"

export StereoFusion_num_threads="8" # Era 24

# ------------------------------------------------------------
# Seleção automática do par inicial do M03
# ------------------------------------------------------------
export INIT_IMAGE_ID1=""
export INIT_IMAGE_ID2=""

export INIT_PAIR_TOP_K="15"
export INIT_PAIR_MIN_INLIERS="50"
export INIT_PAIR_MIN_DEGREE="5"

# Robustez do mapper
export MAPPER_MIN_NUM_MATCHES="15"
export MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS="50"
export MAPPER_INIT_MIN_NUM_INLIERS="80"
export MAPPER_ABS_POSE_MIN_NUM_INLIERS="30"
export MAPPER_ABS_POSE_MIN_INLIER_RATIO="0.20"
export MAPPER_FILTER_MAX_REPROJ_ERROR="3"
export MAPPER_NUM_THREADS="8" # Era 24

# Alinhamento
export ALIGNMENT_MAX_ERROR="10"
export ALIGNMENT_TYPE="enu"

# Falha cedo se o modelo local registrar menos que isso.
export MIN_REGISTERED_IMAGES_LOCAL="10"

# Diretório dos scripts
export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ------------------------------------------------------------
# Derivações automáticas
# ------------------------------------------------------------
case "$DATASET" in
    Dataset_01) export DATASET_SLUG="DS1" ;;
    Dataset_02) export DATASET_SLUG="DS2" ;;
    Dataset_03) export DATASET_SLUG="DS3" ;;
    Dataset_04) export DATASET_SLUG="DS4" ;;
    *) export DATASET_SLUG="$DATASET" ;;
esac

export IMAGES_DIR="$PROJECT_ROOT/00_Datasets/$DATASET/raw_images"

export WORKSPACE="$PROJECT_ROOT/02_Pipelines_LIGEM/$PIPELINE_NAME/workspace_${DATASET_SLUG}/${GPU}"
export LOG_DIR="$PROJECT_ROOT/02_Pipelines_LIGEM/$PIPELINE_NAME/logs"
export LOG_FILE="$LOG_DIR/performance_p1_${DATASET}.csv"
export PIPELINE_LOG="$LOG_DIR/pipeline_p1_${DATASET}.log"
export METRICS_CSV="$LOG_DIR/performance_p1_metrics_${DATASET}.csv"
export INIT_PAIR_AUTO_FILE="$WORKSPACE/init_pair_auto.sh"
export INIT_PAIR_RANKING_CSV="$WORKSPACE/init_pair_ranking.csv"

export DATABASE="$WORKSPACE/database_${DATASET_SLUG,,}.db"
export COORD_FILE_TMP="$WORKSPACE/coords_${DATASET_SLUG,,}_e.txt"
export COORD_FILE="$WORKSPACE/coords_${DATASET_SLUG,,}.txt"

export SPARSE_PATH="$WORKSPACE/sparse"
export SPARSE_RUN="$SPARSE_PATH/0"
export ENU_PATH="$WORKSPACE/enu"
export ENU_META_JSON="$WORKSPACE/enu_origin.json"
export TRANSFORM_PATH="$WORKSPACE/local_to_enu.txt"

export ALIGN_PATH="$WORKSPACE/enu"

export OUTPUT_DATASET_DIR="$PROJECT_ROOT/04_Produtos_Finais/$DATASET_SLUG"
export OUTPUT_PATH="$OUTPUT_DATASET_DIR/${GPU}"
export OUTPUT_DIR="$OUTPUT_PATH"

export DENSE_PATH="$WORKSPACE/dense"
export INPUT_PLY="$OUTPUT_PATH/fused_enu.ply"
export DENSE_LAS="$OUTPUT_PATH/dense_utm_color.las"

# Snapshot de ambiente
export PYTHON_BIN="python"
export ENV_SNAPSHOT_CSV="$LOG_DIR/env_history_${DATASET}.csv"

# ------------------------------------------------------------
# Contagem de imagens do dataset
# ------------------------------------------------------------
if [[ -d "$IMAGES_DIR" ]]; then
    export NUM_IMAGES="$(find "$IMAGES_DIR" -maxdepth 1 -type f \
        \( -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.tif' -o -iname '*.tiff' -o -iname '*.png' \) \
        | wc -l)"
else
    export NUM_IMAGES="0"
fi

# ------------------------------------------------------------
# Resolução do perfil ativo
# ------------------------------------------------------------
resolve_profile_name() {
    local quality="${PIPELINE_QUALITY}"
    local mode="${PIPELINE_SCALE_MODE}"
    local n="${NUM_IMAGES}"

    if [[ "$quality" != "medium" ]]; then
        echo "${quality}"
        return
    fi

    if [[ "$mode" == "manual" ]]; then
        echo "${PIPELINE_PROFILE_NAME}"
        return
    fi

    # auto
    if (( n <= 100 )); then
        echo "medium_small"
    elif (( n <= 500 )); then
        echo "medium_mid"
    else
        echo "medium_large"
    fi
}

export ACTIVE_PROFILE_NAME="$(resolve_profile_name)"

# ------------------------------------------------------------
# Perfil base de memória / cache
# ------------------------------------------------------------
# cache_size depende da memória do computador
# 20 -> L40S / 5060Ti
# 8  -> P1000
case "$GPU" in
    P1000) export RAM="8" ;;
    *)     export RAM="20" ;;
esac

# ------------------------------------------------------------
# Parâmetros dependentes do perfil
# A ideia é manter a qualidade nominal "medium",
# mas com implementação mais escalável.
# ------------------------------------------------------------
case "$ACTIVE_PROFILE_NAME" in
    medium_small)
        # ----------------------------------------------------
        # matching / features
        # ----------------------------------------------------
        export NUM_MATCHES="30000"
        export MAX_NUM_FEATURES="50000"
        export Extraction_max_image_size="1500"

        # mapper
        export MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS="50"

        # M04 - compromisso melhor para datasets pequenos
        export COLMAP_GPU_INDEX="0"
        export PatchMatchStereo_max_image_size="2200"
        export PatchMatchStereo_num_iterations="3"
        export PatchMatchStereo_num_samples="16"
        export PatchMatchStereo_window_radius="6"
        export PatchMatchStereo_window_step="1"
        export PatchMatchStereo_geom_consistency="false"
        export StereoFusion_check_num_images="2"
        export StereoFusion_min_num_pixels="4"
        ;;

    medium_mid)
        # ----------------------------------------------------
        # matching / features
        # ----------------------------------------------------
        export NUM_MATCHES="28000"
        export MAX_NUM_FEATURES="45000"
        export Extraction_max_image_size="1450"

        # mapper
        export MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS="45"

        # M04 - perfil intermediário escalável
        export COLMAP_GPU_INDEX="0"
        export PatchMatchStereo_max_image_size="1800"
        export PatchMatchStereo_num_iterations="3"
        export PatchMatchStereo_num_samples="12"
        export PatchMatchStereo_window_radius="5"
        export PatchMatchStereo_window_step="2"
        export PatchMatchStereo_geom_consistency="false"
        export StereoFusion_check_num_images="2"
        export StereoFusion_min_num_pixels="4"
        ;;

    medium_large)
        # ----------------------------------------------------
        # matching / features
        # ----------------------------------------------------
        export NUM_MATCHES="24000"
        export MAX_NUM_FEATURES="40000"
        export Extraction_max_image_size="1400"

        # mapper
        export MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS="40"

        # M04 - foco em escalabilidade para datasets grandes
        # alinhado à filosofia do FAQ do COLMAP:
        # reduzir max_image_size, samples, iterations e subir window_step
        export COLMAP_GPU_INDEX="0"
        export PatchMatchStereo_max_image_size="1600"
        export PatchMatchStereo_num_iterations="3"
        export PatchMatchStereo_num_samples="10"
        export PatchMatchStereo_window_radius="5"
        export PatchMatchStereo_window_step="2"
        export PatchMatchStereo_geom_consistency="false"
        export StereoFusion_check_num_images="2"
        export StereoFusion_min_num_pixels="4"
        ;;

    *)
        # fallback seguro
        export NUM_MATCHES="28000"
        export MAX_NUM_FEATURES="45000"
        export Extraction_max_image_size="1450"
        export MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS="45"

        export COLMAP_GPU_INDEX="0"
        export PatchMatchStereo_max_image_size="1800"
        export PatchMatchStereo_num_iterations="3"
        export PatchMatchStereo_num_samples="12"
        export PatchMatchStereo_window_radius="5"
        export PatchMatchStereo_window_step="2"
        export PatchMatchStereo_geom_consistency="false"
        export StereoFusion_check_num_images="2"
        export StereoFusion_min_num_pixels="4"
        ;;
esac

# ------------------------------------------------------------
# Módulo 06 - DEM / DSM / DTM / CHM
# Mantido estável; a lógica de perfil hoje está focada
# principalmente em M01/M02/M03/M04.
# ------------------------------------------------------------
export DEM_RESOLUTION="0.05"   # metros
export DEM_NODATA="-9999"

# SMRF - terreno
export SMRF_SCALAR="1.25"
export SMRF_SLOPE="0.15"
export SMRF_THRESHOLD="0.50"
export SMRF_WINDOW="16.0"

# Produtos analíticos
export DTM_OUTPUT_TYPE="idw"
export DTM_WINDOW_SIZE="2"

export DSM_OUTPUT_TYPE="max"
export DSM_WINDOW_SIZE="1"

# Produtos fechados
export DTM_CLOSED_OUTPUT_TYPE="idw"
export DTM_CLOSED_WINDOW_SIZE="10"

export DSM_CLOSED_OUTPUT_TYPE="idw"
export DSM_CLOSED_WINDOW_SIZE="6"

# fillnodata dedicado por superfície
export DTM_FILLNODATA_MAX_DISTANCE="80"
export DSM_FILLNODATA_MAX_DISTANCE="60"
export FILLNODATA_SMOOTHING_ITERATIONS="1"

# superfície híbrida para ortho/uso posterior
export ORTHO_SURFACE_MODE="DSM_THEN_DTM"

# Pré-filtro de outliers baixos antes do SMRF
export LOW_OUTLIER_ENABLE="1"
export LOW_OUTLIER_PERCENTILE="1.0"
export LOW_OUTLIER_MARGIN="5.0"

# remoção estatística de pontos isolados antes do corte altimétrico
export OUTLIER_MEAN_K="12"
export OUTLIER_MULTIPLIER="2.5"

# ------------------------------------------------------------
# M07 - ORTOMOSAICO
# Mantido estável por enquanto. A escalabilidade principal
# está sendo tratada no M04.
# ------------------------------------------------------------
export ORTHO_ENABLED="1"
export ORTHO_RESOLUTION="0.03"
export ORTHO_USE_DSM="1"
export ORTHO_MAX_CANDIDATES="8"
export ORTHO_TILE_SIZE="1024"
export ORTHO_BLEND_MODE="best_angle"
export ORTHO_COMPRESS="DEFLATE"
export ORTHO_JPEG_QUALITY="90"

export ORTHO_TIF="$OUTPUT_PATH/ORTHO.tif"
export ORTHO_VRT="$OUTPUT_PATH/ORTHO.vrt"
export ORTHO_PREVIEW_JPG="$OUTPUT_PATH/ORTHO_preview.jpg"

# ------------------------------------------------------------
# Utilitários
# ------------------------------------------------------------
p1_ensure_dirs() {
    mkdir -p "$WORKSPACE" "$LOG_DIR" "$OUTPUT_DATASET_DIR" "$OUTPUT_PATH" "$OUTPUT_DIR"
    mkdir -p "$SPARSE_PATH" "$ENU_PATH" "$DENSE_PATH"
}

p1_print_config() {
    cat <<EOF
================ CONFIGURAÇÃO V0.2 ================
COLMAP_BIN                       : $COLMAP_BIN
DATASET                          : $DATASET
DATASET_SLUG                     : $DATASET_SLUG
NUM_IMAGES                       : $NUM_IMAGES
PIPELINE_QUALITY                 : $PIPELINE_QUALITY
PIPELINE_SCALE_MODE              : $PIPELINE_SCALE_MODE
PIPELINE_PROFILE_NAME            : $PIPELINE_PROFILE_NAME
ACTIVE_PROFILE_NAME              : $ACTIVE_PROFILE_NAME
GPU                              : $GPU
PROJECT_ROOT                     : $PROJECT_ROOT
PIPELINE_NAME                    : $PIPELINE_NAME
IMAGES_DIR                       : $IMAGES_DIR
WORKSPACE                        : $WORKSPACE
SPARSE_PATH                      : $SPARSE_PATH
DENSE_PATH                       : $DENSE_PATH
OUTPUT_PATH                      : $OUTPUT_PATH
OUTPUT_DIR                       : $OUTPUT_DIR
ENU_META_JSON                    : $ENU_META_JSON
TRANSFORM_PATH                   : $TRANSFORM_PATH
DATABASE                      p1_print   : $DATABASE
COORD_FILE                       : $COORD_FILE
INPUT_PLY                        : $INPUT_PLY
DENSE_LAS                        : $DENSE_LAS
LOG_DIR                          : $LOG_DIR
LOG_FILE                         : $LOG_FILE
RAM                              : $RAM

OUTPUT_DATASET_DIR               : $OUTPUT_DATASET_DIR
OUTPUT_PATH                      : $OUTPUT_PATH
OUTPUT_DIR                       : $OUTPUT_DIR
LOG_FILE                         : $LOG_FILE
PIPELINE_LOG                     : $PIPELINE_LOG
METRICS_CSV                      : $METRICS_CSV
ENV_SNAPSHOT_CSV                 : $ENV_SNAPSHOT_CSV


NUM_MATCHES                      : $NUM_MATCHES
MAX_NUM_FEATURES                 : $MAX_NUM_FEATURES
Extraction_max_image_size        : $Extraction_max_image_size

MAPPER_MIN_NUM_MATCHES               : $MAPPER_MIN_NUM_MATCHES
MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS  : $MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS
MAPPER_INIT_MIN_NUM_INLIERS          : $MAPPER_INIT_MIN_NUM_INLIERS
MAPPER_ABS_POSE_MIN_NUM_INLIERS      : $MAPPER_ABS_POSE_MIN_NUM_INLIERS
MAPPER_ABS_POSE_MIN_INLIER_RATIO     : $MAPPER_ABS_POSE_MIN_INLIER_RATIO
MAPPER_FILTER_MAX_REPROJ_ERROR       : $MAPPER_FILTER_MAX_REPROJ_ERROR
MAPPER_NUM_THREADS                   : $MAPPER_NUM_THREADS

PatchMatchStereo_max_image_size  : $PatchMatchStereo_max_image_size
PatchMatchStereo_num_iterations  : $PatchMatchStereo_num_iterations
PatchMatchStereo_num_samples     : $PatchMatchStereo_num_samples
PatchMatchStereo_window_radius   : $PatchMatchStereo_window_radius
PatchMatchStereo_window_step     : $PatchMatchStereo_window_step
PatchMatchStereo_geom_consistency: $PatchMatchStereo_geom_consistency
StereoFusion_check_num_images    : $StereoFusion_check_num_images
StereoFusion_min_num_pixels      : $StereoFusion_min_num_pixels
StereoFusion_num_threads         : $StereoFusion_num_threads

ORTHO_RESOLUTION                 : $ORTHO_RESOLUTION
ORTHO_MAX_CANDIDATES             : $ORTHO_MAX_CANDIDATES
ORTHO_TILE_SIZE                  : $ORTHO_TILE_SIZE
ORTHO_BLEND_MODE                 : $ORTHO_BLEND_MODE
=================================================
EOF
}
