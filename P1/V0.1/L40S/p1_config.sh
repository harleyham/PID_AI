#!/bin/bash
set -euo pipefail

# ============================================================
# Pipeline P1 - Configuração central
# ============================================================

# Tipo de informação                Função correta
# Mensagem humana (debug, status)   p1_log_info
# Aviso	                            p1_log_warn
# Erro	                            p1_log_error
# Dado mensurável                   p1_metric

# Parâmetros principais
export PROJECT_ROOT="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO"
export PIPELINE_NAME="P1_Tradicional"

export DATASET="Dataset_03"
export GPU="L40S"

# ============================================================
# Seleção automática do par inicial do M03
# ============================================================

# Override manual opcional:
# deixe vazio para usar o par automático escolhido no M02
export INIT_IMAGE_ID1=""
export INIT_IMAGE_ID2=""

# Filtros da seleção automática
export INIT_PAIR_TOP_K="15"
export INIT_PAIR_MIN_INLIERS="50"
export INIT_PAIR_MIN_DEGREE="5"

# Robustez do mapper
export MAPPER_MIN_NUM_MATCHES="15"
export MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS="80"
export MAPPER_INIT_MIN_NUM_INLIERS="80"
export MAPPER_ABS_POSE_MIN_NUM_INLIERS="30"
export MAPPER_ABS_POSE_MIN_INLIER_RATIO="0.20"
export MAPPER_FILTER_MAX_REPROJ_ERROR="3"

# Alinhamento
export ALIGNMENT_MAX_ERROR="10"
export ALIGNMENT_TYPE="enu"

# Falha cedo se o modelo local registrar menos que isso.
export MIN_REGISTERED_IMAGES_LOCAL="10"


# Diretório dos scripts
export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


# ============================================================
# Derivações automáticas
# ============================================================

case "$DATASET" in
    Dataset_01) export DATASET_SLUG="DS1" ;;
    Dataset_02) export DATASET_SLUG="DS2" ;;
    Dataset_03) export DATASET_SLUG="DS3" ;;
    Dataset_04) export DATASET_SLUG="DS4" ;;
    *) export DATASET_SLUG="$DATASET" ;;
esac

export IMAGES_DIR="$PROJECT_ROOT/00_Datasets/$DATASET/raw_images"

export WORKSPACE="$PROJECT_ROOT/02_Pipelines_LIGEM/$PIPELINE_NAME/workspace_${DATASET_SLUG}/$GPU"
export LOG_DIR="$PROJECT_ROOT/02_Pipelines_LIGEM/$PIPELINE_NAME/logs"
export LOG_FILE="$LOG_DIR/performance_P1.csv"
export PIPELINE_LOG="$LOG_DIR/pipeline_P1.log"
export METRICS_CSV="$LOG_DIR/performance_P1_metrics.csv"

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
export OUTPUT_PATH="$OUTPUT_DATASET_DIR/$GPU"
export OUTPUT_DIR="$OUTPUT_PATH/Produtos_Raster"

export DENSE_PATH="$WORKSPACE/dense"
export INPUT_PLY="$OUTPUT_PATH/fused_enu.ply"
export DENSE_LAS="$OUTPUT_PATH/dense_utm_color.las"

# Snapshot de ambiente
export PYTHON_BIN="python"
export ENV_SNAPSHOT_CSV="$LOG_DIR/env_history.csv"

# cache_size depende da memória do computador
# 24 -> L20S
# 8 -> P1000
export RAM="20"

# colmap exhaustive_matcher.FeatureMatching.max_num_matches
export NUM_MATCHES="30000"


# Modulo 01
export MAX_NUM_FEATURES="60000"
export Extraction_max_image_size="1600"


# Modulo 04
export PatchMatchStereo_num_iterations="4"
export PatchMatchStereo_num_samples="20"
export PatchMatchStereo_window_radius="6"
export PatchMatchStereo_window_step="1"
export PatchMatchStereo_filter="1"
export PatchMatchStereo_max_image_size="2600"

export StereoFusion_check_num_images="4"
export StereoFusion_min_num_pixels="8"


# ============================================================
# Utilitários
# ============================================================

p1_ensure_dirs() {
    mkdir -p "$WORKSPACE" "$LOG_DIR" "$OUTPUT_DATASET_DIR" "$OUTPUT_PATH" "$OUTPUT_DIR"
    mkdir -p "$SPARSE_PATH" "$ENU_PATH" "$DENSE_PATH"
}

p1_print_config() {
    cat <<EOF
================ CONFIGURAÇÃO P1 ================
ALIGN_PATH          : $ALIGN_PATH
ALIGNMENT_TYPE      : $ALIGNMENT_TYPE
ALIGNMENT_MAX_ERROR : $ALIGNMENT_MAX_ERROR
COORD_FILE        : $COORD_FILE
DATABASE          : $DATABASE
DATASET           : $DATASET
DATASET_SLUG      : $DATASET_SLUG
DENSE_LAS         : $DENSE_LAS
DENSE_PATH        : $DENSE_PATH
ENU_META_JSON     : $ENU_META_JSON
ENU_PATH          : $ENU_PATH
GPU               : $GPU
IMAGES_DIR        : $IMAGES_DIR
INPUT_PLY         : $INPUT_PLY
LOG_DIR           : $LOG_DIR
LOG_FILE          : $LOG_FILE
MAPPER_MIN_NUM_MATCHES               : $MAPPER_MIN_NUM_MATCHES
MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS  : $MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS
MAPPER_INIT_MIN_NUM_INLIERS          : $MAPPER_INIT_MIN_NUM_INLIERS
MAPPER_ABS_POSE_MIN_NUM_INLIERS      : $MAPPER_ABS_POSE_MIN_NUM_INLIERS
MAPPER_ABS_POSE_MIN_INLIER_RATIO     : $MAPPER_ABS_POSE_MIN_INLIER_RATIO
MAPPER_FILTER_MAX_REPROJ_ERROR       : $MAPPER_FILTER_MAX_REPROJ_ERROR
OUTPUT_DATASET_DIR: $OUTPUT_DATASET_DIR
OUTPUT_DIR        : $OUTPUT_DIR
OUTPUT_PATH       : $OUTPUT_PATH
PIPELINE_NAME     : $PIPELINE_NAME
PROJECT_ROOT      : $PROJECT_ROOT
RAM               : $RAM
SCRIPT_DIR        : $SCRIPT_DIR
SPARSE_PATH       : $SPARSE_PATH
TRANSFORM_PATH    : $TRANSFORM_PATH
WORKSPACE         : $WORKSPACE
=================================================
EOF
}
