#!/bin/bash
set -euo pipefail

# ============================================================
# Pipeline V0.2 - Configuração central
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

export DATASET="Dataset_03"
export GPU="L40S"
export StereoFusion_num_threads="24"


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

export WORKSPACE="$PROJECT_ROOT/02_Pipelines_LIGEM/$PIPELINE_NAME/workspace_${DATASET_SLUG}/$GPU"
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
export OUTPUT_PATH="$OUTPUT_DATASET_DIR/$GPU"
export OUTPUT_DIR="$OUTPUT_PATH"

export DENSE_PATH="$WORKSPACE/dense"
export INPUT_PLY="$OUTPUT_PATH/fused_enu.ply"
export DENSE_LAS="$OUTPUT_PATH/dense_utm_color.las"

# Snapshot de ambiente
export PYTHON_BIN="python"
export ENV_SNAPSHOT_CSV="$LOG_DIR/env_history_${DATASET}.csv"

# cache_size depende da memória do computador
# 24 -> L20S/L40S
# 8  -> P1000
export RAM="20"

# colmap exhaustive_matcher.FeatureMatching.max_num_matches
export NUM_MATCHES="40000"

# ------------------------------------------------------------
# Módulo 01
# ------------------------------------------------------------
export MAX_NUM_FEATURES="60000"
export Extraction_max_image_size="1600"

# ------------------------------------------------------------
# Módulo 04 - reconstrução densa
# Foco atual: aumentar cobertura útil da nuvem densa
# ------------------------------------------------------------
export COLMAP_GPU_INDEX="0"

# patch_match: mais cobertura útil, mantendo um perfil ainda estável
export PatchMatchStereo_max_image_size="2600"
export PatchMatchStereo_num_iterations="4"
export PatchMatchStereo_num_samples="20"
export PatchMatchStereo_window_radius="6"
export PatchMatchStereo_window_step="1"
export PatchMatchStereo_geom_consistency="false"

# stereo_fusion: reduzir rigidez para aumentar cobertura
export StereoFusion_check_num_images="2"
export StereoFusion_min_num_pixels="4"

# ------------------------------------------------------------
# Módulo 06 - DEM / DSM / DTM / CHM
# Foco atual:
# 1) melhorar terra nua
# 2) reduzir vazios
# 3) gerar superfícies fechadas para uso posterior
# ------------------------------------------------------------
export DEM_RESOLUTION="0.05"   # metros
export DEM_NODATA="-9999"

# SMRF - terreno
export SMRF_SCALAR="1.25"
export SMRF_SLOPE="0.15"
export SMRF_THRESHOLD="0.50"
export SMRF_WINDOW="16.0"

# Produtos analíticos (mais fiéis)
export DTM_OUTPUT_TYPE="idw"
export DTM_WINDOW_SIZE="2"

export DSM_OUTPUT_TYPE="max"
export DSM_WINDOW_SIZE="1"

# Produtos fechados (mais contínuos) para ortho/contorno
export DTM_CLOSED_OUTPUT_TYPE="idw"
export DTM_CLOSED_WINDOW_SIZE="8"

export DSM_CLOSED_OUTPUT_TYPE="idw"
export DSM_CLOSED_WINDOW_SIZE="4"

# fillnodata dedicado por superfície
export DTM_FILLNODATA_MAX_DISTANCE="60"
export DSM_FILLNODATA_MAX_DISTANCE="30"
export FILLNODATA_SMOOTHING_ITERATIONS="1"

# superfície híbrida para ortho/uso posterior
export ORTHO_SURFACE_MODE="DSM_THEN_DTM"

# Pré-filtro de outliers baixos antes do SMRF
# Objetivo: remover pontos espúrios muito abaixo da superfície real,
# que estão contaminando o DSM/DTM e derrubando o Z mínimo.


# piso robusto:
# calcula o percentil inferior da cota Z e aceita apenas pontos
# acima de (percentil - margem_em_metros)

#    Leitura desses parâmetros
# LOW_OUTLIER_PERCENTILE="1.0"
# usa o percentil 1% da distribuição de Z como referência robusta;
# LOW_OUTLIER_MARGIN="5.0"
# aceita pontos até 5 metros abaixo desse percentil;
# OUTLIER_MEAN_K="12" e OUTLIER_MULTIPLIER="2.5"
# removem pontos isolados via filtro estatístico antes do corte por Z.
export LOW_OUTLIER_ENABLE="1"
export LOW_OUTLIER_PERCENTILE="1.0"
export LOW_OUTLIER_MARGIN="5.0"

# remoção estatística de pontos isolados antes do corte altimétrico
export OUTLIER_MEAN_K="12"
export OUTLIER_MULTIPLIER="2.5"

# ------------------------------------------------------------
# M07 - ORTOMOSAICO
# Mantido por compatibilidade, mas não é a prioridade agora
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
ALIGN_PATH          : $ALIGN_PATH
ALIGNMENT_TYPE      : $ALIGNMENT_TYPE
ALIGNMENT_MAX_ERROR : $ALIGNMENT_MAX_ERROR
COORD_FILE          : $COORD_FILE
DATABASE            : $DATABASE
DATASET             : $DATASET
DATASET_SLUG        : $DATASET_SLUG
DENSE_LAS           : $DENSE_LAS
DENSE_PATH          : $DENSE_PATH
ENU_META_JSON       : $ENU_META_JSON
ENU_PATH            : $ENU_PATH
GPU                 : $GPU
IMAGES_DIR          : $IMAGES_DIR
INPUT_PLY           : $INPUT_PLY
LOG_DIR             : $LOG_DIR
LOG_FILE            : $LOG_FILE
MAPPER_MIN_NUM_MATCHES               : $MAPPER_MIN_NUM_MATCHES
MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS  : $MAPPER_BA_GLOBAL_MAX_NUM_ITERATIONS
MAPPER_INIT_MIN_NUM_INLIERS          : $MAPPER_INIT_MIN_NUM_INLIERS
MAPPER_ABS_POSE_MIN_NUM_INLIERS      : $MAPPER_ABS_POSE_MIN_NUM_INLIERS
MAPPER_ABS_POSE_MIN_INLIER_RATIO     : $MAPPER_ABS_POSE_MIN_INLIER_RATIO
MAPPER_FILTER_MAX_REPROJ_ERROR       : $MAPPER_FILTER_MAX_REPROJ_ERROR
OUTPUT_DATASET_DIR  : $OUTPUT_DATASET_DIR
OUTPUT_DIR          : $OUTPUT_DIR
OUTPUT_PATH         : $OUTPUT_PATH
PIPELINE_NAME       : $PIPELINE_NAME
PROJECT_ROOT        : $PROJECT_ROOT
RAM                 : $RAM
SCRIPT_DIR          : $SCRIPT_DIR
SPARSE_PATH         : $SPARSE_PATH
TRANSFORM_PATH      : $TRANSFORM_PATH
WORKSPACE           : $WORKSPACE
=================================================
EOF
}