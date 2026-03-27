#!/bin/bash
# Pipeline V0.2 - Módulo 07: Ortomosaico
# Gera o mosaico retificado utilizando o MDS e a nuvem densa.

GPU="L40S"
DATASET="Dataset_02"

PROJECT_ROOT="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO"
WORKSPACE="$PROJECT_ROOT/02_Pipelines_LIGEM/P1_Tradicional/workspace_DS2/L40S"
DENSE_PATH="$WORKSPACE/dense"
DSM_TIF="$PROJECT_ROOT/04_Produtos_Finais/DS2/Produtos_Raster/DSM.tif"
OUTPUT_DIR="$PROJECT_ROOT/04_Produtos_Finais/DS2/Produtos_Raster"
LOG_FILE="$PROJECT_ROOT/02_Pipelines_LIGEM/P1_Tradicional/logs/performance_p1.csv"

# Configurações de saída
ORTHO_NAME="Ortofoto_DS2.tif"

mkdir -p "$OUTPUT_DIR"

if [ ! -f "$DSM_TIF" ]; then
    echo "ERRO: O DSM não foi encontrado. Execute o Módulo 06 primeiro."
    exit 1
fi

echo "------------------------------------------------------" | tee -a "$LOG_FILE"
echo "Iniciando Módulo 07 (Ortomosaico) - $(date)" | tee -a "$LOG_FILE"
start_time=$(date +%s)

# 1. Geração do Ortomosaico via COLMAP
# Nota: O COLMAP usa o workspace da reconstrução densa (onde estão as imagens undistorted)
# para projetar as texturas sobre a superfície.
colmap image_mesher \
    --database_path "$WORKSPACE/database_ds2.db" \
    --image_path "$DENSE_PATH/images" \
    --input_path "$DENSE_PATH/sparse" \
    --output_path "$OUTPUT_DIR/$ORTHO_NAME" \
    --ImageMesher.max_num_features 10000

# 2. Alternativa recomendada para Pipeline LIGEM: 
# Se você estiver usando ferramentas externas como o OpenDroneMap (ODM) para o mosaico 
# ou se preferir usar o GDAL para fundir as projeções, o comando abaixo
# ajuda a organizar as geotiffs individuais se necessário.

# 3. Verificação de Metadados Geoespaciais
# Garante que a ortofoto herde o CRS (EPSG) definido no Módulo 05
# Substitua OUT_EPSG pelo valor detectado anteriormente (Ex: 31983)
# gdal_edit.py -a_srs EPSG:31983 "$OUTPUT_DIR/$ORTHO_NAME"

end_time=$(date +%s)
duration=$((end_time - start_time))

echo "$DATASET,p1_M07_Ortho_$GPU,$duration,seconds,Success" >> "$LOG_FILE"
echo "------------------------------------------------------"
echo "Ortomosaico gerado em: $OUTPUT_DIR/$ORTHO_NAME"
echo "Duração do Módulo: $duration segundos."
echo "------------------------------------------------------"
