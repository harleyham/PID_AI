#!/bin/bash
# Pipeline P1 Consolidado - Projeto LIGEM (L40S)
# Orquestrador com travas de segurança e correções de georreferenciamento

SCRIPTS_DIR="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO/03_Scripts_Common/L40S"
WORKSPACE="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO/02_Pipelines_LIGEM/P1_Tradicional/workspace_DS2/L40S"
COORD_FILE="$WORKSPACE/coords_ds2.txt"

echo "====================================================="
echo "INICIANDO PROCESSAMENTO DATASET 02 - PIPELINE P1"
echo "====================================================="

# Passo 1: Correção de Variável e Extração de Metadados
echo "[PASSO 1] Extraindo coordenadas..."
cd "$SCRIPTS_DIR" && ./p1_01_feature_extraction.sh

# Passo 2: Matching Exaustivo
echo "[PASSO 2] Iniciando Feature Matching ..."
./p1_02_exhaustive_matching.sh

# Passo 3: Mapper e Alinhamento Georreferenciado
echo "[PASSO 3] Gerando Nuvem Esparsa e Alinhando ao GPS..."
./p1_03_sparse_mapper.sh

# Passo 4: Reconstrução Densa
echo "[PASSO 4] Iniciando Densificação (Patch Match Stereo)..."
./p1_04_dense_reconstruction.sh

# Passo 5: Geração do Baseline MDT (PDAL)
echo "[PASSO 5] Filtrando Solo e Gerando MDT..."
./p1_06_DEM.sh

# Passo 6: Relatório de Qualidade Final
echo "[PASSO 6] Gerando Relatório Dinâmico..."
python3 generate_ligem_report.py

echo "====================================================="
echo "PROCESSAMENTO CONCLUÍDO - $(date)"
echo "====================================================="
