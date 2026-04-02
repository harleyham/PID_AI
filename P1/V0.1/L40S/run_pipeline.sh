#!/bin/bash
# Pipeline P1


echo "====================================================="
echo "INICIANDO PROCESSAMENTO DATASET 02 - PIPELINE P1"
echo "====================================================="


./p1_00_check_env.sh

./p1_00_env_snapshot.sh


# Passo 1: Correção de Variável e Extração de Metadados
echo "[PASSO 1] Extraindo coordenadas..."
./p1_01_feature_extraction.sh

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
./p1_05_export_dense_robust.sh

./p1_06_DEM.sh

./p1_07_orthomosaic.sh

# Passo 6: Relatório de Qualidade Final
echo "[PASSO 6] Gerando Relatório Dinâmico..."
python3 generate_ligem_report.py

echo "====================================================="
echo "PROCESSAMENTO CONCLUÍDO - $(date)"
echo "====================================================="
