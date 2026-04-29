# Analise do Pipeline P1

## Estrutura de diretorios usada pelo pipeline

Com a configuracao atual do `p1_config.sh`, o pipeline usa a seguinte organizacao:

```text
/media/ham1/EXT4/PROJETO_LIGEM_HIBRIDO/
|- 00_Datasets/
|  `- Dataset_03/
|     `- raw_images/                  # imagens de entrada
|
|- 02_Pipelines_LIGEM/
|  `- P1_Tradicional/
|     |- logs/                        # logs, metricas, snapshots, relatorio
|     |  |- pipeline_p1_Dataset_03.log
|     |  |- performance_p1_metrics_Dataset_03.csv
|     |  |- env_history_Dataset_03.csv
|     |  `- relatorio_p1_final_Dataset_03.txt
|     |
|     `- workspace_DS3/
|        `- 5060Ti/                   # area intermediaria de processamento
|           |- database_ds3.db
|           |- coords_ds3_e.txt
|           |- coords_ds3.txt
|           |- init_pair_auto.sh
|           |- init_pair_ranking.csv
|           |- local_to_enu.txt
|           |- enu_origin.json
|           |- sparse/
|           |  `- 0/                  # modelo esparso local COLMAP
|           |- enu/                   # modelo alinhado ENU
|           `- dense/                 # saida do M04
|              |- images/
|              |- sparse/
|              |- sparse_txt/
|              `- stereo/
|
|- 04_Produtos_Finais/
|  `- P1_Tradicional/
|     `- DS3/
|        `- 5060Ti/                   # produtos finais publicados
|           |- Esparsa_ENU.ply
|           |- fused_enu.ply
|           |- dense_utm_color.las
|           |- DTM_DS3.tif
|           |- DSM_DS3.tif
|           |- DTM_closed_DS3.tif
|           |- DSM_closed_DS3.tif
|           |- ORTHO_SURFACE_DS3.tif
|           |- CHM_DS3.tif
|           |- ORTHO_DS3.tif
|           |- ORTHO_DS3.vrt
|           |- ORTHO_preview_DS3.jpg
|           `- contours_1m_DS3.gpkg
|
`- 06_External_Processing/
   `- DS03/
      `- ODM_Densa_DS3.laz            # usado so no modo from_m06
```

### Papel de cada area

- `00_Datasets/.../raw_images`: entrada bruta do pipeline.
- `02_Pipelines_LIGEM/.../workspace_*`: arquivos temporarios e intermediarios por dataset/GPU.
- `02_Pipelines_LIGEM/.../logs`: trilha de execucao, metricas e relatorios.
- `04_Produtos_Finais/...`: produtos finais gerados e publicados pelo pipeline.
- `06_External_Processing/...`: insumos externos usados apenas no modo parcial `from_m06`.

### Diretorios-chave resolvidos pelo `p1_config.sh`

- `IMAGES_DIR`: `/media/ham1/EXT4/PROJETO_LIGEM_HIBRIDO/00_Datasets/Dataset_03/raw_images`
- `WORKSPACE`: `/media/ham1/EXT4/PROJETO_LIGEM_HIBRIDO/02_Pipelines_LIGEM/P1_Tradicional/workspace_DS3/5060Ti`
- `LOG_DIR`: `/media/ham1/EXT4/PROJETO_LIGEM_HIBRIDO/02_Pipelines_LIGEM/P1_Tradicional/logs`
- `OUTPUT_PATH`: `/media/ham1/EXT4/PROJETO_LIGEM_HIBRIDO/04_Produtos_Finais/P1_Tradicional/DS3/5060Ti`

### Subpastas do workspace

- `sparse/`: reconstruicao esparsa local.
- `enu/`: reconstruicao alinhada ao referencial ENU.
- `dense/`: artefatos da densificacao, incluindo imagens undistorted, modelo denso e saidas do stereo.

## O que cada modulo executa

A sequencia principal esta definida em `run_pipeline.sh`.

### M00 - Check Env

Arquivos: `p1_00_check_env.sh`, `p1_00_check_env.py`

- Carrega configuracao e logging.
- Inicializa logs e metricas.
- Valida modulos Python obrigatorios.
- Valida binarios do sistema usados no pipeline.
- Valida `COLMAP_BIN` e `PYTHON_BIN`.
- Interrompe cedo se o ambiente nao estiver apto.

### M00B - Env Snapshot

Arquivos: `p1_00_env_snapshot.sh`, `p1_env_snapshot.py`

- Coleta um snapshot tecnico do ambiente da maquina.
- Registra GPU, VRAM, driver, CUDA, GDAL, PDAL, COLMAP e PyTorch.
- Salva uma linha historica em `env_history_*.csv`.

### M01 - Feature Extraction

Arquivo: `p1_01_feature_extraction.sh`

- Garante que o diretorio de imagens existe.
- Remove o banco anterior e cria um novo `database_*.db`.
- Executa `colmap feature_extractor`.
- Extrai GPS EXIF com `exiftool`.
- Converte as coordenadas para o formato esperado pelo pipeline via `p1_fix_coords.py`.

Saidas principais:

- `database_ds3.db`
- `coords_ds3_e.txt`
- `coords_ds3.txt`

### M02 - Exhaustive Matching

Arquivo: `p1_02_exhaustive_matching.sh`

- Usa o banco gerado no M01.
- Executa `colmap exhaustive_matcher`.
- Analisa conectividade entre imagens com `p1_dialog_overlap.py`.
- Escolhe automaticamente o melhor par inicial com `p1_select_init_pair.py`.

Saidas principais:

- `init_pair_auto.sh`
- `init_pair_ranking.csv`

### M03 - Sparse Mapper + ENU

Arquivo: `p1_03_sparse_mapper.sh`

- Usa banco, coordenadas GPS e par inicial do M02.
- Executa `colmap mapper` com fallback entre pares candidatos.
- Valida o modelo esparso pelo numero minimo de imagens registradas.
- Executa `colmap model_aligner` para alinhar ao referencial ENU usando GPS.
- Gera `enu_origin.json` com os metadados do referencial.
- Exporta a esparsa alinhada para PLY com `colmap model_converter`.

Saidas principais:

- `workspace/.../sparse/0`
- `workspace/.../enu`
- `local_to_enu.txt`
- `enu_origin.json`
- `Esparsa_ENU.ply`

### M04 - Dense Reconstruction

Arquivo: `p1_04_dense_reconstruction.sh`

- Usa o modelo alinhado em `enu/`.
- Executa `colmap image_undistorter`.
- Executa `colmap patch_match_stereo`.
- Executa `colmap stereo_fusion`.
- Mede tamanho, numero de pontos e resumo PDAL da nuvem densa.

Saidas principais:

- `fused_enu.ply`
- `dense/images`
- `dense/sparse`
- `dense/stereo`

### M05 - Export Dense

Arquivos: `p1_05_export_dense_robusto.sh`, `p1_05_export_dense.py`, `p1_05_stat.py`

- Usa `fused_enu.ply` e `enu_origin.json`.
- Converte a nuvem do referencial ENU para UTM.
- Preserva RGB no LAS.
- Calcula estatisticas do LAS gerado.

Saida principal:

- `dense_utm_color.las`

### M06 - DEM / DSM / DTM / CHM

Arquivos: `p1_06_DEM.sh`, `p1_06_GPU_DEM.py`

- Usa o LAS denso e o `enu_origin.json`.
- Executa `pdal info --summary`.
- Opcionalmente remove outliers baixos.
- Classifica solo com `filters.smrf`.
- Gera DTM e DSM analiticos com PDAL.
- Fecha vazios com `gdal_fillnodata.py`.
- Monta `ORTHO_SURFACE` robusta.
- Gera hillshade com `gdaldem`.
- Gera `CHM` com `gdal_calc.py`.

Saidas principais:

- `DTM_*.tif`
- `DSM_*.tif`
- `DTM_closed_*.tif`
- `DSM_closed_*.tif`
- `ORTHO_SURFACE_*.tif`
- `CHM_*.tif`
- opcionais: hillshades e `dense_ground_*.laz`

### M07 - Orthomosaic

Arquivos: `p1_07_orthomosaic.sh`, `p1_07_orthomosaic.py`

- Usa a superficie do M06, imagens undistorted do M04, modelo COLMAP denso e `enu_origin.json`.
- Converte `dense/sparse` de binario para TXT.
- Le cameras e poses.
- Projeta pixels das imagens sobre a superficie escolhida.
- Faz blending e monta o raster ortorretificado.
- Gera preview e VRT.

Saidas principais:

- `ORTHO_DS3.tif`
- `ORTHO_DS3.vrt`
- `ORTHO_preview_DS3.jpg`

### M08 - Contours

Arquivos: `p1_08_contours.sh`, `p1_08_contours_stat.py`

- Escolhe o raster base conforme `CONTOUR_INPUT_MODE`.
- Executa `gdal_contour`.
- Gera o vetor de curvas de nivel.
- Calcula estatisticas do vetor e do raster de origem.

Saida principal:

- `contours_1m_DS3.gpkg`

### M09 - Report

Arquivo: `generate_ligem_report.py`

- Le a configuracao ativa.
- Le as metricas acumuladas no CSV.
- Inspeciona banco, modelo ENU, PLY, LAS e rasters.
- Consolida qualidade, tempos e existencia dos produtos.
- Gera o relatorio final textual.

## Modo parcial

Quando `PIPELINE_RUN_MODE=from_m06`:

- o pipeline pula `M00` a `M05`
- usa um LAS externo
- executa `M06` e `M08`
- `M07` fica pulado na implementacao atual
