# Drone Photogrammetry Pipeline

Pipeline modular para processamento de imagens de drone com foco em uso intensivo de GPU, usando COLMAP, PDAL e GDAL.

## Objetivo

Construir um fluxo reproduzível para:

- extração de features e matching
- reconstrução esparsa e alinhamento geográfico
- reconstrução densa
- exportação de nuvem para LAS
- geração de DSM, DTM, CHM e derivados

O baseline de qualidade do projeto é a comparação com produtos gerados no Metashape e no ODM.

## Estrutura

- `L40S/`: versão/configuração do pipeline para a GPU NVIDIA L40S
- `5060Ti/`: versão/configuração do pipeline para a GPU NVIDIA 5060Ti
- `P1000/`: versão/configuração do pipeline para a GPU NVIDIA P1000
- `Src/`: utilitários e arquivos auxiliares relacionados a plugins e testes

Cada diretório de GPU contém os módulos `M00` a `M06` do pipeline.

## Módulos

- `M00`: verificação e snapshot de ambiente
- `M01`: feature extraction e preparação das coordenadas
- `M02`: matching e seleção automática do par inicial
- `M03`: reconstrução esparsa e alinhamento ENU
- `M04`: reconstrução densa
- `M05`: exportação PLY -> LAS
- `M06`: geração de DSM, DTM, CHM e hillshade

## Dependências principais

- COLMAP
- PDAL
- GDAL
- Python 3
- `numpy`
- `laspy`
- `rasterio`

## Observações

- Os arquivos `p1_config.sh` usam caminhos absolutos locais e devem ser adaptados para cada máquina.
- Os produtos gerados, logs e bancos locais não são versionados.
- O desenvolvimento considera diferentes GPUs para comparação de desempenho e qualidade.

## Status

O projeto está em desenvolvimento ativo, com foco inicial no `Dataset_03`.
