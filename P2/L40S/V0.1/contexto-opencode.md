# Contexto OpenCode

## Resumo do Objetivo Atual

O objetivo atual do pipeline `P2` e estruturar um fluxo funcional para classificacao semantica de solo com `SegFormer` no ecossistema NVIDIA, usando:

- `RTX5060Ti` como ambiente principal de desenvolvimento e validacao local
- `L40S` como ambiente de laboratorio para treino e testes mais pesados
- `B300` como alvo futuro de producao

O foco imediato e viabilizar o ciclo:

1. ingestao real da nuvem densa
2. diagnostico real da nuvem
3. geracao real de dataset 2D para segmentacao
4. treino real do `SegFormer` no container TAO

## O Que Foi Implementado Hoje

### M01_INGEST

- deixou de ser apenas contrato
- agora materializa a nuvem normalizada em `workspace_DS3/<GPU>/normalized/`
- registra `sha256` de entrada e saida
- registra `bytes_written`
- o pipeline passou a usar `NORMALIZED_POINT_CLOUD` como saida do `M01`

### M02_DIAGNOSTICO

- diagnostico real implementado
- usa `laspy` como backend principal quando disponivel
- usa `PDAL` como fallback real quando `laspy` nao estiver disponivel
- gera no relatorio:
  - `point_count`
  - `bbox`
  - `z_stats`
  - `point_density_per_m2`
  - `header.version`
  - `point_format`
  - `scales`
  - `offsets`
  - `spatial_reference`
  - `classes_asprs`
  - `class_histogram`
  - `returns`
  - `diagnostic_backend`

### M03_PREPROCESS_DATASET

- primeira versao real implementada
- rasteriza a nuvem normalizada em tiles RGB usando `PDAL + numpy + Pillow`
- gera estrutura de dataset em:
  - `images/train|val|test`
  - `masks/train|val|test`
  - `metadata/`
- gera:
  - `tile_index.geojson`
  - `dataset_manifest.json`
  - `classes.json`
- passou a gerar pseudo-mascaras iniciais por heuristica com classes:
  - `ground`
  - `vegetation`
  - `building`
  - `background`
  - `nodata`
- mascaras agora sao gravadas em RGB para compatibilidade com a `palette` esperada pelo `SegFormer`
- foi implementado descarte de tiles residuais pequenos via:
  - `PREPROCESS_MIN_TILE_AREA_PERCENT`
- o dataset e limpo antes de cada rerun do `M03`

### Ajustes de Configuracao

- `GPU` default alterada para `5060Ti`
- defaults do pipeline alterados para habilitar:
  - `M01`
  - `M02`
  - `M03`
- `TILE_SIZE_METERS` default alterado de `256` para `128`
- adicionada a variavel:
  - `PREPROCESS_MIN_TILE_AREA_PERCENT`

### M04_GROUND_AI Train

- `M04` foi adaptado para foco em `train`
- passou a consumir o dataset real do `M03`
- gera um spec derivado de treino em:
  - `tao_runs/segformer/experiment_train_generated.yaml`
- injeta automaticamente no spec:
  - `root_dir` do dataset
  - `palette`
  - `splits`
  - `workers`
  - `epochs`
  - `batch_size` efetivo
- o treino foi validado com execucao real no `RTX5060Ti`

Artefatos gerados no treino:

- `model_epoch_000_step_00000.pth`
- `model_epoch_000_step_00007.pth`
- `segformer_model_latest.pth`
- `status.json`
- `lightning_logs/`

Metricas observadas no teste de 1 epoca:

- `val_loss = 1.050`
- `val_acc = 0.535`
- `val_miou = 0.148`

## Problemas Pendentes

### Dataset ainda e bootstrap

- as mascaras do `M03` sao pseudo-mascaras heuristicas
- elas nao devem ser tratadas como ground truth final
- o treino atual serve para validacao do pipeline, nao para modelo final de producao

### Dataset pequeno

- apesar de agora haver `train/val/test`, o dataset `DS3` ainda e pequeno
- hoje o split efetivo esta em:
  - `train: 7`
  - `val: 1`
  - `test: 1`

### M04 ainda cobre apenas treino

- o `train` foi validado
- `inference` ainda nao foi implementado no fluxo do `M04`
- ainda nao ha geracao padronizada de `predictions/` para consumo operacional diario

### M05 ainda nao foi implementado de forma real

- ainda nao existe a reintegracao das mascaras previstas ao dominio da nuvem
- ainda nao existe filtragem semantica real de solo
- ainda nao existe geracao real de:
  - `DTM`
  - `DSM`
  - `void_mask`
  - `confidence`

### M06 continua stub

- refinamento de DTM ainda nao foi implementado

### Qualidade do treino

- o treino usa pseudo-rotulos ainda nao revisados manualmente
- isso limita a confiabilidade dos checkpoints atuais

## Proximos Passos Claros Para Amanha

### 1. Implementar o M04 em modo Inference

Objetivo:

- usar checkpoint treinado do `SegFormer`
- executar inferencia sobre `images/test` ou sobre tiles de producao
- gerar saidas padronizadas em `predictions/`
- registrar manifesto correspondente

### 2. Definir estrategia de checkpoints

Objetivo:

- padronizar qual checkpoint usar no dia a dia
- separar claramente:
  - treino experimental
  - checkpoint selecionado para inferencia

### 3. Melhorar a qualidade do dataset

Objetivo:

- revisar manualmente um subconjunto das pseudo-mascaras do `M03`
- criar uma base supervisionada mais confiavel para novo treino

### 4. Planejar e iniciar implementacao real do M05

Objetivo:

- consumir mascaras previstas do `M04`
- projetar a mascara 2D de volta para a nuvem ou grade
- filtrar classe `ground`
- preparar geracao real de `DTM/DSM`

### 5. Revisar operacao por GPU

Objetivo:

- manter `RTX5060Ti` como default local
- validar se o mesmo fluxo roda de forma equivalente na `L40S`
- preservar compatibilidade com a futura `B300`

## Estado Atual Resumido dos Modulos

| Modulo | Estado atual |
|---|---|
| `M01` | real |
| `M02` | real |
| `M03` | real para `images + metadata + pseudo-masks` |
| `M04` | real para `train` |
| `M05` | contrato apenas |
| `M06` | stub |
| `M07` | real em `live` |
| `M08` | real |
