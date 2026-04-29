# P2 Hibrido Post IA

Fluxo curto de uso por ambiente para o pipeline `P2_Hibrido_Post_IA`.

## Pipelines Separados

O P2 agora opera com dois fluxos versionados:

- Treinamento: `P2_PIPELINE_KIND=train`, executado por `./run_train_pipeline.sh`.
- Inferencia: `P2_PIPELINE_KIND=infer`, executado por `./run_infer_pipeline.sh`.

Versionamento principal definido em `p2_config.sh`:

```bash
export P2_TRAIN_VERSION="V0.1"
export P2_INFER_VERSION="V0.1"
export P2_MODEL_VERSION="V0.1"
```

O treinamento grava os resultados TAO no registro central do modelo:

```text
${DATA_HOST_BASE}/models/${GROUND_MODEL_NAME}/${P2_MODEL_VERSION}/tao_runs
```

A inferencia grava workspace, logs e resultados na versao propria do pipeline:

```text
${DATA_HOST_BASE}/infer/${P2_INFER_VERSION}/workspace_<dataset>/<gpu>
${DATA_HOST_BASE}/infer/${P2_INFER_VERSION}/results
```

Uso recomendado:

```bash
./run_train_pipeline.sh
./run_infer_pipeline.sh
```

Para usar outro modelo treinado na inferencia, edite `P2_MODEL_VERSION` em `p2_config.sh` e execute:

```bash
./run_infer_pipeline.sh
```

## Escolha Rapida

Agora a escolha principal e feita diretamente em `p2_config.sh`.

Edite esta linha:

```bash
export GPU="${GPU:-5060Ti}"
```

Raiz unica aplicada em todos os ambientes:

- `PROJECT_ROOT=/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO`
- dados originais, nuvens, tiles, modelos, resultados e scripts operacionais ficam abaixo dessa raiz.

Depois execute o fluxo desejado:

```bash
./run_train_pipeline.sh
./run_infer_pipeline.sh
```

`./run_pipeline.sh` continua existindo como runner base e usa `P2_PIPELINE_KIND=infer` por padrao.

## Padrao de paths

No host:

- raiz unica: `/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO`

No container, em qualquer maquina:

- dados: `/data`
- scripts: `/scripts`
- resultados: `/results`

## Perfis de ambiente

- `env_l40s.sh`
- `env_5060ti.sh`

Esses arquivos definem:

- `HOST_ROOT`
- `DATA_HOST_BASE`
- `SCRIPTS_HOST`
- `RESULTS_HOST`
- `TAO_IMAGE`
- `P2_ENV`

Esses perfis ficam apenas como referencia de ambiente; o fluxo padrao usa a raiz unica definida em `p2_config.sh`.

## Como executar

L40S:

```bash
edite GPU=L40S em p2_config.sh
./run_infer_pipeline.sh
```

RTX5060Ti:

```bash
edite GPU=5060Ti em p2_config.sh
./run_infer_pipeline.sh
```

Nao e necessario usar `source` nem variaveis antes do comando. Ajuste os valores diretamente em `p2_config.sh` e execute o runner desejado.

## Montagem do container

O TAO passa a usar sempre estes mounts:

```bash
-v "${TAO_DATA_HOST}:/data"
-v "${SCRIPTS_HOST}:/scripts"
-v "${RESULTS_HOST}:/results"
```

## Debug manual com Docker Compose

Suba um shell no mesmo ambiente usado pelo pipeline:

```bash
source ./env_5060ti.sh
docker compose -f ./docker-compose.tao.yaml run --rm tao-p2 bash
```

Exemplo direto equivalente ao fluxo atual do M04 em DS3 / RTX5060Ti:

```bash
source ./env_5060ti.sh
docker compose -f ./docker-compose.tao.yaml run --rm tao-p2 \
  bash -lc "segformer train \
    -e /results/DS3/RTX5060Ti/tao_runs/segformer/experiment_train_generated.yaml \
    results_dir=/results/DS3/RTX5060Ti/tao_runs/segformer \
    dataset.segment.batch_size=1"
```

Exemplo para gerar spec default manualmente:

```bash
source ./env_5060ti.sh
docker compose -f ./docker-compose.tao.yaml run --rm tao-p2 \
  bash -lc "segformer default_specs \
    results_dir=/results/DS3/RTX5060Ti/tao_runs/segformer"
```

Se quiser trocar de maquina, basta carregar `env_l40s.sh` no lugar de `env_5060ti.sh`.

## Onde ficam os artefatos

- raiz do projeto: `${PROJECT_ROOT}`
- fotos originais: `${ORIGINAL_IMAGES_DIR}`
- base montada no container em `/data`: `${TAO_DATA_HOST}`
- workspace do pipeline: `${DATA_HOST}/workspace_<dataset>/<gpu>`
- logs: `${DATA_HOST}/logs`
- resultados TAO de inferencia: `${RESULTS_HOST}/<dataset_slug>/<gpu>/tao_runs/segformer`
- modelos treinados: `${DATA_HOST_BASE}/models/${GROUND_MODEL_NAME}/${P2_MODEL_VERSION}/tao_runs`

Exemplo atual em DS3 / RTX5060Ti:

```text
/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO/02_Pipelines_LIGEM/P2_Hibrido_Post_IA/
├── logs/
├── results/DS3/RTX5060Ti/tao_runs/segformer/
└── workspace_DS3/RTX5060Ti/
```
