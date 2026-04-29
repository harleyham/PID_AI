#!/bin/bash
set -euo pipefail

PROJECT_ROOT="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO"
P2_CODE_DIR="$PROJECT_ROOT/03_Scripts_Common/P2/L40S/V0.1"

export P2_PIPELINE_KIND="train"
export P2_EXECUTION_MODE="live"
export GROUND_TAO_SUBCOMMAND="train"
export PREPROCESS_LABEL_SOURCE="${PREPROCESS_LABEL_SOURCE:-classification}"

export P2_01_ENABLED="${P2_01_ENABLED:-1}"
export P2_02_ENABLED="${P2_02_ENABLED:-1}"
export P2_03_ENABLED="${P2_03_ENABLED:-1}"
export P2_04_ENABLED="${P2_04_ENABLED:-1}"
export P2_05_ENABLED="${P2_05_ENABLED:-0}"
export P2_06_ENABLED="${P2_06_ENABLED:-0}"
export P2_07_ENABLED="${P2_07_ENABLED:-0}"
export P2_08_ENABLED="${P2_08_ENABLED:-0}"

exec "$P2_CODE_DIR/run_pipeline.sh"
