#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

from p2_lib.pipeline_common import file_info, module_manifest, write_json


def main() -> int:
    parser = argparse.ArgumentParser(description="P2 M06 - Refinamento do DTM.")
    parser.add_argument("--input-dtm", required=True)
    parser.add_argument("--output-dtm", required=True)
    parser.add_argument("--fill-distance", required=True, type=float)
    parser.add_argument("--smoothing-passes", required=True, type=int)
    parser.add_argument("--preserve-breaklines", required=True)
    parser.add_argument("--output-manifest", required=True)
    args = parser.parse_args()

    manifest = module_manifest(
        module="M06_REFINE_DTM",
        execution_mode="stub",
        inputs={
            "input_dtm": file_info(args.input_dtm),
        },
        outputs={
            "refined_dtm": {"path": args.output_dtm, "materialized": False},
            "manifest": args.output_manifest,
        },
        metrics={
            "fill_distance": args.fill_distance,
            "smoothing_passes": args.smoothing_passes,
            "preserve_breaklines": args.preserve_breaklines,
        },
        notes=[
            "M06 reserva o espaco para fechamento de vazios, suavizacao e preservacao de feicoes do terreno.",
        ],
    )
    write_json(args.output_manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
