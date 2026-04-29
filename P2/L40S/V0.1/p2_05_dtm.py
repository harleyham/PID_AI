#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

from p2_lib.pipeline_common import file_info, module_manifest, write_json


def main() -> int:
    parser = argparse.ArgumentParser(description="P2 M05 - Filtragem semantica do solo e geracao de DTM/DSM.")
    parser.add_argument("--input-point-cloud", required=True)
    parser.add_argument("--dtm-raster", required=True)
    parser.add_argument("--dsm-raster", required=True)
    parser.add_argument("--void-mask-raster", required=True)
    parser.add_argument("--confidence-raster", required=True)
    parser.add_argument("--resolution", required=True, type=float)
    parser.add_argument("--interpolation", required=True)
    parser.add_argument("--power", required=True, type=float)
    parser.add_argument("--search-radius", required=True, type=float)
    parser.add_argument("--output-manifest", required=True)
    args = parser.parse_args()

    input_path = Path(args.input_point_cloud)
    manifest = module_manifest(
        module="M05_DTM_FILTER",
        execution_mode="stub",
        inputs={
            "input_point_cloud": file_info(input_path),
            "interpolation": args.interpolation,
            "semantic_source": "m04_predictions_or_classified_cloud",
        },
        outputs={
            "filtered_ground_point_cloud": {
                "path": str(Path(args.input_point_cloud)),
                "materialized": False,
            },
            "dtm_raster": {"path": args.dtm_raster, "materialized": False},
            "dsm_raster": {"path": args.dsm_raster, "materialized": False},
            "void_mask_raster": {"path": args.void_mask_raster, "materialized": False},
            "confidence_raster": {"path": args.confidence_raster, "materialized": False},
            "manifest": args.output_manifest,
        },
        metrics={
            "resolution": args.resolution,
            "power": args.power,
            "search_radius": args.search_radius,
        },
        notes=[
            "M05 define o contrato da filtragem semantica do solo a partir das predicoes do M04 ou de uma nuvem previamente classificada.",
            "A implementacao-alvo deve reaplicar a mascara 2D ao dominio da nuvem ou da grade antes de gerar DTM, DSM, mascara de vazios e confianca.",
        ],
    )
    write_json(args.output_manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
