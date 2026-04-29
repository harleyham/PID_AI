#!/usr/bin/env python3

import argparse
import sqlite3
import sys
from pathlib import Path

from p2_lib.pipeline_common import file_info, module_manifest, write_json


def count_features_gpkg(gpkg_path: Path, layer_name: str) -> int:
    conn = sqlite3.connect(str(gpkg_path))
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{layer_name}"')
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="P2 M07 - Estatisticas e manifesto de curvas.")
    parser.add_argument("--mode", choices=["stub", "live"], required=True)
    parser.add_argument("--input-raster", required=True)
    parser.add_argument("--input-vector", required=True)
    parser.add_argument("--layer-name", required=True)
    parser.add_argument("--field-name", required=True)
    parser.add_argument("--format", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()

    metrics = {
        "format": args.format,
        "field_name": args.field_name,
    }

    notes = []
    if args.mode == "live" and args.format.upper() == "GPKG" and Path(args.input_vector).exists():
        try:
            metrics["feature_count"] = count_features_gpkg(Path(args.input_vector), args.layer_name)
        except Exception as exc:
            notes.append(f"Falha ao contar feicoes: {exc}")
    else:
        notes.append("Manifesto gerado sem contagem de feicoes materializadas.")

    manifest = module_manifest(
        module="M07_CONTOURS",
        execution_mode=args.mode,
        inputs={
            "input_raster": file_info(args.input_raster),
        },
        outputs={
            "vector_output": file_info(args.input_vector),
            "layer_name": args.layer_name,
            "manifest": args.manifest,
        },
        metrics=metrics,
        notes=notes,
    )
    write_json(args.manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
