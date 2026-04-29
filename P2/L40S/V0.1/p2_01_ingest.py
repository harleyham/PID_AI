#!/usr/bin/env python3

import argparse
import shutil
import sys
from pathlib import Path

from p2_lib.pipeline_common import ensure_parent_dir, file_info, module_manifest, sha256_file, write_json


SUPPORTED_MODES = {"passthrough", "copy"}


def materialize_normalized_cloud(input_path: Path, output_path: Path, normalize_mode: str) -> dict[str, object]:
    if normalize_mode not in SUPPORTED_MODES:
        raise ValueError(f"normalize_mode invalido: {normalize_mode}")

    ensure_parent_dir(output_path)

    if input_path.resolve() == output_path.resolve():
        return {
            "materialized": True,
            "strategy": "in_place_reference",
            "bytes_written": 0,
        }

    shutil.copy2(input_path, output_path)
    return {
        "materialized": True,
        "strategy": "byte_copy",
        "bytes_written": output_path.stat().st_size,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="P2 M01 - Ingestao e normalizacao.")
    parser.add_argument("--input-point-cloud", required=True)
    parser.add_argument("--normalize-mode", default="passthrough")
    parser.add_argument("--normalized-point-cloud", required=True)
    parser.add_argument("--output-manifest", required=True)
    args = parser.parse_args()

    input_path = Path(args.input_point_cloud)
    output_path = Path(args.normalized_point_cloud)
    if not input_path.exists():
        print(f"ERRO: nuvem de entrada nao encontrada: {input_path}", file=sys.stderr)
        return 1

    try:
        materialization = materialize_normalized_cloud(input_path, output_path, args.normalize_mode)
    except ValueError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 1

    input_hash = sha256_file(input_path)
    output_hash = sha256_file(output_path)

    manifest = module_manifest(
        module="M01_INGEST",
        execution_mode="live",
        inputs={
            "input_point_cloud": file_info(input_path),
            "normalize_mode": args.normalize_mode,
        },
        outputs={
            "normalized_point_cloud": {
                **file_info(output_path),
                "materialized": bool(materialization["materialized"]),
                "source_strategy": materialization["strategy"],
            },
            "manifest": args.output_manifest,
        },
        metrics={
            "input_suffix": input_path.suffix.lower(),
            "input_sha256": input_hash,
            "normalized_sha256": output_hash,
            "bytes_written": materialization["bytes_written"],
        },
        notes=[
            "M01 materializa a nuvem normalizada no workspace do pipeline preservando o conteudo do arquivo de entrada.",
            "A normalizacao geometrica ainda nao foi implementada; os modos suportados hoje fazem referencia in-place ou copia fisica do arquivo.",
        ],
    )
    write_json(args.output_manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
