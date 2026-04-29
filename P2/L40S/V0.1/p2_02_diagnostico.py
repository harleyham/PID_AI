#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

from p2_lib.pipeline_common import file_info, sha256_file, summarize_las, summarize_las_with_pdal, write_json


def main() -> int:
    parser = argparse.ArgumentParser(description="P2 M02 - Diagnostico de nuvem de pontos.")
    parser.add_argument("--input-point-cloud", required=True)
    parser.add_argument("--output-report", required=True)
    args = parser.parse_args()

    input_path = Path(args.input_point_cloud)
    if not input_path.exists():
        print(f"ERRO: arquivo nao encontrado em {input_path}", file=sys.stderr)
        return 1

    report = {
        "module": "M02_DIAGNOSTICO",
        "input_point_cloud": file_info(input_path),
        "summary": {},
    }

    suffix = input_path.suffix.lower()
    report["summary"]["input_suffix"] = suffix
    report["summary"]["input_sha256"] = sha256_file(input_path)

    if suffix in {".las", ".laz"}:
        try:
            report["summary"].update(summarize_las(input_path))
            report["summary"]["diagnostic_backend"] = "laspy"
            report["summary"]["fallback"] = False
        except Exception as exc:
            try:
                report["summary"].update(summarize_las_with_pdal(input_path))
                report["summary"]["fallback"] = False
                report["summary"]["laspy_error"] = str(exc)
            except Exception as pdal_exc:
                report["summary"].update({
                    "fallback": True,
                    "error": str(pdal_exc),
                    "laspy_error": str(exc),
                    "message": "Falha ao diagnosticar a nuvem com laspy e PDAL; diagnostico resumido registrado para manter o modulo executavel.",
                })
    else:
        report["summary"].update({
            "fallback": True,
            "message": "Diagnostico detalhado implementado para LAS/LAZ; arquivo registrado apenas de forma generica.",
        })

    write_json(args.output_report, report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
