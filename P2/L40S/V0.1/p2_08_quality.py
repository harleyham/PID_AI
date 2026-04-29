#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

from p2_lib.pipeline_common import load_json_if_exists, write_json


def main() -> int:
    parser = argparse.ArgumentParser(description="P2 M08 - Consolidacao de qualidade.")
    parser.add_argument("--diagnostic-report", required=True)
    parser.add_argument("--dtm-manifest", required=True)
    parser.add_argument("--refine-manifest", required=True)
    parser.add_argument("--contour-manifest", required=True)
    parser.add_argument("--quality-report", required=True)
    args = parser.parse_args()

    inputs = {
        "diagnostic_report": Path(args.diagnostic_report).exists(),
        "dtm_manifest": Path(args.dtm_manifest).exists(),
        "refine_manifest": Path(args.refine_manifest).exists(),
        "contour_manifest": Path(args.contour_manifest).exists(),
    }

    diagnostic = load_json_if_exists(args.diagnostic_report)
    dtm = load_json_if_exists(args.dtm_manifest)
    refine = load_json_if_exists(args.refine_manifest)
    contours = load_json_if_exists(args.contour_manifest)

    artifact_state = {
        "dtm_input_cloud_exists": bool(dtm.get("inputs", {}).get("input_point_cloud", {}).get("exists")),
        "refine_input_dtm_exists": bool(refine.get("inputs", {}).get("input_dtm", {}).get("exists")),
        "contour_input_raster_exists": bool(contours.get("inputs", {}).get("input_raster", {}).get("exists")),
        "contour_vector_exists": bool(contours.get("outputs", {}).get("vector_output", {}).get("exists")),
        "diagnostic_has_fallback": bool(diagnostic.get("summary", {}).get("fallback")),
    }

    quality_report = {
        "module": "M08_QUALITY",
        "inputs_present": inputs,
        "artifact_state": artifact_state,
        "diagnostic": diagnostic,
        "dtm": dtm,
        "refine": refine,
        "contours": contours,
        "summary": {
            "ready_for_live_run": all(inputs.values()) and all(
                not value for key, value in artifact_state.items() if key == "diagnostic_has_fallback"
            ) and artifact_state["dtm_input_cloud_exists"] and artifact_state["refine_input_dtm_exists"] and artifact_state["contour_input_raster_exists"],
            "missing_inputs": [name for name, exists in inputs.items() if not exists],
            "missing_artifacts": [name for name, exists in artifact_state.items() if not exists and name != "diagnostic_has_fallback"],
        },
    }

    write_json(args.quality_report, quality_report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
