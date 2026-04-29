from __future__ import annotations

import json
import os
import subprocess
from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any


def append_container_lib() -> None:
    container_lib = "/script/lib_container"
    if container_lib not in os.sys.path:
        os.sys.path.append(container_lib)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent_dir(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def file_info(path: str | Path) -> dict[str, Any]:
    path_obj = Path(path)
    if not path_obj.exists():
        return {
            "path": str(path_obj),
            "exists": False,
        }

    stat = path_obj.stat()
    return {
        "path": str(path_obj),
        "exists": True,
        "size_bytes": stat.st_size,
        "suffix": path_obj.suffix.lower(),
    }


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    digest = sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, sort_keys=True)
        handle.write("\n")


def module_manifest(
    *,
    module: str,
    execution_mode: str,
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    metrics: dict[str, Any] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "module": module,
        "execution_mode": execution_mode,
        "generated_at": utc_now(),
        "inputs": inputs,
        "outputs": outputs,
        "metrics": metrics or {},
        "notes": notes or [],
    }


def load_json_if_exists(path: str | Path) -> dict[str, Any]:
    path_obj = Path(path)
    if not path_obj.exists():
        return {}
    with open(path_obj, "r", encoding="utf-8") as handle:
        return json.load(handle)


def summarize_las(path: str | Path) -> dict[str, Any]:
    append_container_lib()

    import laspy
    import numpy as np

    las = laspy.read(path)
    header = las.header
    point_count = int(len(las.points))
    summary: dict[str, Any] = {
        "point_count": point_count,
        "has_rgb": all(hasattr(las, channel) for channel in ("red", "green", "blue")),
        "has_intensity": hasattr(las, "intensity"),
        "header": {
            "version": f"{header.version.major}.{header.version.minor}",
            "point_format": int(header.point_format.id),
            "scales": [float(value) for value in header.scales],
            "offsets": [float(value) for value in header.offsets],
        },
    }

    if point_count == 0:
        return summary

    bbox = {
        "min_x": float(np.min(las.x)),
        "max_x": float(np.max(las.x)),
        "min_y": float(np.min(las.y)),
        "max_y": float(np.max(las.y)),
        "min_z": float(np.min(las.z)),
        "max_z": float(np.max(las.z)),
    }
    area_xy = max((bbox["max_x"] - bbox["min_x"]) * (bbox["max_y"] - bbox["min_y"]), 0.0)

    summary.update(
        {
            "bbox": bbox,
            "z_stats": {
                "mean": float(np.mean(las.z)),
                "std": float(np.std(las.z)),
                "min": bbox["min_z"],
                "max": bbox["max_z"],
                "p05": float(np.percentile(las.z, 5)),
                "p50": float(np.percentile(las.z, 50)),
                "p95": float(np.percentile(las.z, 95)),
            },
            "xy_area_m2": area_xy,
            "point_density_per_m2": float(point_count / area_xy) if area_xy > 0 else None,
        }
    )

    if hasattr(las, "classification"):
        class_counter = Counter(int(value) for value in np.asarray(las.classification).tolist())
        summary["classes_asprs"] = sorted(class_counter.keys())
        summary["class_histogram"] = {str(key): int(value) for key, value in sorted(class_counter.items())}

    if hasattr(las, "return_number") and hasattr(las, "number_of_returns"):
        summary["returns"] = {
            "return_number_min": int(np.min(las.return_number)),
            "return_number_max": int(np.max(las.return_number)),
            "number_of_returns_max": int(np.max(las.number_of_returns)),
        }

    return summary


def summarize_las_with_pdal(path: str | Path) -> dict[str, Any]:
    result = subprocess.run(
        ["pdal", "info", "--metadata", "--stats", str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    metadata = payload.get("metadata", {})
    stats = payload.get("stats", {})
    native_bbox = stats.get("bbox", {}).get("native", {}).get("bbox", {})

    statistics_by_name = {
        entry.get("name"): entry for entry in stats.get("statistic", []) if entry.get("name")
    }
    z_stats = statistics_by_name.get("Z", {})
    classification_stats = statistics_by_name.get("Classification", {})

    area_xy = max(
        (float(native_bbox.get("maxx", 0.0)) - float(native_bbox.get("minx", 0.0)))
        * (float(native_bbox.get("maxy", 0.0)) - float(native_bbox.get("miny", 0.0))),
        0.0,
    )
    point_count = int(metadata.get("count", 0))

    summary: dict[str, Any] = {
        "point_count": point_count,
        "has_rgb": all(channel in statistics_by_name for channel in ("Red", "Green", "Blue")),
        "has_intensity": "Intensity" in statistics_by_name,
        "header": {
            "version": f"{metadata.get('major_version', 0)}.{metadata.get('minor_version', 0)}",
            "point_format": int(metadata.get("dataformat_id", 0)),
            "scales": [
                float(metadata.get("scale_x", 0.0)),
                float(metadata.get("scale_y", 0.0)),
                float(metadata.get("scale_z", 0.0)),
            ],
            "offsets": [
                float(metadata.get("offset_x", 0.0)),
                float(metadata.get("offset_y", 0.0)),
                float(metadata.get("offset_z", 0.0)),
            ],
            "compressed": bool(metadata.get("compressed", False)),
        },
        "bbox": {
            "min_x": float(native_bbox.get("minx", 0.0)),
            "max_x": float(native_bbox.get("maxx", 0.0)),
            "min_y": float(native_bbox.get("miny", 0.0)),
            "max_y": float(native_bbox.get("maxy", 0.0)),
            "min_z": float(native_bbox.get("minz", 0.0)),
            "max_z": float(native_bbox.get("maxz", 0.0)),
        },
        "z_stats": {
            "mean": float(z_stats.get("average", 0.0)),
            "std": float(z_stats.get("stddev", 0.0)),
            "min": float(z_stats.get("minimum", native_bbox.get("minz", 0.0))),
            "max": float(z_stats.get("maximum", native_bbox.get("maxz", 0.0))),
        },
        "xy_area_m2": area_xy,
        "point_density_per_m2": float(point_count / area_xy) if area_xy > 0 else None,
        "spatial_reference": metadata.get("srs", {}).get("proj4") or metadata.get("spatialreference"),
    }

    if classification_stats:
        classification_value = int(classification_stats.get("average", 0))
        summary["classes_asprs"] = [classification_value]
        summary["class_histogram"] = {str(classification_value): point_count}

    if "ReturnNumber" in statistics_by_name and "NumberOfReturns" in statistics_by_name:
        summary["returns"] = {
            "return_number_min": int(statistics_by_name["ReturnNumber"].get("minimum", 0)),
            "return_number_max": int(statistics_by_name["ReturnNumber"].get("maximum", 0)),
            "number_of_returns_max": int(statistics_by_name["NumberOfReturns"].get("maximum", 0)),
        }

    summary["diagnostic_backend"] = "pdal"
    return summary
