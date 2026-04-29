#!/usr/bin/env python3

import argparse
import csv
import json
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from PIL import Image

from p2_lib.pipeline_common import ensure_parent_dir, file_info, module_manifest, summarize_las_with_pdal, write_json


SPLITS = ("train", "val", "test")
CLASS_COLORS = {
    0: (139, 69, 19),
    1: (34, 139, 34),
    2: (128, 128, 128),
    3: (64, 64, 64),
    4: (0, 119, 190),
    5: (0, 0, 0),
    255: (255, 255, 255),
}
SEMANTIC_LABELS = {
    0: "ground",
    1: "vegetation",
    2: "building",
    3: "paved_surface",
    4: "water",
    5: "background",
}


def ensure_dataset_dirs(dataset_root: Path) -> dict[str, Path]:
    directories = {
        "dataset_root": dataset_root,
        "images_train_dir": dataset_root / "images" / "train",
        "images_val_dir": dataset_root / "images" / "val",
        "images_test_dir": dataset_root / "images" / "test",
        "masks_train_dir": dataset_root / "masks" / "train",
        "masks_val_dir": dataset_root / "masks" / "val",
        "masks_test_dir": dataset_root / "masks" / "test",
        "metadata_dir": dataset_root / "metadata",
    }

    for path in directories.values():
        path.mkdir(parents=True, exist_ok=True)

    return directories


def export_points_to_csv(input_path: Path, output_csv: Path) -> None:
    subprocess.run(
        [
            "pdal",
            "translate",
            str(input_path),
            str(output_csv),
            "-w",
            "writers.text",
            "--dims",
            "X,Y,Z,Red,Green,Blue,Classification",
            "--overwrite",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def load_points(csv_path: Path) -> np.ndarray:
    with open(csv_path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = [
            (
                float(row["X"]),
                float(row["Y"]),
                float(row["Z"]),
                float(row.get("Red", 0.0)),
                float(row.get("Green", 0.0)),
                float(row.get("Blue", 0.0)),
                float(row.get("Classification", -1.0)),
            )
            for row in reader
        ]

    return np.asarray(rows, dtype=np.float64)


def tile_starts(min_value: float, max_value: float, tile_size: float, stride: float) -> list[float]:
    starts = []
    current = min_value
    while current < max_value:
        starts.append(current)
        if current + tile_size >= max_value:
            break
        current += stride
    return starts


def split_for_index(index: int) -> str:
    remainder = index % 10
    if remainder < 7:
        return "train"
    if remainder == 7:
        return "val"
    return "test"


def parse_class_list(raw_value: str) -> set[int]:
    if not raw_value.strip():
        return set()
    return {int(value.strip()) for value in raw_value.split(",") if value.strip()}


def build_class_mapping(args: argparse.Namespace) -> dict[int, set[int]]:
    return {
        0: parse_class_list(args.asprs_ground_classes),
        1: parse_class_list(args.asprs_vegetation_classes),
        2: parse_class_list(args.asprs_building_classes),
        3: parse_class_list(args.asprs_paved_surface_classes),
        4: parse_class_list(args.asprs_water_classes),
    }


def write_geojson(tile_features: list[dict], output_path: Path) -> None:
    ensure_parent_dir(output_path)
    payload = {
        "type": "FeatureCollection",
        "features": tile_features,
    }
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def rasterize_tile(points: np.ndarray, min_x: float, min_y: float, max_y: float, pixel_size: float, width: int, height: int) -> np.ndarray:
    image_sum = np.zeros((height, width, 3), dtype=np.float64)
    image_count = np.zeros((height, width), dtype=np.float64)

    pixel_x = np.floor((points[:, 0] - min_x) / pixel_size).astype(np.int32)
    pixel_y = np.floor((max_y - points[:, 1]) / pixel_size).astype(np.int32)
    pixel_x = np.clip(pixel_x, 0, width - 1)
    pixel_y = np.clip(pixel_y, 0, height - 1)

    rgb = np.clip(points[:, 3:6], 0, 255)
    np.add.at(image_sum, (pixel_y, pixel_x, 0), rgb[:, 0])
    np.add.at(image_sum, (pixel_y, pixel_x, 1), rgb[:, 1])
    np.add.at(image_sum, (pixel_y, pixel_x, 2), rgb[:, 2])
    np.add.at(image_count, (pixel_y, pixel_x), 1.0)

    valid_mask = image_count > 0
    image = np.zeros((height, width, 3), dtype=np.uint8)
    image[valid_mask] = np.clip(
        image_sum[valid_mask] / image_count[valid_mask, None],
        0,
        255,
    ).astype(np.uint8)
    return image


def rasterize_tile_stats(
    points: np.ndarray,
    min_x: float,
    max_y: float,
    pixel_size: float,
    width: int,
    height: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    pixel_x = np.floor((points[:, 0] - min_x) / pixel_size).astype(np.int32)
    pixel_y = np.floor((max_y - points[:, 1]) / pixel_size).astype(np.int32)
    pixel_x = np.clip(pixel_x, 0, width - 1)
    pixel_y = np.clip(pixel_y, 0, height - 1)

    image_sum = np.zeros((height, width, 3), dtype=np.float64)
    z_sum = np.zeros((height, width), dtype=np.float64)
    image_count = np.zeros((height, width), dtype=np.float64)

    rgb = np.clip(points[:, 3:6], 0, 255)
    np.add.at(image_sum, (pixel_y, pixel_x, 0), rgb[:, 0])
    np.add.at(image_sum, (pixel_y, pixel_x, 1), rgb[:, 1])
    np.add.at(image_sum, (pixel_y, pixel_x, 2), rgb[:, 2])
    np.add.at(z_sum, (pixel_y, pixel_x), points[:, 2])
    np.add.at(image_count, (pixel_y, pixel_x), 1.0)

    valid_mask = image_count > 0
    image = np.zeros((height, width, 3), dtype=np.uint8)
    z_mean = np.full((height, width), np.nan, dtype=np.float64)

    image[valid_mask] = np.clip(
        image_sum[valid_mask] / image_count[valid_mask, None],
        0,
        255,
    ).astype(np.uint8)
    z_mean[valid_mask] = z_sum[valid_mask] / image_count[valid_mask]
    return image, z_mean, valid_mask


def build_pseudo_mask(image: np.ndarray, z_mean: np.ndarray, valid_mask: np.ndarray) -> np.ndarray:
    mask = np.full(valid_mask.shape, 255, dtype=np.uint8)
    if not np.any(valid_mask):
        return mask

    valid_z = z_mean[valid_mask]
    local_ground = float(np.nanpercentile(valid_z, 5))
    height_above_ground = np.zeros_like(z_mean, dtype=np.float64)
    height_above_ground[valid_mask] = z_mean[valid_mask] - local_ground

    red = image[:, :, 0].astype(np.float64)
    green = image[:, :, 1].astype(np.float64)
    blue = image[:, :, 2].astype(np.float64)
    green_dominance = green - np.maximum(red, blue)

    mask[valid_mask] = 5

    ground_mask = valid_mask & (height_above_ground <= 0.75)
    vegetation_mask = valid_mask & (height_above_ground > 0.75) & (green_dominance >= 8.0)
    building_mask = valid_mask & (height_above_ground > 1.5) & ~vegetation_mask

    mask[ground_mask] = 0
    mask[vegetation_mask] = 1
    mask[building_mask] = 2
    return mask


def build_classification_mask(
    points: np.ndarray,
    min_x: float,
    max_y: float,
    pixel_size: float,
    width: int,
    height: int,
    class_mapping: dict[int, set[int]],
) -> tuple[np.ndarray, int, int]:
    mask = np.full((height, width), 255, dtype=np.uint8)
    if points.shape[0] == 0 or points.shape[1] < 7:
        return mask, 0, int(points.shape[0])

    pixel_x = np.floor((points[:, 0] - min_x) / pixel_size).astype(np.int32)
    pixel_y = np.floor((max_y - points[:, 1]) / pixel_size).astype(np.int32)
    pixel_x = np.clip(pixel_x, 0, width - 1)
    pixel_y = np.clip(pixel_y, 0, height - 1)

    asprs_classes = points[:, 6].astype(np.int32)
    semantic_labels = np.full(asprs_classes.shape, -1, dtype=np.int32)
    for label_id, source_classes in class_mapping.items():
        if source_classes:
            semantic_labels[np.isin(asprs_classes, list(source_classes))] = label_id

    total_points_per_pixel = np.zeros((height, width), dtype=np.uint16)
    np.add.at(total_points_per_pixel, (pixel_y, pixel_x), 1)

    valid_label_points = semantic_labels >= 0
    labeled_point_count = int(np.count_nonzero(valid_label_points))
    if labeled_point_count == 0:
        mask[total_points_per_pixel > 0] = 5
        return mask, 0, int(points.shape[0])

    label_counts = np.zeros((height, width, len(SEMANTIC_LABELS)), dtype=np.uint16)
    for label_id in SEMANTIC_LABELS:
        label_point_mask = semantic_labels == label_id
        if np.any(label_point_mask):
            np.add.at(label_counts, (pixel_y[label_point_mask], pixel_x[label_point_mask], label_id), 1)

    total_labeled_per_pixel = np.sum(label_counts, axis=2)
    labeled_pixels = total_labeled_per_pixel > 0
    mask[labeled_pixels] = np.argmax(label_counts[labeled_pixels], axis=1).astype(np.uint8)

    unlabeled_but_occupied = (total_points_per_pixel > 0) & ~labeled_pixels
    mask[unlabeled_but_occupied] = 5
    return mask, labeled_point_count, int(points.shape[0])


def colorize_mask(mask: np.ndarray) -> np.ndarray:
    rgb_mask = np.zeros((mask.shape[0], mask.shape[1], 3), dtype=np.uint8)
    for label_id, color in CLASS_COLORS.items():
        rgb_mask[mask == label_id] = color
    return rgb_mask


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "P2 M03 - Preparacao do dataset 2D para segmentacao semantica. "
            "Rasteriza a nuvem em tiles RGB e descarta recortes residuais abaixo do percentual minimo configurado."
        )
    )
    parser.add_argument("--input-point-cloud", required=True)
    parser.add_argument("--tile-size-meters", required=True, type=float)
    parser.add_argument("--tile-overlap-meters", required=True, type=float)
    parser.add_argument("--voxel-size-meters", required=True, type=float)
    parser.add_argument("--min-tile-area-percent", required=True, type=float)
    parser.add_argument("--label-source", choices=["auto", "classification", "pseudo"], default="auto")
    parser.add_argument("--asprs-ground-classes", default="2")
    parser.add_argument("--asprs-vegetation-classes", default="3,4,5")
    parser.add_argument("--asprs-building-classes", default="6")
    parser.add_argument("--asprs-paved-surface-classes", default="11")
    parser.add_argument("--asprs-water-classes", default="9")
    parser.add_argument("--min-labeled-point-percent", default=5.0, type=float)
    parser.add_argument("--tiles-dir", required=True)
    parser.add_argument("--output-manifest", required=True)
    args = parser.parse_args()

    input_path = Path(args.input_point_cloud)
    if not input_path.exists():
        print(f"ERRO: nuvem de entrada nao encontrada: {input_path}", file=sys.stderr)
        return 1

    if args.tile_size_meters <= 0 or args.voxel_size_meters <= 0:
        print("ERRO: tile_size_meters e voxel_size_meters devem ser positivos", file=sys.stderr)
        return 1

    if args.min_tile_area_percent <= 0 or args.min_tile_area_percent > 100:
        print("ERRO: min_tile_area_percent deve estar no intervalo (0, 100]", file=sys.stderr)
        return 1

    if args.min_labeled_point_percent < 0 or args.min_labeled_point_percent > 100:
        print("ERRO: min_labeled_point_percent deve estar no intervalo [0, 100]", file=sys.stderr)
        return 1

    stride = args.tile_size_meters - args.tile_overlap_meters
    if stride <= 0:
        print("ERRO: tile_overlap_meters deve ser menor que tile_size_meters", file=sys.stderr)
        return 1

    class_mapping = build_class_mapping(args)
    min_labeled_point_ratio = args.min_labeled_point_percent / 100.0

    dataset_root = Path(args.tiles_dir) / "segformer_ds3"
    shutil.rmtree(dataset_root, ignore_errors=True)
    dataset_dirs = ensure_dataset_dirs(dataset_root)
    summary = summarize_las_with_pdal(input_path)
    bbox = summary["bbox"]
    width_pixels = int(math.ceil(args.tile_size_meters / args.voxel_size_meters))
    height_pixels = width_pixels

    temp_csv = Path(tempfile.gettempdir()) / f"p2_preprocess_{input_path.stem}.csv"
    export_points_to_csv(input_path, temp_csv)
    points = load_points(temp_csv)

    x_starts = tile_starts(bbox["min_x"], bbox["max_x"], args.tile_size_meters, stride)
    y_starts = tile_starts(bbox["min_y"], bbox["max_y"], args.tile_size_meters, stride)

    tile_records = []
    tile_features = []
    split_counts = {split: 0 for split in SPLITS}
    tile_index = 0
    dropped_small_tiles = 0
    mask_source_counts = {"classification": 0, "pseudo": 0}
    total_labeled_points = 0
    total_tile_points = 0
    min_tile_area_ratio = args.min_tile_area_percent / 100.0
    full_tile_area = args.tile_size_meters * args.tile_size_meters

    for y_start in y_starts:
        for x_start in x_starts:
            x_end = min(x_start + args.tile_size_meters, bbox["max_x"])
            y_end = min(y_start + args.tile_size_meters, bbox["max_y"])
            tile_area = max((x_end - x_start) * (y_end - y_start), 0.0)
            tile_area_ratio = tile_area / full_tile_area if full_tile_area > 0 else 0.0

            if tile_area_ratio < min_tile_area_ratio:
                dropped_small_tiles += 1
                continue

            mask = (
                (points[:, 0] >= x_start)
                & (points[:, 0] < x_end)
                & (points[:, 1] >= y_start)
                & (points[:, 1] < y_end)
            )
            tile_points = points[mask]
            if tile_points.shape[0] == 0:
                continue

            split = split_for_index(tile_index)
            split_counts[split] += 1
            tile_name = f"tile_{tile_index:04d}"
            image_path = dataset_dirs[f"images_{split}_dir"] / f"{tile_name}.png"

            image, z_mean, valid_mask = rasterize_tile_stats(
                tile_points,
                x_start,
                y_end,
                args.voxel_size_meters,
                width_pixels,
                height_pixels,
            )
            classification_mask, labeled_point_count, tile_point_count = build_classification_mask(
                tile_points,
                x_start,
                y_end,
                args.voxel_size_meters,
                width_pixels,
                height_pixels,
                class_mapping,
            )
            labeled_point_ratio = labeled_point_count / tile_point_count if tile_point_count > 0 else 0.0
            use_classification = args.label_source == "classification" or (
                args.label_source == "auto" and labeled_point_ratio >= min_labeled_point_ratio
            )
            if use_classification:
                semantic_mask = classification_mask
                mask_source = "classification"
            else:
                semantic_mask = build_pseudo_mask(image, z_mean, valid_mask)
                mask_source = "pseudo"

            mask_source_counts[mask_source] += 1
            total_labeled_points += labeled_point_count
            total_tile_points += tile_point_count
            semantic_mask_rgb = colorize_mask(semantic_mask)
            Image.fromarray(image, mode="RGB").save(image_path)
            mask_path = dataset_dirs[f"masks_{split}_dir"] / f"{tile_name}.png"
            Image.fromarray(semantic_mask_rgb, mode="RGB").save(mask_path)

            relative_image_path = image_path.relative_to(dataset_root)
            relative_mask_path = mask_path.relative_to(dataset_root)
            tile_record = {
                "tile_id": tile_name,
                "split": split,
                "image_path": str(relative_image_path),
                "mask_path": str(relative_mask_path),
                "point_count": int(tile_points.shape[0]),
                "bbox": {
                    "min_x": float(x_start),
                    "max_x": float(x_end),
                    "min_y": float(y_start),
                    "max_y": float(y_end),
                },
                "area_ratio": tile_area_ratio,
                "pixel_size_meters": args.voxel_size_meters,
                "width_pixels": width_pixels,
                "height_pixels": height_pixels,
                "mask_source": mask_source,
                "labeled_point_count": labeled_point_count,
                "labeled_point_percent": labeled_point_ratio * 100.0,
            }
            tile_records.append(tile_record)
            tile_features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "tile_id": tile_name,
                        "split": split,
                        "image_path": str(relative_image_path),
                        "mask_path": str(relative_mask_path),
                        "point_count": int(tile_points.shape[0]),
                        "area_ratio": tile_area_ratio,
                        "mask_source": mask_source,
                        "labeled_point_percent": labeled_point_ratio * 100.0,
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [x_start, y_start],
                            [x_end, y_start],
                            [x_end, y_end],
                            [x_start, y_end],
                            [x_start, y_start],
                        ]],
                    },
                }
            )
            tile_index += 1

    if args.label_source == "classification" and total_labeled_points == 0:
        print(
            "ERRO: label_source=classification solicitado, mas nenhuma classe ASPRS mapeada foi encontrada nos tiles.",
            file=sys.stderr,
        )
        return 1

    metadata_dir = dataset_dirs["metadata_dir"]
    tile_index_path = metadata_dir / "tile_index.geojson"
    dataset_manifest_path = metadata_dir / "dataset_manifest.json"
    classes_path = metadata_dir / "classes.json"

    write_geojson(tile_features, tile_index_path)
    write_json(
        dataset_manifest_path,
        {
            "dataset_root": str(dataset_root),
            "tile_count": len(tile_records),
            "split_counts": split_counts,
            "pixel_size_meters": args.voxel_size_meters,
            "tile_size_meters": args.tile_size_meters,
            "tile_overlap_meters": args.tile_overlap_meters,
            "min_tile_area_percent": args.min_tile_area_percent,
            "dropped_small_tiles": dropped_small_tiles,
            "label_source_requested": args.label_source,
            "mask_source_counts": mask_source_counts,
            "asprs_class_mapping": {
                SEMANTIC_LABELS[label_id]: sorted(source_classes)
                for label_id, source_classes in class_mapping.items()
            },
            "min_labeled_point_percent": args.min_labeled_point_percent,
            "total_labeled_points": total_labeled_points,
            "total_tile_points": total_tile_points,
            "total_labeled_point_percent": (total_labeled_points / total_tile_points * 100.0) if total_tile_points > 0 else 0.0,
            "tiles": tile_records,
        },
    )
    write_json(
        classes_path,
        {
            "classes": [
                {"label_id": 0, "name": "ground", "rgb": [139, 69, 19]},
                {"label_id": 1, "name": "vegetation", "rgb": [34, 139, 34]},
                {"label_id": 2, "name": "building", "rgb": [128, 128, 128]},
                {"label_id": 3, "name": "paved_surface", "rgb": [64, 64, 64]},
                {"label_id": 4, "name": "water", "rgb": [0, 119, 190]},
                {"label_id": 5, "name": "background", "rgb": [0, 0, 0]},
                {"label_id": 255, "name": "nodata", "rgb": [255, 255, 255]},
            ],
            "notes": [
                "Quando a fonte classification e usada, as mascaras sao rasterizadas a partir das classes ASPRS mapeadas da nuvem LAS/LAZ.",
                "Quando a fonte pseudo e usada, as mascaras sao geradas por heuristica baseada em altura local e dominancia de verde.",
                "Pseudo-mascaras devem ser revisadas manualmente antes de uso como ground truth definitivo para treino.",
            ],
        },
    )

    manifest = module_manifest(
        module="M03_PREPROCESS_DATASET",
        execution_mode="live",
        inputs={
            "input_point_cloud": file_info(input_path),
            "dataset_role": "segformer_semantic_dataset",
            "label_source_requested": args.label_source,
            "asprs_class_mapping": {
                SEMANTIC_LABELS[label_id]: sorted(source_classes)
                for label_id, source_classes in class_mapping.items()
            },
        },
        outputs={
            "dataset_root": str(dataset_root),
            "images_train_dir": str(dataset_dirs["images_train_dir"]),
            "images_val_dir": str(dataset_dirs["images_val_dir"]),
            "images_test_dir": str(dataset_dirs["images_test_dir"]),
            "masks_train_dir": str(dataset_dirs["masks_train_dir"]),
            "masks_val_dir": str(dataset_dirs["masks_val_dir"]),
            "masks_test_dir": str(dataset_dirs["masks_test_dir"]),
            "metadata_dir": str(metadata_dir),
            "tile_index": file_info(tile_index_path),
            "dataset_manifest": file_info(dataset_manifest_path),
            "classes_manifest": file_info(classes_path),
            "materialized_dataset": len(tile_records) > 0,
            "manifest": args.output_manifest,
        },
        metrics={
            "tile_size_meters": args.tile_size_meters,
            "tile_overlap_meters": args.tile_overlap_meters,
            "voxel_size_meters": args.voxel_size_meters,
            "min_tile_area_percent": args.min_tile_area_percent,
            "tile_count": len(tile_records),
            "dropped_small_tiles": dropped_small_tiles,
            "train_tiles": split_counts["train"],
            "val_tiles": split_counts["val"],
            "test_tiles": split_counts["test"],
            "tile_width_pixels": width_pixels,
            "tile_height_pixels": height_pixels,
            "classification_mask_tiles": mask_source_counts["classification"],
            "pseudo_mask_tiles": mask_source_counts["pseudo"],
            "total_labeled_points": total_labeled_points,
            "total_tile_points": total_tile_points,
            "total_labeled_point_percent": (total_labeled_points / total_tile_points * 100.0) if total_tile_points > 0 else 0.0,
        },
        notes=[
            "M03 rasteriza a nuvem em tiles RGB e grava metadata espacial para consumo do SegFormer.",
            "Tiles cuja area util fica abaixo do percentual minimo configurado sao descartados para evitar recortes residuais pouco representativos.",
            "M03 pode gerar mascaras supervisionadas a partir da dimensao Classification da nuvem ou pseudo-mascaras heuristicas para bootstrap.",
            "Para treino correto, prefira label_source=classification com uma nuvem previamente rotulada e revise a distribuicao de classes no dataset_manifest.json.",
        ],
    )
    write_json(args.output_manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
