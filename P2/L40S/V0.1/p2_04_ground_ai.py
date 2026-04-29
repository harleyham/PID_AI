#!/usr/bin/env python3

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

import yaml

from p2_lib.pipeline_common import file_info, load_json_if_exists, module_manifest, write_json


def to_container_path(path: str, mounts: list[tuple[Path, str]]) -> str:
    resolved_path = Path(path).resolve()

    for host_root, container_root in mounts:
        resolved_root = host_root.resolve()
        try:
            relative_path = resolved_path.relative_to(resolved_root)
        except ValueError:
            continue
        return str(Path(container_root) / relative_path)

    return str(resolved_path)


def build_mounts(args: argparse.Namespace) -> list[tuple[Path, str]]:
    # Check the most specific mount first so /results wins over /data/results.
    return [
        (Path(args.results_host_root), args.container_results_root),
        (Path(args.scripts_host_root), args.container_scripts_root),
        (Path(args.data_host_root), args.container_data_root),
    ]


def build_tao_command(args: argparse.Namespace, container_spec: str, container_results: str) -> list[str]:
    shell_parts = [args.model_name, args.tao_subcommand]
    effective_batch_size = getattr(args, "effective_batch_size", args.batch_size)

    if args.tao_subcommand == "default_specs":
        shell_parts.append(f"results_dir={shlex.quote(container_results)}")
    else:
        shell_parts.extend([
            "-e",
            shlex.quote(container_spec),
            f"results_dir={shlex.quote(container_results)}",
            f"dataset.segment.batch_size={effective_batch_size}",
        ])

    if args.tao_extra_args:
        shell_parts.extend(shlex.split(args.tao_extra_args))

    return [
        "docker",
        "compose",
        "-f",
        args.docker_compose_file,
        "run",
        "--rm",
        "tao-p2",
        "bash",
        "-lc",
        " ".join(shell_parts),
    ]


def parse_gpu_id(device: str) -> int:
    if device.startswith("cuda:"):
        return int(device.split(":", 1)[1])
    return 0


def build_training_spec(args: argparse.Namespace, mounts: list[tuple[Path, str]]) -> tuple[Path, dict]:
    with open(args.spec_file, "r", encoding="utf-8") as handle:
        spec = yaml.safe_load(handle)

    dataset_manifest = load_json_if_exists(args.dataset_manifest)
    classes_manifest = load_json_if_exists(args.classes_manifest)
    split_counts = dataset_manifest.get("split_counts", {})
    train_tiles = int(split_counts.get("train", 0))
    val_tiles = int(split_counts.get("val", 0))
    test_tiles = int(split_counts.get("test", 0))
    if train_tiles == 0 or val_tiles == 0:
        raise ValueError("Dataset do M03 precisa conter pelo menos um tile em train e um em val para treino do SegFormer.")

    generated_spec_path = Path(args.generated_spec_file)
    generated_spec_path.parent.mkdir(parents=True, exist_ok=True)

    container_dataset_root = to_container_path(args.dataset_root, mounts)
    container_results_dir = to_container_path(args.results_dir, mounts)
    palette_entries = [entry for entry in classes_manifest.get("classes", []) if entry.get("label_id") != 255]

    effective_batch_size = max(1, min(args.batch_size, train_tiles, val_tiles))
    args.effective_batch_size = effective_batch_size

    spec["model_name"] = "segformer_ds3_ground_train"
    spec["results_dir"] = container_results_dir
    spec["dataset"]["segment"]["root_dir"] = container_dataset_root
    spec["dataset"]["segment"]["dataset"] = "SFDataset"
    spec["dataset"]["segment"]["num_classes"] = len(palette_entries)
    spec["dataset"]["segment"]["batch_size"] = effective_batch_size
    spec["dataset"]["segment"]["workers"] = args.train_workers
    spec["dataset"]["segment"]["train_split"] = "train"
    spec["dataset"]["segment"]["validation_split"] = "val"
    spec["dataset"]["segment"]["test_split"] = "test"
    spec["dataset"]["segment"]["predict_split"] = "test"
    spec["dataset"]["segment"]["label_transform"] = ""
    spec["dataset"]["segment"]["palette"] = [
        {
            "label_id": entry["label_id"],
            "mapping_class": entry["name"],
            "rgb": entry["rgb"],
            "seg_class": entry["name"],
        }
        for entry in palette_entries
    ]
    spec["train"]["segment"]["weights"] = [1.0 for _ in palette_entries]
    spec["dataset"]["segment"]["quant_calibration_dataset"]["images_dir"] = f"{container_dataset_root}/images/train"
    spec["gen_trt_engine"]["tensorrt"]["calibration"]["cal_image_dir"] = [f"{container_dataset_root}/images/train"]

    spec["train"]["num_epochs"] = args.train_epochs
    spec["train"]["results_dir"] = container_results_dir
    spec["train"]["num_gpus"] = 1
    spec["train"]["gpu_ids"] = [parse_gpu_id(args.device)]
    spec["train"]["checkpoint_interval"] = 1
    spec["train"]["validation_interval"] = 1

    spec["evaluate"]["results_dir"] = container_results_dir
    spec["evaluate"]["batch_size"] = effective_batch_size
    spec["evaluate"]["gpu_ids"] = [parse_gpu_id(args.device)]
    spec["inference"]["results_dir"] = container_results_dir
    spec["inference"]["batch_size"] = effective_batch_size
    spec["inference"]["gpu_ids"] = [parse_gpu_id(args.device)]

    with open(generated_spec_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(spec, handle, sort_keys=False)

    dataset_manifest["effective_batch_size"] = effective_batch_size
    dataset_manifest["requested_batch_size"] = args.batch_size
    dataset_manifest["dataset_train_tiles"] = train_tiles
    dataset_manifest["dataset_val_tiles"] = val_tiles
    dataset_manifest["dataset_test_tiles"] = test_tiles
    return generated_spec_path, dataset_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="P2 M04 - Classificacao IA de terreno.")
    parser.add_argument("--input-point-cloud", required=True)
    parser.add_argument("--output-point-cloud", required=True)
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--batch-size", required=True, type=int)
    parser.add_argument("--device", required=True)
    parser.add_argument("--execution-mode", default="stub")
    parser.add_argument("--pipeline-kind", default="infer")
    parser.add_argument("--pipeline-version", default="unversioned")
    parser.add_argument("--model-version", default="unversioned")
    parser.add_argument("--model-registry-root", default="")
    parser.add_argument("--data-host-root", required=True)
    parser.add_argument("--scripts-host-root", required=True)
    parser.add_argument("--results-host-root", required=True)
    parser.add_argument("--container-data-root", required=True)
    parser.add_argument("--container-scripts-root", required=True)
    parser.add_argument("--container-results-root", required=True)
    parser.add_argument("--docker-compose-file", required=True)
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--generated-spec-file", required=True)
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--dataset-manifest", required=True)
    parser.add_argument("--classes-manifest", required=True)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--tao-subcommand", default="train")
    parser.add_argument("--tao-extra-args", default="")
    parser.add_argument("--train-epochs", required=True, type=int)
    parser.add_argument("--train-workers", required=True, type=int)
    parser.add_argument("--output-manifest", required=True)
    args = parser.parse_args()

    input_path = Path(args.input_point_cloud)
    data_host_root = Path(args.data_host_root)
    scripts_host_root = Path(args.scripts_host_root)
    results_host_root = Path(args.results_host_root)
    compose_path = Path(args.docker_compose_file)
    base_spec_path = Path(args.spec_file)
    results_dir = Path(args.results_dir)
    dataset_root = Path(args.dataset_root)
    dataset_manifest_path = Path(args.dataset_manifest)
    classes_manifest_path = Path(args.classes_manifest)
    output_point_cloud = Path(args.output_point_cloud)
    model_registry_root = Path(args.model_registry_root) if args.model_registry_root else None
    mounts = build_mounts(args)

    if not input_path.exists():
        print(f"ERRO: nuvem de entrada nao encontrada: {input_path}", file=sys.stderr)
        return 1
    if not data_host_root.exists():
        print(f"ERRO: data_host_root nao encontrado: {data_host_root}", file=sys.stderr)
        return 1
    if not scripts_host_root.exists():
        print(f"ERRO: scripts_host_root nao encontrado: {scripts_host_root}", file=sys.stderr)
        return 1
    if not results_host_root.exists():
        print(f"ERRO: results_host_root nao encontrado: {results_host_root}", file=sys.stderr)
        return 1
    if not compose_path.exists():
        print(f"ERRO: docker compose nao encontrado: {compose_path}", file=sys.stderr)
        return 1
    if not base_spec_path.exists():
        print(f"ERRO: spec base nao encontrado: {base_spec_path}", file=sys.stderr)
        return 1
    if not dataset_root.exists():
        print(f"ERRO: dataset root nao encontrado: {dataset_root}", file=sys.stderr)
        return 1
    if not dataset_manifest_path.exists() or not classes_manifest_path.exists():
        print("ERRO: metadata do dataset do M03 nao encontrada.", file=sys.stderr)
        return 1

    results_dir.mkdir(parents=True, exist_ok=True)

    effective_spec_path = base_spec_path
    dataset_manifest = load_json_if_exists(dataset_manifest_path)
    if args.tao_subcommand == "train":
        try:
            effective_spec_path, dataset_manifest = build_training_spec(args, mounts)
        except ValueError as exc:
            print(f"ERRO: {exc}", file=sys.stderr)
            return 1

    container_spec = to_container_path(str(effective_spec_path), mounts)
    container_results = to_container_path(str(results_dir), mounts)
    container_input = to_container_path(str(input_path), mounts)
    container_dataset_root = to_container_path(str(dataset_root), mounts)
    command = build_tao_command(args, container_spec, container_results)

    execution_notes = []
    if args.execution_mode != "stub":
        env = os.environ.copy()
        env["GPU"] = env.get("GPU", "")
        try:
            subprocess.run(command, check=True, env=env)
            execution_notes.append(f"Comando TAO executado com sucesso: {' '.join(command)}")
        except subprocess.CalledProcessError as exc:
            print(f"ERRO: falha ao executar TAO no container (exit_code={exc.returncode})", file=sys.stderr)
            return exc.returncode
    else:
        execution_notes.append("Modo stub ativo; execucao do TAO nao foi disparada.")

    manifest = module_manifest(
        module="M04_GROUND_AI",
        execution_mode=args.execution_mode,
        inputs={
            "input_point_cloud": file_info(input_path),
            "model_name": args.model_name,
            "model_version": args.model_version,
            "pipeline_kind": args.pipeline_kind,
            "pipeline_version": args.pipeline_version,
            "device": args.device,
            "tao_subcommand": args.tao_subcommand,
            "docker_compose_file": file_info(compose_path),
            "spec_file": file_info(base_spec_path),
            "generated_spec_file": file_info(effective_spec_path),
            "dataset_root": file_info(dataset_root),
            "dataset_manifest": file_info(dataset_manifest_path),
            "classes_manifest": file_info(classes_manifest_path),
            "container_input_point_cloud": container_input,
            "container_dataset_root": container_dataset_root,
            "container_spec_file": container_spec,
            "container_results_dir": container_results,
        },
        outputs={
            "classified_point_cloud": {
                "path": args.output_point_cloud,
                "materialized": output_point_cloud.exists(),
                "exists": output_point_cloud.exists(),
            },
            "tao_results_dir": {
                "path": str(results_dir),
                "exists": results_dir.exists(),
            },
            "model_registry_root": file_info(model_registry_root) if model_registry_root else {"path": "", "exists": False},
            "predictions_dir": {
                "path": str(results_dir / "predictions"),
                "exists": (results_dir / "predictions").exists(),
            },
            "checkpoints_dir": {
                "path": str(results_dir),
                "exists": results_dir.exists(),
            },
            "generated_default_spec": file_info(results_dir / "experiment.yaml"),
            "generated_train_spec": file_info(effective_spec_path),
            "manifest": args.output_manifest,
        },
        metrics={
            "batch_size": int(dataset_manifest.get("effective_batch_size", args.batch_size)),
            "requested_batch_size": args.batch_size,
            "train_epochs": args.train_epochs,
            "train_workers": args.train_workers,
            "model_version": args.model_version,
            "dataset_tile_count": int(dataset_manifest.get("tile_count", 0)),
            "dataset_train_tiles": int(dataset_manifest.get("split_counts", {}).get("train", 0)),
            "dataset_val_tiles": int(dataset_manifest.get("split_counts", {}).get("val", 0)),
            "dataset_test_tiles": int(dataset_manifest.get("split_counts", {}).get("test", 0)),
            "tao_command": " ".join(shlex.quote(part) for part in command),
        },
        notes=[
            "M04 executa treino, avaliacao, inferencia ou geracao de spec do SegFormer dentro do container TAO via Docker Compose.",
            "Para treino, o modulo gera um spec derivado a partir do dataset materializado pelo M03, apontando para images/masks por split.",
            "As pseudo-mascaras do M03 servem para bootstrap e devem ser revisadas manualmente antes do uso supervisionado principal.",
        ] + execution_notes,
    )
    write_json(args.output_manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
