#!/usr/bin/env python3
import argparse
import csv
import math
import re
import sqlite3
import sys
from pathlib import Path

from p1_logging import log_info, log_warn, log_error, metric

PAIR_ID_MOD = 2147483647


def pair_id_to_image_ids(pair_id: int) -> tuple[int, int]:
    image_id2 = pair_id % PAIR_ID_MOD
    image_id1 = (pair_id - image_id2) // PAIR_ID_MOD
    return int(image_id1), int(image_id2)


def minmax_norm(value: float, vmin: float, vmax: float) -> float:
    if vmax <= vmin:
        return 1.0
    return (value - vmin) / (vmax - vmin)


def safe_log1p(v: float) -> float:
    return math.log1p(max(0.0, v))


def extract_numeric_index(name: str, fallback_id: int) -> int:
    stem = Path(name).stem
    matches = re.findall(r"(\d+)", stem)
    if matches:
        try:
            return int(matches[-1])
        except ValueError:
            pass
    return int(fallback_id)


def load_image_names(cur) -> dict[int, str]:
    cur.execute("SELECT image_id, name FROM images")
    return {int(i): name for i, name in cur.fetchall()}


def load_pairs(cur) -> list[tuple[int, int, int]]:
    cur.execute("SELECT pair_id, rows FROM two_view_geometries WHERE rows > 0")
    raw_pairs = cur.fetchall()

    decoded_pairs = []
    for pair_id, rows in raw_pairs:
        id1, id2 = pair_id_to_image_ids(int(pair_id))
        decoded_pairs.append((id1, id2, int(rows)))
    return decoded_pairs


def compute_degree(decoded_pairs: list[tuple[int, int, int]]) -> dict[int, int]:
    degree = {}
    for id1, id2, _ in decoded_pairs:
        degree[id1] = degree.get(id1, 0) + 1
        degree[id2] = degree.get(id2, 0) + 1
    return degree


def build_candidates(
    decoded_pairs: list[tuple[int, int, int]],
    degree: dict[int, int],
    image_names: dict[int, str],
    min_inliers: int,
    min_degree: int,
):
    candidates = []
    for id1, id2, inliers in decoded_pairs:
        deg1 = degree.get(id1, 0)
        deg2 = degree.get(id2, 0)

        if inliers < min_inliers:
            continue
        if deg1 < min_degree or deg2 < min_degree:
            continue

        name1 = image_names.get(id1, f"img_{id1}")
        name2 = image_names.get(id2, f"img_{id2}")

        idx1 = extract_numeric_index(name1, id1)
        idx2 = extract_numeric_index(name2, id2)

        seq_gap = abs(idx2 - idx1)
        min_deg = min(deg1, deg2)
        max_deg = max(deg1, deg2)
        balance = (min_deg / max_deg) if max_deg > 0 else 0.0
        deg_mean = 0.5 * (deg1 + deg2)

        candidates.append({
            "image_id1": id1,
            "image_name1": name1,
            "image_id2": id2,
            "image_name2": name2,
            "inliers": inliers,
            "degree1": deg1,
            "degree2": deg2,
            "min_degree": min_deg,
            "mean_degree": round(deg_mean, 6),
            "balance": round(balance, 6),
            "seq_gap": seq_gap,
            "idx1": idx1,   # interno
            "idx2": idx2,   # interno
        })

    return candidates


def choose_gap_prior(seq_gap: int) -> float:
    if seq_gap <= 1:
        return 0.05
    if seq_gap == 2:
        return 0.65
    if seq_gap == 3:
        return 0.85
    if 4 <= seq_gap <= 8:
        return 1.00
    if 9 <= seq_gap <= 15:
        return 0.80
    if 16 <= seq_gap <= 30:
        return 0.55
    return 0.30


def compute_ranked(candidates: list[dict]) -> list[dict]:
    if not candidates:
        return []

    inliers_vals = [c["inliers"] for c in candidates]
    min_deg_vals = [c["min_degree"] for c in candidates]
    mean_deg_vals = [c["mean_degree"] for c in candidates]
    seq_gap_vals = [c["seq_gap"] for c in candidates]

    inliers_min, inliers_max = min(inliers_vals), max(inliers_vals)
    min_deg_min, min_deg_max = min(min_deg_vals), max(min_deg_vals)
    mean_deg_min, mean_deg_max = min(mean_deg_vals), max(mean_deg_vals)
    gap_min, gap_max = min(seq_gap_vals), max(seq_gap_vals)

    ranked = []
    for c in candidates:
        inliers = c["inliers"]
        min_deg = c["min_degree"]
        mean_deg = c["mean_degree"]
        balance = c["balance"]
        seq_gap = c["seq_gap"]

        inliers_log = safe_log1p(inliers)
        inliers_log_min = safe_log1p(inliers_min)
        inliers_log_max = safe_log1p(inliers_max)

        inliers_score = minmax_norm(inliers_log, inliers_log_min, inliers_log_max)
        min_deg_score = minmax_norm(min_deg, min_deg_min, min_deg_max)
        mean_deg_score = minmax_norm(mean_deg, mean_deg_min, mean_deg_max)
        gap_prior = choose_gap_prior(seq_gap)
        gap_norm = minmax_norm(seq_gap, gap_min, gap_max)

        consecutive_penalty = 0.0
        if seq_gap <= 1:
            consecutive_penalty = 0.35
        elif seq_gap == 2:
            consecutive_penalty = 0.10

        low_degree_penalty = 0.0
        if min_deg <= 6:
            low_degree_penalty = 0.25
        elif min_deg <= 8:
            low_degree_penalty = 0.12

        score = (
            0.38 * min_deg_score
            + 0.18 * mean_deg_score
            + 0.20 * balance
            + 0.14 * inliers_score
            + 0.08 * gap_prior
            + 0.02 * gap_norm
            - consecutive_penalty
            - low_degree_penalty
        )

        out = {
            "image_id1": c["image_id1"],
            "image_name1": c["image_name1"],
            "image_id2": c["image_id2"],
            "image_name2": c["image_name2"],
            "inliers": c["inliers"],
            "degree1": c["degree1"],
            "degree2": c["degree2"],
            "min_degree": c["min_degree"],
            "mean_degree": c["mean_degree"],
            "balance": c["balance"],
            "seq_gap": c["seq_gap"],
            "inliers_score": round(inliers_score, 6),
            "min_degree_score": round(min_deg_score, 6),
            "mean_degree_score": round(mean_deg_score, 6),
            "gap_prior": round(gap_prior, 6),
            "gap_norm": round(gap_norm, 6),
            "consecutive_penalty": round(consecutive_penalty, 6),
            "low_degree_penalty": round(low_degree_penalty, 6),
            "score": round(score, 6),
        }
        ranked.append(out)

    ranked.sort(
        key=lambda x: (
            x["score"],
            x["min_degree"],
            x["balance"],
            x["inliers"],
            x["seq_gap"],
        ),
        reverse=True,
    )
    return ranked


def write_out_txt(out_txt: Path, ranked: list[dict], top_k: int) -> None:
    best = ranked[0]
    fallback = ranked[:top_k]

    lines = []
    lines.append("#!/bin/bash")
    lines.append("# Arquivo gerado automaticamente por p1_select_init_pair.py")
    lines.append(f'export INIT_IMAGE_ID1_AUTO="{best["image_id1"]}"')
    lines.append(f'export INIT_IMAGE_ID2_AUTO="{best["image_id2"]}"')
    lines.append(f'export INIT_IMAGE_NAME1_AUTO="{best["image_name1"]}"')
    lines.append(f'export INIT_IMAGE_NAME2_AUTO="{best["image_name2"]}"')
    lines.append(f'export INIT_PAIR_SCORE_AUTO="{best["score"]}"')
    lines.append(f'export INIT_PAIR_FALLBACK_COUNT="{len(fallback)}"')
    lines.append("")

    ids1 = " ".join(str(x["image_id1"]) for x in fallback)
    ids2 = " ".join(str(x["image_id2"]) for x in fallback)
    scores = " ".join(str(x["score"]) for x in fallback)

    lines.append(f'export INIT_PAIR_FALLBACK_IMAGE_ID1_LIST="{ids1}"')
    lines.append(f'export INIT_PAIR_FALLBACK_IMAGE_ID2_LIST="{ids2}"')
    lines.append(f'export INIT_PAIR_FALLBACK_SCORE_LIST="{scores}"')
    lines.append("")

    for i, item in enumerate(fallback, start=1):
        lines.append(f'export INIT_PAIR_CANDIDATE_{i}_IMAGE_ID1="{item["image_id1"]}"')
        lines.append(f'export INIT_PAIR_CANDIDATE_{i}_IMAGE_ID2="{item["image_id2"]}"')
        lines.append(f'export INIT_PAIR_CANDIDATE_{i}_IMAGE_NAME1="{item["image_name1"]}"')
        lines.append(f'export INIT_PAIR_CANDIDATE_{i}_IMAGE_NAME2="{item["image_name2"]}"')
        lines.append(f'export INIT_PAIR_CANDIDATE_{i}_SCORE="{item["score"]}"')
        lines.append("")

    out_txt.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def write_out_csv(out_csv: Path, ranked: list[dict], top_k: int) -> None:
    fieldnames = [
        "image_id1", "image_name1",
        "image_id2", "image_name2",
        "inliers",
        "degree1", "degree2",
        "min_degree", "mean_degree",
        "balance",
        "seq_gap",
        "inliers_score",
        "min_degree_score",
        "mean_degree_score",
        "gap_prior",
        "gap_norm",
        "consecutive_penalty",
        "low_degree_penalty",
        "score",
    ]

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in ranked[:top_k]:
            writer.writerow({k: row[k] for k in fieldnames})


def log_best(log_file, metrics_csv, dataset, gpu, module, best, out_txt, out_csv):
    log_info(
        log_file,
        dataset,
        gpu,
        module,
        "Melhor par inicial automático: "
        f'{best["image_id1"]} ({best["image_name1"]}) / '
        f'{best["image_id2"]} ({best["image_name2"]}) | '
        f'inliers={best["inliers"]} | '
        f'deg=({best["degree1"]},{best["degree2"]}) | '
        f'min_deg={best["min_degree"]} | '
        f'gap={best["seq_gap"]} | '
        f'score={best["score"]}'
    )

    metric(metrics_csv, dataset, gpu, module, "init_pair_image_id1_auto", best["image_id1"], "id")
    metric(metrics_csv, dataset, gpu, module, "init_pair_image_id2_auto", best["image_id2"], "id")
    metric(metrics_csv, dataset, gpu, module, "init_pair_inliers_auto", best["inliers"], "count")
    metric(metrics_csv, dataset, gpu, module, "init_pair_min_degree_auto", best["min_degree"], "count")
    metric(metrics_csv, dataset, gpu, module, "init_pair_seq_gap_auto", best["seq_gap"], "count")
    metric(metrics_csv, dataset, gpu, module, "init_pair_score_auto", best["score"], "score")
    metric(metrics_csv, dataset, gpu, module, "init_pair_out_txt", str(out_txt), "path")
    metric(metrics_csv, dataset, gpu, module, "init_pair_out_csv", str(out_csv), "path")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seleciona automaticamente o melhor par inicial para o COLMAP mapper."
    )
    parser.add_argument("--db-path", required=True, help="Caminho do banco COLMAP")
    parser.add_argument("--out-txt", required=True, help="Arquivo TXT exportando INIT_IMAGE_ID1/2")
    parser.add_argument("--out-csv", required=True, help="CSV com ranking dos melhores pares")
    parser.add_argument("--top-k", type=int, default=15, help="Quantidade de pares no ranking/fallback")
    parser.add_argument("--min-inliers", type=int, default=50, help="Mínimo de inliers no par")
    parser.add_argument("--min-degree", type=int, default=10, help="Mínimo de conectividade por imagem")
    parser.add_argument("--log-file", required=False)
    parser.add_argument("--metrics-csv", required=False)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--gpu", required=True)
    parser.add_argument("--module", required=True)
    args = parser.parse_args()

    db_path = Path(args.db_path)
    out_txt = Path(args.out_txt)
    out_csv = Path(args.out_csv)

    if not db_path.exists():
        msg = f"Banco não encontrado: {db_path}"
        log_error(args.log_file, args.dataset, args.gpu, args.module, msg)
        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "init_pair_db_exists", 0, "bool", "FAILED", str(db_path))
        return 1

    try:
        conn = sqlite3.connect(str(db_path))
    except sqlite3.Error as exc:
        log_error(args.log_file, args.dataset, args.gpu, args.module, f"Erro abrindo SQLite: {exc}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "init_pair_sqlite_open", 0, "bool", "FAILED", str(exc))
        return 2

    try:
        cur = conn.cursor()

        image_names = load_image_names(cur)
        decoded_pairs = load_pairs(cur)

        if not decoded_pairs:
            log_error(args.log_file, args.dataset, args.gpu, args.module,
                      "Nenhum par válido em two_view_geometries")
            metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                   "init_pair_candidates_found", 0, "bool", "FAILED")
            return 3

        degree = compute_degree(decoded_pairs)

        candidates = build_candidates(
            decoded_pairs=decoded_pairs,
            degree=degree,
            image_names=image_names,
            min_inliers=args.min_inliers,
            min_degree=args.min_degree,
        )

        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "init_pair_total_pairs", len(decoded_pairs), "count")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "init_pair_candidates_after_filter", len(candidates), "count")

        if not candidates:
            log_warn(
                args.log_file,
                args.dataset,
                args.gpu,
                args.module,
                "Nenhum candidato passou nos filtros; usando fallback com todos os pares válidos."
            )
            candidates = build_candidates(
                decoded_pairs=decoded_pairs,
                degree=degree,
                image_names=image_names,
                min_inliers=0,
                min_degree=0,
            )

        ranked = compute_ranked(candidates)

        if not ranked:
            log_error(args.log_file, args.dataset, args.gpu, args.module,
                      "Falha ao ranquear candidatos do par inicial.")
            metric(args.metrics_csv, args.dataset, args.gpu, args.module,
                   "init_pair_ranked_ok", 0, "bool", "FAILED")
            return 4

        best = ranked[0]

        write_out_txt(out_txt, ranked, args.top_k)
        write_out_csv(out_csv, ranked, args.top_k)
        log_best(args.log_file, args.metrics_csv, args.dataset, args.gpu, args.module,
                 best, out_txt, out_csv)

        print(f"INIT_IMAGE_ID1_AUTO={best['image_id1']}")
        print(f"INIT_IMAGE_ID2_AUTO={best['image_id2']}")
        print(f"INIT_IMAGE_NAME1_AUTO={best['image_name1']}")
        print(f"INIT_IMAGE_NAME2_AUTO={best['image_name2']}")
        print(f"INIT_PAIR_SCORE_AUTO={best['score']}")
        print(f"INIT_PAIR_FALLBACK_COUNT={min(args.top_k, len(ranked))}")
        print(f"OUT_TXT={out_txt}")
        print(f"OUT_CSV={out_csv}")
        return 0

    except sqlite3.Error as exc:
        log_error(args.log_file, args.dataset, args.gpu, args.module, f"Erro no SQLite: {exc}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "init_pair_sqlite_query", 0, "bool", "FAILED", str(exc))
        return 5
    except Exception as exc:
        log_error(args.log_file, args.dataset, args.gpu, args.module, f"Erro inesperado: {exc}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module,
               "init_pair_unexpected_exception", 0, "bool", "FAILED", str(exc))
        return 6
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())