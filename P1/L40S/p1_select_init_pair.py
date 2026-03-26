#!/usr/bin/env python3
import argparse
import csv
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seleciona automaticamente o melhor par inicial para o COLMAP mapper."
    )
    parser.add_argument("--db-path", required=True, help="Caminho do banco COLMAP")
    parser.add_argument("--out-txt", required=True, help="Arquivo TXT exportando INIT_IMAGE_ID1/2")
    parser.add_argument("--out-csv", required=True, help="CSV com ranking dos melhores pares")
    parser.add_argument("--top-k", type=int, default=15, help="Quantidade de pares no ranking")
    parser.add_argument("--min-inliers", type=int, default=50, help="Mínimo de inliers no par")
    parser.add_argument("--min-degree", type=int, default=5, help="Mínimo de conectividade por imagem")
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
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_db_exists", 0, "bool", "FAILED", str(db_path))
        return 1

    try:
        conn = sqlite3.connect(str(db_path))
    except sqlite3.Error as exc:
        log_error(args.log_file, args.dataset, args.gpu, args.module, f"Erro abrindo SQLite: {exc}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_sqlite_open", 0, "bool", "FAILED", str(exc))
        return 2

    try:
        cur = conn.cursor()

        cur.execute("SELECT image_id, name FROM images")
        image_names = {int(i): name for i, name in cur.fetchall()}

        cur.execute("SELECT pair_id, rows FROM two_view_geometries WHERE rows > 0")
        raw_pairs = cur.fetchall()

        if not raw_pairs:
            log_error(args.log_file, args.dataset, args.gpu, args.module, "Nenhum par válido em two_view_geometries")
            metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_candidates_found", 0, "bool", "FAILED")
            return 3

        # Grau de conectividade por imagem
        degree = {}
        decoded_pairs = []
        for pair_id, rows in raw_pairs:
            id1, id2 = pair_id_to_image_ids(int(pair_id))
            degree[id1] = degree.get(id1, 0) + 1
            degree[id2] = degree.get(id2, 0) + 1
            decoded_pairs.append((id1, id2, int(rows)))

        candidates = []
        for id1, id2, inliers in decoded_pairs:
            deg1 = degree.get(id1, 0)
            deg2 = degree.get(id2, 0)
            if inliers < args.min_inliers:
                continue
            if deg1 < args.min_degree or deg2 < args.min_degree:
                continue
            candidates.append((id1, id2, inliers, deg1, deg2))

        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_total_pairs", len(raw_pairs), "count")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_candidates_after_filter", len(candidates), "count")

        if not candidates:
            log_warn(
                args.log_file,
                args.dataset,
                args.gpu,
                args.module,
                "Nenhum candidato passou nos filtros; usando fallback com todos os pares válidos."
            )
            for id1, id2, inliers in decoded_pairs:
                deg1 = degree.get(id1, 0)
                deg2 = degree.get(id2, 0)
                candidates.append((id1, id2, inliers, deg1, deg2))

        inliers_vals = [c[2] for c in candidates]
        min_deg_vals = [min(c[3], c[4]) for c in candidates]

        inliers_min, inliers_max = min(inliers_vals), max(inliers_vals)
        deg_min, deg_max = min(min_deg_vals), max(min_deg_vals)

        ranked = []
        for id1, id2, inliers, deg1, deg2 in candidates:
            min_deg = min(deg1, deg2)
            max_deg = max(deg1, deg2)
            balance = min_deg / max_deg if max_deg > 0 else 0.0

            score = (
                0.55 * minmax_norm(inliers, inliers_min, inliers_max)
                + 0.30 * minmax_norm(min_deg, deg_min, deg_max)
                + 0.15 * balance
            )

            ranked.append({
                "image_id1": id1,
                "image_name1": image_names.get(id1, f"img_{id1}"),
                "image_id2": id2,
                "image_name2": image_names.get(id2, f"img_{id2}"),
                "inliers": inliers,
                "degree1": deg1,
                "degree2": deg2,
                "min_degree": min_deg,
                "balance": round(balance, 6),
                "score": round(score, 6),
            })

        ranked.sort(key=lambda x: (x["score"], x["inliers"], x["min_degree"]), reverse=True)
        best = ranked[0]

        out_txt.write_text(
            f'export INIT_IMAGE_ID1_AUTO="{best["image_id1"]}"\n'
            f'export INIT_IMAGE_ID2_AUTO="{best["image_id2"]}"\n',
            encoding="utf-8",
        )

        with out_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "image_id1", "image_name1",
                    "image_id2", "image_name2",
                    "inliers", "degree1", "degree2",
                    "min_degree", "balance", "score"
                ],
            )
            writer.writeheader()
            writer.writerows(ranked[:args.top_k])

        log_info(
            args.log_file,
            args.dataset,
            args.gpu,
            args.module,
            f"Melhor par inicial automático: {best['image_id1']} ({best['image_name1']}) / "
            f"{best['image_id2']} ({best['image_name2']}) | inliers={best['inliers']} | "
            f"deg=({best['degree1']},{best['degree2']}) | score={best['score']}"
        )

        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_image_id1_auto", best["image_id1"], "id")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_image_id2_auto", best["image_id2"], "id")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_inliers_auto", best["inliers"], "count")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_score_auto", best["score"], "score")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_out_txt", str(out_txt), "path")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_out_csv", str(out_csv), "path")

        print(f"INIT_IMAGE_ID1_AUTO={best['image_id1']}")
        print(f"INIT_IMAGE_ID2_AUTO={best['image_id2']}")
        print(f"OUT_TXT={out_txt}")
        print(f"OUT_CSV={out_csv}")
        return 0

    except sqlite3.Error as exc:
        log_error(args.log_file, args.dataset, args.gpu, args.module, f"Erro no SQLite: {exc}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "init_pair_sqlite_query", 0, "bool", "FAILED", str(exc))
        return 4
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())