#!/usr/bin/env python3
import argparse
import sqlite3
import sys

from p1_logging import log_info, log_warn, log_error, metric


PAIR_ID_MOD = 2147483647


def pair_id_to_image_ids(pair_id: int) -> tuple[int, int]:
    """Converte pair_id do COLMAP de volta para image_ids."""
    image_id2 = pair_id % PAIR_ID_MOD
    image_id1 = (pair_id - image_id2) // PAIR_ID_MOD
    return int(image_id1), int(image_id2)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Diagnóstico de conectividade do banco COLMAP."
    )
    parser.add_argument("--project-root", required=True, help="Raiz do projeto.")
    parser.add_argument("--db-path", required=True, help="Caminho do database do COLMAP.")
    parser.add_argument("--log-file", required=False, help="Arquivo de log textual.")
    parser.add_argument("--metrics-csv", required=False, help="CSV de métricas.")
    parser.add_argument("--dataset", required=True, help="Nome do dataset.")
    parser.add_argument("--gpu", required=True, help="GPU usada no processamento.")
    parser.add_argument("--module", required=True, help="Nome do módulo.")
    args = parser.parse_args()

    log_info(args.log_file, args.dataset, args.gpu, args.module, f"PROJECT_ROOT: {args.project_root}")
    log_info(args.log_file, args.dataset, args.gpu, args.module, f"DB_PATH: {args.db_path}")

    if not args.db_path:
        log_error(args.log_file, args.dataset, args.gpu, args.module, "DB_PATH vazio")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "db_path_valid", 0, "bool", "FAILED")
        return 1

    try:
        conn = sqlite3.connect(args.db_path)
    except sqlite3.Error as exc:
        log_error(args.log_file, args.dataset, args.gpu, args.module, f"Erro ao abrir SQLite: {exc}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "sqlite_open", 0, "bool", "FAILED", str(exc))
        return 2

    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM images")
        total_images = cur.fetchone()[0]

        cur.execute("SELECT pair_id, rows FROM two_view_geometries WHERE rows > 0")
        pairs = cur.fetchall()

        log_info(args.log_file, args.dataset, args.gpu, args.module, f"Total de imagens no banco: {total_images}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "total_images", total_images, "count")

        total_pairs = len(pairs)
        log_info(args.log_file, args.dataset, args.gpu, args.module, f"Total de pares validados: {total_pairs}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "valid_pairs", total_pairs, "count")

        if total_pairs == 0:
            log_error(
                args.log_file,
                args.dataset,
                args.gpu,
                args.module,
                "ERRO CRÍTICO: Nenhum par de imagens validado. O matching falhou."
            )
            metric(args.metrics_csv, args.dataset, args.gpu, args.module, "matching_ok", 0, "bool", "FAILED")
            return 3

        image_connections: dict[int, int] = {}

        for pair_id, _num_inliers in pairs:
            id1, id2 = pair_id_to_image_ids(pair_id)
            image_connections[id1] = image_connections.get(id1, 0) + 1
            image_connections[id2] = image_connections.get(id2, 0) + 1

        connected_images = len(image_connections)
        orphan_count = total_images - connected_images
        low_conn = [img for img, count in image_connections.items() if count < 3]
        avg_conn = sum(image_connections.values()) / connected_images if connected_images > 0 else 0.0

        log_info(args.log_file, args.dataset, args.gpu, args.module, f"Imagens conectadas: {connected_images}")
        log_info(args.log_file, args.dataset, args.gpu, args.module, f"Média de conexões por imagem: {avg_conn:.2f}")
        log_info(
            args.log_file,
            args.dataset,
            args.gpu,
            args.module,
            f"Imagens com conectividade crítica (<3): {len(low_conn)}"
        )

        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "connected_images", connected_images, "count")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "avg_connections", f"{avg_conn:.2f}", "count")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "low_connectivity_images", len(low_conn), "count")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "orphan_images", orphan_count, "count")

        if orphan_count > 0:
            log_warn(
                args.log_file,
                args.dataset,
                args.gpu,
                args.module,
                f"AVISO: {orphan_count} imagens estão órfãs (sem matches)."
            )

        if avg_conn < 5:
            diagnosis = "sobreposicao_insuficiente"
            status = "WARNING"
            log_warn(args.log_file, args.dataset, args.gpu, args.module, "Diagnóstico: sobreposição insuficiente.")
        else:
            diagnosis = "conectividade_robusta"
            status = "SUCCESS"
            log_info(args.log_file, args.dataset, args.gpu, args.module, "Diagnóstico: conectividade robusta.")

        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "diagnosis", diagnosis, "", status)
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "matching_ok", 1, "bool", status)

        return 0

    except sqlite3.Error as exc:
        log_error(args.log_file, args.dataset, args.gpu, args.module, f"Erro no SQLite: {exc}")
        metric(args.metrics_csv, args.dataset, args.gpu, args.module, "sqlite_query", 0, "bool", "FAILED", str(exc))
        return 4

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
    