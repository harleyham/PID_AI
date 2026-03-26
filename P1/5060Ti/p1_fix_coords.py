import os
import argparse
import sys


def format_for_colmap(input_file: str, output_file: str) -> int:
    if not os.path.exists(input_file):
        print(f"Erro: Arquivo {input_file} nao encontrado.", file=sys.stderr)
        return 1

    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    clean_lines = []
    for line in lines:
        parts = line.strip().split()

        if len(parts) >= 4:
            name, lat, lon, alt = parts[0], parts[1], parts[2], parts[3]
            clean_lines.append(f"{name} {lat} {lon} {alt}")

    if clean_lines:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(clean_lines) + "\n")

        print(f"Sucesso! Arquivo gerado em: {output_file}")
        print(f"Total de imagens processadas: {len(clean_lines)}")
        return 0

    print("Erro: Nenhuma linha valida encontrada para formatar.", file=sys.stderr)
    return 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Formata o arquivo de coordenadas extraído por exiftool para uso no COLMAP."
    )
    parser.add_argument(
        "--project-root",
        required=True,
        help="Raiz do projeto (mantido para padronização do pipeline).",
    )
    parser.add_argument(
        "--input-file",
        required=True,
        help="Arquivo de entrada gerado pelo exiftool.",
    )
    parser.add_argument(
        "--output-file",
        required=True,
        help="Arquivo de saída formatado para o COLMAP.",
    )

    args = parser.parse_args()

    print("Executando conversão")
    print(f"PROJECT_ROOT: {args.project_root}")
    print(f"INPUT_FILE:   {args.input_file}")
    print(f"OUTPUT_FILE:  {args.output_file}")

    return format_for_colmap(args.input_file, args.output_file)


if __name__ == "__main__":
    sys.exit(main())
    