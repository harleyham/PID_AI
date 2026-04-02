#!/bin/bash
# Script para validar coordenadas em arquivos PLY binários

PROJECT_ROOT="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO"
DENSE_PLY="$PROJECT_ROOT/04_Produtos_Finais/DS2/fused.ply"

if [ ! -f "$DENSE_PLY" ]; then
    echo "ERRO: O arquivo $DENSE_PLY não existe."
    exit 1
fi

echo "--- ESTATÍSTICAS DA NUVEM (PDAL INFO) ---"
# O --summary mostra os valores Min/Max de X, Y e Z
pdal info --summary "$DENSE_PLY" | grep -E "minx|maxx|miny|maxy|minz|maxz"

echo ""
echo "--- TESTE DE ESCALA ---"
# Se os valores de X/Y forem muito pequenos (ex: entre -10 e 10), 
# a nuvem está em escala local e o MDT sairá errado.
