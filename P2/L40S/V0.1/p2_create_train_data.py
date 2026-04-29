import sys
sys.path.append('/scripts/lib_container')

import laspy
import numpy as np
from PIL import Image
import os

input_path = "/data/workspace/ODM_P2_AutoLabeled.las"
output_dir = "/data/dataset_ia/train"
os.makedirs(output_dir + "/images", exist_ok=True)
os.makedirs(output_dir + "/masks", exist_ok=True)

def criar_dataset_final():
    print(f"🔍 Verificando arquivo: {input_path}")
    las = laspy.read(input_path)
    
    # DEBUG: Verificar se realmente existem pontos classe 2
    pontos_solo = np.sum(las.classification == 2)
    print(f"📊 Pontos identificados como Solo (Classe 2): {pontos_solo}")
    
    if pontos_solo == 0:
        print("❌ ERRO: Não existem pontos classe 2 na nuvem! O treino será impossível.")
        return

    res = 0.1 # 10cm
    x = ((las.x - las.x.min()) / res).astype(int)
    y = ((las.y - las.y.min()) / res).astype(int)
    
    width, height = x.max() + 1, y.max() + 1
    print(f"🖼️ Dimensões do canvas: {width}x{height}")

    # Criar matrizes vazias
    img_rgb = np.zeros((height, width, 3), dtype=np.uint8)
    # Iniciamos a máscara com 0 (Preto / Não-Solo)
    img_mask = np.zeros((height, width), dtype=np.uint8)

    # Normalização RGB
    r = (las.red / (las.red.max() / 255)).astype(np.uint8)
    g = (las.green / (las.green.max() / 255)).astype(np.uint8)
    b = (las.blue / (las.blue.max() / 255)).astype(np.uint8)

    print("✍️ Desenhando pixels (priorizando solo)...")
    # Ordenamos para que os pontos de solo (classe 2) sejam desenhados POR ÚLTIMO
    # Isso garante que eles não sejam "atropelados" por outros pontos no mesmo pixel
    indices = np.argsort(las.classification) 
    
    for i in indices:
        img_rgb[y[i], x[i]] = [r[i], g[i], b[i]]
        if las.classification[i] == 2:
            img_mask[y[i], x[i]] = 255 # Branco absoluto para visualização

    # Salvar Tiles 512x512
    tile_size = 512
    count = 0
    for i in range(0, height - tile_size, tile_size):
        for j in range(0, width - tile_size, tile_size):
            crop_rgb = img_rgb[i:i+tile_size, j:j+tile_size]
            crop_mask = img_mask[i:i+tile_size, j:j+tile_size]
            
            # Só salvar se houver pelo menos 1% de solo no tile
            if np.sum(crop_mask == 255) > (tile_size * tile_size * 0.01):
                idx = f"{i}_{j}"
                Image.fromarray(crop_rgb).save(f"{output_dir}/images/tile_{idx}.png")
                Image.fromarray(crop_mask).save(f"{output_dir}/masks/tile_{idx}.png")
                count += 1

    print(f"✅ Dataset pronto! {count} pares de imagens gerados com Solo visível.")

if __name__ == "__main__":
    criar_dataset_final()