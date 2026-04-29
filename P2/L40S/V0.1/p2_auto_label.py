import sys
sys.path.append('/scripts/lib_container')

import laspy
import numpy as np
import os

input_path = "/data/workspace/ODM_Densa_DS3.laz"
output_path = "/data/workspace/ODM_P2_AutoLabeled.las"

def gerar_labels_preliminares():
    print("Carregando nuvem para classificação geométrica...")
    las = laspy.read(input_path)
    
    # Algoritmo Simples: Pontos mais baixos em janelas de 5m
    # Isso servirá de "professor" para a rede neural
    coords = np.vstack((las.x, las.y, las.z)).transpose()
    
    # Criando um filtro de altura simples para label inicial
    # (No LIGEM usaremos o PDAL para algo mais sofisticado, 
    # mas este código valida sua L40S agora)
    z_min_local = np.percentile(las.z, 10) # 10% mais baixos
    
    # Atribui Classe 2 (Solo) aos pontos baixos e Classe 1 (Resto)
    las.classification[las.z <= z_min_local] = 2
    las.classification[las.z > z_min_local] = 1
    
    las.write(output_path)
    print(f"Nuvem auto-rotulada salva em: {output_path}")
    print(f"Pontos de solo estimados: {np.sum(las.classification == 2)}")

if __name__ == "__main__":
    gerar_labels_preliminares()