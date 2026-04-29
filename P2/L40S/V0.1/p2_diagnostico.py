import sys
# Adiciona o caminho onde as bibliotecas do host foram montadas
sys.path.append('/script/lib_container')

import laspy
import numpy as np
import os

# Caminhos dentro do container
# input_path = "/data/workspace/ODM_Densa_DS3.laz"
input_path = "/data/workspace/ODM_P2_AutoLabeled.las"  # Usar a nuvem auto-rotulada para diagnóstico

def diagnostico_ligem():
    if not os.path.exists(input_path):
        print(f"Erro: Arquivo não encontrado em {input_path}")
        return

    las = laspy.read(input_path)
    
    print(f"--- Pipeline P2: Diagnóstico de Nuvem Bruta ---")
    print(f"Arquivo: {os.path.basename(input_path)}")
    print(f"Pontos totais: {len(las.points):,}")
    
    # Verificar atributos para IA
    tem_cor = hasattr(las, 'red')
    tem_intensidade = hasattr(las, 'intensity')
    
    print(f"Atributos disponíveis: Intensity={tem_intensidade}, RGB={tem_cor}")
    
    classes_unicas = np.unique(las.classification)
    print(f"Classes ASPRS presentes: {classes_unicas}")

if __name__ == "__main__":
    diagnostico_ligem()