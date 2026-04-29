import os
import numpy as np
from PIL import Image

mask_dir = "/data/dataset_ia/train/masks"

def preparar_para_treino():
    print("🔄 Convertendo máscaras visuais (255) para máscaras de treino (1)...")
    for filename in os.listdir(mask_dir):
        if filename.endswith(".png"):
            path = os.path.join(mask_dir, filename)
            img = Image.open(path).convert('L')
            arr = np.array(img)
            
            # Tudo que é branco (solo) vira classe 1
            # Tudo que é preto vira classe 0
            train_mask = np.where(arr > 127, 1, 0).astype(np.uint8)
            
            Image.fromarray(train_mask).save(path)
    print("✅ Máscaras prontas para o TAO Toolkit!")

if __name__ == "__main__":
    preparar_para_treino()