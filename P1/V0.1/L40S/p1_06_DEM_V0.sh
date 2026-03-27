#!/bin/bash
set -euo pipefail

# Verificar depois:
# Se quiser, posso te mostrar também um truque usado em fotogrametria para gerar hillshade muito mais bonito, que combina multidirectional hillshade + slope, muito usado em produtos de drone.

# Pipeline P1 - Módulo 06: Geração de DSM, DTM, hillshade e CHM
# Versão corrigida para garantir mesma grade/extensão entre DSM e DTM


# GPU="L40S"
# DATASET="Dataset_02"

# PROJECT_ROOT="/media/ham/EXT4/PROJETO_LIGEM_HIBRIDO"
# DENSE_LAS="$PROJECT_ROOT/04_Produtos_Finais/DS2/dense_utm_color.las"
# OUTPUT_DIR="$PROJECT_ROOT/04_Produtos_Finais/DS2/Produtos_Raster"
# LOG_FILE="$PROJECT_ROOT/02_Pipelines_LIGEM/P1_Tradicional/logs/performance_P1.csv"
# WORKSPACE="$PROJECT_ROOT/02_Pipelines_LIGEM/P1_Tradicional/workspace_DS2/L40S"
# ENU_META_JSON="$WORKSPACE/enu_origin.json"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/p1_config.sh"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERRO: arquivo de configuração não encontrado: $CONFIG_FILE"
    exit 1
fi

source "$CONFIG_FILE"
p1_ensure_dirs

RESOLUTION="0.50"
NODATA="-9999"

# Parâmetros SMRF
SMRF_SCALAR="1.25"
SMRF_SLOPE="0.15"
SMRF_THRESHOLD="0.50"
SMRF_WINDOW="16.0"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "ERRO: comando '$1' não encontrado no PATH."
        exit 1
    fi
}

require_cmd pdal
require_cmd gdaldem
require_cmd gdal_calc.py
require_cmd gdal_translate
require_cmd gdalinfo
require_cmd python3

if [ ! -f "$DENSE_LAS" ]; then
    echo "ERRO: Arquivo LAS não encontrado em: $DENSE_LAS"
    exit 1
fi

echo "------------------------------------------------------" | tee -a "$LOG_FILE"
echo "Iniciando Módulo 06 (DSM + DTM + Hillshade + CHM) - $(date)" | tee -a "$LOG_FILE"
echo "Entrada: $DENSE_LAS" | tee -a "$LOG_FILE"
echo "Saída:   $OUTPUT_DIR" | tee -a "$LOG_FILE"
echo "Resolução: ${RESOLUTION} m" | tee -a "$LOG_FILE"
echo "------------------------------------------------------"

start_time=$(date +%s)

TMP_DIR="$(mktemp -d "$OUTPUT_DIR/tmp_m06_XXXXXX")"
cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

GROUND_LAZ="$OUTPUT_DIR/dense_ground.laz"
DTM_RAW="$TMP_DIR/DTM_raw.tif"
DSM_RAW="$TMP_DIR/DSM_raw.tif"
DTM_TIF="$OUTPUT_DIR/DTM.tif"
DSM_TIF="$OUTPUT_DIR/DSM.tif"
DTM_HS="$OUTPUT_DIR/DTM_hillshade.tif"
DSM_HS="$OUTPUT_DIR/DSM_hillshade.tif"
CHM_TIF="$OUTPUT_DIR/CHM.tif"

echo "# EPSG" # | tee -a "$LOG_FILE"
detect_epsg() {
    python3 - <<PY
import json
import pathlib
import subprocess
import sys

dense_las = pathlib.Path(r"$DENSE_LAS")
enu_meta = pathlib.Path(r"$ENU_META_JSON")

def try_las_epsg():
    try:
        data = json.loads(subprocess.check_output(
            ["pdal", "info", str(dense_las), "--metadata"],
            text=True
        ))

        candidates = []

        def walk(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    key = str(k).lower()

                    if key == "epsg":
                        candidates.append(v)

                    if key == "compoundwkt":
                        continue

                    if key == "authority" and str(v).upper() == "EPSG":
                        pass

                    if key == "code":
                        candidates.append(v)

                    walk(v)

            elif isinstance(obj, list):
                for item in obj:
                    walk(item)

        walk(data)

        for c in candidates:
            try:
                s = str(c).strip()
                if s.isdigit() and 1000 <= int(s) <= 999999:
                    return s
            except Exception:
                pass

    except Exception:
        pass

    return None

def try_meta_epsg():
    try:
        if enu_meta.exists():
            meta = json.loads(enu_meta.read_text(encoding="utf-8"))
            epsg = meta.get("epsg")
            if epsg is not None:
                epsg = str(int(epsg))
                if epsg.isdigit():
                    return epsg
    except Exception:
        pass

    return None

epsg = try_las_epsg()
source = "LAS"

if not epsg:
    epsg = try_meta_epsg()
    source = "ENU_META_JSON"

if not epsg:
    print("ERRO: não foi possível determinar o EPSG do módulo 06.", file=sys.stderr)
    sys.exit(1)

print(f"{epsg}|{source}")
PY
}

IFS='|' read -r EPSG_CODE EPSG_SOURCE <<< "$(detect_epsg)"

if [[ ! "$EPSG_CODE" =~ ^[0-9]+$ ]]; then
    echo "ERRO: EPSG inválido detectado: $EPSG_CODE" | tee -a "$LOG_FILE"
    exit 1
fi

SRS="EPSG:${EPSG_CODE}"

echo "SRS detectado: $SRS (origem: $EPSG_SOURCE)" | tee -a "$LOG_FILE"


# Calcular bounds globais da nuvem completa para forçar a mesma grade em DSM e DTM
echo "# Bounds" # | tee -a "$LOG_FILE"
read -r MINX MAXX MINY MAXY MINZ MAXZ <<< "$(python3 - <<PY
import json, subprocess
cmd = ["pdal", "info", r"$DENSE_LAS", "--summary"]
data = json.loads(subprocess.check_output(cmd, text=True))
b = data["summary"]["bounds"]
print(b["minx"], b["maxx"], b["miny"], b["maxy"], b["minz"], b["maxz"])
PY
)"

PDAL_BOUNDS="([$MINX,$MAXX],[$MINY,$MAXY])"

echo "Bounds globais usados na grade:"
echo "  X: $MINX -> $MAXX"
echo "  Y: $MINY -> $MAXY"
echo "  PDAL bounds: $PDAL_BOUNDS"

echo "# Ground" # | tee -a "$LOG_FILE"
# 1) Classificação de solo + exportação da nuvem de terreno
pdal pipeline --stdin <<EOF
{
  "pipeline": [
    {
      "type": "readers.las",
      "filename": "$DENSE_LAS"
    },
    {
      "type": "filters.smrf",
      "scalar": $SMRF_SCALAR,
      "slope": $SMRF_SLOPE,
      "threshold": $SMRF_THRESHOLD,
      "window": $SMRF_WINDOW
    },
    {
      "type": "filters.expression",
      "expression": "Classification == 2"
    },
    {
      "type": "writers.las",
      "filename": "$GROUND_LAZ",
      "minor_version": 4,
      "dataformat_id": 3,
      "compression": "laszip",
      "forward": "all"
    }
  ]
}
EOF

# 2) DTM a partir dos pontos classificados como terreno
# Usa os bounds da nuvem completa para coincidir exatamente com o DSM
echo "# DTM" # | tee -a "$LOG_FILE"
pdal pipeline --stdin <<EOF
{
  "pipeline": [
    {
      "type": "readers.las",
      "filename": "$GROUND_LAZ"
    },
    {
      "type": "writers.gdal",
      "filename": "$DTM_RAW",
      "resolution": $RESOLUTION,
      "bounds": "$PDAL_BOUNDS",
      "output_type": "idw",
      "data_type": "float32",
      "nodata": $NODATA,
      "window_size": 4,
      "gdaldriver": "GTiff",
      "override_srs":"$SRS"
    }
  ]
}
EOF

# 3) DSM a partir de todos os pontos, usando exatamente a mesma grade
echo "# DSM" # | tee -a "$LOG_FILE"
pdal pipeline --stdin <<EOF
{
  "pipeline": [
    {
      "type": "readers.las",
      "filename": "$DENSE_LAS"
    },
    {
      "type": "writers.gdal",
      "filename": "$DSM_RAW",
      "resolution": $RESOLUTION,
      "bounds": "$PDAL_BOUNDS",
      "output_type": "max",
      "data_type": "float32",
      "nodata": $NODATA,
      "window_size": 0,
      "gdaldriver": "GTiff",
      "override_srs":"$SRS"
    }
  ]
}
EOF

# 4) Compressão/tiling para produtos finais
echo "# Compressão" # | tee -a "$LOG_FILE"
gdal_translate "$DTM_RAW" "$DTM_TIF"     -co TILED=YES     -co COMPRESS=DEFLATE     -co PREDICTOR=3     -co BIGTIFF=IF_SAFER

gdal_translate "$DSM_RAW" "$DSM_TIF"     -co TILED=YES     -co COMPRESS=DEFLATE     -co PREDICTOR=3     -co BIGTIFF=IF_SAFER

# 5) Hillshade
echo "# Hillshade" # | tee -a "$LOG_FILE"
gdaldem hillshade "$DTM_TIF" "$DTM_HS"     -multidirectional     -compute_edges     -of GTiff

gdaldem hillshade "$DSM_TIF" "$DSM_HS"     -multidirectional     -compute_edges     -of GTiff

# 6) CHM = DSM - DTM, limitado a valores >= 0 e respeitando NoData
echo "# CHM" # | tee -a "$LOG_FILE"
gdal_calc.py \
    -A "$DSM_TIF" \
    -B "$DTM_TIF" \
    --outfile="$CHM_TIF" \
    --calc="where((A==$NODATA)|(B==$NODATA), $NODATA, maximum(A-B,0))" \
    --NoDataValue="$NODATA" \
    --type=Float32 \
    --overwrite \
    --co="TILED=YES" \
    --co="COMPRESS=DEFLATE" \
    --co="PREDICTOR=3" \
    --co="BIGTIFF=IF_SAFER" \
    --quiet

# 7) Resumo
echo
echo "Resumo dos rasters gerados:"
for f in "$DTM_TIF" "$DSM_TIF" "$DTM_HS" "$DSM_HS" "$CHM_TIF"; do
    echo "------------------------------------------------------"
    echo "$(basename "$f")"

    if [[ "$f" == "$DTM_TIF" ]]; then
        gdalinfo "$f" \
        | grep -E "Size is|Pixel Size|Lower Left|Upper Right|NoData Value|STATISTICS_MINIMUM|STATISTICS_MAXIMUM" \
        | tee -a "$LOG_FILE" || true
    else
        gdalinfo "$f" \
        | grep -E "Size is|Pixel Size|Lower Left|Upper Right|NoData Value" || true
    fi
done

end_time=$(date +%s)
duration=$((end_time - start_time))

if [ -f "$LOG_FILE" ]; then
    echo "$DATASET;P1_M06_Rasters_$GPU;$duration;seconds;Success" >> "$LOG_FILE"
fi

echo "------------------------------------------------------"
echo "Módulo 06 concluído em $duration segundos."
echo "SRS:           $SRS"
echo "Ground LAZ:    $GROUND_LAZ"
echo "DTM:           $DTM_TIF"
echo "DSM:           $DSM_TIF"
echo "DTM Hillshade: $DTM_HS"
echo "DSM Hillshade: $DSM_HS"
echo "CHM:           $CHM_TIF"
echo "------------------------------------------------------"
