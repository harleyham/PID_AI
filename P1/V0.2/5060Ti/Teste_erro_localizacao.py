from pyproj import Geod
import json

gps_txt = "/media/ham1/EXT4/PROJETO_LIGEM_HIBRIDO/02_Pipelines_LIGEM/P1_Tradicional/workspace_DS2/L40S/coords_ds2.txt"
enu_json = "/media/ham1/EXT4/PROJETO_LIGEM_HIBRIDO/02_Pipelines_LIGEM/P1_Tradicional/workspace_DS2/L40S/enu_origin.json"

with open(gps_txt, "r", encoding="utf-8") as f:
    first = f.readline().strip().split()

# formato esperado: nome lat lon alt
_, lat1, lon1, alt1 = first
lat1 = float(lat1)
lon1 = float(lon1)

with open(enu_json, "r", encoding="utf-8") as f:
    meta = json.load(f)

lat2 = float(meta["ref_lat"])
lon2 = float(meta["ref_lon"])

g = Geod(ellps="WGS84")
_, _, dist = g.inv(lon1, lat1, lon2, lat2)

print("Distância horizontal (m):", dist)