# ✅ Carichiamo:

# FastAPI per gestire le rotte e i form.

# Folium per generare mappe interattive.

# GeoPandas e Pandas per gestire dati geografici.

# branca per le scale di colore.

# typing per annotare i tipi di variabili.

from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from urllib.parse import urlencode
import io
import geopandas as gpd
import pandas as pd
import folium
from folium.features import GeoJsonPopup
from branca.colormap import linear

from typing import List, Optional

# ✅ Creiamo l’app FastAPI.
# ✅ Montiamo /static per servire i file statici (incluso latest_map.html).
# ✅ Definiamo la cartella templates dove metteremo index.html.
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Password
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = "Relab"
    correct_password = "Relab"

    if not (credentials.username == correct_username and credentials.password == correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# ✅ Carichiamo il file gpkg con dati geografici.
# ✅ Forziamo il sistema di coordinate a EPSG:4326 per compatibilità Folium.
# ✅ Individuiamo le colonne che contengono “domanda” e calcoliamo la tot_demand.
# ✅ Creiamo res_map per gestire le risoluzioni spaziali selezionabili.

GPKG_PATH = "data/gdf_IT_geoP_light.gpkg"
GPKG_COMUNI =  "data/comuni.gpkg"
GPKG_PROVINCE =  "data/province.gpkg"
GPKG_REGIONI =  "data/regioni.gpkg"

gdf = gpd.read_file(GPKG_PATH).to_crs("EPSG:4326")
gdf["SEZ2011"] = gdf["SEZ2011"].astype(str)

demand_columns = [col for col in gdf.columns if "domanda" in col.lower()]
gdf["tot_demand"] = gdf[demand_columns].apply(pd.to_numeric, errors="coerce").sum(axis=1)


res_map = {
    "Region": "DEN_REG",
    "Province": "DEN_UTS",
    "Municipality": "COMUNE",
    "Census tracts": "SEZ2011"
}


# ✅ Quando apri il sito, genera la mappa iniziale latest_map.html (tot_demand per Comune).
# ✅ Mostra index.html con la sidebar pronta e la mappa nell’iframe.
@app.get("/", response_class=HTMLResponse)
# async def index(request: Request):
async def index(request: Request, username: str = Depends(verify_credentials)):

    params = request.query_params

    selected_columns = params.getlist("selected_columns")
    if not selected_columns:
        selected_columns = ["tot_demand"]

    res_selection = params.get("res_selection")
    if res_selection is None:
        res_selection = "Municipality"  # ⚠️ Se vuoi Region di default, altrimenti "Municipality"
    elif res_selection not in res_map.keys():
        res_selection = "Census tracts"  # fallback se valore invalido

    selected_regions = params.getlist("regions")
    selected_provinces = params.getlist("provinces")
    selected_municipalities = params.getlist("municipalities")

    filtered_gdf = gdf.copy()
    if selected_regions:
        filtered_gdf = filtered_gdf[filtered_gdf["DEN_REG"].isin(selected_regions)]
    if selected_provinces:
        filtered_gdf = filtered_gdf[filtered_gdf["DEN_UTS"].isin(selected_provinces)]
    if selected_municipalities:
        filtered_gdf = filtered_gdf[filtered_gdf["COMUNE"].isin(selected_municipalities)]

    m = generate_map(selected_columns, res_map[res_selection], filtered_gdf)
    m.save("static/latest_map.html")

    return templates.TemplateResponse("index.html", {
        "request": request,
        "demand_columns": demand_columns,
        "res_options": list(res_map.keys()),
        "regions": sorted(gdf["DEN_REG"].dropna().unique()),
        "provinces": sorted(gdf["DEN_UTS"].dropna().unique()),
        "municipalities": sorted(gdf["COMUNE"].dropna().unique()),
        "selected_columns": selected_columns,
        "res_selection": res_selection,
        "selected_regions": selected_regions,
        "selected_provinces": selected_provinces,
        "selected_municipalities": selected_municipalities
    })


# ✅ Filtra il GeoDataFrame in base alle selezioni dell’utente.
# ✅ Genera la mappa aggiornata e sovrascrive latest_map.html.
# ✅ Usa RedirectResponse(url="/", status_code=303):

# Ritorna alla pagina principale dopo l'aggiornamento.

# Non mostra { "status": "success" }.

from urllib.parse import urlencode

@app.post("/update_map")
async def update_map(
    request: Request,
    selected_columns: Optional[List[str]] = Form(None),
    res_selection: str = Form("Municipality"),
    regions: Optional[List[str]] = Form(None),
    provinces: Optional[List[str]] = Form(None),
    municipalities: Optional[List[str]] = Form(None),
    total: Optional[str] = Form(None)
):
    filtered_gdf = gdf.copy()

    if regions:
        filtered_gdf = filtered_gdf[filtered_gdf["DEN_REG"].isin(regions)]
    if provinces:
        filtered_gdf = filtered_gdf[filtered_gdf["DEN_UTS"].isin(provinces)]
    if municipalities:
        filtered_gdf = filtered_gdf[filtered_gdf["COMUNE"].isin(municipalities)]

    if total is not None or not selected_columns:
        selected_columns = ["tot_demand"]

    m = generate_map(selected_columns, res_map[res_selection], filtered_gdf)
    m.save("static/latest_map.html")

    # ✅ Prepara i parametri per il redirect
    params = []
    for col in selected_columns:
        params.append(("selected_columns", col))
    params.append(("res_selection", res_selection))
    for r in regions or []:
        params.append(("regions", r))
    for p in provinces or []:
        params.append(("provinces", p))
    for m in municipalities or []:
        params.append(("municipalities", m))

    url = "/?" + urlencode(params, doseq=True)

    return RedirectResponse(url=url, status_code=303)


# ✅ Crea la mappa:

# Colorata in scala Reds in base alla sum_selected.

# Con pop-up per ID, colonne selezionate e domanda totale.

# Centrata sull’estensione dei dati.

# Salva su latest_map.html.

def generate_map(selected_columns, spatial_col, data=None):
    if data is None:
        data = gdf.copy()

    data[selected_columns] = data[selected_columns].apply(pd.to_numeric, errors="coerce")
    data["sum_selected"] = data[selected_columns].sum(axis=1)

    # --- Aggrega solo i valori (no geometria!)
    agg_dict = {col: "sum" for col in selected_columns}
    agg_dict["sum_selected"] = "sum"
    df_agg = data.groupby(spatial_col).agg(agg_dict).reset_index()

    if spatial_col == "COMUNE":
        gdf_geom = gpd.read_file(GPKG_COMUNI).to_crs("EPSG:4326")
        join_key = "COMUNE"
    elif spatial_col == "DEN_UTS":
        gdf_geom = gpd.read_file(GPKG_PROVINCE).to_crs("EPSG:4326")
        join_key = "DEN_UTS"
    elif spatial_col == "DEN_REG":
        gdf_geom = gpd.read_file(GPKG_REGIONI).to_crs("EPSG:4326")
        join_key = "DEN_REG"
    elif spatial_col == "SEZ2011":
        # Caso speciale: usa direttamente i dati originali
        gdf_geom = data.copy()
        join_key = "SEZ2011"
    else:
        raise ValueError(f"Colonna spaziale non riconosciuta: {spatial_col}")

    # --- Merge tra attributi aggregati e geometria leggera
    if spatial_col != "SEZ2011":
        gdf_final = gdf_geom.merge(df_agg, left_on=join_key, right_on=spatial_col, how="left")
        gdf_final["SEZ2011"] = gdf_final[join_key].astype(str)  # per compatibilità popup
    else:
        gdf_final = gdf_geom.copy()

    bounds = gdf_final.total_bounds
    center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]
    
    print(f"gdf_final rows: {len(gdf_final)}")
    print(f"gdf_final bounds: {gdf_final.total_bounds}")
    
    m = folium.Map(location=center, zoom_start=8, tiles='OpenStreetMap')
    colormap = linear.Reds_09.scale(
        gdf_final["sum_selected"].min(),
        gdf_final["sum_selected"].max()
    )

    def style_fn(feature):
        val = feature["properties"].get("sum_selected")
        return {
            "fillColor": colormap(val) if val else "#cccccc",
            "color": "black",
            "weight": 0.3,
            "fillOpacity": 0.7 if val else 0
        }
    selected_columns = [col for col in selected_columns if col != "tot_demand"]
    popup_fields = ["SEZ2011"] + selected_columns + ["sum_selected"]
    popup_aliases = ["ID"] + selected_columns + ["Total Demand"]

    folium.GeoJson(
        gdf_final,
        style_function=style_fn,
        popup=GeoJsonPopup(fields=popup_fields, aliases=popup_aliases)
    ).add_to(m)

    colormap.caption = "Total Demand"
    colormap.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    print(f"selected_columns: {selected_columns}")
    print(f"spatial_col: {spatial_col}")
    print(f"data rows: {len(data)}")
    print(f"data columns: {list(data.columns)}")

    return m

# Export button
@app.get("/export_csv")
async def export_csv(
    selected_columns: Optional[List[str]] = Query(None),
    res_selection: str = Query("Municipality"),
    regions: Optional[List[str]] = Query(None),
    provinces: Optional[List[str]] = Query(None),
    municipalities: Optional[List[str]] = Query(None),
):
    # Filtra il GeoDataFrame come in / (index)
    filtered_gdf = gdf.copy()

    if regions:
        filtered_gdf = filtered_gdf[filtered_gdf["DEN_REG"].isin(regions)]
    if provinces:
        filtered_gdf = filtered_gdf[filtered_gdf["DEN_UTS"].isin(provinces)]
    if municipalities:
        filtered_gdf = filtered_gdf[filtered_gdf["COMUNE"].isin(municipalities)]

    if not selected_columns:
        selected_columns = ["tot_demand"]

    # Aggiusta i nomi colonne e aggrega come per la mappa
    spatial_col = res_map.get(res_selection, "COMUNE")

    filtered_gdf[selected_columns] = filtered_gdf[selected_columns].apply(pd.to_numeric, errors="coerce")
    filtered_gdf["sum_selected"] = filtered_gdf[selected_columns].sum(axis=1)

    if spatial_col != "SEZ2011":
        agg_dict = {col: "sum" for col in selected_columns}
        agg_dict["sum_selected"] = "sum"
        gdf_tmp = filtered_gdf[[spatial_col] + selected_columns + ["sum_selected"]].copy()
        gdf_final = gdf_tmp.groupby(spatial_col).sum().reset_index()
    else:
        gdf_final = filtered_gdf[[spatial_col] + selected_columns + ["sum_selected"]].copy()

    # Crea CSV in memoria
    output = io.StringIO()
    gdf_final.to_csv(output, index=False)
    output.seek(0)

    filename = f"export_{res_selection}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

