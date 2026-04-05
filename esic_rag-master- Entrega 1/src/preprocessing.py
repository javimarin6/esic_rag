
import json
import os
import re
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
# Rutas
_HERE      = os.path.dirname(os.path.abspath(__file__))
RAW_PATH   = os.path.join(_HERE, "..", "data", "raw",   "productos_dia.json")
CLEAN_PATH = os.path.join(_HERE, "..", "data", "clean", "productos_dia_clean.csv")

# Mapeo de claves nutricionales
CLAVES_PROTEINAS  = ["proteinas", "proteínas"]
CLAVES_CARBOS     = ["hidratos de carbono", "carbohidratos"]
CLAVES_GRASAS     = ["grasas"]
CLAVES_FIBRA      = ["fibra alimentaria", "fibra"]
CLAVES_CALORIAS   = ["valor energetico", "valor energético", "energía"]
CLAVES_SAL        = ["sal"]
CLAVES_AZUCAR     = ["azucares", "azúcares"]
CLAVES_SATURADAS  = ["saturadas"]

# Utilidades
def _numero(texto) -> Optional[float]:
    """Extrae el primer número de un string. Ej: '9.5 gr' → 9.5"""
    if texto is None:
        return None
    match = re.search(r"(\d+)[.,]?(\d*)", str(texto))
    if not match:
        return None
    entero  = match.group(1)
    decimal = match.group(2)
    return float(f"{entero}.{decimal}" if decimal else entero)
def _buscar(nutri_dict: Dict, claves: List[str]) -> Optional[float]:
    """Busca en el dict nutricional la primera clave que coincida."""
    if not isinstance(nutri_dict, dict):
        return None
    nutri_lower = {k.lower().strip(): v for k, v in nutri_dict.items()}
    for clave in claves:
        if clave in nutri_lower:
            return _numero(nutri_lower[clave])
    return None

# Score nutricional
def _score_nutricional(row: pd.Series) -> float:
    score      = 50.0
    proteinas  = row.get("proteinas")  or 0.0
    fibra      = row.get("fibra")      or 0.0
    azucares   = row.get("azucares")   or 0.0
    grasas_sat = row.get("grasas_sat") or 0.0
    calorias   = row.get("calorias")   or 0.0
    score += min(proteinas, 35) * 1.0
    score += min(fibra, 10)     * 2.0
    if azucares > 22.5:
        score -= min((azucares - 22.5) * 0.8, 20)
    if grasas_sat > 5:
        score -= min((grasas_sat - 5)  * 1.5, 15)
    if calorias > 400:
        score -= min((calorias - 400)  * 0.025, 10)
    return round(max(0.0, min(100.0, score)), 2)

# Normalización
def _norm_inverso(serie: pd.Series) -> pd.Series:
    mn, mx = serie.min(), serie.max()
    if mx == mn:
        return pd.Series([0.5] * len(serie), index=serie.index)
    return 1.0 - (serie - mn) / (mx - mn)
def _norm_directo(serie: pd.Series) -> pd.Series:
    mn, mx = serie.min(), serie.max()
    if mx == mn:
        return pd.Series([0.5] * len(serie), index=serie.index)
    return (serie - mn) / (mx - mn)

# Texto de búsqueda para embeddings
def _texto_busqueda(row: pd.Series) -> str:
    partes = []
    if row.get("titulo"):
        partes.append(str(row["titulo"]).lower().strip())
    if row.get("categorias"):
        cats = row["categorias"]
        if isinstance(cats, list):
            partes.append(" ".join(cats).lower())
        else:
            partes.append(str(cats).lower())
    if row.get("descripcion"):
        desc = str(row["descripcion"]).strip()
        if desc:
            partes.append(desc.lower())
    return " | ".join(p for p in partes if p)

# Imputación por media de categoría
def _categoria_principal(categorias) -> str:
    if isinstance(categorias, list) and len(categorias) > 0:
        return str(categorias[0]).strip().lower()
    if isinstance(categorias, str) and categorias.strip():
        return categorias.strip().lower()
    return "__sin_categoria__"
 
 
def _imputar_por_categoria(df: pd.DataFrame, nutri_cols: List[str]) -> pd.DataFrame:
    df["_cat_principal"] = df["categorias"].apply(_categoria_principal)
 
    medias_globales = df[nutri_cols].mean()
 
    for col in nutri_cols:
        # Media por categoría calculada solo sobre valores no-NaN
        media_cat = df.groupby("_cat_principal")[col].transform("mean")

        media_global = medias_globales[col]
        if pd.isna(media_global):
            media_global = 0.0
 
        nan_antes = df[col].isna().sum()
        if nan_antes == 0:
            continue 
 
        df[col] = (
            df[col]
            .fillna(media_cat)
            .fillna(media_global)
            .fillna(0.0)
        )
 
        nan_despues = df[col].isna().sum()
        print(
            f"[Preprocessing] Imputados {nan_antes - nan_despues} NaN en "
            f"'{col}' con media de categoría ({nan_despues} restantes → media global)"
        )
 
    df.drop(columns=["_cat_principal"], inplace=True)
    return df
 
 
# Función principal
def limpiar_datos(datos: Optional[List[Dict]] = None) -> pd.DataFrame:
    # 1. Cargar
    if datos is None:
        if not os.path.exists(RAW_PATH):
            raise FileNotFoundError(
                f"No se encontró {RAW_PATH}. Ejecuta primero acquisition.obtener_datos()"
            )
        with open(RAW_PATH, encoding="utf-8") as f:
            datos = json.load(f)
    if not datos:
        raise ValueError("La lista de datos está vacía.")
    df = pd.DataFrame(datos)
    print(f"[Preprocessing] Productos crudos cargados: {len(df)}")
    
    # 2. Limpieza
    df.drop_duplicates(subset=["url"], inplace=True)

    df["titulo"] = df["titulo"].astype(str).str.strip()
    df = df[df["titulo"].str.len() > 0]

    df["precio"] = pd.to_numeric(df.get("precio_total", pd.Series(dtype=float)), errors="coerce")
    df = df[df["precio"].notna() & (df["precio"] > 0)]
    print(f"[Preprocessing] Tras limpieza básica: {len(df)} productos")

    col = "valores_nutricionales_100_g"
    if col not in df.columns:
        df[col] = [{} for _ in range(len(df))]

    df[col] = df[col].apply(lambda x: x if isinstance(x, dict) else {})
    df["proteinas"]     = df[col].apply(lambda d: _buscar(d, CLAVES_PROTEINAS))
    df["carbohidratos"] = df[col].apply(lambda d: _buscar(d, CLAVES_CARBOS))
    df["grasas"]        = df[col].apply(lambda d: _buscar(d, CLAVES_GRASAS))
    df["fibra"]         = df[col].apply(lambda d: _buscar(d, CLAVES_FIBRA))
    df["calorias"]      = df[col].apply(lambda d: _buscar(d, CLAVES_CALORIAS))
    df["sal"]           = df[col].apply(lambda d: _buscar(d, CLAVES_SAL))
    df["azucares"]      = df[col].apply(lambda d: _buscar(d, CLAVES_AZUCAR))
    df["grasas_sat"]    = df[col].apply(lambda d: _buscar(d, CLAVES_SATURADAS))

    nutri_cols = ["proteinas", "carbohidratos", "grasas", "fibra",
                  "calorias", "sal", "azucares", "grasas_sat"]
    df = _imputar_por_categoria(df, nutri_cols) 

    df["score_nutricional"] = df.apply(_score_nutricional, axis=1)

    df["norm_precio"] = _norm_inverso(df["precio"])
    
    df["norm_nutri"]  = _norm_directo(df["score_nutricional"])

    df["texto_busqueda"] = df.apply(_texto_busqueda, axis=1)
    columnas = [
        "titulo", "url", "precio", "categorias",
        "proteinas", "carbohidratos", "grasas", "fibra",
        "calorias", "sal", "azucares", "grasas_sat",
        "score_nutricional",
        "norm_precio", "norm_nutri",
        "texto_busqueda",
    ]
    columnas_presentes = [c for c in columnas if c in df.columns]
    df = df[columnas_presentes].reset_index(drop=True)

    os.makedirs(os.path.dirname(CLEAN_PATH), exist_ok=True)
    df.to_csv(CLEAN_PATH, index=False, encoding="utf-8")
    print(f"[Preprocessing] Guardado en: {CLEAN_PATH}")
    print(f"[Preprocessing] Productos finales: {len(df)}")
    print(f"[Preprocessing] Columnas: {list(df.columns)}")
    return df

if __name__ == "__main__":
    df = limpiar_datos()
    print("\nMuestra:")
    print(df[["titulo", "precio", "proteinas", "calorias",
              "score_nutricional", "norm_precio", "norm_nutri"]].head(10).to_string())