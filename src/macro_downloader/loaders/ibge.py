import pandas as pd
from typing import List
from .utils import read_smart, sanitize_number_pt

def load_pnad(paths: List[str]) -> pd.DataFrame | None:
    if not paths: return None
    df = read_smart(paths[0], sep=",")
    if df is None: return None
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    if "Taxa_Desocupacao" in df.columns:
        df["Taxa_Desocupacao"] = pd.to_numeric(df["Taxa_Desocupacao"], errors="coerce")
    return df

def load_pesquisas_mensais(paths: List[str]) -> pd.DataFrame | None:
    if not paths: return None
    df = read_smart(paths[0], sep=",")
    if df is None: return None
    if "periodo" in df.columns:
        df["periodo_dt"] = pd.to_datetime(df["periodo"], format="%Y%m", errors="coerce")
    if "valor" in df.columns:
        df["valor"] = sanitize_number_pt(df["valor"])
    return df

def sample_pib_municipios(paths: List[str]) -> pd.DataFrame | None:
    """Carrega só o arquivo de PIB mais recente."""
    if not paths: return None
    return read_smart(paths[0])