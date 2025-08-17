import pandas as pd
from typing import List
from .utils import read_smart, sanitize_number_pt

def load_inmet(paths: List[str]) -> pd.DataFrame | None:
    if not paths: return None
    df = read_smart(paths[0], sep=';')
    if df is None: return None
    if "DATA (YYYY-MM-DD)" in df.columns and "HORA (UTC)" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["DATA (YYYY-MM-DD)"] + " " + df["HORA (UTC)"].str.slice(0, 5), errors="coerce")
    for col in df.columns:
        if df[col].dtype == 'object' and df[col].str.contains(r"^\d+,\d+", na=False).any():
            df[col] = sanitize_number_pt(df[col])
    return df