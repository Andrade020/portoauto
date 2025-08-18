import pandas as pd
from typing import List
from .utils import read_smart

def load_bcb(paths: List[str]) -> pd.DataFrame | None:
    """carrega dado mais novo do bcb"""
    if not paths: return None
    df = read_smart(paths[0], sep=",")
    if df is None: return None
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    if len(df.columns) > 1:
        value_col = df.columns[1]
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    return df