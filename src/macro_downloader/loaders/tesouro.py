import pandas as pd
from typing import List
from .utils import read_smart

def load_tesouro(paths: List[str]) -> pd.DataFrame | None:
    if not paths: return None
    df = read_smart(paths[0])
    if df is None: return None
    if "Data Venda" in df.columns:
        df["Data Venda"] = pd.to_datetime(df["Data Venda"], format="%d/%m/%Y", errors="coerce")
    if "Data Vencimento" in df.columns:
        df["Data Vencimento"] = pd.to_datetime(df["Data Vencimento"], format="%d/%m/%Y", errors="coerce")
    for col in ["Taxa Compra Manha", "PU Compra Manha", "PU Venda Manha"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors="coerce")
    return df