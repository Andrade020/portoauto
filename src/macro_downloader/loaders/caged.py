import pandas as pd
from typing import List
from .utils import read_smart, sanitize_number_pt

def load_caged(paths: List[str]) -> pd.DataFrame | None:
    """Carrega o arquivo de dados mais recente do CAGED."""
    if not paths: return None
    df = read_smart(paths[0], encoding="latin-1")
    if df is None: return None

    if "competênciamov" in df.columns:
        df["competencia_dt"] = pd.to_datetime(df["competênciamov"], format="%Y%m", errors="coerce")
    if "salário" in df.columns:
        df["salario_numeric"] = sanitize_number_pt(df["salário"])

    return df