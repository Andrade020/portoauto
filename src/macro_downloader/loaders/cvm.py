import pandas as pd
from typing import List
from .utils import read_smart, sanitize_number_pt

def load_cvm(paths: List[str]) -> pd.DataFrame | None:
    """Carrega arquivos CSV da CVM para um dado período."""
    if not paths: return None
    all_dfs = [df for path in paths if (df := read_smart(path, encoding="latin-1", sep=';')) is not None]
    if not all_dfs: return None
    
    full_df = pd.concat(all_dfs, ignore_index=True)

    if "DT_COMPTC" in full_df.columns:
        full_df["DT_COMPTC"] = pd.to_datetime(full_df["DT_COMPTC"], errors="coerce")
    for col in full_df.columns:
        if col.startswith("VL_") or col.startswith("VR_"):
            full_df[col] = sanitize_number_pt(full_df[col])
            
    return full_df