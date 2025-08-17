import pandas as pd
from typing import List
from .utils import read_smart

def sample_tse(paths: List[str]) -> pd.DataFrame | None:
    """Carrega uma amostra do maior arquivo do TSE."""
    if not paths: return None
    return read_smart(paths[0])