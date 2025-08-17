import os
import re
import csv
import pandas as pd
from typing import List

# --- Funções de busca de caminhos ---
def find_project_root(marker: str = '.git') -> str:
    """Localiza a raiz do projeto subindo na árvore de diretórios."""
    # Começa 3 níveis acima, pois este arquivo está em src/macro_downloader/loaders
    current_path = os.path.abspath(os.path.join(__file__, "..", "..", ".."))
    while current_path != os.path.dirname(current_path):
        if os.path.isdir(os.path.join(current_path, marker)):
            return current_path
        current_path = os.path.dirname(current_path)
    raise FileNotFoundError(f"Raiz do projeto ('{marker}') não encontrada.")

PROJECT_ROOT = find_project_root()
DIR_MACRO = os.path.join(PROJECT_ROOT, "data_raw", "dados_macro")
DIR_MACRO_ADICIONAIS = os.path.join(PROJECT_ROOT, "data_raw", "dados_macro_adicionais")

# --- Funções de leitura e tratamento de dados ---
def sanitize_number_pt(series: pd.Series) -> pd.Series:
    """Converte uma série textual (com vírgula decimal) para numérica."""
    if not isinstance(series, pd.Series): return series
    s = series.astype(str).str.strip().replace({"": None, "-": None, "–": None, "—": None, "...": None})
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def read_smart(path: str, **kwargs) -> pd.DataFrame | None:
    """Lê um arquivo tabular (CSV, TXT, XLSX, ODS) de forma inteligente."""
    if not os.path.exists(path): return None
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in [".csv", ".txt"]:
            enc = kwargs.get("encoding", "latin-1" if "tse" in path else "utf-8")
            with open(path, 'r', encoding=enc, errors='ignore') as f:
                sample = f.read(4096)
                try:
                    sep = csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
                except csv.Error:
                    sep = kwargs.get("sep", ";")
            return pd.read_csv(path, sep=sep, encoding=enc, on_bad_lines="warn", dtype=object)
        elif ext in [".xlsx", ".xls", ".ods"]:
            return pd.read_excel(path, engine="odf" if ext == ".ods" else None, dtype=object)
    except Exception:
        return None
    return None