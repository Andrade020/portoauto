# -*- coding: utf-8 -*-
"""
carregar_dados_adicionais.py
----------------------------
Lê e padroniza os dados baixados por 'novos_dados.py', imprimindo cabeçalhos,
tipos propostos, amostras, e (opcionalmente) salvando arquivos Parquet "bronze".

Fontes tratadas:
- IBGE (PMC/PMS/PIM): CSV (API v3, view=flat) -> normaliza `valor` (float) e `periodo` (datetime mensal).
- IBGE (PIB Municípios): XLSX/ODS/CSV -> amostra, sem padronização pesada neste passo.
- CAGED: TXT ';' -> tenta normalizar campos monetários (ex.: 'salário').
- TSE: CSV ';' latin-1 -> amostra, sem padronização pesada neste passo.
- INMET: CSV ';' -> combina DATA + HORA para timestamp, tenta converter colunas numéricas com vírgula.

Uso:
    python carregar_dados_adicionais.py --rows 8 --save-parquet
"""

import os
import re
import argparse
from glob import glob
from datetime import datetime
import pandas as pd

# ----------------- CONFIG -----------------
HERE = os.path.dirname(__file__)
BASE_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "dados_adicionais_fidc"))
BRONZE_DIR = os.path.join(BASE_DIR, "_bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)

# -----------------------------------------
# Utils
# -----------------------------------------
def print_section(title):
    print("\n" + "="*len(title))
    print(title)
    print("="*len(title))

def print_sub(title):
    print("\n-- " + title)

def detect_csv_delimiter(path, max_bytes=4096):
    import csv
    try:
        with open(path, "rb") as f:
            sample = f.read(max_bytes)
        try:
            sample_str = sample.decode("utf-8")
            enc = "utf-8"
        except UnicodeDecodeError:
            sample_str = sample.decode("latin-1", errors="ignore")
            enc = "latin-1"
        dialect = csv.Sniffer().sniff(sample_str, delimiters=";,|\t")
        return dialect.delimiter, enc
    except Exception:
        return None, None

def read_csv_smart(path, nrows=None, sep=None, enc=None, dtype="object"):
    # tenta detectar
    if sep is None or enc is None:
        dsep, denc = detect_csv_delimiter(path)
        sep = sep or dsep or ";"
        enc = enc or denc or "utf-8"
    return pd.read_csv(path, sep=sep, encoding=enc, on_bad_lines="skip",
                       dtype=dtype, nrows=nrows)

def sanitize_number_pt(series):
    """Converte série string com vírgula decimal para float; trata -, …, NaN."""
    if series is None:
        return series
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "-": None, "–": None, "—": None, "...": None})
    # remove separador de milhar (ponto) quando vier com vírgula decimal
    s = s.str.replace(".", "", regex=False)
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

# -----------------------------------------
# IBGE (PMC/PMS/PIM) - normalização
# -----------------------------------------
PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4, "maio": 5, "junho": 6,
    "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
}
def parse_periodo_pt(s):
    if not isinstance(s, str):
        return None
    s = s.strip().lower()
    if s == "mês":
        return None
    # "janeiro 2000"
    m = re.match(r"^([a-zç]+)\s+(\d{4})$", s)
    if m:
        mm = PT_MONTHS.get(m.group(1), None)
        yy = int(m.group(2))
        if mm:
            return datetime(yy, mm, 1)
    # "2000-05"
    try:
        return datetime.fromisoformat(s + "-01")
    except Exception:
        pass
    # "2000"
    if re.fullmatch(r"\d{4}", s):
        return datetime(int(s), 1, 1)
    return None

def load_ibge_flat(path, preview_rows=8):
    df = read_csv_smart(path, nrows=None, sep=",", enc="utf-8")
    # remove linha-rótulo (periodo == "Mês") e vazios
    df = df[df["periodo"].astype(str).str.lower() != "mês"].copy()
    df["periodo_dt"] = df["periodo"].map(parse_periodo_pt)
    df["valor_float"] = sanitize_number_pt(df["valor"])
    # ordena por período se possível
    if df["periodo_dt"].notna().any():
        df = df.sort_values("periodo_dt")
    # impressão
    print("   Colunas:", list(df.columns))
    print("   Amostra normalizada:")
    cols_show = [c for c in ["localidade", "periodo", "periodo_dt", "valor", "valor_float"] if c in df.columns]
    print(df[cols_show].head(preview_rows))
    if df["periodo_dt"].notna().any():
        print(f"   Intervalo de periodo: {df['periodo_dt'].min().date()} → {df['periodo_dt'].max().date()}")
    return df

# -----------------------------------------
# IBGE - PIB Municípios (somente amostra neste passo)
# -----------------------------------------
def load_pib_municipios(base_dir, preview_rows=8):
    base = os.path.join(base_dir, "ibge", "contas_regionais")
    pats = ["*.xlsx","*.ods","*.csv","*.txt"]
    files = []
    for p in pats:
        files.extend(glob(os.path.join(base, p)))
    if not files:
        print("   (nenhum arquivo tabular encontrado)")
        return None
    files.sort(key=lambda p: os.path.getsize(p), reverse=True)
    path = files[0]
    ext = os.path.splitext(path)[1].lower()
    print_sub(f"Arquivo: {os.path.basename(path)}")
    try:
        if ext in [".xlsx", ".xls", ".ods"]:
            engine = "odf" if ext == ".ods" else None
            xls = pd.ExcelFile(path, engine=engine)
            print("   Abas:", xls.sheet_names[:10])
            df = xls.parse(xls.sheet_names[0], nrows=max(preview_rows, 20), dtype="object")
        else:
            df = read_csv_smart(path, nrows=max(preview_rows, 20))
        print("   Colunas:", list(df.columns))
        print("   Amostra:")
        print(df.head(preview_rows))
        return None  # não padronizamos agora
    except Exception as e:
        print(f"   ERRO ao ler amostra: {e}")
        return None

# -----------------------------------------
# CAGED - normalização básica
# -----------------------------------------
def load_caged(base_dir, preview_rows=8):
    base = os.path.join(base_dir, "trabalho", "caged")
    files = sorted(glob(os.path.join(base, "CAGEDMOV*.txt")), reverse=True)
    if not files:
        print("   (CAGEDMOV*.txt não encontrado)")
        return None
    path = files[0]
    print_sub(f"Arquivo: {os.path.basename(path)}")
    df = read_csv_smart(path, nrows=None, sep=";", enc=None)
    # normaliza salário (se houver)
    for col in ["salário", "valorsaláriofixo"]:
        if col in df.columns:
            df[col + "_float"] = sanitize_number_pt(df[col])
    # coerções úteis (amostra)
    for col in ["competênciamov", "competênciadec"]:
        if col in df.columns:
            # YYYYMM -> primeiro dia do mês
            def _to_date(s):
                s = str(s).strip()
                if re.fullmatch(r"\d{6}", s):
                    return datetime.strptime(s, "%Y%m")
                return None
            df[col + "_dt"] = df[col].map(_to_date)
    print("   Colunas:", list(df.columns)[:50])
    print("   Amostra:")
    print(df.head(preview_rows))
    return df

# -----------------------------------------
# TSE - amostra
# -----------------------------------------
def load_tse(base_dir, preview_rows=8):
    base = os.path.join(base_dir, "tse", "eleitorado")
    csvs = sorted(glob(os.path.join(base, "*.csv")), key=lambda p: os.path.getsize(p), reverse=True)
    if not csvs:
        print("   (nenhum CSV encontrado)")
        return None
    path = csvs[0]
    print_sub(f"Arquivo: {os.path.basename(path)}")
    df = read_csv_smart(path, nrows=max(preview_rows, 2000), sep=";", enc="latin-1")
    print("   Colunas:", list(df.columns))
    print("   Amostra:")
    print(df.head(preview_rows))
    return None  # sem padronizar agora

# -----------------------------------------
# INMET - normalização básica
# -----------------------------------------
def load_inmet(base_dir, preview_rows=8):
    base = os.path.join(base_dir, "inmet", "clima")
    arquivos = sorted(glob(os.path.join(base, "estacao_*.csv")))
    if not arquivos:
        print("   (nenhum 'estacao_*.csv' encontrado)")
        return []
    out = []
    for path in arquivos:
        print_sub(os.path.basename(path))
        df = read_csv_smart(path, nrows=None, sep=";", enc=None)
        # timestamp se houver DATA/HORA
        if "DATA (YYYY-MM-DD)" in df.columns and "HORA (UTC)" in df.columns:
            def _parse_dt(row):
                d = str(row["DATA (YYYY-MM-DD)"]).strip()
                h = str(row["HORA (UTC)"]).strip()
                try:
                    return datetime.strptime(d + " " + h, "%Y-%m-%d %H")
                except Exception:
                    # alguns arquivos trazem HH:MM
                    try:
                        return datetime.strptime(d + " " + h, "%Y-%m-%d %H:%M")
                    except Exception:
                        return None
            df["timestamp_utc"] = df[["DATA (YYYY-MM-DD)", "HORA (UTC)"]].apply(_parse_dt, axis=1)

        # tenta converter colunas numéricas (vírgula -> ponto)
        for col in df.columns:
            if col == "timestamp_utc":
                continue
            if df[col].astype(str).str.contains(r"\d,?\d", regex=True).mean() > 0.5:
                # heurística fraca: muitos valores com dígitos -> tentar converter
                df[col + "_num"] = sanitize_number_pt(df[col])

        print("   Colunas:", list(df.columns)[:50])
        print("   Amostra:")
        print(df.head(preview_rows))
        if "timestamp_utc" in df.columns and df["timestamp_utc"].notna().any():
            print(f"   Intervalo (timestamp_utc): {df['timestamp_utc'].min()} → {df['timestamp_utc'].max()}")
        out.append((path, df))
    return out

# -----------------------------------------
# Main
# -----------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=8, help="linhas nas amostras impressas")
    ap.add_argument("--save-parquet", action="store_true", help="salva Parquet bronze em _bronze/")
    args = ap.parse_args()

    print_section("CARREGAMENTO & PADRONIZAÇÃO - DADOS ADICIONAIS (FIDC)")
    print(f"Base: {BASE_DIR}")

    # IBGE (PMC/PMS/PIM)
    ibge_dir = os.path.join(BASE_DIR, "ibge", "pesquisas_mensais")
    ibge_files = [
        ("ibge_pmc", os.path.join(ibge_dir, "pmc_volume_vendas.csv")),
        ("ibge_pms", os.path.join(ibge_dir, "pms_receita_servicos.csv")),
        ("ibge_pim", os.path.join(ibge_dir, "pim_producao_industrial.csv")),
    ]
    for name, path in ibge_files:
        print_section(f"IBGE - {name.upper()}")
        if not os.path.isfile(path):
            print("   (arquivo não encontrado)")
            continue
        df = load_ibge_flat(path, preview_rows=args.rows)
        if args.save_parquet and df is not None and not df.empty:
            outp = os.path.join(BRONZE_DIR, f"{name}.parquet")
            df.to_parquet(outp, index=False)
            print(f"   [SALVO] {outp}")

    # IBGE (PIB Municípios) - amostra
    print_section("IBGE - PIB MUNICÍPIOS (amostra)")
    load_pib_municipios(BASE_DIR, preview_rows=args.rows)

    # CAGED
    print_section("CAGED - MOVIMENTAÇÕES")
    df_caged = load_caged(BASE_DIR, preview_rows=args.rows)
    if args.save_parquet and isinstance(df_caged, pd.DataFrame) and not df_caged.empty:
        outp = os.path.join(BRONZE_DIR, "caged_mov.parquet")
        # cuidado com 428MB — salve tudo apenas se quiser; senão poderíamos salvar só colunas úteis:
        df_caged.to_parquet(outp, index=False)
        print(f"   [SALVO] {outp}")

    # TSE
    print_section("TSE - PERFIL DO ELEITORADO (amostra)")
    load_tse(BASE_DIR, preview_rows=args.rows)

    # INMET
    print_section("INMET - ESTAÇÕES")
    inmet_list = load_inmet(BASE_DIR, preview_rows=args.rows)
    if args.save_parquet:
        for path, df in inmet_list:
            if isinstance(df, pd.DataFrame) and not df.empty:
                cod = os.path.splitext(os.path.basename(path))[0].split("_", 1)[-1]
                outp = os.path.join(BRONZE_DIR, f"inmet_{cod}.parquet")
                df.to_parquet(outp, index=False)
                print(f"   [SALVO] {outp}")

    print_section("FIM")

if __name__ == "__main__":
    main()
