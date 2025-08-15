# -*- coding: utf-8 -*-
"""
explorar_dados_adicionais.py
----------------------------
Mostra cabeçalhos e amostras dos arquivos baixados por 'novos_dados.py',
para facilitar a inspeção inicial e definir os parsers definitivos.

Exibe:
- IBGE (PMC/PMS/PIM): colunas, amostra, tentativa de conversão de 'valor' p/ numérico e intervalo de 'periodo'.
- IBGE (PIB Municípios): lista um arquivo grande (xlsx/ods/csv) e mostra cabeçalho/abas/amostra.
- CAGED: detecta delimitador, mostra colunas e amostra.
- TSE: escolhe um CSV grande, mostra colunas e amostra.
- INMET: para cada 'estacao_*.csv', mostra colunas, amostra e intervalo de datas se disponível.

Uso:
  python explorar_dados_adicionais.py --rows 8
"""

import os
import re
import argparse
from glob import glob
from datetime import datetime

import pandas as pd

# --- Localização do diretório base (mesma convenção do validador) ---
BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "dados_adicionais_fidc")
BASE_DIR = os.path.abspath(BASE_DIR)

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

def try_read_csv(path, sep=None, enc=None, nrows=10, **kwargs):
    """
    Leitura robusta para uma amostra.
    """
    candidates = []
    det_sep, det_enc = detect_csv_delimiter(path)
    if det_sep or det_enc:
        candidates.append((det_sep or ";", det_enc or "utf-8"))
    if sep and enc:
        candidates.append((sep, enc))
    candidates += [
        (";", "utf-8"),
        (",", "utf-8"),
        (";", "latin-1"),
        (",", "latin-1"),
        ("|", "utf-8"),
        ("\t", "utf-8"),
    ]
    last_err = None
    for s, e in candidates:
        try:
            df = pd.read_csv(path, sep=s, encoding=e, on_bad_lines="skip",
                             nrows=nrows, dtype="object", **kwargs)
            return df, s, e, None
        except Exception as ex:
            last_err = str(ex)
            continue
    return None, None, None, last_err

def summarize_df(df, rows=5):
    if df is None or df.empty:
        print("   (sem linhas na amostra)")
        return
    print(f"   Linhas (amostra): {len(df)} | Colunas: {len(df.columns)}")
    print("   Colunas:", list(df.columns))
    print("   Amostra:")
    print(df.head(rows))

# ---------------- IBGE (PMC/PMS/PIM) ----------------

def explorar_ibge_pesquisas(rows):
    base = os.path.join(BASE_DIR, "ibge", "pesquisas_mensais")
    arquivos = [
        ("PMC - volume", os.path.join(base, "pmc_volume_vendas.csv")),
        ("PMS - receita/volume", os.path.join(base, "pms_receita_servicos.csv")),
        ("PIM - produção", os.path.join(base, "pim_producao_industrial.csv")),
    ]
    print_section("IBGE - Pesquisas Mensais (API v3)")
    for rotulo, path in arquivos:
        print_sub(f"{rotulo}: {os.path.basename(path)}")
        if not os.path.isfile(path):
            print("   (arquivo não encontrado)")
            continue
        df, sep, enc, err = try_read_csv(path, nrows=max(rows, 50))
        if df is None:
            print(f"   ERRO lendo CSV: {err}")
            continue
        print(f"   Delimitador: {sep} | Encoding: {enc}")
        summarize_df(df, rows=rows)

        # Tentativa de conversão de 'valor' e análise de 'periodo'
        if "valor" in df.columns:
            coerced = pd.to_numeric(df["valor"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
            ok = coerced.notna().sum()
            print(f"   'valor' numérico (na amostra): {ok}/{len(coerced)}")
        if "periodo" in df.columns:
            per = df["periodo"].dropna().astype(str)
            # heurística para YYYYMM/ YYYY
            def _to_date(s):
                s = s.strip()
                if re.fullmatch(r"\d{6}", s):
                    return datetime.strptime(s, "%Y%m")
                if re.fullmatch(r"\d{4}", s):
                    return datetime.strptime(s, "%Y")
                # casos tipo "2024-05" (raro aqui)
                try:
                    return datetime.fromisoformat(s + "-01")
                except Exception:
                    return None
            dd = per.map(_to_date).dropna()
            if not dd.empty:
                print(f"   Intervalo de periodo (amostra): {dd.min().date()} → {dd.max().date()}")

# ---------------- IBGE (PIB Municípios) ----------------

def explorar_ibge_pib(rows):
    print_section("IBGE - PIB dos Municípios")
    base = os.path.join(BASE_DIR, "ibge", "contas_regionais")
    if not os.path.isdir(base):
        print("   (pasta não encontrada)")
        return
    # prioriza planilha grande
    candidatos = []
    for pat in ("*.xlsx","*.ods","*.csv","*.txt"):
        candidatos += glob(os.path.join(base, pat))
    if not candidatos:
        print("   (nenhum arquivo tabular encontrado)")
        return
    candidatos.sort(key=lambda p: os.path.getsize(p), reverse=True)
    alvo = candidatos[0]
    print_sub(f"Arquivo exemplo: {os.path.basename(alvo)}  ({os.path.getsize(alvo):,} bytes)")

    ext = os.path.splitext(alvo)[1].lower()
    try:
        if ext in [".xlsx", ".xls", ".ods"]:
            # lista abas
            try:
                xls = pd.ExcelFile(alvo, engine="odf" if ext==".ods" else None)
                print(f"   Abas: {xls.sheet_names[:10]}")
                df = xls.parse(xls.sheet_names[0], nrows=max(rows, 20), dtype="object")
            except Exception as e:
                print(f"   Falha ao listar/abrir como planilha ({e}). Tentando como CSV...")
                df, _, _, err = try_read_csv(alvo, nrows=max(rows, 20))
                if df is None:
                    print(f"   ERRO lendo como CSV: {err}")
                    return
        else:
            df, sep, enc, err = try_read_csv(alvo, nrows=max(rows, 20))
            if df is None:
                print(f"   ERRO lendo CSV/TXT: {err}")
                return
            print(f"   Delimitador: {sep} | Encoding: {enc}")
        summarize_df(df, rows=rows)
    except Exception as e:
        print(f"   ERRO: {e}")

# ---------------- CAGED ----------------

def explorar_caged(rows):
    print_section("Trabalho - Novo CAGED")
    base = os.path.join(BASE_DIR, "trabalho", "caged")
    if not os.path.isdir(base):
        print("   (pasta não encontrada)")
        return
    files = sorted(glob(os.path.join(base, "CAGEDMOV*.txt")))
    if not files:
        print("   (CAGEDMOV*.txt não encontrado)")
        return
    files.sort(reverse=True)
    alvo = files[0]
    print_sub(f"Arquivo: {os.path.basename(alvo)}  ({os.path.getsize(alvo):,} bytes)")
    df, sep, enc, err = try_read_csv(alvo, nrows=max(rows, 50), low_memory=False)
    if df is None:
        print(f"   ERRO lendo TXT: {err}")
        return
    print(f"   Delimitador: {sep} | Encoding: {enc}")
    summarize_df(df, rows=rows)

# ---------------- TSE ----------------

def explorar_tse(rows):
    print_section("TSE - Perfil do Eleitorado")
    base = os.path.join(BASE_DIR, "tse", "eleitorado")
    if not os.path.isdir(base):
        print("   (pasta não encontrada)")
        return
    csvs = sorted(glob(os.path.join(base, "*.csv")))
    if not csvs:
        print("   (nenhum CSV encontrado)")
        return
    csvs.sort(key=lambda p: os.path.getsize(p), reverse=True)
    alvo = csvs[0]
    print_sub(f"Arquivo: {os.path.basename(alvo)}  ({os.path.getsize(alvo):,} bytes)")
    df, sep, enc, err = try_read_csv(alvo, nrows=max(rows, 50))
    if df is None:
        print(f"   ERRO lendo CSV: {err}")
        return
    print(f"   Delimitador: {sep} | Encoding: {enc}")
    summarize_df(df, rows=rows)

# ---------------- INMET ----------------

def _parse_date_pt(s):
    # tenta 'DATA (YYYY-MM-DD)' já vem nesse formato; fallback para ISO
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None

def explorar_inmet(rows):
    print_section("INMET - Estações (Dados Históricos)")
    base = os.path.join(BASE_DIR, "inmet", "clima")
    if not os.path.isdir(base):
        print("   (pasta não encontrada)")
        return
    arquivos = sorted(glob(os.path.join(base, "estacao_*.csv")))
    if not arquivos:
        print("   (nenhum 'estacao_*.csv' encontrado)")
        return
    for p in arquivos:
        print_sub(os.path.basename(p))
        df, sep, enc, err = try_read_csv(p, sep=";", nrows=max(rows, 50))
        if df is None:
            print(f"   ERRO lendo CSV: {err}")
            continue
        print(f"   Delimitador: {sep} | Encoding: {enc}")
        summarize_df(df, rows=rows)

        # intervalo de datas, se houver
        if "DATA (YYYY-MM-DD)" in df.columns:
            dd = df["DATA (YYYY-MM-DD)"].dropna().astype(str).map(_parse_date_pt)
            dd = dd.dropna()
            if not dd.empty:
                print(f"   Intervalo de DATA (amostra): {dd.min().date()} → {dd.max().date()}")

# ---------------- main ----------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=8, help="linhas para exibir em cada amostra (default=8)")
    args = parser.parse_args()

    print_section("EXPLORAÇÃO INICIAL - DADOS ADICIONAIS (FIDC)")
    print(f"Base: {BASE_DIR}")

    explorar_ibge_pesquisas(args.rows)
    explorar_ibge_pib(args.rows)
    explorar_caged(args.rows)
    explorar_tse(args.rows)
    explorar_inmet(args.rows)

if __name__ == "__main__":
    main()
