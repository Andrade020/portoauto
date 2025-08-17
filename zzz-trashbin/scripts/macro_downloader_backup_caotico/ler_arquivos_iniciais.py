
#%%
# -*- coding: utf-8 -*-
r"""
amostrar_arquivos.py
--------------------
Lê os arquivos baixados e imprime cabeçalho + amostra (sem exportar).
- CSV/TXT: tenta detectar delimitador/encoding; lê poucas linhas.
- XLSX/ODS: lista abas e lê a primeira.
- ZIP: lista conteúdo; tenta amostrar um CSV interno.
- PDF/Outros: informa tipo/ignora.

Uso:
  python amostrar_arquivos.py --rows 8 --only ibge,tse
"""

import os
import io
import csv
import zipfile
import argparse
from glob import glob
from datetime import datetime

import pandas as pd

HERE = os.path.dirname(__file__)
BASE_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "dados_adicionais_fidc"))

# ----------------- utilidades -----------------

def print_section(title: str):
    print("\n" + "="*len(title))
    print(title)
    print("="*len(title))

def print_sub(title: str):
    print("\n-- " + title)

def bytes_to_human(n: int) -> str:
    units = ["B","KB","MB","GB","TB"]
    i = 0
    x = float(n)
    while x >= 1024 and i < len(units)-1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"

def sniff_csv(path, max_bytes=65536):
    """Detecta delimitador e encoding (heurística simples)."""
    try:
        with open(path, "rb") as f:
            raw = f.read(max_bytes)
        try:
            text = raw.decode("utf-8")
            enc = "utf-8"
        except UnicodeDecodeError:
            text = raw.decode("latin-1", errors="ignore")
            enc = "latin-1"
        try:
            dialect = csv.Sniffer().sniff(text, delimiters=";,|\t,")
            sep = dialect.delimiter
        except Exception:
            # heurística: conta separadores
            counts = {d: text.count(d) for d in [";", ",", "|", "\t"]}
            sep = max(counts, key=counts.get)
        return sep, enc
    except Exception:
        return None, None

def read_csv_sample(path, nrows=10, sep=None, enc=None, **kwargs):
    """Leitura robusta de uma amostra CSV/TXT."""
    candidates = []
    dsep, denc = sniff_csv(path)
    if dsep or denc:
        candidates.append((dsep or ";", denc or "utf-8"))
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
                             nrows=nrows, dtype="object", low_memory=True, **kwargs)
            return df, s, e, None
        except Exception as ex:
            last_err = str(ex)
            continue
    return None, None, None, last_err

def summarize_df(df: pd.DataFrame, rows=8):
    if df is None or df.empty:
        print("   (sem linhas na amostra)")
        return
    print(f"   Linhas (amostra): {len(df)} | Colunas: {len(df.columns)}")
    print("   Colunas:", list(df.columns))
    print("   Amostra:")
    print(df.head(rows))

# ----------------- leitores por tipo -----------------

def handle_csv_or_txt(path, rows):
    df, sep, enc, err = read_csv_sample(path, nrows=max(rows, 50))
    if df is None:
        print(f"   ERRO lendo CSV/TXT: {err}")
        return
    print(f"   Delimitador: {sep} | Encoding: {enc}")
    summarize_df(df, rows=rows)

def handle_excel(path, rows):
    ext = os.path.splitext(path)[1].lower()
    engine = "odf" if ext == ".ods" else None
    try:
        xls = pd.ExcelFile(path, engine=engine)
        sheets = xls.sheet_names
        print(f"   Abas: {sheets[:10]}")
        df = xls.parse(sheets[0], nrows=max(rows, 20), dtype="object")
        summarize_df(df, rows=rows)
    except Exception as e:
        print(f"   ERRO abrindo planilha: {e}")

def handle_zip(path, rows):
    print("   Conteúdo do ZIP:")
    try:
        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
            for n in names[:20]:
                print(f"     - {n}")
            # tenta amostrar o maior CSV interno
            csv_candidates = [n for n in names if n.lower().endswith((".csv",".txt",".tsv"))]
            if csv_candidates:
                # escolhe o maior (se info disponível), senão o primeiro
                try:
                    infos = {n: zf.getinfo(n).file_size for n in csv_candidates}
                    target = sorted(infos, key=infos.get, reverse=True)[0]
                except Exception:
                    target = csv_candidates[0]
                print(f"\n   Amostrando interno: {target}")
                with zf.open(target, "r") as fh:
                    # tenta ler com pandas diretamente do buffer
                    # detecta encoding lendo um pedaço
                    head_bytes = fh.read(65536)
                    fh.seek(0)
                    try:
                        text = head_bytes.decode("utf-8")
                        enc = "utf-8"
                    except UnicodeDecodeError:
                        text = head_bytes.decode("latin-1", errors="ignore")
                        enc = "latin-1"
                    # delimitador
                    try:
                        sep = csv.Sniffer().sniff(text, delimiters=";,|\t,").delimiter
                    except Exception:
                        counts = {d: text.count(d) for d in [";", ",", "|", "\t"]}
                        sep = max(counts, key=counts.get)
                    # carrega
                    wrapper = io.TextIOWrapper(zf.open(target, "r"), encoding=enc, errors="ignore")
                    df = pd.read_csv(wrapper, sep=sep, on_bad_lines="skip",
                                     nrows=max(rows, 50), dtype="object", low_memory=True)
                    print(f"   Delimitador: {sep} | Encoding: {enc}")
                    summarize_df(df, rows=rows)
            else:
                print("   (nenhum CSV/TXT dentro do zip para amostrar)")
    except Exception as e:
        print(f"   ERRO lendo ZIP: {e}")

# ----------------- varredura -----------------

def iter_all_files(base_dir, roots_filter=None):
    """
    Gera (root, fullpath, relpath, size).
    roots_filter: conjunto {'ibge','tse',...} para filtrar a primeira pasta.
    """
    for root, _, files in os.walk(base_dir):
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, base_dir)
            top = rel.split(os.sep, 1)[0]
            if roots_filter and top.lower() not in roots_filter:
                continue
            try:
                size = os.path.getsize(full)
            except OSError:
                size = None
            yield top, full, rel, size

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=BASE_DIR, help="diretório base (default: ../../dados_adicionais_fidc)")
    ap.add_argument("--rows", type=int, default=8, help="linhas a exibir por amostra (default=8)")
    ap.add_argument("--only", default="", help="filtrar por pastas raiz (ex.: ibge,tse,inmet,trabalho)")
    args = ap.parse_args()

    base = os.path.abspath(args.base)
    roots_filter = set(x.strip().lower() for x in args.only.split(",") if x.strip()) or None

    print_section("AMOSTRAGEM DE ARQUIVOS - DADOS ADICIONAIS (FIDC)")
    print(f"Base: {base}")
    if roots_filter:
        print(f"Filtro de pastas: {sorted(roots_filter)}")

    if not os.path.isdir(base):
        print("ERRO: diretório base não encontrado.")
        return

    for top, full, rel, size in iter_all_files(base, roots_filter=roots_filter):
        ext = os.path.splitext(full)[1].lower()
        print_sub(f"{rel}  ({bytes_to_human(size) if size is not None else '?'})")

        try:
            if ext in [".csv", ".txt"]:
                handle_csv_or_txt(full, rows=args.rows)
            elif ext in [".xlsx", ".xls", ".ods"]:
                handle_excel(full, rows=args.rows)
            elif ext == ".zip":
                handle_zip(full, rows=args.rows)
            elif ext == ".pdf":
                print("   PDF detectado — não amostrado (apenas referência).")
            else:
                print("   Tipo não tratado especificamente — ignorado.")
        except Exception as e:
            print(f"   ERRO inesperado ao tratar '{rel}': {e}")

if __name__ == "__main__":
    main()
