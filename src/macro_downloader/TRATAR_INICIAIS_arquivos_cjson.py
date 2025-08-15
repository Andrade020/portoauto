# -*- coding: utf-8 -*-
"""
amostrar_a_partir_do_json.py
----------------------------
Lê o JSON de validação (gerado pelo validar_dados_adicionais.py),
usa as pistas por seção (path/file/sample_file/files_preview, delimiter, encoding)
e imprime cabeçalho + amostra de cada arquivo detectado.

Uso:
  python amostrar_a_partir_do_json.py --rows 8
  python amostrar_a_partir_do_json.py --json "<caminho_do_relatorio>.json" --only IBGE_PMC,TSE_PERFIL
"""

import os
import io
import re
import csv
import json
import argparse
from typing import List, Tuple, Optional

import pandas as pd

# ---------------- utilidades ----------------

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

def sniff_csv_from_bytes(raw: bytes) -> Tuple[str, str]:
    """Tenta detectar encoding e separador a partir de bytes."""
    try:
        text = raw.decode("utf-8")
        enc = "utf-8"
    except UnicodeDecodeError:
        text = raw.decode("latin-1", errors="ignore")
        enc = "latin-1"
    try:
        sep = csv.Sniffer().sniff(text, delimiters=";,|\t,").delimiter
    except Exception:
        counts = {d: text.count(d) for d in [";", ",", "|", "\t"]}
        sep = max(counts, key=counts.get) if counts else ";"
    return sep, enc

def sniff_csv_from_path(path: str, max_bytes=65536) -> Tuple[Optional[str], Optional[str]]:
    try:
        with open(path, "rb") as f:
            raw = f.read(max_bytes)
        sep, enc = sniff_csv_from_bytes(raw)
        return sep, enc
    except Exception:
        return None, None

def read_csv_sample(path: str, nrows=50, sep_hint=None, enc_hint=None):
    """Leitura robusta (pequena amostra) usando dicas do JSON e fallback por sniff."""
    candidates = []
    if sep_hint or enc_hint:
        candidates.append((sep_hint or ";", enc_hint or "utf-8"))
    dsep, denc = sniff_csv_from_path(path)
    if dsep or denc:
        candidates.append((dsep or ";", denc or "utf-8"))
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
                             nrows=max(nrows, 50), dtype="object", low_memory=True)
            return df, s, e, None
        except Exception as ex:
            last_err = str(ex)
            continue
    return None, None, None, last_err

def read_excel_sample(path: str, nrows=20):
    ext = os.path.splitext(path)[1].lower()
    engine = "odf" if ext == ".ods" else None
    try:
        xls = pd.ExcelFile(path, engine=engine)
        sheets = xls.sheet_names
        df = xls.parse(sheets[0], nrows=max(nrows, 20), dtype="object")
        return df, sheets, None
    except Exception as e:
        return None, None, str(e)

def summarize_df(df: pd.DataFrame, rows=8):
    if df is None or df.empty:
        print("   (sem linhas na amostra)")
        return
    print(f"   Linhas (amostra): {len(df)} | Colunas: {len(df.columns)}")
    print("   Colunas:", list(df.columns))
    print("   Amostra:")
    print(df.head(rows))

# ---------------- seleção de arquivos a partir da seção ----------------

def files_from_section(sec: dict) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    Retorna uma lista de (filepath, sep_hint, enc_hint) para amostrar,
    com base nos campos da seção.
    """
    out = []
    # 1) campos mais específicos
    if "file" in sec and isinstance(sec["file"], str):
        out.append((sec["file"], sec.get("delimiter_detected") or sec.get("delimiter"), sec.get("encoding_detected") or sec.get("encoding")))
    if "sample_file" in sec and isinstance(sec["sample_file"], str):
        out.append((sec["sample_file"], sec.get("delimiter"), sec.get("encoding")))

    # 2) INMET: lista em files_preview
    if sec.get("files_preview") and isinstance(sec["files_preview"], list):
        for fp in sec["files_preview"]:
            fpath = fp.get("file")
            if isinstance(fpath, str):
                out.append((fpath, fp.get("delimiter"), fp.get("encoding")))

    # 3) se 'path' for um arquivo direto, considere também
    p = sec.get("path")
    if isinstance(p, str) and os.path.isfile(p):
        out.append((p, sec.get("delimiter"), sec.get("encoding")))

    # Remove duplicados preservando ordem
    seen = set()
    dedup = []
    for f, s, e in out:
        key = (os.path.normcase(f), s or "", e or "")
        if key not in seen:
            dedup.append((f, s, e))
            seen.add(key)
    return dedup

# ---------------- processamento por arquivo ----------------

def handle_file(path: str, rows: int, sep_hint=None, enc_hint=None):
    ext = os.path.splitext(path)[1].lower()
    size = None
    try:
        size = os.path.getsize(path)
    except OSError:
        pass
    size_str = bytes_to_human(size) if size is not None else "?"

    print_sub(f"{path}  ({size_str})")

    if ext in [".csv", ".txt"]:
        df, sep, enc, err = read_csv_sample(path, nrows=rows, sep_hint=sep_hint, enc_hint=enc_hint)
        if df is None:
            print(f"   ERRO lendo CSV/TXT: {err}")
            return
        print(f"   Delimitador: {sep} | Encoding: {enc}")
        summarize_df(df, rows=rows)
        return

    if ext in [".xlsx", ".xls", ".ods"]:
        df, sheets, err = read_excel_sample(path, nrows=rows)
        if df is None:
            print(f"   ERRO lendo planilha: {err}")
            return
        print(f"   Abas: {sheets[:10] if sheets else '(?)'}")
        summarize_df(df, rows=rows)
        return

    if ext == ".zip":
        print("   ZIP detectado — listando conteúdo (amostra de CSV interno se houver):")
        try:
            with open(path, "rb") as fh:
                pass
        except Exception as e:
            print(f"   ERRO abrindo ZIP: {e}")
            return
        try:
            with io.BytesIO(open(path, "rb").read()) as bio:
                with pd.io.common.ZipFile(bio) as zf:
                    names = zf.namelist()
                    for n in names[:20]:
                        print(f"     - {n}")
                    # tenta amostrar um CSV/TXT interno maior
                    csv_candidates = [n for n in names if n.lower().endswith((".csv",".txt",".tsv"))]
                    if csv_candidates:
                        try:
                            infos = {n: zf.getinfo(n).file_size for n in csv_candidates}
                            target = sorted(infos, key=infos.get, reverse=True)[0]
                        except Exception:
                            target = csv_candidates[0]
                        print(f"   Amostrando interno: {target}")
                        with zf.open(target) as fh:
                            raw = fh.read(65536)
                            sep, enc = sniff_csv_from_bytes(raw)
                            fh2 = zf.open(target)
                            wrapper = io.TextIOWrapper(fh2, encoding=enc, errors="ignore")
                            df = pd.read_csv(wrapper, sep=sep, on_bad_lines="skip",
                                             nrows=max(rows, 50), dtype="object")
                            print(f"   Delimitador: {sep} | Encoding: {enc}")
                            summarize_df(df, rows=rows)
                    else:
                        print("   (nenhum CSV/TXT encontrado no ZIP)")
        except Exception as e:
            print(f"   ERRO processando ZIP: {e}")
        return

    if ext == ".pdf":
        print("   PDF detectado — não será amostrado (apenas referência).")
        return

    print("   Tipo não tratado especificamente — ignorando.")

# ---------------- main ----------------

def main():
    here = os.path.dirname(__file__)
    default_json = os.path.abspath(os.path.join(here, "..", "..", "dados_adicionais_fidc", "_validacao", "relatorio_validacao.json"))

    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default=default_json, help="caminho do relatório JSON de validação")
    ap.add_argument("--rows", type=int, default=8, help="linhas para exibir por amostra (default=8)")
    ap.add_argument("--only", default="", help="filtrar por 'source' (ex.: IBGE_PMC,INMET,TSE_PERFIL)")
    args = ap.parse_args()

    print_section("AMOSTRAGEM A PARTIR DO JSON (Validação)")
    print(f"Relatório: {args.json}")

    if not os.path.isfile(args.json):
        print("ERRO: arquivo JSON não encontrado.")
        return

    # O json.dump padrão do Python permite NaN/Infinity; json.load também aceita
    with open(args.json, "r", encoding="utf-8") as f:
        data = json.load(f)

    only = set([s.strip() for s in args.only.split(",") if s.strip()]) or None

    base_dir = data.get("base_dir")
    if base_dir:
        print(f"Base declarada no JSON: {base_dir}")

    sections = data.get("sections", [])
    for sec in sections:
        src = sec.get("source", "(sem source)")
        if only and src not in only:
            continue
        print_section(f"SEÇÃO: {src}")

        files = files_from_section(sec)
        if not files:
            p = sec.get("path")
            if p and os.path.isdir(p):
                print(f"   (sem arquivo único para amostrar; 'path' é diretório: {p})")
            else:
                print("   (nenhum arquivo determinável a partir do JSON)")
            continue

        for fpath, sep_hint, enc_hint in files:
            # normaliza eventuais ..\..\ do JSON
            fpath = os.path.normpath(fpath)
            if not os.path.exists(fpath):
                print_sub(f"{fpath}")
                print("   AVISO: caminho não existe no disco (verifique mapeamento/base_dir).")
                continue
            handle_file(fpath, rows=args.rows, sep_hint=sep_hint, enc_hint=enc_hint)

if __name__ == "__main__":
    main()
