# -*- coding: utf-8 -*-
"""
validar_dados_adicionais.py
---------------------------
Valida arquivos baixados por 'novos_dados.py' (formato, tamanho, leitura, colunas).
Gera um relatório no console e um JSON para uso posterior no pipeline.

Regras checadas por fonte:
- IBGE (PMC, PMS, PIM): CSVs com colunas ['localidade','periodo','valor'].
- IBGE (PIB Municípios): ao menos 1 planilha/arquivo extraído; tentamos ler um cabeçalho.
- CAGED (Novo CAGED): TXT separado por ';' (amostra), leitura com pandas (nrows=500).
- TSE (Perfil do Eleitorado): ao menos 1 arquivo extraído; tentamos ler um CSV grande (amostra).
- INMET (estações): CSV(s) 'estacao_*.csv' separados por ';', com datas/horas se disponíveis.

Saída:
- Console (resumo e alertas)
- JSON: dados_adicionais_fidc/_validacao/relatorio_validacao.json

Requisitos:
- pandas
"""

import os
import io
import json
import csv
import re
from datetime import datetime
from glob import glob

import pandas as pd

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "dados_adicionais_fidc")
OUT_DIR  = os.path.join(BASE_DIR, "_validacao")
os.makedirs(OUT_DIR, exist_ok=True)

# --------- Utils ---------

def bytes_to_human(n):
    # 1024-based
    units = ["B","KB","MB","GB","TB"]
    i = 0
    x = float(n)
    while x >= 1024 and i < len(units)-1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"

def file_exists(path):
    try:
        return os.path.isfile(path)
    except Exception:
        return False

def file_size(path):
    try:
        return os.path.getsize(path)
    except Exception:
        return None

def detect_csv_delimiter(path, max_bytes=4096):
    try:
        with open(path, "rb") as f:
            sample = f.read(max_bytes)
        try:
            sample_str = sample.decode("utf-8")
            enc = "utf-8"
        except UnicodeDecodeError:
            sample_str = sample.decode("latin-1", errors="ignore")
            enc = "latin-1"
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample_str, delimiters=";,|\t")
        return dialect.delimiter, enc
    except Exception:
        return None, None

def try_read_csv(path, sep=None, enc=None, nrows=500, **kwargs):
    """
    Tenta ler CSV de forma robusta:
    1) Se 'sep' não fornecido: tenta detectar; 2) UTF-8; 3) Latin-1; 4) fallback sep=';'
    """
    # Tentativas (sep, enc):
    candidates = []
    if sep is None or enc is None:
        det_sep, det_enc = detect_csv_delimiter(path)
        if det_sep or det_enc:
            candidates.append((det_sep or sep or ";", det_enc or enc or "utf-8"))
    if sep and enc:
        candidates.append((sep, enc))
    # fallback combos
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
            df = pd.read_csv(path, sep=s, encoding=e, on_bad_lines="skip", nrows=nrows, dtype="object", **kwargs)
            return df, s, e, None
        except Exception as ex:
            last_err = str(ex)
            continue
    return None, None, None, last_err

def try_read_table_like(path, nrows=100):
    """
    Lê cabeçalho de XLSX/ODS/CSV/TXT com heurística.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".txt"]:
        return try_read_csv(path, nrows=nrows)
    elif ext in [".xlsx", ".xls"]:
        try:
            df = pd.read_excel(path, nrows=nrows, dtype="object")
            return df, None, "binary-xlsx", None
        except Exception as e:
            return None, None, None, f"read_excel: {e}"
    elif ext == ".ods":
        try:
            df = pd.read_excel(path, engine="odf", nrows=nrows, dtype="object")
            return df, None, "binary-ods", None
        except Exception as e:
            return None, None, None, f"read_excel(ods): {e}"
    else:
        return None, None, None, f"Extensão não suportada: {ext}"

def summarize_df(df, n=5):
    out = {}
    if df is None or df.empty:
        return {"rows": 0, "cols": 0, "columns": []}
    out["rows"] = len(df)
    out["cols"] = len(df.columns)
    out["columns"] = list(df.columns[:50])
    # amostra de linhas (dicts)
    out["sample"] = df.head(n).to_dict(orient="records")
    return out

def status_from_issues(issues):
    if any("ERRO" in i.upper() for i in issues):
        return "error"
    if any("AVISO" in i.upper() for i in issues):
        return "warn"
    return "ok"

def print_section(title):
    print("\n" + "="*len(title))
    print(title)
    print("="*len(title))

# --------- Validadores por fonte ---------

def validar_ibge_pesquisas():
    """
    Espera CSVs:
      - ibge/pesquisas_mensais/pmc_volume_vendas.csv
      - ibge/pesquisas_mensais/pms_receita_servicos.csv
      - ibge/pesquisas_mensais/pim_producao_industrial.csv
    Colunas esperadas: ['localidade','periodo','valor'] (API v3 view=flat).
    """
    base = os.path.join(BASE_DIR, "ibge", "pesquisas_mensais")
    expected = [
        ("IBGE_PMC", os.path.join(base, "pmc_volume_vendas.csv")),
        ("IBGE_PMS", os.path.join(base, "pms_receita_servicos.csv")),
        ("IBGE_PIM", os.path.join(base, "pim_producao_industrial.csv")),
    ]
    results = []
    for label, path in expected:
        issues = []
        info = {"source": label, "path": path, "exists": file_exists(path), "size_bytes": file_size(path)}
        if not info["exists"]:
            issues.append("ERRO: arquivo não encontrado.")
        else:
            if not info["size_bytes"] or info["size_bytes"] < 100:  # heurística
                issues.append("AVISO: arquivo muito pequeno.")
            df, sep, enc, err = try_read_csv(path, nrows=1000)
            info["delimiter"] = sep
            info["encoding"] = enc
            if df is None:
                issues.append(f"ERRO: falha ao ler CSV (amostra). Detalhes: {err}")
            else:
                cols_req = {"localidade","periodo","valor"}
                missing = cols_req - set(df.columns)
                if missing:
                    issues.append(f"AVISO: colunas esperadas ausentes: {sorted(missing)}")
                # checar se 'valor' é numérico em amostra
                if "valor" in df.columns:
                    try:
                        coerced = pd.to_numeric(df["valor"].str.replace(",", ".", regex=False), errors="coerce")
                        if coerced.notna().sum() == 0:
                            issues.append("AVISO: coluna 'valor' não parece numérica (amostra).")
                    except Exception:
                        issues.append("AVISO: falha ao testar numericidade de 'valor'.")
                info["preview"] = summarize_df(df)
        info["issues"] = issues
        info["status"] = status_from_issues(issues)
        results.append(info)
    return results

def validar_ibge_pib_municipios():
    """
    Em ibge/contas_regionais/ deve haver conteúdo extraído do zip.
    Valida existência de pelo menos um arquivo tabular (xlsx/ods/csv/txt) e tenta ler cabeçalho.
    """
    base = os.path.join(BASE_DIR, "ibge", "contas_regionais")
    results = []
    issues = []
    info = {"source": "IBGE_PIB_MUN", "path": base, "exists": os.path.isdir(base)}
    if not info["exists"]:
        issues.append("ERRO: pasta base não encontrada.")
    else:
        # procura arquivos tabulares
        pats = ["*.xlsx","*.ods","*.csv","*.txt"]
        files = []
        for p in pats:
            files.extend(glob(os.path.join(base, p)))
        info["found_files"] = len(files)
        if not files:
            issues.append("ERRO: nenhum arquivo tabular encontrado após extração.")
        else:
            # escolhe o maior (heurística)
            files.sort(key=lambda p: os.path.getsize(p), reverse=True)
            top = files[0]
            info["sample_file"] = top
            info["sample_size_bytes"] = file_size(top)
            df, sep, enc, err = try_read_table_like(top, nrows=200)
            if df is None:
                issues.append(f"AVISO: não consegui ler amostra do arquivo '{os.path.basename(top)}' ({err}).")
            else:
                info["preview"] = summarize_df(df)
    info["issues"] = issues
    info["status"] = status_from_issues(issues)
    results.append(info)
    return results

def validar_caged():
    """
    Em trabalho/caged/ deve haver CAGEDMOVYYYYMM.txt ; delimitador ';'
    """
    base = os.path.join(BASE_DIR, "trabalho", "caged")
    results = []
    issues = []
    info = {"source": "CAGED", "path": base, "exists": os.path.isdir(base)}
    if not info["exists"]:
        issues.append("ERRO: pasta base não encontrada.")
    else:
        files = sorted(glob(os.path.join(base, "CAGEDMOV*.txt")))
        if not files:
            issues.append("ERRO: arquivo CAGEDMOV*.txt não encontrado.")
        else:
            # pega o mais recente pelo nome
            files.sort(reverse=True)
            path = files[0]
            info["file"] = path
            info["size_bytes"] = file_size(path)
            # delimiter/encoding
            sep, enc = detect_csv_delimiter(path)
            info["delimiter_detected"] = sep
            info["encoding_detected"] = enc
            # amostra com pandas
            df, s, e, err = try_read_csv(path, sep=sep or ";", enc=enc or "latin-1", nrows=1000, low_memory=False)
            if df is None:
                issues.append(f"ERRO: falha ao ler amostra do CAGED ({err}).")
            else:
                info["preview"] = summarize_df(df)
                if info["size_bytes"] and info["size_bytes"] < 1024 * 100:
                    issues.append("AVISO: arquivo CAGED menor que 100KB (pode estar incompleto).")
    info["issues"] = issues
    info["status"] = status_from_issues(issues)
    results.append(info)
    return results

def validar_tse():
    """
    Em tse/eleitorado/ devem existir arquivos extraídos do ZIP.
    Busca um CSV grande e tenta ler amostra.
    """
    base = os.path.join(BASE_DIR, "tse", "eleitorado")
    results = []
    issues = []
    info = {"source": "TSE_PERFIL", "path": base, "exists": os.path.isdir(base)}
    if not info["exists"]:
        issues.append("ERRO: pasta base não encontrada.")
    else:
        csvs = sorted(glob(os.path.join(base, "*.csv")))
        if not csvs:
            # tenta outros formatos
            outros = sorted(glob(os.path.join(base, "*.*")))
            if not outros:
                issues.append("ERRO: nenhum arquivo extraído encontrado.")
            else:
                # escolhe o maior e tenta ler
                outros.sort(key=lambda p: os.path.getsize(p), reverse=True)
                sample = outros[0]
                info["sample_file"] = sample
                info["sample_size_bytes"] = file_size(sample)
                df, sep, enc, err = try_read_table_like(sample, nrows=1000)
                if df is None:
                    issues.append(f"AVISO: falha ao ler amostra do arquivo '{os.path.basename(sample)}' ({err}).")
                else:
                    info["preview"] = summarize_df(df)
        else:
            # escolhe o maior CSV
            csvs.sort(key=lambda p: os.path.getsize(p), reverse=True)
            sample = csvs[0]
            info["sample_file"] = sample
            info["sample_size_bytes"] = file_size(sample)
            df, sep, enc, err = try_read_csv(sample, nrows=1000)
            if df is None:
                issues.append(f"AVISO: falha ao ler amostra do CSV '{os.path.basename(sample)}' ({err}).")
            else:
                info["delimiter"] = sep
                info["encoding"] = enc
                info["preview"] = summarize_df(df)
    info["issues"] = issues
    info["status"] = status_from_issues(issues)
    results.append(info)
    return results

def validar_inmet():
    """
    Em inmet/clima/ devem existir 'estacao_*.csv'. 
    Espera-se separador ';' e colunas de data/hora quando disponíveis.
    """
    base = os.path.join(BASE_DIR, "inmet", "clima")
    results = []
    issues = []
    info = {"source": "INMET", "path": base, "exists": os.path.isdir(base)}
    if not info["exists"]:
        issues.append("ERRO: pasta base não encontrada.")
    else:
        arquivos = sorted(glob(os.path.join(base, "estacao_*.csv")))
        info["found_files"] = len(arquivos)
        if not arquivos:
            issues.append("ERRO: nenhum 'estacao_*.csv' encontrado.")
        else:
            # valida cada arquivo, mas guarda só um preview resumido
            previews = []
            total_rows = 0
            for p in arquivos:
                size = file_size(p)
                df, sep, enc, err = try_read_csv(p, sep=";", enc=None, nrows=2000)
                rec = {
                    "file": p,
                    "size_bytes": size,
                    "delimiter": sep,
                    "encoding": enc,
                }
                if df is None:
                    rec["error"] = err
                    issues.append(f"AVISO: falha ao ler amostra de {os.path.basename(p)} ({err}).")
                else:
                    rec["rows_preview"] = len(df)
                    rec["cols"] = len(df.columns)
                    cols = df.columns.tolist()
                    # checar colunas padrão
                    missing = []
                    for k in ["DATA (YYYY-MM-DD)", "HORA (UTC)"]:
                        if k not in cols:
                            missing.append(k)
                    if missing:
                        rec["note"] = f"Colunas padrão ausentes: {missing}"
                    total_rows += len(df)
                previews.append(rec)
            info["files_preview"] = previews
            if total_rows == 0:
                issues.append("AVISO: nenhum dado lido nas amostras INMET (pode ser variação de layout).")
    info["issues"] = issues
    info["status"] = status_from_issues(issues)
    results.append(info)
    return results

# --------- Execução ---------

def main():
    print_section("VALIDAÇÃO - DADOS ADICIONAIS (FIDC)")
    print(f"Base: {BASE_DIR}")

    full_report = {
        "base_dir": BASE_DIR,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "sections": []
    }

    sections = []
    # Ordem de validação
    sections.extend(validar_ibge_pesquisas())
    sections.extend(validar_ibge_pib_municipios())
    sections.extend(validar_caged())
    sections.extend(validar_tse())
    sections.extend(validar_inmet())
    full_report["sections"] = sections

    # ---- Console resumo ----
    print_section("RESUMO")
    ok = sum(1 for s in sections if s.get("status") == "ok")
    warn = sum(1 for s in sections if s.get("status") == "warn")
    err = sum(1 for s in sections if s.get("status") == "error")
    print(f"OK: {ok} | AVISOS: {warn} | ERROS: {err}")

    for s in sections:
        label = s["source"]
        status = s["status"].upper()
        path = s.get("path") or s.get("file") or ""
        print(f"- {label:<15} [{status}] -> {path}")
        if s.get("issues"):
            for it in s["issues"]:
                print(f"    - {it}")

    # ---- Salva JSON ----
    out_json = os.path.join(OUT_DIR, "relatorio_validacao.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(full_report, f, ensure_ascii=False, indent=2)

    print_section("SAÍDA")
    print(f"Relatório JSON: {out_json}")

if __name__ == "__main__":
    main()
