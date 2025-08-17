# -*- coding: utf-8 -*-
"""
carregar_dados_brutos.py
-------------------------
Script unificado para ler, padronizar e salvar em formato Parquet (bronze)
os dados brutos.

Adaptado para identificar e processar de forma inteligente apenas os arquivos
do período mais recente para cada fonte de dados.
"""

import os
import re
import csv
import argparse
from glob import glob
from datetime import datetime
import pandas as pd
from typing import List

# ==============================================================================
# 1. FUNÇÕES UTILITÁRIAS E CONFIGURAÇÃO DE CAMINHOS
# ==============================================================================

def find_project_root(marker: str = '.git') -> str:
    """Localiza a raiz do projeto subindo na árvore de diretórios."""
    current_path = os.path.abspath(os.path.dirname(__file__))
    while current_path != os.path.dirname(current_path):
        if os.path.isdir(os.path.join(current_path, marker)):
            return current_path
        current_path = os.path.dirname(current_path)
    raise FileNotFoundError(f"Não foi possível encontrar a raiz do projeto (marcador '{marker}').")

try:
    PROJECT_ROOT = find_project_root()
    print(f"Raiz do projeto encontrada: {PROJECT_ROOT}")
    DIR_MACRO = os.path.join(PROJECT_ROOT, "data_raw", "dados_macro")
    DIR_MACRO_ADICIONAIS = os.path.join(PROJECT_ROOT, "data_raw", "dados_macro_adicionais")
    BRONZE_DIR = os.path.join(PROJECT_ROOT, "data_processed", "bronze")
    os.makedirs(BRONZE_DIR, exist_ok=True)
except FileNotFoundError as e:
    print(f"ERRO CRÍTICO: {e}")
    exit(1)

def print_section(title: str):
    print(f"\n{'=' * (len(title) + 4)}\n| {title} |\n{'=' * (len(title) + 4)}")

def print_sub(title: str):
    print(f"\n-- {title}")

def sanitize_number_pt(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().replace({"": None, "-": None, "–": None, "—": None, "...": None})
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def read_smart(path: str, nrows: int = None, **kwargs) -> pd.DataFrame | None:
    if not os.path.exists(path):
        print("    -> ERRO: Arquivo não encontrado.")
        return None
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in [".csv", ".txt"]:
            enc = kwargs.get("encoding", "latin-1" if "tse" in path else "utf-8")
            # Ler uma amostra para detectar o separador
            with open(path, 'r', encoding=enc, errors='ignore') as f:
                sample = f.read(4096)
                try:
                    sep = csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
                except csv.Error:
                    sep = kwargs.get("sep", ";")
            return pd.read_csv(path, sep=sep, encoding=enc, on_bad_lines="warn", dtype=object, nrows=nrows)
        elif ext in [".xlsx", ".xls", ".ods"]:
            return pd.read_excel(path, engine="odf" if ext == ".ods" else None, nrows=nrows, dtype=object)
    except Exception as e:
        print(f"    -> ERRO ao ler o arquivo {os.path.basename(path)}: {e}")
    return None

# ==============================================================================
# 3. MÓDULOS DE CARREGAMENTO E PADRONIZAÇÃO (ADAPTADOS)
# ==============================================================================

def load_cvm(paths: List[str], preview_rows: int) -> pd.DataFrame | None:
    """Carrega e une múltiplos arquivos CSV da CVM para um dado período."""
    print_sub(f"Fonte: CVM - Informes FIDC (período mais recente)")
    if not paths: return None
    
    all_dfs = []
    for path in paths:
        df = read_smart(path, encoding="latin-1", sep=';')
        if df is not None:
            all_dfs.append(df)

    if not all_dfs: return None
    
    full_df = pd.concat(all_dfs, ignore_index=True)
    print(f"   -> {len(paths)} arquivo(s) lido(s) e unidos, totalizando {len(full_df)} linhas.")

    if "DT_COMPTC" in full_df.columns:
        full_df["DT_COMPTC"] = pd.to_datetime(full_df["DT_COMPTC"], errors="coerce")
    for col in full_df.columns:
        if col.startswith("VL_") or col.startswith("VR_"):
            full_df[f"{col}_numeric"] = sanitize_number_pt(full_df[col])
            
    print(full_df.head(preview_rows))
    return full_df

def load_caged(paths: List[str], preview_rows: int) -> pd.DataFrame | None:
    """Carrega o arquivo de dados mais recente do CAGED."""
    print_sub(f"Fonte: CAGED ({os.path.basename(paths[0])})")
    df = read_smart(paths[0], encoding="latin-1") # CAGED é um arquivo por mês
    if df is None: return None

    if "competênciamov" in df.columns:
        df["competencia_dt"] = pd.to_datetime(df["competênciamov"], format="%Y%m", errors="coerce")
    if "salário" in df.columns:
        df["salario_numeric"] = sanitize_number_pt(df["salário"])

    print(df.head(preview_rows))
    return df

# --- Outras funções load_* permanecem as mesmas, pois operam em arquivo único ---
def load_bcb(path: str, preview_rows: int) -> pd.DataFrame | None:
    print_sub(f"Fonte: BCB - SGS ({os.path.basename(path)})")
    df = read_smart(path, sep=",")
    if df is None: return None
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    if len(df.columns) > 1:
        df[df.columns[1]] = pd.to_numeric(df[df.columns[1]], errors="coerce")
    print(df.head(preview_rows))
    return df

def load_ibge_pnad(path: str, preview_rows: int) -> pd.DataFrame | None:
    print_sub(f"Fonte: IBGE - PNAD Contínua ({os.path.basename(path)})")
    df = read_smart(path, sep=",")
    if df is None: return None
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    if "Taxa_Desocupacao" in df.columns:
        df["Taxa_Desocupacao"] = pd.to_numeric(df["Taxa_Desocupacao"], errors="coerce")
    print(df.head(preview_rows))
    return df
    
def load_tesouro(path: str, preview_rows: int) -> pd.DataFrame | None:
    print_sub(f"Fonte: Tesouro Nacional ({os.path.basename(path)})")
    df = read_smart(path, sep=",")
    if df is None: return None
    if "Data Venda" in df.columns:
        df["Data Venda"] = pd.to_datetime(df["Data Venda"], format="%d/%m/%Y", errors="coerce")
    if "Data Vencimento" in df.columns:
        df["Data Vencimento"] = pd.to_datetime(df["Data Vencimento"], format="%d/%m/%Y", errors="coerce")
    for col in ["Taxa Compra Manha", "PU Compra Manha", "PU Venda Manha"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors="coerce")
    print(df.head(preview_rows))
    return df

def load_ibge_pesquisas_mensais(path: str, preview_rows: int) -> pd.DataFrame | None:
    print_sub(f"Fonte: IBGE - Pesquisas Mensais ({os.path.basename(path)})")
    df = read_smart(path, sep=",")
    if df is None: return None
    if "periodo" in df.columns:
        df["periodo_dt"] = pd.to_datetime(df["periodo"], format="%Y%m", errors="coerce")
    if "valor" in df.columns:
        df["valor_numeric"] = sanitize_number_pt(df["valor"])
    print(df.head(preview_rows))
    return df

def load_inmet(path: str, preview_rows: int) -> pd.DataFrame | None:
    print_sub(f"Fonte: INMET ({os.path.basename(path)})")
    df = read_smart(path, sep=';')
    if df is None: return None
    if "DATA (YYYY-MM-DD)" in df.columns and "HORA (UTC)" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["DATA (YYYY-MM-DD)"] + " " + df["HORA (UTC)"].str.slice(0, 5), errors="coerce")
    for col in df.columns:
        if df[col].dtype == 'object' and df[col].str.contains(r"^\d+,\d+", na=False).any():
            df[f"{col}_numeric"] = sanitize_number_pt(df[col])
    print(df.head(preview_rows))
    return df

def load_generic_sample(path: str, source_name: str, preview_rows: int):
    print_sub(f"Fonte: {source_name} ({os.path.basename(path)}) - Amostra")
    df = read_smart(path, nrows=preview_rows)
    if df is not None:
        print(df.head(preview_rows))

# ==============================================================================
# 4. EXECUÇÃO PRINCIPAL (COM LÓGICA DE SELEÇÃO)
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Script para carregar e padronizar dados brutos.")
    parser.add_argument("--rows", type=int, default=5, help="Número de linhas para exibir nas amostras.")
    parser.add_argument("--save-parquet", action="store_true", help="Salva os DataFrames padronizados em formato Parquet.")
    args = parser.parse_args()

    tarefas = {
        # Fontes com múltiplos arquivos por período
        "cvm_fidc": (load_cvm, os.path.join(DIR_MACRO, "cvm", "informes_mensais", "inf_mensal_fidc_*.csv"), 'yyyymm_multi'),
        "caged": (load_caged, os.path.join(DIR_MACRO_ADICIONAIS, "trabalho", "caged", "CAGEDMOV*.txt"), 'yyyymm'),
        # Fontes para amostragem com seleção especial
        "ibge_pib_municipios": (load_generic_sample, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "contas_regionais", "*.*"), 'yyyy'),
        "tse_eleitorado": (load_generic_sample, os.path.join(DIR_MACRO_ADICIONAIS, "tse", "eleitorado", "*.csv"), 'size'),
        # Fontes com arquivo único
        "bcb_selic": (load_bcb, os.path.join(DIR_MACRO, "bcb", "sgs", "selic.csv"), 'single'),
        "bcb_cdi": (load_bcb, os.path.join(DIR_MACRO, "bcb", "sgs", "cdi.csv"), 'single'),
        "bcb_ipca": (load_bcb, os.path.join(DIR_MACRO, "bcb", "sgs", "ipca.csv"), 'single'),
        "ibge_pnad": (load_ibge_pnad, os.path.join(DIR_MACRO, "ibge", "pnad_continua", "taxa_desocupacao.csv"), 'single'),
        "tesouro_direto": (load_tesouro, os.path.join(DIR_MACRO, "tesouro_nacional", "precos_taxas", "tesouro_direto_historico.csv"), 'single'),
        "ibge_pmc": (load_ibge_pesquisas_mensais, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pmc_volume_vendas.csv"), 'single'),
        "ibge_pms": (load_ibge_pesquisas_mensais, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pms_receita_servicos.csv"), 'single'),
        "ibge_pim": (load_ibge_pesquisas_mensais, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pim_producao_industrial.csv"), 'single'),
        "inmet_A904": (load_inmet, os.path.join(DIR_MACRO_ADICIONAIS, "inmet", "clima", "estacao_A904.csv"), 'single'),
    }

    print_section("INICIANDO CARREGAMENTO E PADRONIZAÇÃO DE DADOS BRUTOS")
    
    for nome, (loader, path_pattern, strategy) in tarefas.items():
        files = glob(path_pattern)
        if not files:
            print(f"\n-- AVISO: Nenhum arquivo encontrado para '{nome}'")
            continue

        files_to_process = []
        if strategy == 'single':
            files_to_process = files[:1]
        elif strategy == 'size':
            files_to_process = [max(files, key=os.path.getsize)]
        elif 'yyyymm' in strategy:
            latest_date = 0
            for f in files:
                match = re.search(r'(\d{6})', os.path.basename(f))
                if match:
                    latest_date = max(latest_date, int(match.group(1)))
            if latest_date > 0:
                files_to_process = [f for f in files if str(latest_date) in f]
        elif strategy == 'yyyy':
            latest_date = 0
            for f in files:
                matches = re.findall(r'(\d{4})', os.path.basename(f))
                if matches:
                    latest_date = max(latest_date, max(int(y) for y in matches))
            if latest_date > 0:
                # Pega o primeiro arquivo que contiver o ano mais recente
                files_to_process = [f for f in files if str(latest_date) in f][:1]

        if not files_to_process:
            print(f"\n-- AVISO: Nenhum arquivo selecionado para '{nome}' após aplicar a estratégia.")
            continue

        # Executa o loader
        if loader == load_generic_sample:
            loader(files_to_process[0], nome.upper(), args.rows)
        elif strategy == 'yyyymm_multi' or strategy == 'yyyymm':
             df = loader(files_to_process, args.rows)
        else:
            df = loader(files_to_process[0], args.rows)

        # Salva em Parquet se solicitado
        if args.save_parquet and 'df' in locals() and df is not None and not df.empty:
            output_path = os.path.join(BRONZE_DIR, f"{nome}.parquet")
            try:
                df.to_parquet(output_path, index=False)
                print(f"    -> [SALVO] Arquivo Parquet salvo em: {output_path}")
            except Exception as e:
                print(f"    -> ERRO ao salvar Parquet: {e}")

    print_section("PROCESSAMENTO CONCLUÍDO")

if __name__ == "__main__":
    main()