# -*- coding: utf-8 -*-
"""
carregar_dados_brutos.py
-------------------------
Script unificado para ler, padronizar e salvar em formato Parquet (bronze)
os dados brutos baixados pelos scripts de download.

Este script centraliza as rotinas de leitura para as seguintes fontes:
- CVM (Informes FIDC)
- BCB (Séries Temporais SGS)
- IBGE (PNAD, PMC, PMS, PIM, PIB dos Municípios)
- Tesouro Nacional
- CAGED
- TSE (Perfil do Eleitorado)
- INMET (Dados Climáticos)

Uso:
  # Apenas exibe amostras dos dados encontrados
  python carregar_dados_brutos.py

  # Exibe amostras com 10 linhas e salva os arquivos Parquet
  python carregar_dados_brutos.py --rows 10 --save-parquet
"""

import os
import re
import csv
import argparse
from glob import glob
from datetime import datetime
import pandas as pd

# ==============================================================================
# 1. CONFIGURAÇÃO
# ==============================================================================

# Define o diretório atual do script para construir os caminhos relativos
HERE = os.path.dirname(os.path.abspath(__file__))

# Diretórios de dados brutos (raw)
DIR_MACRO = os.path.join(HERE, "data_raw", "dados_macro")
DIR_MACRO_ADICIONAIS = os.path.join(HERE, "data_raw", "dados_macro_adicionais")

# Diretório de saída para os dados processados (camada bronze)
BRONZE_DIR = os.path.join(HERE, "data_processed", "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)


# ==============================================================================
# 2. FUNÇÕES UTILITÁRIAS
# ==============================================================================

def print_section(title: str):
    """Imprime um título de seção formatado."""
    print(f"\n{'=' * (len(title) + 4)}")
    print(f"| {title} |")
    print(f"{'=' * (len(title) + 4)}")

def print_sub(title: str):
    """Imprime um subtítulo formatado."""
    print(f"\n-- {title}")

def sanitize_number_pt(series: pd.Series) -> pd.Series:
    """Converte uma série textual (com vírgula decimal) para numérica."""
    if series is None:
        return None
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "-": None, "–": None, "—": None, "...": None})
    s = s.str.replace(".", "", regex=False)  # Remove separador de milhar
    s = s.str.replace(",", ".", regex=False)  # Troca vírgula decimal por ponto
    return pd.to_numeric(s, errors="coerce")

def read_smart(path: str, nrows: int = None, **kwargs) -> pd.DataFrame | None:
    """
    Lê um arquivo tabular (CSV, TXT, XLSX, ODS) de forma inteligente.
    Retorna um DataFrame ou None em caso de erro.
    """
    if not os.path.exists(path):
        print("    -> ERRO: Arquivo não encontrado.")
        return None

    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in [".csv", ".txt"]:
            # Detecta delimitador e encoding para CSVs
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                sample = f.read(4096)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
                    sep = dialect.delimiter
                except csv.Error:
                    sep = kwargs.get("sep", ";") # Fallback
            
            enc = kwargs.get("encoding", "latin-1" if "tse" in path else "utf-8")
            return pd.read_csv(path, sep=sep, encoding=enc, on_bad_lines="warn", dtype=object, nrows=nrows)

        elif ext in [".xlsx", ".xls", ".ods"]:
            engine = "odf" if ext == ".ods" else None
            return pd.read_excel(path, engine=engine, nrows=nrows, dtype=object)

    except Exception as e:
        print(f"    -> ERRO ao ler o arquivo: {e}")
        return None
    return None

# ==============================================================================
# 3. MÓDULOS DE CARREGAMENTO E PADRONIZAÇÃO
# ==============================================================================

def load_cvm(path: str, preview_rows: int) -> pd.DataFrame | None:
    """Carrega e padroniza dados da CVM."""
    print_sub(f"Fonte: CVM - Informes FIDC ({os.path.basename(path)})")
    df = read_smart(path, encoding="latin-1")
    if df is None: return None
    
    # Padronização de datas e valores
    if "DT_COMPTC" in df.columns:
        df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")
    for col in df.columns:
        if "VL_" in col or "VR_" in col:
            df[f"{col}_numeric"] = sanitize_number_pt(df[col])
            
    print(df.head(preview_rows))
    return df

def load_bcb(path: str, preview_rows: int) -> pd.DataFrame | None:
    """Carrega e padroniza dados do BCB."""
    print_sub(f"Fonte: BCB - SGS ({os.path.basename(path)})")
    df = read_smart(path, sep=",")
    if df is None: return None
    
    # Padronização de datas e valores
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    value_col = df.columns[1] # A segunda coluna é geralmente o valor
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    
    print(df.head(preview_rows))
    return df

def load_ibge_pnad(path: str, preview_rows: int) -> pd.DataFrame | None:
    """Carrega e padroniza dados da PNAD Contínua."""
    print_sub(f"Fonte: IBGE - PNAD Contínua ({os.path.basename(path)})")
    df = read_smart(path, sep=",")
    if df is None: return None

    # Padronização de datas e valores
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    if "Taxa_Desocupacao" in df.columns:
        df["Taxa_Desocupacao"] = pd.to_numeric(df["Taxa_Desocupacao"], errors="coerce")

    print(df.head(preview_rows))
    return df
    
def load_tesouro(path: str, preview_rows: int) -> pd.DataFrame | None:
    """Carrega e padroniza dados do Tesouro Nacional."""
    print_sub(f"Fonte: Tesouro Nacional ({os.path.basename(path)})")
    df = read_smart(path, sep=",")
    if df is None: return None

    # Padronização de datas e valores
    if "Data Venda" in df.columns:
        df["Data Venda"] = pd.to_datetime(df["Data Venda"], format="%d/%m/%Y", errors="coerce")
    if "Data Vencimento" in df.columns:
        df["Data Vencimento"] = pd.to_datetime(df["Data Vencimento"], format="%d/%m/%Y", errors="coerce")
    for col in ["Taxa Compra Manha", "PU Compra Manha", "PU Venda Manha"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(df.head(preview_rows))
    return df

def load_ibge_pesquisas_mensais(path: str, preview_rows: int) -> pd.DataFrame | None:
    """Carrega dados das pesquisas mensais do IBGE (PMC, PMS, PIM)."""
    print_sub(f"Fonte: IBGE - Pesquisas Mensais ({os.path.basename(path)})")
    df = read_smart(path, sep=",")
    if df is None: return None

    # Padronização
    if "periodo" in df.columns:
        df["periodo_dt"] = pd.to_datetime(df["periodo"], format="%Y%m", errors="coerce")
    if "valor" in df.columns:
        df["valor_numeric"] = sanitize_number_pt(df["valor"])

    print(df.head(preview_rows))
    return df

def load_caged(path: str, preview_rows: int) -> pd.DataFrame | None:
    """Carrega e padroniza dados do CAGED."""
    print_sub(f"Fonte: CAGED ({os.path.basename(path)})")
    df = read_smart(path, encoding="latin-1")
    if df is None: return None

    # Padronização
    if "competênciamov" in df.columns:
        df["competencia_dt"] = pd.to_datetime(df["competênciamov"], format="%Y%m", errors="coerce")
    if "salário" in df.columns:
        df["salario_numeric"] = sanitize_number_pt(df["salário"])

    print(df.head(preview_rows))
    return df

def load_inmet(path: str, preview_rows: int) -> pd.DataFrame | None:
    """Carrega e padroniza dados do INMET."""
    print_sub(f"Fonte: INMET ({os.path.basename(path)})")
    df = read_smart(path)
    if df is None: return None

    # Padronização
    if "DATA (YYYY-MM-DD)" in df.columns and "HORA (UTC)" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(
            df["DATA (YYYY-MM-DD)"] + " " + df["HORA (UTC)"].str.slice(0, 5),
            errors="coerce"
        )
    for col in df.columns:
        # Heurística para converter colunas numéricas
        if df[col].astype(str).str.contains(r"^\d+,\d+$").any():
             df[f"{col}_numeric"] = sanitize_number_pt(df[col])

    print(df.head(preview_rows))
    return df

def load_generic_sample(path: str, source_name: str, preview_rows: int):
    """Carrega apenas uma amostra de arquivos complexos (PIB, TSE)."""
    print_sub(f"Fonte: {source_name} ({os.path.basename(path)}) - Amostra")
    df = read_smart(path, nrows=preview_rows)
    if df is not None:
        print(df.head(preview_rows))

# ==============================================================================
# 4. EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    """Função principal para orquestrar a leitura e salvamento dos dados."""
    parser = argparse.ArgumentParser(description="Script para carregar e padronizar dados brutos.")
    parser.add_argument("--rows", type=int, default=5, help="Número de linhas para exibir nas amostras.")
    parser.add_argument("--save-parquet", action="store_true", help="Salva os DataFrames padronizados em formato Parquet.")
    args = parser.parse_args()

    # Dicionário mapeando o nome do dado, o loader e o diretório base
    tarefas = {
        "cvm_fidc": (load_cvm, os.path.join(DIR_MACRO, "cvm", "informes_mensais", "inf_mensal_fidc_*.zip")),
        "bcb_selic": (load_bcb, os.path.join(DIR_MACRO, "bcb", "sgs", "selic.csv")),
        "bcb_cdi": (load_bcb, os.path.join(DIR_MACRO, "bcb", "sgs", "cdi.csv")),
        "bcb_ipca": (load_bcb, os.path.join(DIR_MACRO, "bcb", "sgs", "ipca.csv")),
        "ibge_pnad": (load_ibge_pnad, os.path.join(DIR_MACRO, "ibge", "pnad_continua", "taxa_desocupacao.csv")),
        "tesouro_direto": (load_tesouro, os.path.join(DIR_MACRO, "tesouro_nacional", "precos_taxas", "tesouro_direto_historico.csv")),
        "ibge_pmc": (load_ibge_pesquisas_mensais, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pmc_volume_vendas.csv")),
        "ibge_pms": (load_ibge_pesquisas_mensais, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pms_receita_servicos.csv")),
        "ibge_pim": (load_ibge_pesquisas_mensais, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pim_producao_industrial.csv")),
        "caged": (load_caged, os.path.join(DIR_MACRO_ADICIONAIS, "trabalho", "caged", "CAGEDMOV*.txt")),
        "inmet_A904": (load_inmet, os.path.join(DIR_MACRO_ADICIONAIS, "inmet", "clima", "estacao_A904.csv")),
        # Tarefas que apenas mostram amostras
        "ibge_pib_municipios": (load_generic_sample, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "contas_regionais", "*.*")),
        "tse_eleitorado": (load_generic_sample, os.path.join(DIR_MACRO_ADICIONAIS, "tse", "eleitorado", "*.csv")),
    }

    print_section("INICIANDO CARREGAMENTO E PADRONIZAÇÃO DE DADOS BRUTOS")
    
    for nome, (loader, path_pattern) in tarefas.items():
        files = glob(path_pattern)
        if not files:
            print(f"\n-- AVISO: Nenhum arquivo encontrado para '{nome}' com o padrão '{path_pattern}'")
            continue
        
        # Pega o arquivo mais recente ou maior, dependendo do caso
        path = max(files, key=os.path.getmtime)
        
        if loader == load_generic_sample:
            loader(path, nome.upper(), args.rows)
        else:
            df = loader(path, args.rows)
            if args.save_parquet and df is not None and not df.empty:
                output_path = os.path.join(BRONZE_DIR, f"{nome}.parquet")
                try:
                    df.to_parquet(output_path, index=False)
                    print(f"    -> [SALVO] Arquivo Parquet salvo em: {output_path}")
                except Exception as e:
                    print(f"    -> ERRO ao salvar Parquet: {e}")

    print_section("PROCESSAMENTO CONCLUÍDO")

if __name__ == "__main__":
    main()