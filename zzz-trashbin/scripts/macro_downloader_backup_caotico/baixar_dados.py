# -*- coding: utf-8 -*-
"""
carregar_dados_brutos.py
-------------------------
Script unificado para ler, padronizar e salvar em formato Parquet (bronze)
os dados brutos baixados pelos scripts de download.

Este script localiza a raiz do projeto (procurando pelo diretório .git)
para construir os caminhos corretamente, não importando de onde ele é executado.

Fontes tratadas:
- CVM (Informes FIDC)
- BCB (Séries Temporais SGS)
- IBGE (PNAD, PMC, PMS, PIM, PIB dos Municípios)
- Tesouro Nacional
- CAGED
- TSE (Perfil do Eleitorado)
- INMET (Dados Climáticos)

Uso:
  # Apenas exibe amostras dos dados encontrados
  python seu/caminho/para/carregar_dados_brutos.py

  # Exibe amostras com 10 linhas e salva os arquivos Parquet
  python seu/caminho/para/carregar_dados_brutos.py --rows 10 --save-parquet
"""

import os
import re
import csv
import argparse
from glob import glob
from datetime import datetime
import pandas as pd

# ==============================================================================
# 1. FUNÇÕES UTILITÁRIAS (incluindo a busca pela raiz do projeto)
# ==============================================================================

def find_project_root(marker: str = '.git') -> str:
    """
    Localiza a raiz do projeto subindo na árvore de diretórios a partir deste script.
    A raiz é identificada pela presença de um marcador (padrão: diretório '.git').
    """
    current_path = os.path.abspath(os.path.dirname(__file__))
    while current_path != os.path.dirname(current_path): # Para de subir ao chegar na raiz do sistema
        if os.path.isdir(os.path.join(current_path, marker)):
            return current_path
        current_path = os.path.dirname(current_path)
    raise FileNotFoundError(f"Não foi possível encontrar a raiz do projeto (marcador '{marker}').")

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
# 2. CONFIGURAÇÃO DE CAMINHOS
# ==============================================================================

# Localiza a raiz do projeto e constrói os caminhos a partir dela
try:
    PROJECT_ROOT = find_project_root()
    print(f"Raiz do projeto encontrada em: {PROJECT_ROOT}")

    # Diretórios de dados brutos (raw)
    DIR_MACRO = os.path.join(PROJECT_ROOT, "data_raw", "dados_macro")
    DIR_MACRO_ADICIONAIS = os.path.join(PROJECT_ROOT, "data_raw", "dados_macro_adicionais")

    # Diretório de saída para os dados processados (camada bronze)
    BRONZE_DIR = os.path.join(PROJECT_ROOT, "data_processed", "bronze")
    os.makedirs(BRONZE_DIR, exist_ok=True)

except FileNotFoundError as e:
    print(f"ERRO CRÍTICO: {e}")
    print("Certifique-se de que o script está dentro de um repositório git ou ajuste o marcador.")
    exit(1) # Encerra o script se não encontrar a raiz

# ==============================================================================
# 3. MÓDULOS DE CARREGAMENTO E PADRONIZAÇÃO
# ==============================================================================
# As funções load_* (load_cvm, load_bcb, etc.) permanecem as mesmas
# e foram omitidas aqui para abreviar. Cole-as do script anterior.

def load_cvm(path: str, preview_rows: int) -> pd.DataFrame | None:
    """Carrega e padroniza dados da CVM."""
    print_sub(f"Fonte: CVM - Informes FIDC ({os.path.basename(path)})")
    # A CVM costuma usar latin-1. O zip pode conter múltiplos CSVs, vamos pegar o maior.
    if path.endswith('.zip'):
        import zipfile
        with zipfile.ZipFile(path, 'r') as z:
            # Encontra o maior arquivo CSV dentro do ZIP
            csv_files = [f for f in z.infolist() if f.filename.lower().endswith('.csv')]
            if not csv_files:
                print("    -> ERRO: Nenhum arquivo CSV encontrado no ZIP da CVM.")
                return None
            target_file = max(csv_files, key=lambda f: f.file_size)
            print(f"    -> Lendo '{target_file.filename}' de dentro do ZIP.")
            with z.open(target_file.filename) as f:
                df = pd.read_csv(f, sep=';', encoding='latin-1', on_bad_lines='warn', dtype=object)
    else: # Fallback se não for zip
        df = read_smart(path, encoding="latin-1")

    if df is None: return None
    
    # Padronização de datas e valores
    if "DT_COMPTC" in df.columns:
        df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")
    for col in df.columns:
        if col.startswith("VL_") or col.startswith("VR_"):
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
    if len(df.columns) > 1:
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
    df = read_smart(path, sep=",") # O arquivo original é ';', mas o downloader salva como ','
    if df is None:
        # Tenta com ';' como fallback
        df = read_smart(path, sep=";")
        if df is None: return None

    # Padronização de datas e valores
    date_cols = [col for col in df.columns if 'Data' in col]
    for col in date_cols:
         df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")

    numeric_cols = ["Taxa Compra Manha", "PU Compra Manha", "PU Venda Manha"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace(',', '.'), errors="coerce")

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
        # Tenta combinar data e hora, lidando com formatos "HHMM" ou "HH:MM"
        hora_utc = df["HORA (UTC)"].astype(str).str.replace(" UTC", "").str.strip()
        hora_formatada = hora_utc.str.slice(0, 2) + ":" + hora_utc.str.slice(2, 4)
        df["timestamp_utc"] = pd.to_datetime(
            df["DATA (YYYY-MM-DD)"] + " " + hora_formatada,
            errors="coerce"
        )
    for col in df.columns:
        # Heurística para converter colunas numéricas
        if df[col].dtype == 'object' and df[col].str.contains(r"^\d+,\d+", na=False).any():
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
    parser = argparse.ArgumentParser(description="Script para carregar e padronizar dados brutos a partir da raiz do projeto.")
    parser.add_argument("--rows", type=int, default=5, help="Número de linhas para exibir nas amostras.")
    parser.add_argument("--save-parquet", action="store_true", help="Salva os DataFrames padronizados em formato Parquet.")
    args = parser.parse_args()

    tarefas = {
        "cvm_fidc": (load_cvm, os.path.join(DIR_MACRO, "cvm", "informes_mensais", "inf_mensal_fidc_*.csv")),
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
        "ibge_pib_municipios": (load_generic_sample, os.path.join(DIR_MACRO_ADICIONAIS, "ibge", "contas_regionais", "*.*")),
        "tse_eleitorado": (load_generic_sample, os.path.join(DIR_MACRO_ADICIONAIS, "tse", "eleitorado", "*.csv")),
    }

    print_section("INICIANDO CARREGAMENTO E PADRONIZAÇÃO DE DADOS BRUTOS")
    
    for nome, (loader, path_pattern) in tarefas.items():
        files = glob(path_pattern)
        if not files:
            print(f"\n-- AVISO: Nenhum arquivo encontrado para '{nome}' com o padrão '{path_pattern}'")
            continue
        
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