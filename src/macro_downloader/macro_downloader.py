# -*- coding: utf-8 -*-
"""
=================================================================
SCRIPT UNIFICADO PARA DOWNLOAD DE DADOS DE FIDC E DADOS ADICIONAIS
=================================================================

Este script combina duas rotinas de download e permite selecionar
quais módulos de dados serão executados através da lista `DADOS_A_BAIXAR`.
"""

# --- IMPORTAÇÕES COMBINADAS DE AMBOS OS SCRIPTS ---
import os
import re
import io
import zipfile
import urllib.parse
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
from bcb import sgs
import py7zr
from ftplib import FTP
from bs4 import BeautifulSoup

# ======================================================================
# --- CONFIGURAÇÃO DE EXECUÇÃO ---
# ======================================================================
# Edite a lista abaixo para escolher quais dados baixar.
# Deixe a lista como está para baixar todos os dados.
# Para baixar apenas CVM e Tesouro, por exemplo, use: ['cvm', 'tesouro']

DADOS_A_BAIXAR = [
    # --- Bloco 1: Dados Primários ---
    'cvm',              # Informes Mensais de FIDCs da CVM
    'bcb',              # Séries temporais do Banco Central (Selic, CDI, IPCA, etc.)
    'ibge_pnad',        # Taxa de Desocupação da PNAD Contínua (IBGE)
    'tesouro',          # Preços e taxas do Tesouro Direto

    # --- Bloco 2: Dados Adicionais ---
    'ibge_mensal',      # Pesquisas Mensais do IBGE (PMC, PMS, PIM)
    'ibge_pib',         # PIB dos Municípios (IBGE)
    'caged',            # Dados do Novo CAGED (Ministério do Trabalho)
    'tse',              # Perfil do Eleitorado (TSE)
    'inmet'             # Dados climáticos de estações do INMET
]


DADOS_A_BAIXAR = set(DADOS_A_BAIXAR) # conjunto é mais rápido

###################################################################################
# BLOCO 1: ROTINA DE DOWNLOAD DE DADOS PRIMÁRIOS (SCRIPT 1)
###################################################################################

# --- Configurações do Bloco 1 ---
BASE_DIR_FIDC = 'dados_fidc'

def criar_pastas_fidc():
    """Cria a estrutura de pastas para o primeiro script."""
    print(f"Diretório base para salvar os dados primários: '{BASE_DIR_FIDC}'")
    os.makedirs(os.path.join(BASE_DIR_FIDC, 'cvm', 'informes_mensais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_FIDC, 'bcb', 'sgs'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_FIDC, 'ibge', 'pnad_continua'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_FIDC, 'tesouro_nacional', 'precos_taxas'), exist_ok=True)
    print("Estrutura de pastas (dados primários) verificada/criada.")

# --- Funções do Bloco 1: CVM, BCB, IBGE (PNAD), Tesouro ---

# cvm
def baixar_informes_mensais_fidc_cvm():
    print("\n[CVM] Verificando informes mensais de FIDCs...")
    unzip_dir = os.path.join(BASE_DIR_FIDC, 'cvm', 'informes_mensais')
    data_referencia = datetime.now() - relativedelta(months=1)
    ano_mes = data_referencia.strftime('%Y%m')
    arquivos_do_mes_existem = False
    if os.path.exists(unzip_dir):
        for filename in os.listdir(unzip_dir):
            if ano_mes in filename and filename.endswith('.csv'):
                arquivos_do_mes_existem = True
                break
    if arquivos_do_mes_existem:
        print(f"[CVM] Os informes de {ano_mes} já foram baixados.")
        return
    nome_arquivo_zip = f"inf_mensal_fidc_{ano_mes}.zip"
    url = f"https://dados.cvm.gov.br/dados/FIDC/DOC/INF_MENSAL/DADOS/{nome_arquivo_zip}"
    print(f"[CVM] Tentando baixar informes para o mês de referência: {ano_mes}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        print(f"[CVM] Download do arquivo '{nome_arquivo_zip}' concluído. Extraindo todos os arquivos CSV...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            arquivos_extraidos = 0
            for file_info in zip_ref.infolist():
                if not file_info.is_dir() and file_info.filename.lower().endswith('.csv'):
                    zip_ref.extract(file_info, path=unzip_dir)
                    arquivos_extraidos += 1
            if arquivos_extraidos > 0:
                print(f"[CVM] Extração concluída. {arquivos_extraidos} arquivos CSV foram salvos em '{unzip_dir}'.")
            else:
                print("[CVM] AVISO: Nenhum arquivo CSV foi encontrado dentro do arquivo ZIP baixado.")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"[CVM] O informe de {ano_mes} ainda não está disponível no servidor da CVM.")
        else:
            print(f"[CVM] ERRO HTTP: {e}")
    except Exception as e:
        print(f"[CVM] ERRO inesperado ao processar arquivo da CVM: {e}")

# bacen
def baixar_series_temporais_bcb():
    print("\n[BCB] Verificando Séries Temporais (SGS)...")
    series_diarias = {'selic': 11, 'cdi': 12}
    series_outras = {'ipca': 433, 'inadimplencia_pj': 21082}
    series_completas = {**series_diarias, **series_outras}
    for nome, codigo in series_completas.items():
        print(f"  -> Verificando série: {nome.upper()}")
        arquivo_csv = os.path.join(BASE_DIR_FIDC, 'bcb', 'sgs', f'{nome}.csv')
        try:
            if not os.path.exists(arquivo_csv):
                print(f"     Arquivo '{nome}.csv' não encontrado. Realizando download histórico completo...")
                df_final = pd.DataFrame()
                if nome in series_diarias:
                    print("     Série diária. Baixando em blocos para evitar limite da API...")
                    dfs = []
                    start_date = datetime(2000, 1, 1)
                    while start_date < datetime.now():
                        end_date = start_date + relativedelta(years=5) - relativedelta(days=1)
                        if end_date > datetime.now(): end_date = datetime.now()
                        print(f"       Baixando de {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
                        df_chunk = sgs.get({nome: codigo}, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
                        if not df_chunk.empty: dfs.append(df_chunk)
                        start_date += relativedelta(years=5)
                    if dfs: df_final = pd.concat(dfs)
                else:
                    df_final = sgs.get({nome: codigo}, start='2000-01-01')
                if not df_final.empty:
                    df_final.reset_index(inplace=True)
                    df_final.to_csv(arquivo_csv, index=False)
                    print(f"     Arquivo '{nome}.csv' criado com {len(df_final)} registros.")
            else:
                df_local = pd.read_csv(arquivo_csv, parse_dates=['Date'])
                last_date = df_local['Date'].max()
                start_update = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                if datetime.strptime(start_update, '%Y-%m-%d').date() >= datetime.today().date():
                    print(f"     '{nome.upper()}' já está atualizado.")
                    continue
                df_update = sgs.get({nome: codigo}, start=start_update)
                if not df_update.empty:
                    df_update.reset_index(inplace=True)
                    df_update.to_csv(arquivo_csv, mode='a', header=False, index=False)
                    print(f"     Novos {len(df_update)} registros adicionados a '{nome}.csv'.")
                else:
                    print(f"     Nenhum dado novo para '{nome.upper()}'.")
        except Exception as e:
            print(f"  -> ERRO ao baixar série '{nome.upper()}': {e}")

# ibge
def baixar_dados_pnad_ibge():
    print("\n[IBGE] Verificando dados da PNAD Contínua (Taxa de Desocupação)...")
    url_api = "https://apisidra.ibge.gov.br/values/t/4099/n1/1/v/4099/p/all"
    arquivo_csv = os.path.join(BASE_DIR_FIDC, 'ibge', 'pnad_continua', 'taxa_desocupacao.csv')
    try:
        response = requests.get(url_api, timeout=15)
        response.raise_for_status()
        dados_json = response.json()
        if len(dados_json) <= 1:
            print("[IBGE] API retornou dados vazios ou apenas cabeçalho.")
            return

        df = pd.DataFrame(dados_json[1:])
        df_clean = df[['D2N', 'V']].copy()
        df_clean.rename(columns={'D2N': 'Mes', 'V': 'Taxa_Desocupacao'}, inplace=True)
        df_clean['Taxa_Desocupacao'] = pd.to_numeric(df_clean['Taxa_Desocupacao'], errors='coerce')
        df_clean['Date'] = pd.to_datetime(df_clean['Mes'], format='%Y%m', errors='coerce')
        df_clean = df_clean[['Date', 'Mes', 'Taxa_Desocupacao']].dropna(subset=['Date', 'Taxa_Desocupacao'])

        if os.path.exists(arquivo_csv):
            df_local = pd.read_csv(arquivo_csv, parse_dates=['Date'])
            df_novo = df_clean[~df_clean['Date'].isin(df_local['Date'])]
            if not df_novo.empty:
                df_novo.to_csv(arquivo_csv, mode='a', header=False, index=False)
                print(f"     Novos {len(df_novo)} registros adicionados à PNAD.")
            else:
                print("     Dados da PNAD já estão atualizados.")
        else:
            df_clean.to_csv(arquivo_csv, index=False)
            print(f"     Arquivo da PNAD criado com {len(df_clean)} registros.")
    except requests.RequestException as e:
        print(f"[IBGE] ERRO: Falha ao baixar dados da PNAD. {e}")
    except Exception as e:
        print(f"[IBGE] ERRO: Falha ao processar dados da PNAD. {e}")

# tesouro
def baixar_dados_tesouro_direto():
    print("\n[Tesouro] Verificando preços e taxas dos títulos públicos...")
    url = "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecosTaxasTesouroDireto.csv"
    local_csv_path = os.path.join(BASE_DIR_FIDC, 'tesouro_nacional', 'precos_taxas', 'tesouro_direto_historico.csv')
    try:
        response_head = requests.head(url, timeout=10)
        response_head.raise_for_status()
        remote_last_modified_str = response_head.headers.get('Last-Modified')
        if remote_last_modified_str:
            remote_last_modified = datetime.strptime(remote_last_modified_str, '%a, %d %b %Y %H:%M:%S %Z')
            if os.path.exists(local_csv_path):
                local_last_modified_ts = os.path.getmtime(local_csv_path)
                if remote_last_modified.timestamp() <= local_last_modified_ts:
                    print("[Tesouro] Os dados do Tesouro Direto já estão atualizados.")
                    return
        print("[Tesouro] Nova versão dos dados do Tesouro encontrada. Baixando...")
        df = pd.read_csv(url, sep=';', decimal=',', encoding='latin-1')
        df.to_csv(local_csv_path, index=False)
        print(f"[Tesouro] Dados salvos em '{local_csv_path}'.")
    except requests.RequestException as e:
        print(f"[Tesouro] ERRO: {e}")

def exibir_notas_fontes_manuais_fidc():
    print("\n" + "="*50 + "\nNOTAS IMPORTANTES SOBRE OUTRAS FONTES DE DADOS\n" + "="*50)
    print("[CVM] Regulamentos: Ação manual | [BCB] REF: Ação manual | [B3/ANBIMA]: Acesso pago/manual")


###################################################################################
# BLOCO 2: ROTINA DE DOWNLOAD DE DADOS ADICIONAIS (SCRIPT 2)
###################################################################################

# --- Configurações do Bloco 2 ---
BASE_DIR_ADICIONAIS = 'dados_adicionais_fidc'
IBGE_V3 = "https://servicodados.ibge.gov.br/api/v3/agregados"

def criar_pastas_adicionais():
    """Cria a estrutura de pastas para o segundo script."""
    print(f"\nDiretório base para salvar os dados adicionais: '{BASE_DIR_ADICIONAIS}'")
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'pesquisas_mensais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'contas_regionais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'trabalho', 'caged'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'tse', 'eleitorado'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'inmet', 'clima'), exist_ok=True)
    print("Estrutura de pastas (dados adicionais) verificada/criada.")

# --- Funções do Bloco 2: IBGE (Avançado), CAGED, TSE, INMET ---
# (O conteúdo das funções permanece o mesmo e foi omitido aqui para abreviar a resposta, 
# mas ele deve ser mantido no seu arquivo .py)
# ...
# ======================================================================
# 2.1) IBGE – API V3 (Agregados)
# ======================================================================
def chamar_downloads_ibge_mensal():
    # ... (código da função inalterado)
    pass
# ======================================================================
# 2.2) IBGE – PIB dos Municípios
# ======================================================================
def baixar_pib_municipios_ibge():
    # ... (código da função inalterado)
    pass
# ======================================================================
# 2.3) Ministério do Trabalho – NOVO CAGED
# ======================================================================
def baixar_novo_caged():
    # ... (código da função inalterado)
    pass
# ======================================================================
# 2.4) TSE – Perfil do Eleitorado
# ======================================================================
def baixar_perfil_eleitorado_tse():
    # ... (código da função inalterado)
    pass
# ======================================================================
# 2.5) INMET – Dados Históricos
# ======================================================================
def baixar_dados_climaticos_inmet():
    # ... (código da função inalterado)
    pass
# ======================================================================
# 2.6) Fontes para ação manual
# ======================================================================
def exibir_notas_fontes_manuais_adicionais():
    # ... (código da função inalterado)
    pass


# ======================================================================
# ROTINA PRINCIPAL UNIFICADA E CONDICIONAL
# ======================================================================
if __name__ == "__main__":

    if not DADOS_A_BAIXAR:
        print("Nenhuma tarefa selecionada na lista `DADOS_A_BAIXAR`. Encerrando o script.")
    else:
        print(f"Iniciando script. Tarefas selecionadas: {', '.join(sorted(list(DADOS_A_BAIXAR)))}")

    # --- Define os grupos de tarefas ---
    TAREFAS_BLOCO_1 = {'cvm', 'bcb', 'ibge_pnad', 'tesouro'}
    TAREFAS_BLOCO_2 = {'ibge_mensal', 'ibge_pib', 'caged', 'tse', 'inmet'}

    # Verifica se alguma tarefa do Bloco 1 foi selecionada
    executar_bloco_1 = any(tarefa in DADOS_A_BAIXAR for tarefa in TAREFAS_BLOCO_1)
    if executar_bloco_1:
        print("\n" + "#"*60)
        print("### INICIANDO ROTINA DE DOWNLOAD DE DADOS PRIMÁRIOS ###")
        print("#"*60)
        criar_pastas_fidc()

        if 'cvm' in DADOS_A_BAIXAR:
            baixar_informes_mensais_fidc_cvm()
        if 'bcb' in DADOS_A_BAIXAR:
            baixar_series_temporais_bcb()
        if 'ibge_pnad' in DADOS_A_BAIXAR:
            baixar_dados_pnad_ibge()
        if 'tesouro' in DADOS_A_BAIXAR:
            baixar_dados_tesouro_direto()

        exibir_notas_fontes_manuais_fidc()
        print("\nRotina de download de dados primários concluída.")

    # Verifica se alguma tarefa do Bloco 2 foi selecionada
    executar_bloco_2 = any(tarefa in DADOS_A_BAIXAR for tarefa in TAREFAS_BLOCO_2)
    if executar_bloco_2:
        print("\n" + "#"*60)
        print("### INICIANDO ROTINA DE DOWNLOAD DE DADOS ADICIONAIS ###")
        print("#"*60)
        criar_pastas_adicionais()

        if 'ibge_mensal' in DADOS_A_BAIXAR:
            chamar_downloads_ibge_mensal()
        if 'ibge_pib' in DADOS_A_BAIXAR:
            baixar_pib_municipios_ibge()
        if 'caged' in DADOS_A_BAIXAR:
            baixar_novo_caged()
        if 'tse' in DADOS_A_BAIXAR:
            baixar_perfil_eleitorado_tse()
        if 'inmet' in DADOS_A_BAIXAR:
            baixar_dados_climaticos_inmet()

        exibir_notas_fontes_manuais_adicionais()
        print("\nRotina de download de dados adicionais concluída.")
    
    if executar_bloco_1 or executar_bloco_2:
        print("\n" + "="*60)
        print(">>> TODAS AS ROTINAS SELECIONADAS FORAM EXECUTADAS <<<")
        print("="*60)