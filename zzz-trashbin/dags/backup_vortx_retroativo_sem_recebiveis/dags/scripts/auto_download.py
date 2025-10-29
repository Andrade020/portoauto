# -*- coding: utf-8 -*-

"""
lógica para baixar e processar os dados macroeconômicos.

As funções aqui serão chamadas pelas tarefas na DAG do Airflow.
"""

import os
import re
import io
import zipfile
import urllib.parse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ftplib import FTP

import pandas as pd
import requests
import py7zr
from bcb import sgs
from bs4 import BeautifulSoup

#? ---------------------------------------------------->
#? ---------------------------------------------------->
# ? auxiliares ---------------------------------------->
def _str_norm(s):
    return re.sub(r'\s+', ' ', (s or '')).strip().lower()

def _match_all_terms(text, termos):
    t = _str_norm(text)
    return all(_str_norm(term) in t for term in termos)

def _is_year_dir(name):
    n = (name or '').strip('/').lower()
    return bool(re.fullmatch(r'\d{4}(_\d{4})?', n))

def _dir_end_year(name):
    n = (name or '').strip('/').lower()
    try:
        return int(n.split('_')[-1])
    except:
        return -1
#? ---------------------------------------------------->
#? ---------------------------------------------------->
#? ---------------------------------------------------->

# Funções que serão executadas como tarefas
def create_all_folders(base_dir: str, base_dir_adicionais: str):
    """Cria todas as estruturas de pastas necessárias para os dados."""
    print(f"Diretório base para dados primários: '{base_dir}'")
    os.makedirs(os.path.join(base_dir, 'cvm', 'informes_mensais'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'bcb', 'sgs'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'ibge', 'pnad_continua'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'tesouro_nacional', 'precos_taxas'), exist_ok=True)
    print("Estrutura de pastas (primárias) verificada/criada.")

    print(f"\nDiretório base para dados adicionais: '{base_dir_adicionais}'")
    os.makedirs(os.path.join(base_dir_adicionais, 'ibge', 'pesquisas_mensais'), exist_ok=True)
    os.makedirs(os.path.join(base_dir_adicionais, 'ibge', 'contas_regionais'), exist_ok=True)
    os.makedirs(os.path.join(base_dir_adicionais, 'trabalho', 'caged'), exist_ok=True)
    os.makedirs(os.path.join(base_dir_adicionais, 'tse', 'eleitorado'), exist_ok=True)
    os.makedirs(os.path.join(base_dir_adicionais, 'inmet', 'clima'), exist_ok=True)
    print("Estrutura de pastas (adicionais) verificada/criada.")


def baixar_informes_mensais_fidc_cvm():
    print("\n[CVM] Verificando informes mensais de FIDCs...")
    unzip_dir = os.path.join(BASE_DIR, 'cvm', 'informes_mensais')
    for months_ago in [1, 2]:
        data_referencia = datetime.now() - relativedelta(months=months_ago)
        ano_mes = data_referencia.strftime('%Y%m')
        if os.path.exists(unzip_dir) and any(ano_mes in f for f in os.listdir(unzip_dir) if f.endswith('.csv')):
            print(f"[CVM] Os informes de {ano_mes} já foram baixados.")
            continue
        nome_arquivo_zip = f"inf_mensal_fidc_{ano_mes}.zip"
        url = f"https://dados.cvm.gov.br/dados/FIDC/DOC/INF_MENSAL/DADOS/{nome_arquivo_zip}"
        print(f"[CVM] Tentando baixar informes para o mês de referência: {ano_mes}")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            print(f"[CVM] Download de '{nome_arquivo_zip}' concluído. Extraindo...")
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                zip_ref.extractall(unzip_dir)
            print(f"[CVM] Extração para '{ano_mes}' concluída.")
            return
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"[CVM] O informe de {ano_mes} ainda não está disponível.")
            else:
                print(f"[CVM] ERRO HTTP: {e}")
                break
        except Exception as e:
            print(f"[CVM] ERRO inesperado: {e}")
            break

def baixar_series_temporais_bcb():
    print("\n[BCB] Verificando Séries Temporais (SGS)...")
    series_diarias = {'selic': 11, 'cdi': 12}
    series_outras = {'ipca': 433, 'inadimplencia_pj': 21082}
    series_completas = {**series_diarias, **series_outras}
    for nome, codigo in series_completas.items():
        print(f"  -> Verificando série: {nome.upper()}")
        arquivo_csv = os.path.join(BASE_DIR, 'bcb', 'sgs', f'{nome}.csv')
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
                if pd.to_datetime(start_update).date() >= datetime.today().date():
                    print(f"     '{nome.upper()}' já está atualizado.")
                    continue
                df_update = sgs.get({nome: codigo}, start=start_update)
                if not df_update.empty:
                    df_update.reset_index(inplace=True)
                    df_update.to_csv(arquivo_csv, mode='a', header=False, index=False)
                    print(f"     {len(df_update)} novos registros adicionados a '{nome}.csv'.")
                else:
                    print(f"     Nenhum dado novo para '{nome.upper()}'.")
        except Exception as e:
            print(f"  -> ERRO ao baixar série '{nome.upper()}': {e}")

def baixar_dados_pnad_ibge():
    print("\n[IBGE] Verificando dados da PNAD Contínua...")
    url_api = "https://apisidra.ibge.gov.br/values/t/4099/n1/1/v/4099/p/all"
    arquivo_csv = os.path.join(BASE_DIR, 'ibge', 'pnad_continua', 'taxa_desocupacao.csv')
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
        df_clean = df_clean.dropna(subset=['Date', 'Taxa_Desocupacao'])

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
    except Exception as e:
        print(f"[IBGE] ERRO ao processar dados da PNAD: {e}")

def baixar_dados_tesouro_direto():
    print("\n[Tesouro] Verificando preços e taxas dos títulos públicos...")
    url = "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecosTaxasTesouroDireto.csv"
    local_csv_path = os.path.join(BASE_DIR, 'tesouro_nacional', 'precos_taxas', 'tesouro_direto_historico.csv')
    try:
        response_head = requests.head(url, timeout=10)
        response_head.raise_for_status()
        remote_last_modified_str = response_head.headers.get('Last-Modified')
        if remote_last_modified_str and os.path.exists(local_csv_path):
            remote_dt = datetime.strptime(remote_last_modified_str, '%a, %d %b %Y %H:%M:%S %Z')
            local_ts = os.path.getmtime(local_csv_path)
            if remote_dt.timestamp() <= local_ts:
                print("     Os dados do Tesouro Direto já estão atualizados.")
                return
        print("     Nova versão encontrada. Baixando arquivo completo...")
        df = pd.read_csv(url, sep=';', decimal=',', encoding='latin-1')
        df.to_csv(local_csv_path, index=False)
        print(f"     Dados salvos em '{local_csv_path}'.")
    except Exception as e:
        print(f"[Tesouro] ERRO: {e}")

# ======================================================================
# 2A PARTE: dados adicionais que pensei
def criar_pastas_adicionais():
    print(f"\nDiretório base para dados adicionais: '{BASE_DIR_ADICIONAIS}'")
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'pesquisas_mensais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'contas_regionais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'trabalho', 'caged'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'tse', 'eleitorado'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR_ADICIONAIS, 'inmet', 'clima'), exist_ok=True)
    print("Estrutura de pastas (adicionais) verificada/criada.")

def _str_norm(s):
    return re.sub(r'\s+', ' ', (s or '')).strip().lower()

def _match_all_terms(text, termos):
    t = _str_norm(text)
    return all(_str_norm(term) in t for term in termos)

def buscar_variavel_por_texto(agregado, termos_busca):
    IBGE_V3_API = "https://servicodados.ibge.gov.br/api/v3/agregados"
    url = f"{IBGE_V3_API}/{agregado}/variaveis?localidades=N1[all]"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    vars_json = r.json()
    for v in vars_json:
        desc = v.get('nome') or v.get('variavel') or v.get('descricao') or ''
        if _match_all_terms(desc, termos_busca):
            return v.get('id')
    for v in vars_json:
        desc = v.get('nome') or v.get('variavel') or v.get('descricao') or ''
        if 'índice' in _str_norm(desc):
            return v.get('id')
    return None

def baixar_dados_agregados_v3_auto(agregado, termos_busca, nome_arquivo):
    print(f"  -> Verificando (v3): {nome_arquivo}")
    out_csv = os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'pesquisas_mensais', f'{nome_arquivo}.csv')
    try:
        var_id = buscar_variavel_por_texto(agregado, termos_busca)
        if not var_id:
            print(f"     AVISO: Não foi possível encontrar a variável para {termos_busca} no agregado {agregado}.")
            return
        IBGE_V3_API = "https://servicodados.ibge.gov.br/api/v3/agregados"
        url = f"{IBGE_V3_API}/{agregado}/periodos/all/variaveis/{var_id}?localidades=N1[all]&view=flat"
        r = requests.get(url, timeout=90)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or len(data) == 0: return
        df = pd.DataFrame(data)
        df.to_csv(out_csv, index=False)
        print(f"     '{nome_arquivo}.csv' baixado/atualizado com {len(df)} linhas.")
    except Exception as e:
        print(f"     ERRO (v3) ao processar '{nome_arquivo}': {e}")

def chamar_downloads_ibge_mensal():
    print("\n[IBGE] Verificando Pesquisas Mensais (PMC, PMS, PIM)...")
    baixar_dados_agregados_v3_auto('8880', ['índice', 'volume'], 'pmc_volume_vendas')
    baixar_dados_agregados_v3_auto('5906', ['índice', 'volume'], 'pms_receita_servicos')
    baixar_dados_agregados_v3_auto('8160', ['índice', 'produção'], 'pim_producao_industrial')

def _is_year_dir(name):
    n = (name or '').strip('/').lower()
    return bool(re.fullmatch(r'\d{4}(_\d{4})?', n))

def _dir_end_year(name):
    n = (name or '').strip('/').lower()
    try:
        return int(n.split('_')[-1])
    except:
        return -1

def baixar_pib_municipios_ibge():
    print("\n[IBGE] Verificando PIB dos Municípios...")
    base = "https://ftp.ibge.gov.br/Pib_Municipios/"
    try:
        idx = requests.get(base, timeout=45)
        idx.raise_for_status()
        soup = BeautifulSoup(idx.text, "html.parser")
        subdirs = [a.get('href') for a in soup.find_all('a') if _is_year_dir(a.get('href'))]
        if not subdirs:
            print("     ERRO: Nenhuma pasta de ano encontrada no FTP do PIB.")
            return
        latest_dir = sorted(subdirs, key=_dir_end_year, reverse=True)[0]
        url_ano = urllib.parse.urljoin(base, latest_dir)
        
        # Tenta o subdiretório /base, o mais confiável
        url_base = urllib.parse.urljoin(url_ano, "base/")
        idx_base = requests.get(url_base, timeout=45)
        if idx_base.ok:
            soup_b = BeautifulSoup(idx_base.text, "html.parser")
            zips = [a.get('href') for a in soup_b.find_all('a') if 'base_de_dados' in a.get('href', '') and 'xlsx.zip' in a.get('href', '')]
            if zips:
                target_zip = zips[0]
                url_zip = urllib.parse.urljoin(url_base, target_zip)
                print(f"     Baixando '{target_zip}'...")
                r = requests.get(url_zip, timeout=180)
                r.raise_for_status()
                out_dir = os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'contas_regionais')
                with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                    zf.extractall(out_dir)
                print("     Extração concluída.")
                return
        print("     Nenhum ZIP de 'base_de_dados' encontrado no diretório mais recente.")
    except Exception as e:
        print(f"     ERRO ao processar PIB dos Municípios: {e}")

def baixar_novo_caged():
    print("\n[Trabalho] Verificando dados do Novo CAGED...")
    for months_ago in [2, 3]:
        data_ref = datetime.now() - relativedelta(months=months_ago)
        ano, ano_mes = data_ref.strftime('%Y'), data_ref.strftime('%Y%m')
        
        caminho_final_txt = os.path.join(BASE_DIR_ADICIONAIS, 'trabalho', 'caged', f"CAGEDMOV{ano_mes}.txt")
        if os.path.exists(caminho_final_txt):
            print(f"     Dados do CAGED para {ano_mes} já existem.")
            # Se encontrou o mais recente, não precisa procurar o mais antigo
            if months_ago == 2:
                return
            continue

        arq_7z = f"CAGEDMOV{ano_mes}.7z"
        local_7z_path = os.path.join(BASE_DIR_ADICIONAIS, 'trabalho', 'caged', arq_7z)
        download_ok = False
        
        # Tentativa 1: FTP
        try:
            print(f"     Tentando via FTP para {ano_mes}...")
            with FTP("ftp.mtps.gov.br", timeout=120) as ftp:
                ftp.login()
                ftp.cwd(f"/pdet/microdados/NOVO CAGED/{ano}/{ano_mes}/")
                with open(local_7z_path, 'wb') as f:
                    ftp.retrbinary(f"RETR {arq_7z}", f.write)
                download_ok = True
        except Exception:
            print(f"     Falha no FTP. Tentando via HTTP para {ano_mes}...")
            try:
                url = f"https://pdet.mte.gov.br/microdados/NOVO%20CAGED/{ano}/{ano_mes}/{arq_7z}"
                r = requests.get(url, timeout=300)
                r.raise_for_status()
                with open(local_7z_path, 'wb') as f:
                    f.write(r.content)
                download_ok = True
            except Exception as e_http:
                print(f"     Falha no FTP e HTTP para {ano_mes}: {e_http}")
                continue
        
        # Se o download (FTP ou HTTP) ocorreu, verifica o tamanho e descompacta
        if download_ok:
            # VERIFICAÇÃO DE TAMANHO: um arquivo de erro HTML é muito pequeno.
            if os.path.getsize(local_7z_path) < 100 * 1024: # 100 KB
                print(f"     Erro: O arquivo baixado para {ano_mes} é muito pequeno e provavelmente inválido. Deletando.")
                os.remove(local_7z_path)
                continue
            
            try:
                print(f"     Descompactando {arq_7z}...")
                with py7zr.SevenZipFile(local_7z_path, 'r') as z:
                    z.extractall(path=os.path.dirname(local_7z_path))
                os.remove(local_7z_path)
                print("     Arquivo descompactado.")
                return # Sucesso, para a função
            except Exception as e_zip:
                print(f"     Erro ao descompactar {arq_7z}: {e_zip}")
                if os.path.exists(local_7z_path):
                    os.remove(local_7z_path)

def baixar_perfil_eleitorado_tse():
    print("\n[TSE] Verificando Perfil do Eleitorado...")
    url = "https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_ATUAL.zip"
    out_dir = os.path.join(BASE_DIR_ADICIONAIS, 'tse', 'eleitorado')
    arquivo_zip = os.path.join(out_dir, 'perfil_eleitorado_ATUAL.zip')
    try:
        response_head = requests.head(url, timeout=15)
        response_head.raise_for_status()
        remote_last_modified = response_head.headers.get('Last-Modified')
        if os.path.exists(arquivo_zip) and remote_last_modified:
            local_mtime = os.path.getmtime(arquivo_zip)
            remote_dt = datetime.strptime(remote_last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            if local_mtime >= remote_dt.timestamp():
                print("     Dados do TSE já estão atualizados.")
                return
        print("     Nova versão encontrada. Baixando...")
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        with open(arquivo_zip, 'wb') as f: f.write(response.content)
        print("     Download completo. Extraindo...")
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(out_dir)
        print("     Extração concluída.")
    except Exception as e:
        print(f"     ERRO ao baixar dados do TSE: {e}")

def baixar_dados_climaticos_inmet():
    print("\n[INMET] Verificando dados climáticos...")
    pasta = os.path.join(BASE_DIR_ADICIONAIS, 'inmet', 'clima')
    estacoes = ['A904']
    anos = [datetime.now().year, datetime.now().year - 1]
    for ano in anos:
        url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
        zpath = os.path.join(pasta, f"inmet_{ano}.zip")
        try:
            print(f"     Verificando/Baixando dados para o ano {ano}...")
            r = requests.get(url, timeout=300)
            if r.status_code != 200: continue
            with open(zpath, "wb") as f: f.write(r.content)
            with zipfile.ZipFile(zpath, 'r') as zf:
                for cod in estacoes:
                    alvos = [n for n in zf.namelist() if cod in n and n.upper().endswith('.CSV')]
                    if not alvos: continue
                    dfs = []
                    for nome in alvos:
                        with zf.open(nome) as f:
                            try:
                                dfs.append(pd.read_csv(f, sep=';', encoding='latin-1', skiprows=8, decimal=',', on_bad_lines='skip'))
                            except Exception as e_csv:
                                print(f"       Aviso: não foi possível ler {nome} de {ano}.zip. Erro: {e_csv}")
                    if not dfs: continue
                    df_ano = pd.concat(dfs, ignore_index=True)
                    out_csv = os.path.join(pasta, f"estacao_{cod}.csv")
                    if os.path.exists(out_csv):
                        df_local = pd.read_csv(out_csv, sep=';')
                        subset_cols = [col for col in ['Data', 'Hora UTC'] if col in df_local.columns and col in df_ano.columns]
                        if subset_cols:
                           df_final = pd.concat([df_local, df_ano]).drop_duplicates(subset=subset_cols, keep='last')
                        else: # Se não houver colunas comuns para chave, apenas concatena
                           df_final = pd.concat([df_local, df_ano])
                    else:
                        df_final = df_ano
                    df_final.to_csv(out_csv, index=False, sep=';')
                    print(f"     Estação {cod} atualizada com dados de {ano}.")
            os.remove(zpath)
        except Exception as e:
            print(f"     ERRO ao processar INMET para o ano {ano}: {e}")

    print("\n[INMET] Verificando dados climáticos...")
    pasta = os.path.join(base_dir_adicionais, 'inmet', 'clima')
    estacoes = ['A904']
    anos = [datetime.now().year, datetime.now().year - 1]
    for ano in anos:
        url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
        zpath = os.path.join(pasta, f"inmet_{ano}.zip")
        try:
            print(f"     Verificando/Baixando dados para o ano {ano}...")
            r = requests.get(url, timeout=300)
            if r.status_code != 200: continue
            with open(zpath, "wb") as f: f.write(r.content)
            with zipfile.ZipFile(zpath, 'r') as zf:
                for cod in estacoes:
                    alvos = [n for n in zf.namelist() if cod in n and n.upper().endswith('.CSV')]
                    if not alvos: continue
                    dfs = []
                    for nome in alvos:
                        with zf.open(nome) as f:
                            try:
                                dfs.append(pd.read_csv(f, sep=';', encoding='latin-1', skiprows=8, decimal=',', on_bad_lines='skip'))
                            except Exception as e_csv:
                                print(f"       Aviso: não foi possível ler {nome} de {ano}.zip. Erro: {e_csv}")
                    if not dfs: continue
                    df_ano = pd.concat(dfs, ignore_index=True)
                    out_csv = os.path.join(pasta, f"estacao_{cod}.csv")
                    if os.path.exists(out_csv):
                        df_local = pd.read_csv(out_csv, sep=';')
                        subset_cols = [col for col in ['Data', 'Hora UTC'] if col in df_local.columns and col in df_ano.columns]
                        if subset_cols:
                           df_final = pd.concat([df_local, df_ano]).drop_duplicates(subset=subset_cols, keep='last')
                        else:
                           df_final = pd.concat([df_local, df_ano])
                    else:
                        df_final = df_ano
                    df_final.to_csv(out_csv, index=False, sep=';')
                    print(f"     Estação {cod} atualizada com dados de {ano}.")
            os.remove(zpath)
        except Exception as e:
            print(f"     ERRO ao processar INMET para o ano {ano}: {e}")