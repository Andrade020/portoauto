 #obs:
# esse versão eu não baixava necesariamente os últimos dados, então refiz o programa em outro
# .py, procure por ele

 #  eu estou usando caminhos relativos, entao nao dá pra executar no modo interativo, 
# como jupyter
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


# --- confgr incl ---
BASE_DIR = os.path.join('data_raw','dados_macro')
print(f"Diretório base para salvar os dados: '{BASE_DIR}'")
#################################################################

def criar_pastas():
    os.makedirs(os.path.join(BASE_DIR, 'cvm', 'informes_mensais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'bcb', 'sgs'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'ibge', 'pnad_continua'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'tesouro_nacional', 'precos_taxas'), exist_ok=True)
    print("Estrutura de pastas verificada/criada.")
####################################################################

# cvm
def baixar_informes_mensais_fidc_cvm():
    print("\n[CVM] Verificando informes mensais de FIDCs...")
    unzip_dir = os.path.join(BASE_DIR, 'cvm', 'informes_mensais')
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
###############################################################################

# bacen
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

#  ibge 
def baixar_dados_pnad_ibge():

    print("\n[IBGE] Verificando dados da PNAD Contínua (Taxa de Desocupação)...")
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
        
        # corrc: 'errrs='corce'' ira transf qlq 'mes' invld em nat (not a time)
        df_clean['Date'] = pd.to_datetime(df_clean['Mes'], format='%Y%m', errors='coerce')
        
        # esta linha agra ira remvr tanto linhs sem 'tax_ds' qunto linhs com data invld
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
    local_csv_path = os.path.join(BASE_DIR, 'tesouro_nacional', 'precos_taxas', 'tesouro_direto_historico.csv')
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
          ###################################################################################################

#def exibir_notas_fontes_manuais():
#    print("\n" + "="*50 + "\nNOTAS IMPORTANTES SOBRE OUTRAS FONTES DE DADOS\n" + "="*50)
#    print("[CVM] Regulamentos: Ação manual | [BCB] REF: Ação manual | [B3/ANBIMA]: Acesso pago/manual")

###################################################################################
# BLOCO 2: ROTINA DE DOWNLOAD DE DADOS ADICIONAIS (SCRIPT 2)
###################################################################################

# --- Configurações do Bloco 2 ---
BASE_DIR_ADICIONAIS = os.path.join( 'data_raw','dados_macro_adicionais')
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

# ======================================================================
# 1) IBGE – API V3 (Agregados) — coleta automática de variável (view=flat)
# ======================================================================

IBGE_V3 = "https://servicodados.ibge.gov.br/api/v3/agregados"

def _str_norm(s):
    return re.sub(r'\s+', ' ', (s or '')).strip().lower()

def _match_all_terms(text, termos):
    t = _str_norm(text)
    return all(_str_norm(term) in t for term in termos)

def buscar_variavel_por_texto(agregado, termos_busca, nivel_local="N1"):
    """
    Busca uma variável do agregado cuja descrição contenha TODOS os termos indicados.
    Usa endpoint: /agregados/{agregado}/variaveis?localidades=N1[all]
    Retorna (var_id, var_desc) ou (None, None) se não encontrar.
    """
    url = f"{IBGE_V3}/{agregado}/variaveis?localidades={nivel_local}[all]"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    vars_json = r.json()
    # A resposta pode vir como lista de objetos de variável
    for v in vars_json:
        desc = v.get('nome') or v.get('variavel') or v.get('descricao') or ''
        if _match_all_terms(desc, termos_busca):
            return v.get('id'), desc
    # Se não bateu todos, tenta heurística: pegar a primeira variável com "índice"
    for v in vars_json:
        desc = v.get('nome') or v.get('variavel') or v.get('descricao') or ''
        if 'índice' in _str_norm(desc) or 'indice' in _str_norm(desc):
            return v.get('id'), desc
    return None, None

def baixar_dados_agregados_v3_auto(agregado, termos_busca, nome_arquivo, nivel_local="N1"):
    """
    1) Descobre variável pelo texto (ex.: ['índice','volume']) no agregado.
    2) Baixa /periodos/all/variaveis/{var}?localidades=N1[all]&view=flat
    3) Salva CSV.
    """
    print(f"  -> Verificando (v3): {nome_arquivo}")
    out_csv = os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'pesquisas_mensais', f'{nome_arquivo}.csv')
    if os.path.exists(out_csv):
        print(f"     Arquivo '{nome_arquivo}.csv' já existe. Pulando.")
        return

    try:
        var_id, var_desc = buscar_variavel_por_texto(agregado, termos_busca, nivel_local=nivel_local)
        if not var_id:
            print(f"     Não localizei variável compatível em {agregado} para termos {termos_busca}.")
            return

        url = (f"{IBGE_V3}/{agregado}/periodos/all/variaveis/{var_id}"
               f"?localidades={nivel_local}[all]&view=flat")
        r = requests.get(url, timeout=90)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or len(data) == 0:
            print(f"     API v3 retornou vazio para '{nome_arquivo}'.")
            return

        # 'view=flat' normalmente traz colunas padronizadas (V, D1N, D2N, etc.)
        df = pd.DataFrame(data)
        # Renomeia algumas colunas comuns, se existirem:
        ren = {}
        if 'V' in df.columns: ren['V'] = 'valor'
        if 'D2N' in df.columns: ren['D2N'] = 'periodo'
        if 'D1N' in df.columns: ren['D1N'] = 'localidade'
        df = df.rename(columns=ren)
        # Mantém apenas algumas colunas úteis quando existirem
        cols_prior = [c for c in ['localidade','periodo','valor'] if c in df.columns]
        if cols_prior:
            df = df[cols_prior + [c for c in df.columns if c not in cols_prior]]

        df.to_csv(out_csv, index=False)
        print(f"     '{nome_arquivo}.csv' criado com {len(df)} linhas (agregado {agregado}, var {var_id}).")

    except Exception as e:
        print(f"     ERRO (v3) ao processar '{nome_arquivo}': {e}")

def chamar_downloads_ibge_mensal():
    print("\n[IBGE] Verificando Pesquisas Mensais (PMC, PMS, PIM)...")
    # Agregados atuais (base 2022 / pós-substituições oficiais do IBGE):
    # PMC (varejo): 8880 – procurar algo como "índice" + "volume"
    baixar_dados_agregados_v3_auto(agregado='8880',
                                   termos_busca=['índice','volume'],
                                   nome_arquivo='pmc_volume_vendas')

    # PMS (serviços): 5906 – procurar "índice" + "volume" + "serviços"
    baixar_dados_agregados_v3_auto(agregado='5906',
                                   termos_busca=['índice','volume','servi'],
                                   nome_arquivo='pms_receita_servicos')

    # PIM (produção industrial): 8160 – procurar "índice" + "produção"
    baixar_dados_agregados_v3_auto(agregado='8160',
                                   termos_busca=['índice','produ'],
                                   nome_arquivo='pim_producao_industrial')


# ======================================================================
# 2) IBGE – PIB dos Municípios (descoberta dinâmica em /Pib_Municipios/<ANO>/base/)
# ======================================================================


def _is_year_dir(name):
    # aceita "2021/" ou "2010_2021/"
    n = name.strip('/').lower()
    return bool(re.fullmatch(r'\d{4}(_\d{4})?', n))

def _dir_end_year(name):
    n = name.strip('/').lower()
    if '_' in n:
        a, b = n.split('_', 1)
        try:
            return int(b)
        except:
            return int(a)
    try:
        return int(n)
    except:
        return -1

def baixar_pib_municipios_ibge():
    print("\n[IBGE] Verificando PIB dos Municípios...")
    base = "https://ftp.ibge.gov.br/Pib_Municipios/"

    try:
        idx = requests.get(base, timeout=45)
        idx.raise_for_status()
        soup = BeautifulSoup(idx.text, "html.parser")

        # lista subpastas com anos
        subdirs = [a.get('href') for a in soup.find_all('a') if (a.get('href') or '').endswith('/')]
        anos = [d for d in subdirs if _is_year_dir(d)]
        if not anos:
            raise RuntimeError("Nenhuma pasta de ano encontrada em /Pib_Municipios/")

        # escolhe a pasta cujo 'ano final' seja o mais alto
        anos.sort(key=_dir_end_year, reverse=True)
        pasta_ano = anos[0]
        url_ano = urllib.parse.urljoin(base, pasta_ano)

        # prioriza /base/ com zip "base_de_dados_*_xlsx.zip"
        url_base = urllib.parse.urljoin(url_ano, "base/")
        try:
            idx_base = requests.get(url_base, timeout=45)
            idx_base.raise_for_status()
            soup_b = BeautifulSoup(idx_base.text, "html.parser")
            zips = [a.get('href') for a in soup_b.find_all('a') if (a.get('href') or '').lower().endswith('.zip')]
            # filtra por base_de_dados_*_xlsx.zip se existir
            zips_pref = [z for z in zips if 'base_de_dados' in z.lower() and 'xlsx' in z.lower()]
            target_zip = (sorted(zips_pref) or sorted(zips))[-1] if (zips_pref or zips) else None
            if target_zip:
                url_zip = urllib.parse.urljoin(url_base, target_zip)
                out_zip = os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'contas_regionais', target_zip)
                if os.path.exists(out_zip):
                    print("     Base do PIB dos Municípios já foi baixada. Pulando.")
                    return
                print(f"     Baixando '{target_zip}' de {url_zip} ...")
                r = requests.get(url_zip, timeout=180)
                r.raise_for_status()
                with open(out_zip, 'wb') as f:
                    f.write(r.content)
                print("     Extraindo...")
                with zipfile.ZipFile(out_zip, 'r') as zf:
                    zf.extractall(os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'contas_regionais'))
                print("     Extração concluída.")
                return
        except Exception:
            pass

        # fallback: tentar /xlsx/tabelas_completas.xlsx
        for sub in ['xlsx/', 'ods/']:
            url_alt = urllib.parse.urljoin(url_ano, sub)
            resp = requests.get(url_alt, timeout=45)
            if resp.ok:
                soup_alt = BeautifulSoup(resp.text, "html.parser")
                arquivos = [a.get('href') for a in soup_alt.find_all('a') if a.get('href')]
                # prioriza tabelas_completas
                candidatos = [x for x in arquivos if 'tabelas_completas' in x.lower()]
                if not candidatos:
                    # qualquer planilha como último recurso
                    candidatos = [x for x in arquivos if x.lower().endswith(('.xlsx','.ods','.zip'))]
                if candidatos:
                    nome = sorted(candidatos)[-1]
                    url_arq = urllib.parse.urljoin(url_alt, nome)
                    out_path = os.path.join(BASE_DIR_ADICIONAIS, 'ibge', 'contas_regionais', nome)
                    if os.path.exists(out_path):
                        print("     Base do PIB dos Municípios já foi baixada (formato alternativo). Pulando.")
                        return
                    print(f"     Baixando '{nome}' de {url_arq} ...")
                    r = requests.get(url_arq, timeout=180)
                    r.raise_for_status()
                    with open(out_path, 'wb') as f:
                        f.write(r.content)
                    print("     Download concluído (formato alternativo).")
                    return

        raise RuntimeError("Não foi possível localizar ZIP/planilha de PIB em /<ANO>/{base|xlsx|ods}/")

    except Exception as e:
        print(f"     ERRO ao processar PIB dos Municípios: {e}")


# ======================================================================
# 3) Ministério do Trabalho – NOVO CAGED (FTP oficial + fallback HTTP)
# ======================================================================

def baixar_novo_caged():
    print("\n[Trabalho] Verificando dados do Novo CAGED...")
    data_ref = datetime.now() - relativedelta(months=2)
    ano = data_ref.strftime('%Y')
    ano_mes = data_ref.strftime('%Y%m')

    caminho_final_txt = os.path.join(BASE_DIR_ADICIONAIS, 'trabalho', 'caged', f"CAGEDMOV{ano_mes}.txt")
    if os.path.exists(caminho_final_txt):
        print(f"     Dados do CAGED para {ano_mes} já existem. Pulando.")
        return

    ftp_host = "ftp.mtps.gov.br"
    ftp_dir = f"/pdet/microdados/NOVO CAGED/{ano}/{ano_mes}/"
    arq_7z = f"CAGEDMOV{ano_mes}.7z"
    local_7z_path = os.path.join(BASE_DIR_ADICIONAIS, 'trabalho', 'caged', arq_7z)

    try:
        print(f"     Conectando ao FTP {ftp_host} ...")
        with FTP(ftp_host, timeout=120) as ftp:
            ftp.login()  # anônimo
            ftp.cwd(ftp_dir)
            with open(local_7z_path, 'wb') as f:
                ftp.retrbinary(f"RETR {arq_7z}", f.write)

        print("     Download completo (FTP). Descompactando .7z...")
        with py7zr.SevenZipFile(local_7z_path, 'r') as z:
            z.extractall(path=os.path.join(BASE_DIR_ADICIONAIS, 'trabalho', 'caged'))
        os.remove(local_7z_path)
        print("     Arquivo descompactado e .7z removido.")

    except Exception as e:
        print(f"     Erro no FTP ({e}). Tentando fallback HTTP...")
        url = f"https://pdet.mte.gov.br/microdados/NOVO%20CAGED/{ano}/{ano_mes}/{arq_7z}"
        try:
            r = requests.get(url, timeout=300)
            r.raise_for_status()
            with open(local_7z_path, 'wb') as f:
                f.write(r.content)
            print("     Download (HTTP) ok. Descompactando .7z...")
            with py7zr.SevenZipFile(local_7z_path, 'r') as z:
                z.extractall(path=os.path.join(BASE_DIR_ADICIONAIS, 'trabalho', 'caged'))
            os.remove(local_7z_path)
            print("     Arquivo descompactado e .7z removido.")
        except Exception as e2:
            print(f"     ERRO ao processar CAGED (HTTP fallback): {e2}")


# ======================================================================
# 4) TSE – Perfil do Eleitorado (HEAD + diff de timestamp)
# ======================================================================

def baixar_perfil_eleitorado_tse():
    print("\n[TSE] Verificando Perfil do Eleitorado...")
    url = "https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_ATUAL.zip"
    arquivo_zip = os.path.join(BASE_DIR_ADICIONAIS, 'tse', 'eleitorado', 'perfil_eleitorado.zip')
    try:
        response_head = requests.head(url, timeout=15)
        response_head.raise_for_status()
        remote_last_modified = response_head.headers.get('Last-Modified')

        if os.path.exists(arquivo_zip) and remote_last_modified:
            local_mtime = os.path.getmtime(arquivo_zip)
            remote_dt = datetime.strptime(remote_last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            if local_mtime >= remote_dt.timestamp():
                print("     Dados do TSE já estão atualizados. Pulando.")
                return

        print("     Nova versão dos dados do TSE encontrada. Baixando...")
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        with open(arquivo_zip, 'wb') as f:
            f.write(response.content)
        print("     Download completo. Extraindo...")
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(BASE_DIR_ADICIONAIS, 'tse', 'eleitorado'))
        print("     Extração concluída.")
    except Exception as e:
        print(f"     ERRO ao baixar dados do TSE: {e}")


# ======================================================================
# INMET – via "Dados Históricos" (ZIP anual) -> filtra estações alvo
# ======================================================================

def _baixar_zip_inmet_ano(ano, pasta_dest):
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
    try:
        h = requests.head(url, timeout=30)
        if not h.ok:
            print(f"     {ano}.zip indisponível ({h.status_code}).")
            return None
        print(f"     Baixando {ano}.zip de {url} ...")
        r = requests.get(url, timeout=300)
        r.raise_for_status()
        zpath = os.path.join(pasta_dest, f"inmet_{ano}.zip")
        with open(zpath, "wb") as f:
            f.write(r.content)
        return zpath
    except Exception as e:
        print(f"     Falhou baixar {ano}.zip: {e}")
        return None

def _ler_csv_inmet(fileobj):
    """
    Lê CSV do INMET tentando primeiro pular 8 linhas de metadados (padrão frequente),
    depois sem skiprows. Retorna DataFrame (ou None).
    """
    for skip in (8, 0):
        try:
            df = pd.read_csv(fileobj, sep=';', encoding='latin-1',
                             on_bad_lines='skip', dtype='object', skiprows=skip)
            if df is not None and len(df.columns) >= 5:
                return df
        except Exception:
            fileobj.seek(0) if hasattr(fileobj, 'seek') else None
            continue
    return None

def _normalizar_colunas_inmet(df):
    # Mapeia variações comuns de nomes
    ren = {
        'Data': 'DATA (YYYY-MM-DD)',
        'Hora UTC': 'HORA (UTC)',
        'HORA (UTC)': 'HORA (UTC)'
    }
    df = df.rename(columns={c: ren.get(c, c) for c in df.columns})
    # Apenas ordena se as colunas padrão existirem
    base_cols = [c for c in ['ESTACAO', 'DATA (YYYY-MM-DD)', 'HORA (UTC)'] if c in df.columns]
    if base_cols:
        outras = [c for c in df.columns if c not in base_cols]
        df = df[base_cols + outras]
    return df

def baixar_dados_climaticos_inmet():
    print("\n[INMET] Verificando dados climáticos (Dados Históricos)...")
    pasta = os.path.join(BASE_DIR_ADICIONAIS, 'inmet', 'clima')
    os.makedirs(pasta, exist_ok=True)

    # Ajuste aqui as estações prioritárias (A904 = Sorriso):
    estacoes_alvo = ['A904']  # pode incluir ['A904','A917','A901', ...]
    anos_tentar = [datetime.now().year, datetime.now().year - 1]

    urls_testadas = []
    total_registros = 0
    try:
        for ano in anos_tentar:
            zpath = _baixar_zip_inmet_ano(ano, pasta)
            if not zpath:
                urls_testadas.append(f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip")
                continue

            try:
                with zipfile.ZipFile(zpath, 'r') as zf:
                    nomes = zf.namelist()
                    # para cada estação, pegue os arquivos cujo nome contenha o código
                    for cod in estacoes_alvo:
                        alvos = [n for n in nomes if cod.upper() in os.path.basename(n).upper()
                                 and n.lower().endswith(('.csv', '.txt'))]
                        if not alvos:
                            continue

                        # lê cada arquivo da estação e acumula
                        dfs = []
                        for nome in sorted(alvos):
                            try:
                                with zf.open(nome) as fh:
                                    by = io.BytesIO(fh.read())
                                    df = _ler_csv_inmet(by)
                                    if df is None or df.empty:
                                        continue
                                    df['__ARQUIVO_ORIGEM__'] = os.path.basename(nome)
                                    dfs.append(_normalizar_colunas_inmet(df))
                            except Exception:
                                continue

                        if dfs:
                            df_all = pd.concat(dfs, ignore_index=True)
                            # Deduplicação simples por (Data, Hora) se existir:
                            keys = [k for k in ['DATA (YYYY-MM-DD)', 'HORA (UTC)'] if k in df_all.columns]
                            if keys:
                                df_all = df_all.drop_duplicates(subset=keys)

                            out_csv = os.path.join(pasta, f"estacao_{cod}.csv")
                            # append/merge se já existir
                            if os.path.exists(out_csv):
                                try:
                                    df_old = pd.read_csv(out_csv, sep=';', on_bad_lines='skip', dtype='object')
                                    cols_comuns = list(set(df_old.columns) & set(df_all.columns))
                                    if cols_comuns:
                                        df_merged = pd.concat([df_old[cols_comuns], df_all[cols_comuns]], ignore_index=True)
                                        if keys and all(k in df_merged.columns for k in keys):
                                            df_merged = df_merged.drop_duplicates(subset=keys)
                                        df_merged.to_csv(out_csv, index=False, sep=';')
                                    else:
                                        df_all.to_csv(out_csv, index=False, sep=';')
                                except Exception:
                                    df_all.to_csv(out_csv, index=False, sep=';')
                            else:
                                df_all.to_csv(out_csv, index=False, sep=';')

                            nlin = len(df_all)
                            total_registros += nlin
                            print(f"     OK: '{os.path.basename(out_csv)}' atualizado ({nlin} linhas do ano {ano}).")
            finally:
                # opcional: remover o zip para economizar espaço
                try:
                    os.remove(zpath)
                except Exception:
                    pass

        if total_registros == 0:
            print("     Não encontrei arquivos para as estações e anos testados.")
            readme = os.path.join(pasta, "README.txt")
            with open(readme, "w", encoding="utf-8") as f:
                f.write("INMET - Nenhum arquivo encontrado nos Dados Históricos para as estações/anos configurados.\n")
                f.write("Tente abrir manualmente (verifica se existe e baixa no navegador):\n")
                for ano in anos_tentar:
                    u = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
                    urls_testadas.append(u)
                    f.write(f" - {u}\n")
            print(f"     Anotei as URLs testadas em: {readme}")
        else:
            print(f"     Total de linhas salvas/atualizadas (todas as estações): {total_registros}")

    except Exception as e:
        print(f"     ERRO ao processar INMET (Dados Históricos): {e}")


# ======================================================================
# 6) Fontes para ação manual (mantido)
# ======================================================================

def exibir_notas_fontes_manuais():
    print("\n" + "=" * 50)
    print("FONTES ADICIONAIS RECOMENDADAS (AÇÃO MANUAL)")
    print("=" * 50)
    print("""
[FGV IBRE] Índices de Confiança:
  - Link: https://portal.fgv.br/ibre/estatisticas
[FENABRAVE] Emplacamentos de Veículos:
  - Link: https://www.fenabrave.org.br/
[ANEEL] Tarifas de Energia:
  - Link: https://www.gov.br/aneel/pt-br/dados/tarifas
[Consumidor.gov.br] Reclamações:
  - Link: https://www.consumidor.gov.br/pages/indicadores/
""")


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
        criar_pastas()

        if 'cvm' in DADOS_A_BAIXAR:
            baixar_informes_mensais_fidc_cvm()
        if 'bcb' in DADOS_A_BAIXAR:
            baixar_series_temporais_bcb()
        if 'ibge_pnad' in DADOS_A_BAIXAR:
            baixar_dados_pnad_ibge()
        if 'tesouro' in DADOS_A_BAIXAR:
            baixar_dados_tesouro_direto()

        #exibir_notas_fontes_manuais_fidc()
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

        #exibir_notas_fontes_manuais_adicionais()
        print("\nRotina de download de dados adicionais concluída.")
    
    if executar_bloco_1 or executar_bloco_2:
        print("\n" + "="*60)
        print(">>> TODAS AS ROTINAS SELECIONADAS FORAM EXECUTADAS <<<")
        print("="*60)