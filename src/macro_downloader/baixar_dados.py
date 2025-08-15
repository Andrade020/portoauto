import os
import requests
import pandas as pd
import zipfile
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bcb import sgs

# --- CONFIGURAÇÃO INICIAL ---
BASE_DIR = 'dados_fidc'
print(f"Diretório base para salvar os dados: '{BASE_DIR}'")

def criar_pastas():
    os.makedirs(os.path.join(BASE_DIR, 'cvm', 'informes_mensais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'bcb', 'sgs'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'ibge', 'pnad_continua'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'tesouro_nacional', 'precos_taxas'), exist_ok=True)
    print("Estrutura de pastas verificada/criada.")

# --- 1. COMISSÃO DE VALORES MOBILIÁRIOS (CVM) ---
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

# --- 2. BANCO CENTRAL (BCB) ---
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

# --- 3. IBGE (Instituto Brasileiro de Geografia e Estatística) ---
def baixar_dados_pnad_ibge():
    """
    CORREÇÃO 4: Adiciona 'errors='coerce'' para tornar a conversão de data robusta
    a dados inválidos que possam vir da API.
    """
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
        
        # CORREÇÃO: 'errors='coerce'' irá transformar qualquer 'Mes' inválido em NaT (Not a Time)
        df_clean['Date'] = pd.to_datetime(df_clean['Mes'], format='%Y%m', errors='coerce')
        
        # Esta linha agora irá remover tanto linhas sem 'Taxa_Desocupacao' quanto linhas com data inválida
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


# --- 4. TESOURO NACIONAL ---
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

def exibir_notas_fontes_manuais():
    print("\n" + "="*50 + "\nNOTAS IMPORTANTES SOBRE OUTRAS FONTES DE DADOS\n" + "="*50)
    print("[CVM] Regulamentos: Ação manual | [BCB] REF: Ação manual | [B3/ANBIMA]: Acesso pago/manual")

if __name__ == "__main__":
    criar_pastas()
    baixar_informes_mensais_fidc_cvm()
    baixar_series_temporais_bcb()
    baixar_dados_pnad_ibge()
    baixar_dados_tesouro_direto()
    exibir_notas_fontes_manuais()
    print("\nRotina de download concluída.")