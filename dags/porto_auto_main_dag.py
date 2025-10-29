import pendulum
import sys
import pandas as pd
import json
from pathlib import Path
from datetime import timedelta, datetime
from configparser import ConfigParser # Importa aqui no topo

# Imports do Airflow
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
# 'chain' não é mais necessário para esta lógica
# from airflow.utils.helpers import chain 
### 
print("0")
# --- DEFINIÇÕES DE CAMINHO ---
PATH_BASE = "/home/felipe/portoauto"
PATH_FERIADOS = Path(PATH_BASE) / "feriados_nacionais.csv"
PATH_JSONL_LOG = Path(PATH_BASE) / "processing_log.jsonl"
PATH_CONFIG = '/home/felipe/portoauto/config.cfg'

# --- MODIFICAÇÃO DE sys.path ---
sys.path.append(str(Path(PATH_BASE) / "dags" / "scripts"))

# --- IMPORTE O HANDLER PRIMEIRO E FORA DO TRY! ---
try:
    from common.vortx_handler import VortxHandler
except ImportError as e_handler:
    print(f"Erro CRÍTICO: Não foi possível importar o VortxHandler: {e_handler}")
    # Se não puder importar o Handler, define um Dummy que falha explicitamente
    class VortxHandler:
        def __init__(self, *args, **kwargs):
             raise ImportError("VortxHandler real não pôde ser importado.")
        # Adicione métodos dummy que levantam erro se necessário por outras partes do código
        def config(self): raise ImportError("Dummy VortxHandler")
        # ... outros métodos ...

# --- AGORA TENTE IMPORTAR OS SCRIPTS ---
try:
    from get_docs_fundo import main as main_docs
    from get_carteira_fundo import main as main_carteira
    from get_demonstrativo_caixa import main as main_caixa
    from get_extrato_bancario import main as main_extrato
    from get_recebiveis_dashboard import main as main_recebiveis_dashboard
    # main_relatorio já está definida globalmente
except ImportError as e_scripts:
    print(f"AVISO: Falha ao importar um ou mais scripts: {e_scripts}. Definindo Dummies.")
    # Define Dummies para os que podem ter falhado (SINTAXE CORRETA)
    if 'main_docs' not in locals():
        def main_docs(date): print(f"DUMMY DOCS: {date}")
    if 'main_carteira' not in locals():
        def main_carteira(date): print(f"DUMMY CARTEIRA: {date}")
    if 'main_caixa' not in locals():
        def main_caixa(date): print(f"DUMMY CAIXA: {date}")
    if 'main_extrato' not in locals():
        def main_extrato(date): print(f"DUMMY EXTRATO: {date}")
    if 'main_recebiveis_dashboard' not in locals():
        def main_recebiveis_dashboard(date): print(f"DUMMY DASHBOARD: {date}")

def _formatar_cnpj_com_mascara(cnpj_sem_mascara: str) -> str:
    """Formata CNPJ limpo para formato com máscara."""
    # Adicionado tratamento para CNPJs curtos ou inválidos
    if not cnpj_sem_mascara or len(cnpj_sem_mascara) < 14:
         return cnpj_sem_mascara # Retorna como está se inválido
    return f"{cnpj_sem_mascara[:2]}.{cnpj_sem_mascara[2:5]}.{cnpj_sem_mascara[5:8]}/{cnpj_sem_mascara[8:12]}-{cnpj_sem_mascara[12:]}"

def main_relatorio(data_str: str, tipo_relatorio: str):
    """
    Busca um relatório de download (Base64).
    """
    # Adicionado import do pendulum aqui dentro
    import pendulum 
    
    print(f"--- Iniciando busca de Relatório '{tipo_relatorio}' para {data_str} ---")
    # Instancia o handler aqui dentro (requer import global)
    handler = VortxHandler() 
    
    try:
        # Pega as configs
        cnpj_sem_mascara = handler.config.get('VORTX_RECEBIVEIS', 'cnpj_sem_mascara')
        extensao = handler.config.get('VORTX_RECEBIVEIS', 'extensao_relatorio')
        cnpj_com_mascara = _formatar_cnpj_com_mascara(cnpj_sem_mascara)
        
        url_endpoint = "/relatorios/download"
        params = {
            "cnpjFundo": cnpj_com_mascara,
            "tipoRelatorio": tipo_relatorio,
            "extensao": extensao
        }

        # Lógica de data
        if tipo_relatorio == "estoque":
            params["dataInicial"] = data_str
            params["dataFinal"] = data_str
            # Define data_ini/fim para nome do arquivo
            data_ini_nome = data_str
            data_fim_nome = data_str
        else:
            dia_da_execucao = pendulum.parse(data_str)
            inicio_mes_atual = dia_da_execucao.start_of('month')
            data_fim_mes_anterior = inicio_mes_atual.subtract(days=1)
            data_ini_mes_anterior = data_fim_mes_anterior.start_of('month')

            params["dataInicial"] = data_ini_mes_anterior.to_date_string()
            params["dataFinal"] = data_fim_mes_anterior.to_date_string()
            # Define data_ini/fim para nome do arquivo
            data_ini_nome = params["dataInicial"]
            data_fim_nome = params["dataFinal"]
            print(f"Relatório mensal. Buscando período: {data_ini_nome} a {data_fim_nome}")

        response = handler.make_recebiveis_request(url_endpoint, params)
        response_data = response.json()
        print(">>> Resposta recebida com sucesso. Decodificando...")

        base64_arquivo = response_data.get("arquivo")
        nome_arquivo_original = response_data.get("nome")
        
        if not base64_arquivo or not nome_arquivo_original:
            raise Exception(f"API não retornou 'arquivo' ou 'nome'. Resposta: {response_data}")

        # Chama save_base64_file com as datas corretas para o nome do arquivo
        handler.save_base64_file(base64_arquivo, 
                                 source_name=f"recebiveis_{tipo_relatorio}", 
                                 tipo=tipo_relatorio, 
                                 data_ref=data_str, # Data de referência da task
                                 data_ini=data_ini_nome, # Data inicial do relatório
                                 data_fim=data_fim_nome) # Data final do relatório
        
        print(f"--- Relatório '{tipo_relatorio}' concluído ---")

    except Exception as e:
        print(f"ERRO: Falha ao buscar o relatório '{tipo_relatorio}' para data {data_str}. {e}")
        # Propaga o erro para o Airflow marcar a task como falha
        raise Exception(f"Falha ao buscar o relatório '{tipo_relatorio}' para data {data_str}: {e}")

def salvar_log_jsonl(data_str: str, task_name: str, status: str, mensagem: str):
    """Anexa log ao arquivo JSONL."""
    log_entry = {
        "data": data_str, "task": task_name, "status": status,
        "mensagem": mensagem, "timestamp": datetime.now().isoformat()
    }
    try:
        with open(PATH_JSONL_LOG, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    except Exception as e:
        print(f"ERRO CRÍTICO ao salvar log JSONL: {e}")

def is_first_business_day(dt: pendulum.DateTime, feriados: set) -> bool:
    """Verifica se é o primeiro dia útil do mês."""
    if dt.day_of_week >= 5 or dt.to_date_string() in feriados: return False
    for d in range(1, dt.day):
        dt_anterior = dt.replace(day=d)
        if dt_anterior.day_of_week < 5 and dt_anterior.to_date_string() not in feriados:
            return False
    return True

def carregar_feriados(path_feriados):
    """Lê feriados do CSV."""
    try:
        df = pd.read_csv(path_feriados)
        feriados_dt = pd.to_datetime(df['Data'], errors='coerce').dropna()
        return set(feriados_dt.dt.strftime('%Y-%m-%d'))
    except FileNotFoundError:
        print(f"Aviso: Arquivo de feriados não encontrado em {path_feriados}.")
        return set()
    except Exception as e:
        print(f"Erro ao carregar feriados: {e}")
        return set()


def get_previous_business_days(end_date_str: str, num_days: int, feriados: set) -> list[str]:
    """Calcula os N dias úteis anteriores (inclusive)."""
    business_days = []
    current_date = pendulum.parse(end_date_str)
    attempts = 0 # Segurança contra loops infinitos
    max_attempts = num_days + 30 # Tenta buscar um pouco mais que N dias
    
    while len(business_days) < num_days and attempts < max_attempts:
        if current_date.day_of_week < 5 and current_date.to_date_string() not in feriados:
            business_days.append(current_date.to_date_string())
        current_date = current_date.subtract(days=1)
        attempts += 1
        if current_date.year < 2000: break # Limite de segurança

    if attempts >= max_attempts and len(business_days) < num_days:
         print(f"AVISO: Não foi possível encontrar {num_days} dias úteis após {max_attempts} tentativas.")

    business_days.reverse() # Retorna do mais antigo para o mais novo
    return business_days

# --- DEFINIÇÃO DA DAG ---
default_args = {
    "owner": "felipe",
    "email": ["seu_email@provedor.com"], # <<< MUDE SEU EMAIL AQUI
    "email_on_failure": True, # Envia email se uma task FALHAR
    "email_on_retry": False,
}

@dag(
    dag_id="porto_auto_vortx_pipeline_lookback", 
    start_date=pendulum.datetime(2025, 10, 28, tz="America/Sao_Paulo"),
    schedule="0 12 * * *", # Meio-dia
    catchup=False,
    default_args=default_args,
    doc_md="DAG para pipeline da Vórtx com lookback de N dias úteis.",
    tags=["portoauto", "vortx", "lookback"],
    max_active_runs=1,
    concurrency=16 # Permite mais tasks paralelas
)
def porto_auto_pipeline_lookback():

    @task(task_id="get_lookback_dates")
    def get_lookback_dates_task(**kwargs):
        """Calcula os N dias úteis anteriores a D-1."""
        logical_date = pendulum.parse(kwargs['ds']) 
        try:
             parser = ConfigParser()
             parser.read(PATH_CONFIG)
             n_days = int(parser.get('DEFAULT', 'lookback_business_days', fallback=3))
        except Exception as e:
             print(f"AVISO: Falha ao ler lookback_business_days. Usando 3. Erro: {e}")
             n_days = 3
             
        feriados = carregar_feriados(PATH_FERIADOS)
        lookback_end_date = logical_date.subtract(days=1) # D-1
        
        dates_to_process = get_previous_business_days(
            lookback_end_date.to_date_string(), n_days, feriados
        )
        if not dates_to_process:
             print("Nenhum dia útil encontrado para processar no período de lookback.")
             # Retorna lista vazia para o expand não criar tasks
             # Ou você pode querer falhar a DAG aqui
             # from airflow.exceptions import AirflowSkipException
             # raise AirflowSkipException("Nenhum dia útil para processar.")
        
        print(f"Processando {len(dates_to_process)} dias úteis: {dates_to_process}")
        return dates_to_process

    # --- TASKS INDIVIDUAIS (Hard Fail) ---
    # Removido 'soft_fail' e verificação de 'skip'
    
    @task
    def run_docs(data_str: str): 
        try:
            main_docs(data_str)
            salvar_log_jsonl(data_str, "docs", "sucesso", "Documentos OK") 
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "docs", "falha", error_message)
            print(f"HARD FAIL: Task run_docs falhou para {data_str}. Erro: {error_message}")
            raise e 

    @task
    def run_carteira(data_str: str): 
        try:
            main_carteira(data_str)
            salvar_log_jsonl(data_str, "carteira", "sucesso", "Carteira OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "carteira", "falha", error_message)
            print(f"HARD FAIL: Task run_carteira falhou para {data_str}. Erro: {error_message}")
            raise e

    @task
    def run_caixa(data_str: str): 
        try:
            main_caixa(data_str)
            salvar_log_jsonl(data_str, "caixa", "sucesso", "Caixa OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "caixa", "falha", error_message)
            print(f"HARD FAIL: Task run_caixa falhou para {data_str}. Erro: {error_message}")
            raise e

    @task
    def run_extrato(data_str: str): 
        try:
            main_extrato(data_str)
            salvar_log_jsonl(data_str, "extrato", "sucesso", "Extrato OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "extrato", "falha", error_message)
            print(f"HARD FAIL: Task run_extrato falhou para {data_str}. Erro: {error_message}")
            raise e

    @task
    def run_relatorio_estoque(data_str: str): 
        try:
            main_relatorio(data_str, tipo_relatorio="estoque")
            salvar_log_jsonl(data_str, "relatorio_estoque", "sucesso", "Relatório Estoque OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "relatorio_estoque", "falha", error_message)
            print(f"HARD FAIL: Task run_relatorio_estoque falhou para {data_str}. Erro: {error_message}")
            raise e

    @task
    def run_relatorio_aquisicao(data_str: str): 
        feriados = carregar_feriados(PATH_FERIADOS)
        # Tenta parsear a data, se falhar, considera inválida e ignora
        try:
             dt_parsed = pendulum.parse(data_str)
        except Exception:
             print(f"Pulando 'aquisicao' para data inválida: {data_str}")
             salvar_log_jsonl(data_str, "relatorio_aquisicao", "ignorado", "Data inválida")
             return

        if not is_first_business_day(dt_parsed, feriados):
            print(f"Pulando 'aquisicao' para {data_str}: não é o 1º dia útil do mês.")
            salvar_log_jsonl(data_str, "relatorio_aquisicao", "ignorado", "Não é 1º dia útil")
            return 
            
        try:
            main_relatorio(data_str, tipo_relatorio="aquisicao")
            salvar_log_jsonl(data_str, "relatorio_aquisicao", "sucesso", "Relatório Aquisição OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "relatorio_aquisicao", "falha", error_message)
            print(f"HARD FAIL: Task run_relatorio_aquisicao falhou para {data_str}. Erro: {error_message}")
            raise e
            
    @task
    def run_relatorio_liquidacao(data_str: str): 
        feriados = carregar_feriados(PATH_FERIADOS)
        try:
             dt_parsed = pendulum.parse(data_str)
        except Exception:
             print(f"Pulando 'liquidacao' para data inválida: {data_str}")
             salvar_log_jsonl(data_str, "relatorio_liquidacao", "ignorado", "Data inválida")
             return

        if not is_first_business_day(dt_parsed, feriados):
            print(f"Pulando 'liquidacao' para {data_str}: não é o 1º dia útil do mês.")
            salvar_log_jsonl(data_str, "relatorio_liquidacao", "ignorado", "Não é 1º dia útil")
            return

        try:
            main_relatorio(data_str, tipo_relatorio="liquidacao")
            salvar_log_jsonl(data_str, "relatorio_liquidacao", "sucesso", "Relatório Liquidação OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "relatorio_liquidacao", "falha", error_message)
            print(f"HARD FAIL: Task run_relatorio_liquidacao falhou para {data_str}. Erro: {error_message}")
            raise e

    @task
    def run_dashboard_recebiveis(data_str: str): 
        try:
            main_recebiveis_dashboard(data_str)
            salvar_log_jsonl(data_str, "dashboard_recebiveis", "sucesso", "Dashboard Recebíveis OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "dashboard_recebiveis", "falha", error_message)
            print(f"HARD FAIL: Task run_dashboard_recebiveis falhou para {data_str}. Erro: {error_message}")
            raise e

    # --- TASK GROUP ÚNICO ---
    @task_group(group_id="processar_dia_vortx")
    def processar_dia_vortx_group(data_str: str):
        """Grupo de tasks para processar todos os tipos para um dia."""
        run_docs(data_str)
        run_carteira(data_str)
        run_caixa(data_str)
        run_extrato(data_str)
        run_dashboard_recebiveis(data_str)
        run_relatorio_estoque(data_str)
        run_relatorio_aquisicao(data_str)
        run_relatorio_liquidacao(data_str)

    # --- DEFINE A ORDEM DE EXECUÇÃO ---
    lista_de_datas = get_lookback_dates_task()
    processar_dia_vortx_group.expand(data_str=lista_de_datas)

# Chama a função para registrar a DAG
porto_auto_pipeline_lookback()