import pendulum
import sys
import pandas as pd
import json
from pathlib import Path
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.models import Variable

# --- CONFIGURAÇÕES DO PIPELINE ---
DATA_INICIO_GERAL = '2025-01-01'
# DATA_INICIO_GERAL = Variable.get("porto_auto_data_inicio", '2025-01-01')

PATH_BASE = "/home/felipe/portoauto"
PATH_FERIADOS = Path(PATH_BASE) / "feriados_nacionais.csv"
PATH_PYTHON = "/home/felipe/.pyenv/versions/3.13.0/bin/python"

# (REQ 2) Novo caminho para o log de status em JSON
PATH_JSON_STATUS = Path(PATH_BASE) / "processing_status.json"

sys.path.append(str(Path(PATH_BASE) / "dags" / "scripts"))

try:
    from get_docs_fundo import main as main_docs
    from get_carteira_fundo import main as main_carteira
    from get_demonstrativo_caixa import main as main_caixa
    from get_extrato_bancario import main as main_extrato
except ImportError:
    print("Erro: Não foi possível importar os scripts. Verifique os caminhos e o __init__.py")
    def main_docs(date): print(f"DUMMY DOCS: {date}")
    def main_carteira(date): print(f"DUMMY CARTEIRA: {date}")
    def main_caixa(date): print(f"DUMMY CAIXA: {date}")
    def main_extrato(date): print(f"DUMMY EXTRATO: {date}")

# --- (REQ 2) Funções de Log JSON ---

def carregar_status_json() -> dict:
    """Lê o arquivo de status JSON e retorna um dicionário."""
    try:
        with open(PATH_JSON_STATUS, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Se o arquivo não existe ou está corrompido, começa do zero
        return {}

def salvar_status_json(data_str: str, status: str, mensagem: str):
    """
    Carrega o JSON, atualiza uma data e salva o arquivo inteiro.
    Isto é "atômico" o suficiente para tasks sequenciais.
    """
    print(f"Salvando status: {data_str} | {status} | {mensagem[:50]}...")
    # Carrega o conteúdo atual
    status_completo = carregar_status_json()
    
    # Atualiza a entrada para esta data
    status_completo[data_str] = {
        "status": status,
        "mensagem": mensagem,
        "timestamp": datetime.now().isoformat()
    }
    
    # Escreve o arquivo inteiro de volta
    try:
        with open(PATH_JSON_STATUS, 'w') as f:
            json.dump(status_completo, f, indent=4)
    except Exception as e:
        print(f"ERRO CRÍTICO: Não foi possível salvar o log JSON! {e}")

# --- Fim das Funções JSON ---

def carregar_feriados(path_feriados):
    """Lê o CSV de feriados e retorna um set de datas."""
    try:
        df = pd.read_csv(path_feriados)
        feriados_dt = pd.to_datetime(df['Data'], errors='coerce')
        return set(feriados_dt.dt.strftime('%Y-%m-%d').dropna())
    except FileNotFoundError:
        print(f"Aviso: Arquivo de feriados não encontrado em {path_feriados}. Seguindo sem feriados.")
        return set()

default_args = {
    "owner": "felipe",
    "email": ["seu_email@provedor.com"], # <<< MUDE AQUI O SEU EMAIL
    "email_on_failure": True,
    "email_on_retry": False,
}

@dag(
    dag_id="porto_auto_vortx_pipeline_inteligente",
    start_date=pendulum.datetime(2025, 10, 15, tz="America/Sao_Paulo"),
    schedule="0 12 * * *", 
    catchup=False,
    default_args=default_args,
    doc_md="""
    DAG "inteligente" para pipeline da Vórtx.
    1. Processa D-1 (dia corrente) com retentativas.
    2. Calcula o backfill de dias úteis (presente -> passado).
    3. Processa o backfill sequencialmente.
    4. Salva todo o status (sucesso/falha) em 'processing_status.json'.
    """,
    tags=["portoauto", "vortx", "backfill", "inteligente"],
)
def porto_auto_pipeline():

    @task(task_id="get_datas_para_processar")
    def get_datas_para_processar(**kwargs):
        """
        Calcula a lista de dias de backfill a processar.
        Exclui fins de semana, feriados e dias com "sucesso" no log JSON.
        """
        data_inicio = pendulum.parse(DATA_INICIO_GERAL)
        data_fim_backfill = pendulum.parse(kwargs['data_interval_end'].to_date_string()).subtract(days=1)
        
        feriados = carregar_feriados(PATH_FERIADOS)
        # (REQ 2) Lê o novo log de status JSON
        status_json = carregar_status_json()
        
        datas_a_processar = []
        
        print("--- Calculando Backfill ---")
        print(f"Início: {data_inicio.to_date_string()}")
        print(f"Fim: {data_fim_backfill.to_date_string()}")
        print(f"Feriados carregados: {len(feriados)}")
        print(f"Status JSON carregado: {len(status_json)} datas")

        dt = data_inicio
        while dt <= data_fim_backfill:
            data_str = dt.to_date_string()
            
            # (REQ 2) Verifica o status no JSON
            status_data = status_json.get(data_str, {}).get("status")
            
            # Pula se já foi processado com sucesso
            if status_data == 'sucesso':
                dt = dt.add(days=1)
                continue

            # Pula fins de semana (dia 5=Sáb, 6=Dom)
            if dt.day_of_week >= 5:
                dt = dt.add(days=1)
                continue
            
            # Pula feriados
            if data_str in feriados:
                dt = dt.add(days=1)
                continue
                
            datas_a_processar.append(data_str)
            dt = dt.add(days=1)
        
        print(f"Total de {len(datas_a_processar)} dias de backfill a processar.")
        
        # (REQ 1) Inverte a lista para processar do mais novo para o mais antigo
        datas_a_processar.reverse()
        
        return datas_a_processar

    @task(
        task_id="processar_dia_corrente",
        retries=24, # A cada 30 min, do meio-dia até meia-noite
        retry_delay=timedelta(minutes=30),
    )
    def processar_dia_corrente(**kwargs):
        """
        Processa D-1 (yesterday_ds) com lógica de retentativa especial.
        """
        data_str = pendulum.parse(kwargs['data_interval_end'].to_date_string()).subtract(days=1).to_date_string()
        print(f"--- Processando Dia Corrente (D-1): {data_str} ---")
        
        status_json = carregar_status_json()
        if status_json.get(data_str, {}).get("status") == 'sucesso':
            print(f"Dia {data_str} já foi processado com sucesso. Pulando.")
            return

        feriados = carregar_feriados(PATH_FERIADOS)
        dt = pendulum.parse(data_str)
        if dt.day_of_week >= 5:
            print(f"Dia {data_str} é fim de semana. Pulando.")
            return
        if data_str in feriados:
            print(f"Dia {data_str} é feriado. Pulando.")
            return

        try:
            main_docs(data_str)
            main_carteira(data_str)
            main_caixa(data_str)
            main_extrato(data_str)
            
            # (REQ 2) Salva sucesso no JSON
            salvar_status_json(data_str, "sucesso", "Processamento D-1 concluído")
            print(f"SUCESSO Dia Corrente: {data_str}")
        
        except Exception as e:
            # (REQ 2) Salva falha no JSON
            salvar_status_json(data_str, "falha_D-1", str(e))
            print(f"FALHA Dia Corrente: {data_str}. Tentando novamente... Erro: {e}")
            raise e

    # (REQ 2) Nova task sequencial para o backfill
    @task(task_id="processar_backfill_sequencial")
    def processar_backfill_sequencial(lista_de_datas: list):
        """
        Processa o backfill sequencialmente (um dia por vez)
        para poder escrever no log JSON de forma segura.
        """
        print(f"Iniciando backfill sequencial para {len(lista_de_datas)} dias.")
        
        for i, data_str in enumerate(lista_de_datas):
            print(f"\n--- Processando Backfill ({i+1}/{len(lista_de_datas)}): {data_str} ---")
            try:
                main_docs(data_str)
                main_carteira(data_str)
                main_caixa(data_str)
                main_extrato(data_str)
                
                # (REQ 2) Salva sucesso no JSON
                salvar_status_json(data_str, "sucesso", "Backfill concluído")
                print(f"SUCESSO Backfill: {data_str}")
                
            except Exception as e:
                # (REQ 2) Salva falha no JSON e continua
                salvar_status_json(data_str, "falha_backfill", str(e))
                print(f"FALHA Backfill: {data_str}. Seguindo para o próximo. Erro: {e}")
                # Não levanta exceção para o loop continuar

    # --- Define a Ordem de Execução ---
    
    task_dia_corrente = processar_dia_corrente()
    lista_de_datas = get_datas_para_processar()
    
    # A task de backfill agora é chamada com a lista
    task_backfill = processar_backfill_sequencial(lista_de_datas)

    # Roda D-1 e o cálculo da lista em paralelo.
    # Quando AMBOS terminarem, inicia o backfill sequencial.
    [task_dia_corrente, lista_de_datas] >> task_backfill

# Chama a função para registrar a DAG
porto_auto_pipeline()