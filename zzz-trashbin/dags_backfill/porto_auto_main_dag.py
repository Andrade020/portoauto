import pendulum
import sys
import pandas as pd
import json
from pathlib import Path
from datetime import timedelta, datetime

########## 
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.utils.helpers import chain # Importante para a sequência
PATH_BASE = "/home/felipe/portoauto"
sys.path.append(str(Path(PATH_BASE) / "dags" / "scripts"))

try:
    from get_docs_fundo import main as main_docs
    from get_carteira_fundo import main as main_carteira
    from get_demonstrativo_caixa import main as main_caixa
    from get_extrato_bancario import main as main_extrato
    from get_recebiveis_dashboard import main_recebiveis_dashboard
    # Adicione a importação do VortxHandler AQUI:
    from common.vortx_handler import VortxHandler 
except ImportError as e:
    print(f"Erro: Não foi possível importar os scripts ou o handler: {e}")
    # ... (os dummies existentes) ...
    def main_docs(date): print(f"DUMMY DOCS: {date}")
    # ...
    def main_recebiveis_dashboard(date): print(f"DUMMY DASHBOARD: {date}")
    # Adicione um dummy para o Handler também
    class VortxHandler: pass


def _formatar_cnpj_com_mascara(cnpj_sem_mascara: str) -> str:
    """Formata um CNPJ limpo (só números) para o formato XX.XXX.XXX/XXXX-XX."""
    return f"{cnpj_sem_mascara[:2]}.{cnpj_sem_mascara[2:5]}.{cnpj_sem_mascara[5:8]}/{cnpj_sem_mascara[8:12]}-{cnpj_sem_mascara[12:]}"

#                                                    👇
# <<< AQUI ESTÁ A CORREÇÃO: a função PRECISA aceitar 'tipo_relatorio'
#                                                    👇
def main_relatorio(data_str: str, tipo_relatorio: str):
    """
    Busca um relatório de download (Base64).
    - "estoque" usa data_str como dataInicial e dataFinal.
    - "aquisicao" e "liquidacao" usam data_str para calcular o *mês anterior*.
    """
    print(f"--- Iniciando busca de Relatório '{tipo_relatorio}' para {data_str} ---")
    handler = VortxHandler()
    
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
    else:
        # Aquisicao/Liquidacao buscam o MÊS ANTERIOR.
        dia_da_execucao = pendulum.parse(data_str) # Ex: 2025-10-01

        # --- NOVA LÓGICA SEM end_of_month() ---
        # 1. Pega o primeiro dia do mês da execução
        inicio_mes_atual = dia_da_execucao.start_of('month') # Ex: 2025-10-01
        # 2. Subtrai um dia para cair no último dia do mês anterior
        data_fim_mes_anterior = inicio_mes_atual.subtract(days=1) # Ex: 2025-09-30
        # 3. Pega o primeiro dia desse mês anterior
        data_ini_mes_anterior = data_fim_mes_anterior.start_of('month') # Ex: 2025-09-01
        # --- FIM DA NOVA LÓGICA ---

        params["dataInicial"] = data_ini_mes_anterior.to_date_string()
        params["dataFinal"] = data_fim_mes_anterior.to_date_string()
        print(f"Relatório mensal. Buscando período: {params['dataInicial']} a {params['dataFinal']}")

    try:
        response = handler.make_recebiveis_request(url_endpoint, params)
        response_data = response.json()
        print(">>> Resposta recebida com sucesso. Decodificando...")

        base64_arquivo = response_data.get("arquivo")
        nome_arquivo_original = response_data.get("nome")
        
        if not base64_arquivo or not nome_arquivo_original:
            raise Exception(f"API não retornou 'arquivo' ou 'nome'. Resposta: {response_data}")

        nome_arquivo_final = f"{tipo_relatorio}_{cnpj_sem_mascara}_{params['dataInicial']}_a_{params['dataFinal']}.{extensao}"
        source_name = f"recebiveis_{tipo_relatorio}"

        handler.save_base64_file(base64_arquivo, source_name, nome_arquivo_final)
        
        print(f"--- Relatório '{tipo_relatorio}' concluído ---")

    except Exception as e:
        print(f"ERRO: Falha ao buscar o relatório '{tipo_relatorio}'. {e}")
        raise Exception(f"Falha ao buscar o relatório '{tipo_relatorio}': {e}")
# --- CONFIGURAÇÕES DO PIPELINE ---
DATA_INICIO_GERAL = '2024-01-01'
# DATA_INICIO_GERAL = Variable.get("porto_auto_data_inicio", '2025-01-01')

PATH_BASE = "/home/felipe/portoauto"
PATH_FERIADOS = Path(PATH_BASE) / "feriados_nacionais.csv"
PATH_PYTHON = "/home/felipe/.pyenv/versions/3.13.0/bin/python"

# (REQ 2 - Alterado) Novo caminho para o log de status em JSONL
# JSONL (JSON Lines) é um formato onde cada linha é um JSON.
# É seguro para "append" em paralelo.
PATH_JSONL_LOG = Path(PATH_BASE) / "processing_log.jsonl"

sys.path.append(str(Path(PATH_BASE) / "dags" / "scripts"))

try:
    from get_docs_fundo import main as main_docs
    from get_carteira_fundo import main as main_carteira
    from get_demonstrativo_caixa import main as main_caixa
    from get_extrato_bancario import main as main_extrato
except ImportError as e:
    print(f"Erro: Não foi possível importar os scripts: {e}")
    # Funções dummy para o Airflow não quebrar
    def main_docs(date): print(f"DUMMY DOCS: {date}")
    def main_carteira(date): print(f"DUMMY CARTEIRA: {date}")
    def main_caixa(date): print(f"DUMMY CAIXA: {date}")
    def main_extrato(date): print(f"DUMMY EXTRATO: {date}")

# --- (REQ 2) Funções de Log JSONL ---

def salvar_log_jsonl(data_str: str, task_name: str, status: str, mensagem: str):
    """
    Anexa (append) um log de status ao arquivo JSONL.
    Esta operação é segura para paralelismo.
    """
    log_entry = {
        "data": data_str,
        "task": task_name,
        "status": status,
        "mensagem": mensagem,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        with open(PATH_JSONL_LOG, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    except Exception as e:
        print(f"ERRO CRÍTICO: Não foi possível salvar o log JSONL! {e}")

def is_first_business_day(dt: pendulum.DateTime, feriados: set) -> bool:
    """
    Verifica se a data (dt) é o primeiro dia útil do mês.
    """
    if dt.day_of_week >= 5: # Pula Sábado/Domingo
        return False
    if dt.to_date_string() in feriados:
        return False
        
    # Verifica se todos os dias *anteriores* neste mês também não eram úteis
    dia_atual = dt.day
    for d in range(1, dia_atual):
        dt_anterior = dt.replace(day=d)
        if dt_anterior.day_of_week < 5 and dt_anterior.to_date_string() not in feriados:
            # Encontrou um dia útil antes deste, então este NÃO é o primeiro
            return False
            
    # Se passou por tudo, é o primeiro dia útil
    return True

def carregar_dias_com_sucesso() -> set:
    """
    Lê o log JSONL e retorna um set de datas onde
    TODAS as tasks *esperadas* para aquele dia tiveram "sucesso" ou "ignorado".
    """
    sucesso_por_data = {} # ex: {'2025-10-20': {'docs', 'carteira'}}
    
    # Carrega os feriados para saber a lógica de "primeiro dia útil"
    feriados = carregar_feriados(PATH_FERIADOS)
    
    if not PATH_JSONL_LOG.exists():
        return set()
        
    with open(PATH_JSONL_LOG, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line)
                # (MUDANÇA AQUI) Trata "ignorado" como um tipo de sucesso
                status = entry.get("status")
                if status == "sucesso" or status == "ignorado":
                    data = entry["data"]
                    task_name = entry["task"]
                    if data not in sucesso_por_data:
                        sucesso_por_data[data] = set()
                    sucesso_por_data[data].add(task_name)
            except json.JSONDecodeError:
                continue # ignora linha corrompida

    dias_com_sucesso = set()
    
    # Define as tasks base (diárias)
    tasks_base = {
        'docs', 'carteira', 'caixa', 'extrato',
        'relatorio_estoque', # (REQ 2) Estoque é diário
        'dashboard_recebiveis'
    }
    # Define as tasks mensais
    tasks_mensais = {
        'relatorio_aquisicao',
        'relatorio_liquidacao'
        # (REQ 1) 'recompra' foi removido
    }
    
    for data, tasks_com_sucesso_log in sucesso_por_data.items():
        # Para cada data, calcula quais tasks eram necessárias
        tasks_necessarias_para_este_dia = tasks_base.copy()
        
        try:
            # Verifica se esta data era um primeiro dia útil
            if is_first_business_day(pendulum.parse(data), feriados):
                tasks_necessarias_para_este_dia.update(tasks_mensais)
        except Exception:
            # Ignora datas mal formatadas, se houver
            continue
            
        # Verifica se o log tem todas as tasks que eram necessárias
        if tasks_com_sucesso_log.issuperset(tasks_necessarias_para_este_dia):
            dias_com_sucesso.add(data)
            
    return dias_com_sucesso

# --- Fim das Funções JSONL ---

def carregar_feriados(path_feriados):
    """Lê o CSV de feriados e retorna um set de datas."""
    try:
        df = pd.read_csv(path_feriados)
        feriados_dt = pd.to_datetime(df['Data'], errors='coerce')
        return set(feriados_dt.dt.strftime('%Y-%m-%d').dropna())
    except FileNotFoundError:
        print(f"Aviso: Arquivo de feriados não encontrado em {path_feriados}.")
        return set()

default_args = {
    "owner": "felipe",
    "email": ["melhor_deixar_sem_para_nao_endoidar@portorealasset.com.brasil"], # <<< MUDE AQUI O SEU EMAIL
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
    Usa TaskGroups para separar as 4 coletas na UI.
    Roda D-1 e depois o backfill (novo p/ antigo) sequencialmente.
    Salva log de status em 'processing_log.jsonl'.
    """,
    tags=["portoauto", "vortx", "taskgroup", "inteligente"],
)

def porto_auto_pipeline():
    @task(task_id="get_datas_para_processar")
    def get_datas_para_processar(**kwargs):
        """
        Calcula a lista de dias de backfill a processar.
        Exclui fins de semana, feriados e dias com "sucesso" no log JSONL.
        """
        data_inicio = pendulum.parse(DATA_INICIO_GERAL)
        # O backfill não deve incluir D-1, que é tratado pela task 'dia_corrente'
        data_fim_backfill = pendulum.parse(kwargs['data_interval_end'].to_date_string()).subtract(days=2)
        
        feriados = carregar_feriados(PATH_FERIADOS)
        dias_com_sucesso = carregar_dias_com_sucesso()
        
        datas_a_processar = []
        
        print("--- Calculando Backfill ---")
        print(f"Início: {data_inicio.to_date_string()}")
        print(f"Fim (exclusivo): {data_fim_backfill.to_date_string()}")
        print(f"Feriados carregados: {len(feriados)}")
        print(f"Dias com sucesso total: {len(dias_com_sucesso)}")

        dt = data_inicio
        while dt <= data_fim_backfill:
            data_str = dt.to_date_string()
            
            # Pula se já foi processado com sucesso
            if data_str in dias_com_sucesso:
                dt = dt.add(days=1); continue
            # Pula fins de semana
            if dt.day_of_week >= 5:
                dt = dt.add(days=1); continue
            # Pula feriados
            if data_str in feriados:
                dt = dt.add(days=1); continue
                
            datas_a_processar.append(data_str)
            dt = dt.add(days=1)
        
        print(f"Total de {len(datas_a_processar)} dias de backfill a processar.")
        
        # Inverte a lista para processar do mais novo para o mais antigo
        datas_a_processar.reverse()
        
        return datas_a_processar

    # --- (REQ 4) DEFINIÇÃO DAS 4 TASKS INDIVIDUAIS ---
    # Cada uma tem seu próprio log e status de falha

    @task
    def run_docs(data_str: str, soft_fail: bool = False):
        if data_str == "skip":
            print(f"Task run_docs pulada (sinal 'skip' recebido).")
            return
        try:
            main_docs(data_str)
            salvar_log_jsonl(data_str, "docs", "sucesso", "Documentos OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "docs", "falha", str(e))
            if not soft_fail:
                raise e # Hard Fail (para D-1)
            print(f"SOFT FAIL: Task run_docs falhou para {data_str}, mas continuando. Erro: {error_message}")

    @task
    def run_carteira(data_str: str, soft_fail: bool = False):
        if data_str == "skip":
            print(f"Task run_carteira pulada (sinal 'skip' recebido).")
            return
        try:
            main_carteira(data_str)
            salvar_log_jsonl(data_str, "carteira", "sucesso", "Carteira OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "carteira", "falha", str(e))
            if not soft_fail:
                raise e
            print(f"SOFT FAIL: Task run_carteira falhou para {data_str}, mas continuando. Erro: {error_message}")


    @task
    def run_caixa(data_str: str, soft_fail: bool = False):
        if data_str == "skip":
            print(f"Task run_caixa pulada (sinal 'skip' recebido).")
            return
        try:
            main_caixa(data_str)
            salvar_log_jsonl(data_str, "caixa", "sucesso", "Caixa OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "caixa", "falha", str(e))
            if not soft_fail:
                raise e
            print(f"SOFT FAIL: Task run_caixa falhou para {data_str}, mas continuando. Erro: {error_message}")

    @task
    def run_extrato(data_str: str, soft_fail: bool = False):
        if data_str == "skip":
            print(f"Task run_extrato pulada (sinal 'skip' recebido).")
            return
        try:
            main_extrato(data_str)
            salvar_log_jsonl(data_str, "extrato", "sucesso", "Extrato OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "extrato", "falha", str(e))
            if not soft_fail:
                raise e
            print(f"SOFT FAIL: Task run_extrato falhou para {data_str}, mas continuando. Erro: {error_message}")

    ########################################################################################
    @task
    def run_relatorio_estoque(data_str: str, soft_fail: bool = False):
        if data_str == "skip":
            print(f"Task run_relatorio_estoque pulada (sinal 'skip' recebido).")
            return
        try:
            # (MUDANÇA AQUI) Estoque é diário
            main_relatorio(data_str, tipo_relatorio="estoque")
            salvar_log_jsonl(data_str, "relatorio_estoque", "sucesso", "Relatório Estoque OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "relatorio_estoque", "falha", str(e))
            if not soft_fail:
                raise e
            print(f"SOFT FAIL: Task run_relatorio_estoque falhou para {data_str}, mas continuando. Erro: {error_message}")

    @task
    def run_relatorio_aquisicao(data_str: str, soft_fail: bool = False):
        if data_str == "skip":
            print(f"Task run_relatorio_aquisicao pulada (sinal 'skip' recebido).")
            return
        
        # (REQ 3) Roda apenas no 1º dia útil
        feriados = carregar_feriados(PATH_FERIADOS)
        if not is_first_business_day(pendulum.parse(data_str), feriados):
            print(f"Pulando 'aquisicao' para {data_str}: não é o 1º dia útil do mês.")
            salvar_log_jsonl(data_str, "relatorio_aquisicao", "ignorado", "Não é 1º dia útil")
            return
            
        try:
            # (MUDANÇA AQUI) Passa o tipo correto
            main_relatorio(data_str, tipo_relatorio="aquisicao")
            salvar_log_jsonl(data_str, "relatorio_aquisicao", "sucesso", "Relatório Aquisição OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "relatorio_aquisicao", "falha", str(e))
            if not soft_fail:
                raise e
            print(f"SOFT FAIL: Task run_relatorio_aquisicao falhou para {data_str}, mas continuando. Erro: {error_message}")

    @task
    def run_relatorio_liquidacao(data_str: str, soft_fail: bool = False):
        if data_str == "skip":
            print(f"Task run_relatorio_liquidacao pulada (sinal 'skip' recebido).")
            return

        # (REQ 3) Roda apenas no 1º dia útil
        feriados = carregar_feriados(PATH_FERIADOS)
        if not is_first_business_day(pendulum.parse(data_str), feriados):
            print(f"Pulando 'liquidacao' para {data_str}: não é o 1º dia útil do mês.")
            salvar_log_jsonl(data_str, "relatorio_liquidacao", "ignorado", "Não é 1º dia útil")
            return

        try:
            # (MUDANÇA AQUI) Passa o tipo correto
            main_relatorio(data_str, tipo_relatorio="liquidacao")
            salvar_log_jsonl(data_str, "relatorio_liquidacao", "sucesso", "Relatório Liquidação OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "relatorio_liquidacao", "falha", str(e))
            if not soft_fail:
                raise e
            print(f"SOFT FAIL: Task run_relatorio_liquidacao falhou para {data_str}, mas continuando. Erro: {error_message}")

    @task
    def run_dashboard_recebiveis(data_str: str, soft_fail: bool = False):
        if data_str == "skip":
            print(f"Task run_dashboard_recebiveis pulada (sinal 'skip' recebido).")
            return
        try:
            main_recebiveis_dashboard(data_str)
            salvar_log_jsonl(data_str, "dashboard_recebiveis", "sucesso", "Dashboard Recebíveis OK")
        except Exception as e:
            error_message = str(e)
            salvar_log_jsonl(data_str, "dashboard_recebiveis", "falha", str(e))
            if not soft_fail:
                raise e
            print(f"SOFT FAIL: Task run_dashboard_recebiveis falhou para {data_str}, mas continuando. Erro: {error_message}")
    # --- (REQ 4) O TASK GROUP ---
    # Este é o "molde" que agrupa as 4 tasks
    
    @task_group(group_id="processar_dia_corrente_group")
    def processar_dia_corrente_group(data_str: str):
        """
        Grupo de tasks para o D-1 com 'Hard Fail'.
        Se falhar, a task falha e envia email.
        """
        run_docs(data_str, soft_fail=False)
        run_carteira(data_str, soft_fail=False)
        run_caixa(data_str, soft_fail=False)
        run_extrato(data_str, soft_fail=False)

    @task_group(group_id="processar_backfill_group")
    def processar_backfill_group(data_str: str):
        """
        Grupo de tasks para o Backfill com 'Soft Fail'.
        Se falhar, registra no log mas a task fica verde
        para não quebrar a corrente (chain).
        """
        run_docs(data_str, soft_fail=True)
        run_carteira(data_str, soft_fail=True)
        run_caixa(data_str, soft_fail=True)
        run_extrato(data_str, soft_fail=True)


    @task_group(group_id="processar_dia_corrente_group")
    def processar_dia_corrente_group(data_str: str):
        # ... (tasks existentes) ...
        run_docs(data_str, soft_fail=False)
        run_carteira(data_str, soft_fail=False)
        run_caixa(data_str, soft_fail=False)
        run_extrato(data_str, soft_fail=False)
        #task dashboard
        run_dashboard_recebiveis(data_str, soft_fail=False)
        
        # relatorios de remessa
        run_relatorio_estoque(data_str, soft_fail=False)
        run_relatorio_aquisicao(data_str, soft_fail=False)
        run_relatorio_liquidacao(data_str, soft_fail=False)

    @task_group(group_id="processar_backfill_group")
    def processar_backfill_group(data_str: str):
        # ... (tasks existentes) ...
        run_docs(data_str, soft_fail=True)
        run_carteira(data_str, soft_fail=True)
        run_caixa(data_str, soft_fail=True)
        run_extrato(data_str, soft_fail=True)
        # ADICIONE AS NOVAS TASKS
        run_dashboard_recebiveis(data_str, soft_fail=True)
        
        # ADICIONE AS 4 NOVAS TASKS:
        run_relatorio_estoque(data_str, soft_fail=True)
        run_relatorio_aquisicao(data_str, soft_fail=True)
        run_relatorio_liquidacao(data_str, soft_fail=True)


    
    @task(
        task_id="processar_dia_corrente_D-1",
        retries=24, # A cada 30 min, do meio-dia até meia-noite
        retry_delay=timedelta(minutes=30),
    )
    def processar_dia_corrente(**kwargs):
        """
        Wrapper para o dia D-1 que verifica se é dia útil
        antes de chamar o TaskGroup.
        """
        data_str = pendulum.parse(kwargs['data_interval_end'].to_date_string()).subtract(days=1).to_date_string()
        print(f"--- Processando Dia Corrente (D-1): {data_str} ---")
        
        dias_com_sucesso = carregar_dias_com_sucesso()
        if data_str in dias_com_sucesso:
            print(f"Dia {data_str} já foi processado com sucesso. Pulando.")
            return "skip"

        feriados = carregar_feriados(PATH_FERIADOS)
        dt = pendulum.parse(data_str)
        if dt.day_of_week >= 5:
            print(f"Dia {data_str} é fim de semana. Pulando.")
            return "skip"
        if data_str in feriados:
            print(f"Dia {data_str} é feriado. Pulando.")
            return "skip"
        
        # Se for dia útil, retorna a data para o TaskGroup
        return data_str

    # --- DEFINE A ORDEM DE EXECUÇÃO ---
    
    # 1. Roda as tasks de cálculo e de dia corrente em paralelo
    lista_de_datas = get_datas_para_processar()
    data_dia_corrente = processar_dia_corrente()
    
    # 2. Chama o TaskGroup para o dia corrente (com Hard Fail)
    tg_dia_corrente = processar_dia_corrente_group(data_dia_corrente)

    # 3. Mapeia o TaskGroup de Backfill (com Soft Fail)
    backfill_task_groups = processar_backfill_group.expand(data_str=lista_de_datas)

    # 4. Encadeia (chain) tudo em sequência:
    
    # Garante que o backfill rode sequencialmente
    chain(backfill_task_groups)

    # Define a ordem geral:
    # Roda D-1, e SÓ DEPOIS, inicia toda a cadeia de backfill
    data_dia_corrente >> tg_dia_corrente >> backfill_task_groups

# Chama a função para registrar a DAG
porto_auto_pipeline()