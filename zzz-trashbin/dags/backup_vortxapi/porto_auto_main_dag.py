from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Caminho para os scripts
SCRIPTS_PATH = "/home/felipe/portoauto/dags/scripts"

# Caminho exato para o interpretador Python do pyenv
PYTHON_EXECUTABLE = "/home/felipe/.pyenv/versions/3.13.0/bin/python"

with DAG(
    dag_id="porto_auto_vortx_pipeline",
    start_date=pendulum.datetime(2025, 10, 15, tz="America/Sao_Paulo"),
    schedule="0 12 * * *", # todo dia ao meio-dia
    catchup=False,
    doc_md="""
    DAG para baixar dados diários da API Vórtx.
    Fontes: Documentos, Carteira, Demonstrativo de Caixa e Extrato Bancário.
    As tasks rodam em paralelo.
    """,
    default_args={
        "owner": "felipe",
        "retries": 3,
        "retry_delay": timedelta(minutes=15),
    },
    tags=["portoauto", "vortx", "dados-brutos"],
) as dag:

    # A variável de template {{ ds }} do Airflow passa a data de execução no formato YYYY-MM-DD
    get_docs = BashOperator(
        task_id="get_docs_fundo",
        bash_command=PYTHON_EXECUTABLE + " " + SCRIPTS_PATH + "/get_docs_fundo.py {{ ds }}",
    )

    get_carteira = BashOperator(
        task_id="get_carteira_fundo",
        bash_command=PYTHON_EXECUTABLE + " " + SCRIPTS_PATH + "/get_carteira_fundo.py {{ ds }}",
    )

    get_caixa = BashOperator(
        task_id="get_demonstrativo_caixa",
        bash_command=PYTHON_EXECUTABLE + " " + SCRIPTS_PATH + "/get_demonstrativo_caixa.py {{ ds }}",
    )

    get_extrato = BashOperator(
        task_id="get_extrato_bancario",
        bash_command=PYTHON_EXECUTABLE + " " + SCRIPTS_PATH + "/get_extrato_bancario.py {{ ds }}",
    )