# -*- coding: utf-8 -*-

import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from scripts import macro_downloader_logic

# -------------------------------------------------------------------------------------
# CONFIGURAÇÃO DA DAG E CONSTANTES
# -------------------------------------------------------------------------------------

#! #TODO: Mudar PATH
DATA_BASE_PATH = '/opt/airflow/data_raw'
BASE_DIR = os.path.join(DATA_BASE_PATH, 'dados_macro')
BASE_DIR_ADICIONAIS = os.path.join(DATA_BASE_PATH, 'dados_macro_adicionais')
#! -------------------------------------------------------------------------------------

@dag(
    dag_id='dag_macro_data_downloader',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['data', 'macro', 'download'], # posso adicionar uma descricao em markdown!! 
    doc_md="""
    ### DAG para Download de Dados Macroeconômicos
    Orquestra o download de dados de várias fontes (CVM, BCB, IBGE, etc.).
    A lógica de download está no script `scripts/macro_downloader_logic.py`.
    """
)
def macro_data_downloader_dag():
    """
    ### DAG de Orquestração
    Esta DAG apenas chama as funções de lógica e define a ordem de execução.
    """

    @task(task_id="create_all_folders")
    def create_folders_task():
        """Tarefa que chama a lógica de criação de pastas."""
        macro_downloader_logic.create_all_folders(BASE_DIR, BASE_DIR_ADICIONAIS)

    # ======================================================================
    # Wrapper: chamadno cada uma das funcoes do script de download
    # ======================================================================
    
    @task(task_id="download_cvm_fidc")
    def task_cvm():
        macro_downloader_logic.baixar_informes_mensais_fidc_cvm(base_dir=BASE_DIR)

    @task(task_id="download_bcb_sgs")
    def task_bcb():
        macro_downloader_logic.baixar_series_temporais_bcb(base_dir=BASE_DIR)

    @task(task_id="download_ibge_pnad")
    def task_pnad():
        macro_downloader_logic.baixar_dados_pnad_ibge(base_dir=BASE_DIR)

    @task(task_id="download_tesouro_direto")
    def task_tesouro():
        macro_downloader_logic.baixar_dados_tesouro_direto(base_dir=BASE_DIR)
    
    @task(task_id="download_ibge_pesquisas_mensais")
    def task_ibge_mensal():
        macro_downloader_logic.chamar_downloads_ibge_mensal(base_dir_adicionais=BASE_DIR_ADICIONAIS)
        
    @task(task_id="download_ibge_pib_municipios")
    def task_ibge_pib():
        macro_downloader_logic.baixar_pib_municipios_ibge(base_dir_adicionais=BASE_DIR_ADICIONAIS)
        
    @task(task_id="download_caged")
    def task_caged():
        macro_downloader_logic.baixar_novo_caged(base_dir_adicionais=BASE_DIR_ADICIONAIS)
        
    @task(task_id="download_tse_eleitorado")
    def task_tse():
        macro_downloader_logic.baixar_perfil_eleitorado_tse(base_dir_adicionais=BASE_DIR_ADICIONAIS)

    @task(task_id="download_inmet_clima")
    def task_inmet():
        macro_downloader_logic.baixar_dados_climaticos_inmet(base_dir_adicionais=BASE_DIR_ADICIONAIS)

    @task(task_id="end_of_pipeline", trigger_rule=TriggerRule.ALL_DONE)
    def end_task():
        """Tarefa final que sempre executa."""
        print("="*60 + "\n>>> ROTINAS DE DOWNLOAD FINALIZADAS (com ou sem erros) <<<\n" + "="*60)

    #* -------------------------------------------------------------------------------------
    #* FLUXO
    #* -------------------------------------------------------------------------------------

    setup_task = create_folders_task()
    
    download_tasks = [
        task_cvm(),
        task_bcb(),
        task_pnad(),
        task_tesouro(),
        task_ibge_mensal(),
        task_ibge_pib(),
        task_caged(),
        task_tse(),
        task_inmet()
    ]

    teardown_task = end_task()

    setup_task >> download_tasks >> teardown_task

dag = macro_data_downloader_dag() # Instancio a DAG