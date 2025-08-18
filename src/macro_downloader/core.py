import os
import re
from glob import glob
import pandas as pd
from .loaders import caged, cvm, bcb, ibge, tesouro, inmet, tse, utils

DATA_SOURCES = {
    "cvm": (cvm.load_cvm, os.path.join(utils.DIR_MACRO, "cvm", "informes_mensais", "inf_mensal_fidc_*.csv"), 'yyyymm_multi'),
    "caged": (caged.load_caged, os.path.join(utils.DIR_MACRO_ADICIONAIS, "trabalho", "caged", "CAGEDMOV*.txt"), 'yyyymm'),
    "pib_municipios": (ibge.sample_pib_municipios, os.path.join(utils.DIR_MACRO_ADICIONAIS, "ibge", "contas_regionais", "*.*"), 'yyyy'),
    "tse": (tse.sample_tse, os.path.join(utils.DIR_MACRO_ADICIONAIS, "tse", "eleitorado", "*.csv"), 'size'),
    "bcb_selic": (bcb.load_bcb, os.path.join(utils.DIR_MACRO, "bcb", "sgs", "selic.csv"), 'single'),
    "bcb_cdi": (bcb.load_bcb, os.path.join(utils.DIR_MACRO, "bcb", "sgs", "cdi.csv"), 'single'),
    "bcb_ipca": (bcb.load_bcb, os.path.join(utils.DIR_MACRO, "bcb", "sgs", "ipca.csv"), 'single'),
    "bcb_inadimplencia_pj": (bcb.load_bcb, os.path.join(utils.DIR_MACRO, "bcb", "sgs", "inadimplencia_pj.csv"), 'single'),
    "ibge_pnad": (ibge.load_pnad, os.path.join(utils.DIR_MACRO, "ibge", "pnad_continua", "taxa_desocupacao.csv"), 'single'),
    "tesouro_direto": (tesouro.load_tesouro, os.path.join(utils.DIR_MACRO, "tesouro_nacional", "precos_taxas", "tesouro_direto_historico.csv"), 'single'),
    "ibge_pmc": (ibge.load_pesquisas_mensais, os.path.join(utils.DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pmc_volume_vendas.csv"), 'single'),
    "ibge_pms": (ibge.load_pesquisas_mensais, os.path.join(utils.DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pms_receita_servicos.csv"), 'single'),
    "ibge_pim": (ibge.load_pesquisas_mensais, os.path.join(utils.DIR_MACRO_ADICIONAIS, "ibge", "pesquisas_mensais", "pim_producao_industrial.csv"), 'single'),
    "inmet_sorriso": (inmet.load_inmet, os.path.join(utils.DIR_MACRO_ADICIONAIS, "inmet", "clima", "estacao_A904.csv"), 'single'),
}
"""origens dos dados"""


def _find_files(pattern: str, strategy: str):
    files = glob(pattern)
    if not files: return []
    if strategy == 'single': return files[:1]
    if strategy == 'size': return [max(files, key=os.path.getsize)]
    
    latest_date = 0
    date_regex = r'(\d{4})' if strategy == 'yyyy' else r'(\d{6})'
    
    for f in files:
        matches = re.findall(date_regex, os.path.basename(f))
        if matches:
            latest_date = max(latest_date, max(int(d) for d in matches))
    
    if latest_date > 0:
        return [f for f in files if str(latest_date) in os.path.basename(f)]
        
    return [max(files, key=os.path.getmtime)]

def get_data(source_name: str) -> pd.DataFrame | None:
    if source_name not in DATA_SOURCES:
        raise ValueError(f"Fonte '{source_name}' não reconhecida. Fontes: {list(DATA_SOURCES.keys())}")
    loader_func, path_pattern, strategy = DATA_SOURCES[source_name]
    files_to_process = _find_files(path_pattern, strategy)
    if not files_to_process:
        # print(f"Aviso: Nenhum arquivo encontrado para a fonte '{source_name}'.")
        return None
    return loader_func(files_to_process)