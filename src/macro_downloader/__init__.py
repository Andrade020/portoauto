"""
Macro Downloader Data Package
-----------------------------
Pacote para carregar facilmente dados macroeconômicos em DataFrames do pandas.
"""
import pandas as pd
from .core import get_data

__all__ = [
    'get_caged', 'get_cvm', 'get_pib_municipios', 'get_tse', 'get_bcb_selic',
    'get_bcb_cdi', 'get_bcb_ipca', 'get_bcb_inadimplencia_pj', 'get_ibge_pnad',
    'get_tesouro_direto', 'get_ibge_pmc', 'get_ibge_pms', 'get_ibge_pim',
    'get_inmet_sorriso'
]

def get_caged() -> pd.DataFrame:
    """Carrega os dados mais recentes do Novo CAGED."""
    return get_data("caged")

def get_cvm() -> pd.DataFrame:
    """Carrega e une os arquivos do informe mensal mais recente da CVM para FIDCs."""
    return get_data("cvm")

def get_pib_municipios(sample: bool = True) -> pd.DataFrame:
    """Carrega uma amostra dos dados mais recentes do PIB dos Municípios."""
    return get_data("pib_municipios")

def get_tse(sample: bool = True) -> pd.DataFrame:
    """Carrega uma amostra dos dados mais recentes de perfil do eleitorado do TSE."""
    return get_data("tse")

def get_bcb_selic() -> pd.DataFrame:
    """Carrega a série histórica da taxa SELIC."""
    return get_data("bcb_selic")

def get_bcb_cdi() -> pd.DataFrame:
    """Carrega a série histórica da taxa CDI."""
    return get_data("bcb_cdi")

def get_bcb_ipca() -> pd.DataFrame:
    """Carrega a série histórica do IPCA."""
    return get_data("bcb_ipca")

def get_bcb_inadimplencia_pj() -> pd.DataFrame:
    """Carrega a série histórica de inadimplência de pessoas jurídicas."""
    return get_data("bcb_inadimplencia_pj")

def get_ibge_pnad() -> pd.DataFrame:
    """Carrega a série histórica da taxa de desocupação (PNAD Contínua)."""
    return get_data("ibge_pnad")

def get_tesouro_direto() -> pd.DataFrame:
    """Carrega o histórico de preços e taxas do Tesouro Direto."""
    return get_data("tesouro_direto")

def get_ibge_pmc() -> pd.DataFrame:
    """Carrega a série histórica da Pesquisa Mensal de Comércio (PMC)."""
    return get_data("ibge_pmc")

def get_ibge_pms() -> pd.DataFrame:
    """Carrega a série histórica da Pesquisa Mensal de Serviços (PMS)."""
    return get_data("ibge_pms")

def get_ibge_pim() -> pd.DataFrame:
    """Carrega a série histórica da Pesquisa Industrial Mensal (PIM)."""
    return get_data("ibge_pim")

def get_inmet_sorriso() -> pd.DataFrame:
    """Carrega os dados climáticos da estação de Sorriso/MT (A904)."""
    return get_data("inmet_sorriso")