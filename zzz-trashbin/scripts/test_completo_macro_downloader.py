import pandas as pd
from macro_downloader import (
    get_caged, get_cvm, get_pib_municipios, get_tse, get_bcb_selic,
    get_bcb_cdi, get_bcb_ipca, get_bcb_inadimplencia_pj, get_ibge_pnad,
    get_tesouro_direto, get_ibge_pmc, get_ibge_pms, get_ibge_pim,
    get_inmet_sorriso
)

# exibicao
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 120)


dados = {}
""" dic com todos os dfs"""

# 
funcoes_de_carga = {
    "CAGED": get_caged,
    "CVM_FIDC": get_cvm,
    "PIB_Municipios_Amostra": get_pib_municipios,
    "TSE_Eleitorado_Amostra": get_tse,
    "BCB_SELIC": get_bcb_selic,
    "BCB_CDI": get_bcb_cdi,
    "BCB_IPCA": get_bcb_ipca,
    "BCB_Inadimplencia_PJ": get_bcb_inadimplencia_pj,
    "IBGE_PNAD": get_ibge_pnad,
    "Tesouro_Direto": get_tesouro_direto,
    "IBGE_PMC": get_ibge_pmc,
    "IBGE_PMS": get_ibge_pms,
    "IBGE_PIM": get_ibge_pim,
    "INMET_Sorriso": get_inmet_sorriso,
}
"""funções de carregamento disponíveis"""

print("======================================================")
print("==     Iniciando carregamento de todos os dados     ==")
print("======================================================")

for nome, funcao in funcoes_de_carga.items():
    print(f"\nCarregando: {nome}...")
    df = funcao()
    if df is not None and not df.empty:
        dados[nome] = df
        print(f"✅ Sucesso! {nome} carregado com {df.shape[0]} linhas e {df.shape[1]} colunas.")
        print("Amostra:")
        print(df.head(2))
    else:
        print(f"⚠️ Aviso: Nenhum dado encontrado ou erro ao carregar {nome}.")

print("\n\n======================================================")
print("==               CARGA FINALIZADA               ==")
print("======================================================")
print("Resumo dos DataFrames carregados no dicionário 'dados':")
for nome, df in dados.items():
    print(f"- {nome:<25} | Shape: {df.shape}")

# Agora você pode acessar cada DataFrame facilmente:
# print("\nExibindo os últimos 5 registros do Tesouro Direto:")
# print(dados["Tesouro_Direto"].tail())