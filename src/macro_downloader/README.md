Pacote macro_downloaderEste pacote fornece uma interface simples e de alto nível para carregar os dados macroeconômicos baixados pelo downloader.py diretamente em DataFrames do pandas.O objetivo é permitir que qualquer script, notebook ou análise acesse os dados mais recentes com uma única linha de código, abstraindo a complexidade de encontrar, ler e padronizar os arquivos brutos.FuncionalidadesAPI Simples: Funções intuitivas como get_caged(), get_bcb_selic(), etc.Carregamento Inteligente: Identifica automaticamente os arquivos do período mais recente para fontes com múltiplas datas (CVM, CAGED).Padronização Básica: Converte colunas de data e valores numéricos para os tipos corretos, facilitando a análise imediata.Reutilizável: Pode ser importado em qualquer script ou notebook dentro do seu ambiente virtual.📋 Pré-requisitosAntes de usar este pacote, é essencial que você tenha executado o script de download pelo menos uma vez para que os dados brutos existam no diretório data_raw/.# Execute o downloader para buscar os dados da internet
python src/macro_downloader/downloader.py
⚙️ InstalaçãoPara usar o pacote macro_downloader em seus outros códigos, você precisa instalá-lo em seu ambiente Python. A instalação em "modo editável" (-e) é recomendada, pois quaisquer futuras melhorias no pacote serão refletidas instantaneamente.Abra o terminal na pasta raiz do projeto (onde o arquivo pyproject.toml está localizado) e execute:pip install -e .
🚀 Como UsarExemplo RápidoApós a instalação, você pode importar e usar as funções em qualquer script:from macro_downloader import get_ibge_pnad, get_tesouro_direto

# Carrega a série completa da PNAD
df_pnad = get_ibge_pnad()
print("--- Últimos dados da PNAD Contínua ---")
print(df_pnad.tail())

# Carrega o histórico do Tesouro Direto
df_tesouro = get_tesouro_direto()
print("\n--- Últimos dados do Tesouro Direto ---")
print(df_tesouro.tail())
📋 Comando para Carregar TODOS os DadosEste é o script completo para importar todos os dados disponíveis de uma só vez. Ele armazena cada DataFrame em um dicionário chamado dados para fácil acesso.import pandas as pd
from macro_downloader import (
    get_caged, get_cvm, get_pib_municipios, get_tse, get_bcb_selic,
    get_bcb_cdi, get_bcb_ipca, get_bcb_inadimplencia_pj, get_ibge_pnad,
    get_tesouro_direto, get_ibge_pmc, get_ibge_pms, get_ibge_pim,
    get_inmet_sorriso
)

# Define a largura máxima de exibição das colunas no pandas
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 120)

# Dicionário para armazenar todos os DataFrames carregados
dados = {}

# Lista de todas as funções de carregamento
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

# Agora você pode acessar cada DataFrame facilmente
# Exemplo:
# print("\nExibindo os últimos 5 registros do Tesouro Direto:")
# print(dados["Tesouro_Direto"].tail())
📚 Referência da API (Funções Disponíveis)FunçãoFonte de DadosNotasget_caged()Novo CAGED (Ministério do Trabalho)Carrega o arquivo do último mês disponível.get_cvm()CVM Informes Mensais FIDCUne todos os arquivos do último mês disponível.get_pib_municipios()IBGE - PIB dos MunicípiosRetorna uma amostra do arquivo do último ano.get_tse()TSE - Perfil do EleitoradoRetorna uma amostra do maior arquivo CSV.get_bcb_selic()Banco Central (SGS)Série histórica completa da taxa SELIC.get_bcb_cdi()Banco Central (SGS)Série histórica completa da taxa CDI.get_bcb_ipca()Banco Central (SGS)Série histórica completa do IPCA.get_bcb_inadimplencia_pj()Banco Central (SGS)Série histórica de inadimplência PJ.get_ibge_pnad()IBGE - PNAD ContínuaSérie histórica completa da taxa de desocupação.get_tesouro_direto()Tesouro NacionalHistórico completo de preços e taxas.get_ibge_pmc()IBGE - Pesquisa Mensal de ComércioSérie histórica completa da PMC.get_ibge_pms()IBGE - Pesquisa Mensal de ServiçosSérie histórica completa da PMS.get_ibge_pim()IBGE - Pesquisa Industrial MensalSérie histórica completa da PIM.get_inmet_sorriso()INMET - Estação A904 (Sorriso/MT)Dados climáticos consolidados da estação.