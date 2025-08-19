import pandas as pd
import os
from datetime import datetime

# Builds Locais #!----------------------------------------------------------------
from gerador_relatorio import GeradorRelatorioHTML
from modulos_analise import calcular_metricas_gerais, calcular_prazo_medio_ponderado


# --- 1. CONFIGURAÇÃO ---
# O geral que vamos mudar está aqui
DATA_FOLDER = 'data'
OUTPUT_FOLDER = 'output'
LOGO_PATH = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\relatorios_empresa\images\logo_inv.png' # Adapte para o caminho da sua logo
"""todo: mudar para a logo do config"""

TITULO_DO_RELATORIO = "Análise de Carteira (Teste)"
SUBTITULO_DO_RELATORIO = "Resultados com Dados Gerados"
DATA_HOJE = datetime.now().strftime('%d/%m/%Y')


TEST_DATA_FILE = os.path.join(DATA_FOLDER, 'dados_de_teste.xlsx')

os.makedirs(OUTPUT_FOLDER, exist_ok=True) #* Crio a pasta de saída se ela não existir


#? Dados de Exemplo #****************************************
#! fUNÇÃO TOY GERADA POR LLM    -----------------------------
def gerar_dados_teste(num_linhas=1000, output_folder='data_temp'):
    """
    Gera um arquivo Excel com dados de teste para simular a análise.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    print(f"Gerando {num_linhas} linhas de dados de teste...")

    data = {
        'CCB': [f'CCB_{1000 + i}' for i in range(num_linhas)],
        'ValorPresente': np.random.uniform(500, 15000, num_linhas),
        'ValorNominal': lambda df: df['ValorPresente'] * np.random.uniform(1.1, 1.5, num_linhas),
        'PDDTotal': lambda df: df['ValorPresente'] * np.random.uniform(0.01, 0.15, num_linhas),
        'Originador': np.random.choice(['Originador Alfa', 'Originador Beta', 'Originador Gama'], num_linhas, p=[0.5, 0.3, 0.2]),
        'Produto': np.random.choice(['Crédito Pessoal', 'Cartão Benefício', 'Crédito Imobiliário'], num_linhas),
        'UF': np.random.choice(['SP', 'RJ', 'MG', 'BA', 'RS'], num_linhas, p=[0.4, 0.2, 0.2, 0.1, 0.1]),
        'DataGeracao': pd.to_datetime('today'),
        'DataAquisicao': pd.to_datetime('today') - pd.to_timedelta(np.random.randint(30, 365, num_linhas), unit='d'),
        'Prazo': np.random.randint(12, 72, num_linhas)
    }

    df = pd.DataFrame(data)

    # Avalia funções lambda, se houver
    for col, val in data.items():
        if callable(val):
            df[col] = val(df)

    # Arredondamento
    for col in ['ValorPresente', 'ValorNominal', 'PDDTotal']:
        df[col] = df[col].round(2)

    filepath = os.path.join(output_folder, 'dados_de_teste.xlsx')
    df.to_excel(filepath, index=False)
    print(f"Dados de teste salvos em: {filepath}")
    return filepath

#!-----------------------------------------------------------

gerar_dados_teste(num_linhas=2000, output_folder=DATA_FOLDER)

print(f"Lendo dados de teste de: {TEST_DATA_FILE}")
df = pd.read_excel(TEST_DATA_FILE)


#? Análises de Exemplo
print("\nIniciando as análises...")
analise_por_originador = calcular_metricas_gerais(df, 'Originador')
analise_por_produto = calcular_metricas_gerais(df, 'Produto')
analise_por_uf = calcular_metricas_gerais(df, 'UF')

# eex de uma análise diferente
prazo_medio_por_originador = calcular_prazo_medio_ponderado(df, 'Originador')

# exempo:  duas tabelas de análise
if not prazo_medio_por_originador.empty:
    analise_por_originador = pd.merge(analise_por_originador, prazo_medio_por_originador, on='Originador')


#?  >>>>>>>>>>>>      COMO GERAR O RELATÓRIO      <<<<<<<<<<<<<<<
print("\nGerando o relatório HTML...")
# Inicializa o nosso gerador com as configurações de título e logo
report_generator = GeradorRelatorioHTML(
    titulo_relatorio=TITULO_DO_RELATORIO,
    subtitulo=SUBTITULO_DO_RELATORIO,
    data_relatorio=DATA_HOJE,
    logo_path=LOGO_PATH
)
"""objeto gerado a partir do título e subtítulo"""

# FORMATADORES PARA AS COLUNAS 
# -> estou usando isso porque devemos definir individualmente. 
# por exemplo, suponha que você quer que tenha uma coluna como valor em reais, 
# vai querer 2 casas depois da vírgula. 
# para a tir, provavelmente vai querer 3 ou 4 ... 

formatters = {
    'Valor Líquido (R$ MM)': lambda x: f'{x:,.2f}',
    'Valor Presente (R$ MM)': lambda x: f'{x:,.2f}',
    'Nº de Contratos': lambda x: f'{x:,.0f}'.replace(',', '.'),
    '% PDD': lambda x: f'{x:,.2f}%',
    'Prazo Médio Ponderado (meses)': lambda x: f'{x:,.1f}'
}

# aqui está : adiciono como objeto uma nova tabela
report_generator.adicionar_secao_colapsavel(
    titulo="Análise por Originador",
    descricao="Desempenho da carteira consolidado por cada originador parceiro.",
    dataframe=analise_por_originador,
    formatters=formatters,
    aberto=True # Deixa esta seção já aberta
)

report_generator.adicionar_secao_colapsavel(
    titulo="Análise por Produto",
    descricao="Quebra dos resultados por tipo de produto de crédito.",
    dataframe=analise_por_produto,
    formatters=formatters
)

report_generator.adicionar_secao_colapsavel(
    titulo="Análise por UF",
    descricao="Distribuição geográfica da carteira e suas métricas.",
    dataframe=analise_por_uf,
    formatters=formatters
)

# Salva o arquivo final
output_file = os.path.join(OUTPUT_FOLDER, 'relatorio_de_teste.html')
report_generator.salvar(output_file)