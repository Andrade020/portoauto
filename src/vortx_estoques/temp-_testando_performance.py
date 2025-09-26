#%%
import pandas as pd
import numpy as np
from IPython.display import display

# --- 1. LEITURA E PREPARAÇÃO INICIAL DOS DADOS ---
caminho_rentabilidade = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src/vortx_estoques\data\135972-Rentabilidade_Sintetica.csv"

colunas_corretas = [
    'carteira', 'nomeFundo', 'tipoCarteira', 'administrador', 'periodo', 
    'emissao', 'data', 'valorCota', 'quantidade', 'numeroCotistas', 
    'variacaoDia', 'variacaoPeriodo', 'captacoes', 'resgate', 'eventos', 
    'pl', 'coluna_extra'
]

df_long = pd.read_csv(
    caminho_rentabilidade, 
    sep=';', 
    encoding='latin1',
    header=None,
    names=colunas_corretas
)

# --- 2. LIMPEZA E CONVERSÃO DE TIPOS ---
df_clean = df_long.iloc[1:].copy()

colunas_numericas = [
    'valorCota', 'pl', 'captacoes', 'resgate', 'eventos', 'variacaoDia'
]

def limpar_converter_numerico(series):
    return pd.to_numeric(
        series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False),
        errors='coerce'
    )

for col in colunas_numericas:
    df_clean[col] = limpar_converter_numerico(df_clean[col])

df_clean['data'] = pd.to_datetime(df_clean['data'], dayfirst=True)
df_clean['mes'] = df_clean['data'].dt.to_period('M')

df_clean = df_clean.sort_values(by=['carteira', 'nomeFundo', 'data'])

# --- 3. CÁLCULO DOS RETORNOS DIÁRIOS (COM MÉTODO DE DIETZ) ---

# Método 1: OFICIAL (via 'variacaoDia') - Continua sendo nossa referência.
df_clean['retorno_diario_oficial'] = df_clean['variacaoDia'] / 100

# Método 2: VERIFICAÇÃO (usando o Método de Dietz Modificado)
df_clean['pl_anterior'] = df_clean.groupby(['carteira', 'nomeFundo'])['pl'].shift(1)
df_clean['fluxo_caixa'] = df_clean['captacoes'] - df_clean['resgate'] - df_clean['eventos']

# O numerador não muda
numerador = df_clean['pl'] - df_clean['pl_anterior'] - df_clean['fluxo_caixa']

# O denominador agora considera o capital médio do dia
denominador_dietz = df_clean['pl_anterior'] + (df_clean['fluxo_caixa'] * 0.5)

# Aplicamos a nova fórmula
df_clean['retorno_diario_verificacao'] = np.where(
    denominador_dietz.notna() & (denominador_dietz != 0), 
    numerador / denominador_dietz, 
    0
)


# --- 4. FUNÇÃO PARA AGREGAR E EXIBIR PERFORMANCE ---

def calcular_e_exibir_performance(df, coluna_retorno, titulo):
    """Calcula a performance mensal a partir de uma coluna de retorno diário e exibe a tabela formatada."""
    df_calc = df.dropna(subset=[coluna_retorno]).copy()
    df_calc = df_calc[np.isfinite(df_calc[coluna_retorno])]
    df_calc['fator_diario'] = 1 + df_calc[coluna_retorno]
    
    fator_mensal = df_calc.groupby(['carteira', 'nomeFundo', 'mes'])['fator_diario'].prod()
    performance_mensal = (fator_mensal - 1).reset_index()
    performance_mensal.rename(columns={'fator_diario': 'performance_mensal'}, inplace=True)
    
    performance_pivot = performance_mensal.pivot_table(
        index=['carteira', 'nomeFundo'],
        columns='mes',
        values='performance_mensal'
    )
    
    performance_pivot_formatted = performance_pivot.style.format("{:.2%}", na_rep="-").set_caption(titulo)
    display(performance_pivot_formatted)

# --- 5. EXECUÇÃO E EXIBIÇÃO DAS TABELAS COMPARATIVAS ---

print("\n" + "="*80)
print("GERANDO AS TABELAS DE PERFORMANCE (MÉTODO DE VERIFICAÇÃO ATUALIZADO)")
print("="*80 + "\n")

# Tabela 1: O método oficial, para relatórios e comunicação externa.
calcular_e_exibir_performance(
    df_clean,
    'retorno_diario_oficial',
    'Tabela 1: Performance Mensal OFICIAL (via `variacaoDia`)'
)

# Tabela 2: O método de verificação, agora mais preciso.
calcular_e_exibir_performance(
    df_clean,
    'retorno_diario_verificacao',
    'Tabela 2: Performance de VERIFICAÇÃO (via PL com Dietz Modificado)'
)
#%%

# --- CÉLULA DE DEPURAÇÃO (VERSÃO FINAL E DINÂMICA) ---

# Configura o pandas para um formato de número mais legível e maior largura
pd.options.display.float_format = '{:,.4f}'.format
pd.set_option('display.width', 200)

# --- NÃO HÁ FILTROS DE DATA ---
# O código agora analisa o DataFrame completo para encontrar as maiores divergências em todo o histórico.

# 1. Copia o DataFrame principal para não alterar o original
df_debug_dinamico = df_clean.copy()

# 2. Calcula a diferença diária entre os dois métodos para todos os dados
df_debug_dinamico['diferenca_retorno'] = df_debug_dinamico['retorno_diario_verificacao'] - df_debug_dinamico['retorno_diario_oficial']

# 3. Seleciona as colunas mais importantes para a análise
colunas_debug = [
    'data',
    'nomeFundo',
    'retorno_diario_oficial',
    'retorno_diario_verificacao',
    'diferenca_retorno',
    'pl_anterior',
    'pl',
    'captacoes',
    'resgate',
    'eventos',
    'fluxo_caixa'
]

# 4. Ordena pela diferença absoluta para encontrar os dias mais problemáticos em todo o histórico
df_debug_sorted = df_debug_dinamico[colunas_debug].sort_values(
    by='diferenca_retorno', 
    key=abs,  # Ordena pelo valor absoluto da diferença
    ascending=False
)

print("\n" + "="*80)
print("TABELA DE DEPURAÇÃO DINÂMICA - TODO O PERÍODO")
print("Ordenado pelos dias com maior divergência entre o método Oficial e o de Verificação")
print("="*80 + "\n")

display(df_debug_sorted.head(15)) # Mostra os 15 dias mais divergentes de todo o histórico


#%% 
# --- CÓDIGO DE DEPURAÇÃO - V2 (FORMATAÇÃO CORRIGIDA) ---

# Configurações de visualização robustas
pd.options.display.float_format = '{:,.4f}'.format
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None) # <--- CORREÇÃO AQUI

# Copia o DataFrame principal
df_debug_dinamico = df_clean.copy()

# Calcula a diferença diária
df_debug_dinamico['diferenca_retorno'] = df_debug_dinamico['retorno_diario_verificacao'] - df_debug_dinamico['retorno_diario_oficial']

# Seleciona as colunas
colunas_debug = [
    'data', 'nomeFundo', 'retorno_diario_oficial', 'retorno_diario_verificacao', 'diferenca_retorno',
    'pl_anterior', 'pl', 'captacoes', 'resgate', 'eventos', 'fluxo_caixa'
]

# Ordena pela maior diferença absoluta
df_debug_sorted = df_debug_dinamico[colunas_debug].sort_values(
    by='diferenca_retorno', key=abs, ascending=False
)

print("\n" + "="*80)
print("TABELA DE DEPURAÇÃO DINÂMICA (FORMATAÇÃO CORRIGIDA)")
print("Ordenado pelos dias com maior divergência entre o método Oficial e o de Verificação")
print("="*80 + "\n")

display(df_debug_sorted.head(15))