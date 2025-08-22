# -*- coding: utf-8 -*-
"""
Script de Depuração para Análise de Contratos Vencidos
"""

# =============================================================================
# Bibliotecas e Configurações Essenciais
# =============================================================================
import pandas as pd
import numpy as np

pd.options.display.max_columns = 100
pd.options.display.max_rows = 200
pd.options.display.float_format = '{:.2f}'.format # Formatação para facilitar a leitura

# =============================================================================
# 1. Leitura e Preparação dos Dados (Parte Mínima Necessária)
# =============================================================================
# ATENÇÃO: Verifique se os caminhos abaixo estão corretos
path_starcard = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\StarCard.xlsx'
path_originadores = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\Originadores.xlsx'

print("Lendo e preparando os dados...")
try:
    df_starcard = pd.read_excel(path_starcard)
    df_originadores = pd.read_excel(path_originadores, usecols=['CCB']) # Lendo apenas o CCB para unir
except FileNotFoundError as e:
    print(f"[ERRO] Arquivo não encontrado. Verifique o caminho: {e}")
    # Encerra o script se os arquivos não forem encontrados
    exit()

# Unindo as fontes de dados
df_merged = pd.merge(df_starcard, df_originadores.drop_duplicates(subset='CCB'), on='CCB', how='left')

# Renomeando colunas essenciais
df_merged = df_merged.rename(columns={
    'Data Referencia': 'DataGeracao',
    'Data Vencimento': 'DataVencimento',
})

# Limpeza das colunas de data (passo crucial para a depuração)
# Usamos 'coerce' para transformar datas inválidas em NaT (Not a Time)
df_merged['DataGeracao'] = pd.to_datetime(df_merged['DataGeracao'], errors='coerce')
df_merged['DataVencimento'] = pd.to_datetime(df_merged['DataVencimento'], errors='coerce')

# Vamos chamar nosso dataframe de trabalho de 'df_debug'
df_debug = df_merged[['CCB', 'DataGeracao', 'DataVencimento']].copy()

print("Dados carregados com sucesso.")
print("-" * 50)

# =============================================================================
# 2. VERIFICAÇÕES DE SANIDADE DOS DADOS (Ponto Chave da Depuração)
# =============================================================================
print("Iniciando verificações de sanidade das datas...")

# V2.1: Verificar se há valores nulos ou inválidos nas datas
null_geracao = df_debug['DataGeracao'].isnull().sum()
null_vencimento = df_debug['DataVencimento'].isnull().sum()

if null_geracao > 0 or null_vencimento > 0:
    print(f"\n[ALERTA] Foram encontrados valores nulos/inválidos nas colunas de data:")
    print(f" - Datas de Geração Nulas: {null_geracao}")
    print(f" - Datas de Vencimento Nulas: {null_vencimento}")
    # Removemos as linhas com datas nulas para não afetar o cálculo
    df_debug.dropna(subset=['DataGeracao', 'DataVencimento'], inplace=True)
    print(" -> Linhas com datas nulas foram removidas para a análise.\n")
else:
    print("Nenhum valor nulo encontrado nas colunas de data. Ótimo!")

# V2.2: Verificar o intervalo das datas para ver se fazem sentido
print("Intervalo de datas no arquivo:")
print(f" - Data de Geração (Referência): de {df_debug['DataGeracao'].min().strftime('%Y-%m-%d')} a {df_debug['DataGeracao'].max().strftime('%Y-%m-%d')}")
print(f" - Data de Vencimento: de {df_debug['DataVencimento'].min().strftime('%Y-%m-%d')} a {df_debug['DataVencimento'].max().strftime('%Y-%m-%d')}")

# V2.3: Verificar se a 'DataGeracao' é uma data única (MUITO IMPORTANTE)
datas_geracao_unicas = df_debug['DataGeracao'].unique()
if len(datas_geracao_unicas) > 1:
    print(f"\n[ALERTA IMPORTANTE] Existem {len(datas_geracao_unicas)} datas de referência (DataGeracao) diferentes no arquivo.")
    print("Isso pode distorcer a análise de dias de atraso se não for esperado.")
    print("A análise continuará usando a data de referência de cada linha.")
else:
    print(f"\nO arquivo possui uma única data de referência: {pd.to_datetime(datas_geracao_unicas[0]).strftime('%Y-%m-%d')}. Perfeito!")

print("-" * 50)

# =============================================================================
# 3. Cálculo dos Dias de Atraso e Flags de Vencimento
# =============================================================================
print("Calculando dias de atraso...")

# O cálculo é feito linha a linha, respeitando a DataGeracao de cada registro
df_debug['dias_de_atraso'] = (df_debug['DataGeracao'] - df_debug['DataVencimento']).dt.days

# V3.1: Exibir um resumo estatístico dos dias de atraso para entender a distribuição
print("\nResumo dos 'dias_de_atraso' calculados:")
print(df_debug['dias_de_atraso'].describe())
print("\nValores negativos significam parcelas 'a vencer'.")
print("Valores positivos significam parcelas 'vencidas'.")

# V3.2: Criar flags para cada faixa de vencimento solicitada
# Nota: dias_de_atraso >= 1 já significa vencido.
df_debug['_ParcelaVencida_1d'] = (df_debug['dias_de_atraso'] >= 1).astype(int)
df_debug['_ParcelaVencida_30d'] = (df_debug['dias_de_atraso'] >= 30).astype(int)
df_debug['_ParcelaVencida_60d'] = (df_debug['dias_de_atraso'] >= 60).astype(int)

# V3.3: Exibir uma amostra para verificação manual
print("\nAmostra dos dados com dias de atraso e flags (5 vencidas e 5 a vencer):")
vencidas_sample = df_debug[df_debug['dias_de_atraso'] > 0].head()
avencer_sample = df_debug[df_debug['dias_de_atraso'] <= 0].head()
display(pd.concat([vencidas_sample, avencer_sample]))
print("-" * 50)

# =============================================================================
# 4. Lógica de Contaminação e Cálculo Final dos Percentuais
# =============================================================================
print("Aplicando a lógica de contaminação por contrato (CCB)...")

# Dicionário para armazenar os resultados
resultados = {}

# Número total de contratos únicos na base
total_contratos_unicos = df_debug['CCB'].nunique()
resultados['Total de Contratos Únicos'] = total_contratos_unicos

# Loop para calcular para cada faixa de dias
for dias in [1, 30, 60]:
    flag_parcela = f'_ParcelaVencida_{dias}d'
    
    # 1. Agrupar por CCB e verificar se ALGUMA parcela está vencida
    contratos_com_parcela_vencida = df_debug.groupby('CCB')[flag_parcela].max()
    
    # 2. Filtrar para obter a lista de CCBs "contaminados"
    ccbs_vencidos = contratos_com_parcela_vencida[contratos_com_parcela_vencida == 1].index
    
    # 3. Contar quantos CCBs únicos estão nessa condição
    num_contratos_vencidos = len(ccbs_vencidos)
    
    # 4. Calcular o percentual
    percentual_vencido = (num_contratos_vencidos / total_contratos_unicos) * 100 if total_contratos_unicos > 0 else 0
    
    # Armazenar resultados
    resultados[f'Nº Contratos Vencidos (>{dias}d)'] = num_contratos_vencidos
    resultados[f'% Contratos Vencidos (>{dias}d)'] = percentual_vencido

print("\nCálculo finalizado.")
print("=" * 60)
print("RESULTADOS DA DEPURAÇÃO")
print("=" * 60)
print(f"  Total de Contratos Únicos na Carteira: {resultados['Total de Contratos Únicos']:,}")
print("-" * 60)
print(f"  Contratos com pelo menos uma parcela vencida há > 1 dia:")
print(f"    - Nº de Contratos: {resultados['Nº Contratos Vencidos (>1d)']:,}")
print(f"    - Percentual: {resultados['% Contratos Vencidos (>1d)']:.2f}%")
print("-" * 60)
print(f"  Contratos com pelo menos uma parcela vencida há > 30 dias:")
print(f"    - Nº de Contratos: {resultados['Nº Contratos Vencidos (>30d)']:,}")
print(f"    - Percentual: {resultados['% Contratos Vencidos (>30d)']:.2f}%")
print("-" * 60)
print(f"  Contratos com pelo menos uma parcela vencida há > 60 dias:")
print(f"    - Nº de Contratos: {resultados['Nº Contratos Vencidos (>60d)']:,}")
print(f"    - Percentual: {resultados['% Contratos Vencidos (>60d)']:.2f}%")
print("=" * 60)