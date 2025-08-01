# -*- coding: utf-8 -*-
"""
Script consolidado para diagnóstico e verificação da base de estoque.
"""

#%%
# =============================================================================
# Bloco 1: Importação de Bibliotecas e Configurações Iniciais
# =============================================================================
# Descrição: Importa as bibliotecas necessárias para a análise (pandas, glob, numpy, matplotlib)
# e define configurações de exibição para o pandas, garantindo que mais colunas e
# linhas sejam mostradas nas saídas.

import pandas as pd
from glob import glob
import numpy as np
import matplotlib.pyplot as plt

# Configurações de exibição do Pandas
pd.options.display.max_columns = 100
pd.options.display.max_rows = 200


#%%
# =============================================================================
# Bloco 2: Carregamento e Leitura dos Dados de Estoque
# =============================================================================
# Descrição: Localiza todos os arquivos CSV de estoque, define os tipos de dados
# para colunas específicas (data, texto, numérico) para otimizar a leitura e,
# em seguida, carrega e concatena todos os arquivos em um único DataFrame.

# Padrão para encontrar os arquivos de estoque
patt = '/home/felipe/portoauto/data_temp/fct_2025-07-14/*Estoque*.csv'  # Usando o caminho mais específico do primeiro script
list_files = glob(patt)
print("Arquivos encontrados:", list_files)

# Definição das colunas por tipo de dado
colunas_data = [
    'DataEmissao', 'DataAquisicao', 'DataVencimento', 'DataGeracao'
]

colunas_texto = [
    'Situacao', 'PES_TIPO_PESSOA', 'CedenteCnpjCpf', 'TIT_CEDENTE_ENT_CODIGO',
    'CedenteNome', 'Cnae', 'SecaoCNAEDescricao', 'NotaPdd', 'SAC_TIPO_PESSOA',
    'SacadoCnpjCpf', 'SacadoNome', 'IdTituloVortx', 'TipoAtivo', 'NumeroBoleto',
    'NumeroTitulo', 'CampoChave', 'PagamentoParcial', 'Coobricacao',
    'CampoAdicional1', 'CampoAdicional2', 'CampoAdicional3', 'CampoAdicional4',
    'CampoAdicional5', 'IdTituloVortxOriginador', 'Registradora',
    'IdContratoRegistradora', 'IdTituloRegistradora', 'CCB', 'Convênio'
]

# Dicionário de tipos para colunas de texto
dtype_texto = {col: str for col in colunas_texto}

# Leitura e concatenação dos arquivos CSV
dfs = []
for file in list_files:
    print(f"Lendo o arquivo: {file}")
    df_ = pd.read_csv(file, sep=';', encoding='latin1', dtype=dtype_texto,
                      decimal=',', parse_dates=colunas_data, dayfirst=True)
    dfs.append(df_)

df_final = pd.concat(dfs, ignore_index=True)
print("Leitura e concatenação concluídas.")


#%%
# =============================================================================
# Bloco 3: Engenharia de Atributos (Criação de Colunas Auxiliares)
# =============================================================================
# Descrição: Cria novas colunas no DataFrame para facilitar análises futuras.
# - _ValorLiquido: Valor Presente ajustado pelo PDD.
# - _ValorVencido: Identifica o valor de títulos já vencidos.
# - _MuitosContratos: Flag para sacados com um número elevado de contratos (CCB).
# - _MuitosEntes: Flag para sacados associados a múltiplos convênios.

# Criar coluna de Valor Líquido
df_final['_ValorLiquido'] = df_final['ValorPresente'] - df_final['PDDTotal']

# Criar coluna de Valor Vencido
df_final['_ValorVencido'] = (df_final['DataVencimento'] <= df_final['DataGeracao']).astype('int') * df_final['ValorPresente']

# Identificar sacados com muitos contratos (CCB)
sacado_contratos = df_final.groupby('SacadoNome')['CCB'].nunique()
k = 3
mask_contratos = sacado_contratos >= k
sacado_contratos_alto = sacado_contratos[mask_contratos].index
df_final['_MuitosContratos'] = df_final['SacadoNome'].isin(sacado_contratos_alto).astype(str)

# Identificar sacados com muitos entes (Convênios)
sacados_entes = df_final.groupby('SacadoCnpjCpf')['Convênio'].nunique()
k2 = 3
mask_entes = sacados_entes >= k2
sacados_entes_alto = sacados_entes[mask_entes].index
df_final['_MuitosEntes'] = df_final['SacadoCnpjCpf'].isin(sacados_entes_alto).astype(str)

print("Criação de colunas auxiliares concluída.")


#%%
# =============================================================================
# Bloco 4: Verificação Inicial do DataFrame
# =============================================================================
# Descrição: Realiza uma verificação rápida do DataFrame, mostrando o uso de
# memória e o valor total do estoque (Valor Presente).

memoria_mb = df_final.memory_usage(deep=True).sum() / 1024**2
print(f"Uso de memória do DataFrame: {memoria_mb:.2f} MB")

valor_total_estoque = df_final["ValorPresente"].sum()
print(f"Valor Presente Total do Estoque: R$ {valor_total_estoque:_.2f}".replace('.', ',').replace('_', '.'))


#%%
# =============================================================================
# Bloco 5: Análise Exploratória Geral com value_counts()
# =============================================================================
# Descrição: Itera sobre as colunas de texto para entender a distribuição de suas
# categorias. As perguntas no comentário guiam esta exploração.

# Comentários / dúvidas do segundo script:
# 1. [Situacao] - diferença entre sem cobrança e aditado
# 2. [SAC_TIPO_PESSOA] - tipo J = jurídico?? tem isso?
# 3. [SacadoCnpjCpf] - por que tem CNPJ?
# 4. [SacadoNome] 'BMP SOCIEDADE DE CREDITO DIRETO S.A'
# 5. [SacadoCnpjCpf'] - verificar consistência dos CPFs
# 6. [TipoAtivo] - CCB e Contrato. Por que tem contrato?
# 7. [DataGeracao] - data de referência ou data de processamento?
# 8. [SacadoCnpjCpf] - tem sacado com 1040 linhas (!)
# 9. [Convênio] - sacados com muitos convênios (3)

df_final2 = df_final[~df_final['Situacao'].isna()].copy()

print("Analisando a contagem de valores para colunas de texto (geral):")
for col in df_final2.select_dtypes(include=['object']).columns:
    print(f"--- Análise da coluna: {col} ---")
    print(df_final2[col].value_counts(dropna=False))
    print('*' * 80)


#%%
# =============================================================================
# Bloco 6: Verificação do Impacto de Linhas com 'Situacao' Nula
# =============================================================================
# Descrição: Analisa se as linhas com 'Situacao' nula têm impacto significativo
# no cálculo do Valor Presente total.

print("Verificando o impacto das linhas com 'Situacao' nula...")
v1_ = df_final['ValorPresente'].sum()
v2_ = df_final[~df_final['Situacao'].isna()]['ValorPresente'].sum()
print(f"Valores são próximos? {np.isclose(v1_, v2_)}")

# Libera memória
del df_final


#%%
# =============================================================================
# Bloco 7: Análise de Sacados com SAC_TIPO_PESSOA == 'J'
# =============================================================================
# Descrição: Isola e analisa registros onde o tipo de sacado é 'J'.

print("Analisando sacados com tipo 'J'...")
df_sacado_J = df_final2[df_final2['SAC_TIPO_PESSOA'] == 'J']
if not df_sacado_J.empty:
    print("Tamanhos de 'SacadoCnpjCpf' para tipo 'J':", df_sacado_J['SacadoCnpjCpf'].map(len).unique())
    display(df_sacado_J.sample(min(5, len(df_sacado_J))))
else:
    print("Nenhum sacado do tipo 'J' encontrado.")


#%%
# =============================================================================
# Bloco 8: Análise Específica de Sacados com CNPJ
# =============================================================================
# Descrição: Filtra sacados com CNPJ e realiza uma análise detalhada sobre eles,
# incluindo uma nova verificação de value_counts() para todas as colunas
# categóricas dentro deste subconjunto.

print("Analisando sacados com CNPJ...")
df_final2_cnpj = df_final2[df_final2['SacadoCnpjCpf'].map(len) == 18].copy()

if not df_final2_cnpj.empty:
    print("\nNomes únicos de sacados com CNPJ:")
    print(df_final2_cnpj['SacadoNome'].unique())

    # >>> VERIFICAÇÃO EXTRA ADICIONADA AQUI <<<
    print("\n--- value_counts() para o subconjunto de Sacados com CNPJ ---")
    for col in df_final2_cnpj.select_dtypes(include=['object']).columns:
        print(f"--- Análise da coluna: {col} (Apenas CNPJ) ---")
        print(df_final2_cnpj[col].value_counts(dropna=False))
        print('*'*80)

    print("\nEstatísticas descritivas para dados numéricos de sacados com CNPJ:")
    display(df_final2_cnpj.describe(include=[np.number]))

    print("\nSoma dos valores numéricos:")
    display(df_final2_cnpj.select_dtypes(include=[np.number]).sum())

    # Exportar para Excel
    # df_final2_cnpj.to_excel('df_final2_cnpj.xlsx')
else:
    print("Nenhum sacado com formato de CNPJ (18 caracteres) encontrado.")


#%%
# =============================================================================
# Bloco 9: Validação de CPFs na Coluna 'SacadoCnpjCpf'
# =============================================================================
# Descrição: Aplica uma função para validar a estrutura matemática de CPFs.

def validar_cpf(cpf):
    """Função para validar um número de CPF."""
    cpf = ''.join(filter(str.isdigit, str(cpf)))
    if len(cpf) != 11 or cpf == cpf[0] * 11: return False
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    resto = (soma * 10) % 11
    if resto == 10: resto = 0
    if resto != int(cpf[9]): return False
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    resto = (soma * 10) % 11
    if resto == 10: resto = 0
    if resto != int(cpf[10]): return False
    return True

mask_cpf = df_final2['SacadoCnpjCpf'].map(len) == 14
df_final2['CPF_válido'] = False
df_final2.loc[mask_cpf, 'CPF_válido'] = df_final2.loc[mask_cpf, 'SacadoCnpjCpf'].apply(validar_cpf)
cpfs_invalidos = df_final2[(mask_cpf) & (~df_final2['CPF_válido'])]['SacadoCnpjCpf'].unique()
print(f"Encontrados {len(cpfs_invalidos)} CPFs com estrutura inválida.")


#%%
# =============================================================================
# Bloco 10: Análise do Percentual de PDD por Variável Categórica
# =============================================================================
# Descrição: Calcula o percentual de PDD agrupado por diversas variáveis
# categóricas para identificar categorias de maior risco.

cat_cols = [
    'Situacao', 'CedenteNome', 'SAC_TIPO_PESSOA', 'PagamentoParcial',
    'TipoAtivo', '_MuitosContratos', '_MuitosEntes', 'Convênio'
]

pdd_ref = (1 - df_final2['_ValorLiquido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"PDD de Referência (Total): {pdd_ref:.2f}%\n")

for col in cat_cols:
    print(f"--- Análise de PDD por '{col}' ---")
    aux_ = df_final2.groupby(col)[['_ValorLiquido', 'ValorPresente']].sum() / 1e6
    aux_['%PDD'] = (1 - aux_['_ValorLiquido'] / aux_['ValorPresente']) * 100
    if col == 'Convênio':
        aux_ = aux_.sort_values('ValorPresente', ascending=False)
    display(aux_.head(20))
    print("\n" + "="*80 + "\n")


#%%
# =============================================================================
# Bloco 11: Análise do Percentual de Títulos Vencidos por Variável Categórica
# =============================================================================
# Descrição: Calcula a proporção do valor vencido em relação ao Valor Presente
# para cada categoria, identificando os grupos com maior inadimplência.

venc_ref = (df_final2['_ValorVencido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"Percentual de Vencidos de Referência (Total): {venc_ref:.2f}%\n")

for col in cat_cols:
    print(f"--- Análise de Vencidos por '{col}' ---")
    aux_ = df_final2.groupby(col)[['_ValorVencido', 'ValorPresente']].sum() / 1e6
    aux_['%Vencido'] = (aux_['_ValorVencido'] / aux_['ValorPresente']) * 100
    if col == 'Convênio':
        aux_ = aux_.sort_values('ValorPresente', ascending=False)
    else:
        aux_ = aux_.sort_values('%Vencido', ascending=False)
    display(aux_.head(20))
    print("\n" + "="*80 + "\n")


#%%
# =============================================================================
# Bloco 12: Verificação de Sacados Presentes em Múltiplos Convênios ('Entes')
# =============================================================================
# Descrição: Identifica sacados com pulverização entre diferentes parceiros.

print("Analisando sacados presentes em múltiplos convênios...")
sacados_multi_entes = df_final2.groupby('SacadoCnpjCpf')['Convênio'].agg(['nunique', pd.unique])
sacados_multi_entes = sacados_multi_entes.sort_values('nunique', ascending=False)
display(sacados_multi_entes.head(35))


#%%
# =============================================================================
# Bloco 13: Verificação de Consistência das Datas
# =============================================================================
# Descrição: Realiza verificações de sanidade nas colunas de data.

print("Verificando consistência das datas...")
check1 = (df_final2['DataEmissao'] > df_final2['DataAquisicao']).sum()
print(f"Registros com Data de Emissão > Data de Aquisição: {check1}")
check2 = (df_final2['DataAquisicao'] > df_final2['DataVencimento']).sum()
print(f"Registros com Data de Aquisição > Data de Vencimento: {check2}")


#%%
# =============================================================================
# Bloco 14: Verificação de Consistência dos Valores Monetários
# =============================================================================
# Descrição: Realiza verificações de sanidade nos valores financeiros.

print("\nVerificando consistência dos valores...")
check_v1 = (df_final2['ValorAquisicao'] > df_final2['ValorNominal']).sum()
print(f"Registros com Valor de Aquisição > Valor Nominal: {check_v1}")
check_v2 = (df_final2['ValorAquisicao'] > df_final2['ValorPresente']).sum()
print(f"Registros com Valor de Aquisição > Valor Presente: {check_v2}")
check_v3 = (df_final2['ValorPresente'] > df_final2['ValorNominal']).sum()
print(f"Registros com Valor Presente > Valor Nominal: {check_v3}")


#%%
# =============================================================================
# Bloco 15: Análise Focada de Métricas de Desempenho (PDD e Vencidos)
# =============================================================================
# Descrição: Este bloco executa uma análise direcionada das métricas de PDD
# e Vencidos, segmentando os resultados por Cedente, Tipo de Contrato (TipoAtivo)
# e Ente Consignado (Convênio), conforme solicitado.

# Lista das dimensões para a análise
dimensoes_analise = {
    'Cedentes': 'CedenteNome',
    'Tipo de Contrato': 'TipoAtivo',
    'Ente Consignado': 'Convênio'
}

print("="*80)
print("INICIANDO ANÁLISE FOCADA DE MÉTRICAS DE DESEMPENHO")
print("="*80)

# --- 1. ANÁLISE DE PERCENTUAL DE PDD POR SEGMENTO ---
print("\n\n--- [Análise de Risco: % PDD] ---\n")
pdd_ref = (1 - df_final2['_ValorLiquido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"PDD de Referência da Carteira Total: {pdd_ref:.2f}%\n")

for nome_analise, coluna in dimensoes_analise.items():
    print(f"--> Análise de PDD por '{nome_analise}' (Coluna: '{coluna}')")

    # Agrupa os dados, soma os valores e converte para milhões para melhor leitura
    aux_pdd = df_final2.groupby(coluna)[['_ValorLiquido', 'ValorPresente']].sum() / 1e6
    aux_pdd.rename(columns={'ValorPresente': 'ValorPresente (M)', '_ValorLiquido': 'ValorLiquido (M)'}, inplace=True)

    # Calcula o %PDD
    aux_pdd['%PDD'] = (1 - aux_pdd['ValorLiquido (M)'] / aux_pdd['ValorPresente (M)']) * 100

    # Ordena por Valor Presente para mostrar os maiores segmentos primeiro
    aux_pdd = aux_pdd.sort_values('ValorPresente (M)', ascending=False)

    # Exibe a tabela com os resultados
    display(aux_pdd.head(20))
    print("\n")


# --- 2. ANÁLISE DE PERCENTUAL DE VENCIDOS POR SEGMENTO ---
print("\n\n" + "="*80)
print("\n--- [Análise de Inadimplência: % Vencido] ---\n")
venc_ref = (df_final2['_ValorVencido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"Percentual de Vencidos da Carteira Total: {venc_ref:.2f}%\n")

for nome_analise, coluna in dimensoes_analise.items():
    print(f"--> Análise de Vencidos por '{nome_analise}' (Coluna: '{coluna}')")

    # Agrupa os dados, soma os valores e converte para milhões
    aux_venc = df_final2.groupby(coluna)[['_ValorVencido', 'ValorPresente']].sum() / 1e6
    aux_venc.rename(columns={'ValorPresente': 'ValorPresente (M)', '_ValorVencido': 'ValorVencido (M)'}, inplace=True)
    
    # Calcula o % Vencido
    aux_venc['%Vencido'] = (aux_venc['ValorVencido (M)'] / aux_venc['ValorPresente (M)']) * 100

    # Ordena pelo %Vencido para destacar os piores desempenhos
    aux_venc = aux_venc.sort_values('%Vencido', ascending=False)

    # Exibe a tabela com os resultados
    display(aux_venc.head(20))
    print("\n")

print("="*80)
print("FIM DA ANÁLISE FOCADA")
print("="*80)