
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
caminho_feriados = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\feriados_nacionais.xls'
patt = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\data_temp\fct_2025-07-14\*Estoque*.csv'  # Usando o caminho mais específico do primeiro script
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
    #! DISPLAY, NO JUPYTER
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



#%%
# =============================================================================
# Bloco 16: Cálculo da TIR (Taxa Interna de Retorno) da Carteira a Vencer (v8)
# =============================================================================
# Descrição: Este bloco calcula a TIR mensal da carteira a vencer.
# v8: Adiciona um tratamento para os casos em que a TIR resulta em NaN.
#     Especificamente, se a PDD de um segmento for >= 100% do seu Valor Presente,
#     a TIR Líquida (PDD) e a TIR completa são definidas como -100%,
#     representando a perda total do principal.

from scipy.optimize import newton
import numpy as np
import pandas as pd

# --- 1. DEFINIÇÕES DE CUSTOS E FUNÇÃO AUXILIAR XIRR ---

COST_DICT = {
    'ASSEMBLEIA. MATO GROSSO': [0.03, 2.14],
    'GOV. ALAGOAS': [0.035, 5.92],
}
DEFAULT_COST = COST_DICT.get('GOV. ALAGOAS', [0.035, 5.92])

def calculate_xirr(cash_flows, days):
    cash_flows, days = np.array(cash_flows), np.array(days)
    def npv(rate):
        if rate <= -1: return float('inf')
        return np.sum(cash_flows / (1 + rate) ** (days / 21.0))
    try:
        return newton(npv, 0.015)
    except (RuntimeError, ValueError):
        return np.nan

# --- 2. PREPARAÇÃO DOS DADOS PARA O CÁLCULO ---

print("="*80)
print("INICIANDO CÁLCULO DA TIR (TAXA INTERNA DE RETORNO)")
print("="*80)

ref_date = df_final2['DataGeracao'].max()
print(f"Data de Referência para o cálculo: {ref_date.strftime('%d/%m/%Y')}")

try:
    df_feriados = pd.read_excel(caminho_feriados)
    holidays = pd.to_datetime(df_feriados['Data']).values.astype('datetime64[D]')
    print(f"Sucesso: {len(holidays)} feriados carregados de '{caminho_feriados}'.")
except Exception as e:
    print(f"[AVISO] Não foi possível carregar feriados. Erro: {e}")
    holidays = []

df_avencer = df_final2[df_final2['DataVencimento'] > ref_date].copy()
print(f"Total de {len(df_avencer)} parcelas a vencer consideradas na análise.")

try:
    start_dates = np.datetime64(ref_date.date())
    end_dates = df_avencer['DataVencimento'].values.astype('datetime64[D]')
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.busday_count(start_dates, end_dates, holidays=holidays)
    df_avencer = df_avencer[df_avencer['_DIAS_UTEIS_'] > 0]
except Exception as e:
    print(f"[ERRO] Não foi possível calcular os dias úteis: {e}")
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.nan

df_avencer['CustoVariavel'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[0])
df_avencer['CustoFixo'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[1])
df_avencer['CustoTotal'] = df_avencer['CustoFixo'] + (df_avencer['CustoVariavel'] * df_avencer['ValorNominal'])
df_avencer['ReceitaLiquida'] = df_avencer['ValorNominal'] - df_avencer['CustoTotal']


# --- 3. CÁLCULO DA TIR POR SEGMENTO (COM TRATAMENTO DE NAN) ---

dimensoes = ['Convênio', 'CedenteNome', 'TipoAtivo']
all_tirs = []

segmentos_para_analise = [('Carteira Total', 'Todos')] + \
                         [(dim, seg) for dim in dimensoes for seg in df_avencer[dim].dropna().unique()]

for tipo_dimensao, segmento in segmentos_para_analise:
    if tipo_dimensao == 'Carteira Total':
        df_segmento = df_avencer
    else:
        df_segmento = df_avencer[df_avencer[tipo_dimensao] == segmento]

    if df_segmento.empty or df_segmento['_DIAS_UTEIS_'].isnull().all():
        continue

    vp_bruto = df_segmento['ValorPresente'].sum()
    tir_bruta, tir_pdd, tir_custos, tir_completa = np.nan, np.nan, np.nan, np.nan
    
    if vp_bruto > 0:
        pdd_total = df_segmento['PDDTotal'].sum()
        pdd_rate = pdd_total / vp_bruto
        
        # 3.1. TIR Bruta
        fluxos_brutos = df_segmento.groupby('_DIAS_UTEIS_')['ValorNominal'].sum()
        tir_bruta = calculate_xirr([-vp_bruto] + fluxos_brutos.values.tolist(), [0] + fluxos_brutos.index.tolist())

        # 3.2. TIR Líquida (PDD)
        fluxos_pdd = (df_segmento['ValorNominal'] * (1 - pdd_rate)).groupby(df_segmento['_DIAS_UTEIS_']).sum()
        tir_pdd = calculate_xirr([-vp_bruto] + fluxos_pdd.values.tolist(), [0] + fluxos_pdd.index.tolist())
        #* o caso de PDD >= 100%
        if pd.isna(tir_pdd) and pdd_rate >= 1:
            tir_pdd = -1.0 # Representa -100% de retorno

        # 3.3. TIR Líquida (Custos)
        fluxos_custos = df_segmento.groupby('_DIAS_UTEIS_')['ReceitaLiquida'].sum()
        tir_custos = calculate_xirr([-vp_bruto] + fluxos_custos.values.tolist(), [0] + fluxos_custos.index.tolist())

        # 3.4. TIR Líquida (PDD & Custos)
        fluxos_completos = (df_segmento['ReceitaLiquida'] * (1 - pdd_rate)).groupby(df_segmento['_DIAS_UTEIS_']).sum()
        tir_completa = calculate_xirr([-vp_bruto] + fluxos_completos.values.tolist(), [0] + fluxos_completos.index.tolist())
        #* o caso de PDD >= 100%
        if pd.isna(tir_completa) and pdd_rate >= 1:
            tir_completa = -1.0 # Representa -100% de retorno

    all_tirs.append({
        'Dimensão': tipo_dimensao,
        'Segmento': segmento,
        'Valor Presente (M)': vp_bruto / 1e6,
        'TIR Bruta a.m. (%)': tir_bruta * 100 if pd.notna(tir_bruta) else np.nan,
        'TIR Líquida (PDD) a.m. (%)': tir_pdd * 100 if pd.notna(tir_pdd) else np.nan,
        'TIR Líquida (Custos) a.m. (%)': tir_custos * 100 if pd.notna(tir_custos) else np.nan,
        'TIR Líquida (PDD & Custos) a.m. (%)': tir_completa * 100 if pd.notna(tir_completa) else np.nan,
    })

# --- 4. APRESENTAÇÃO DOS RESULTADOS ---

if all_tirs:
    df_tir_summary = pd.DataFrame(all_tirs)
    print("\n\n--- Tabela de Resumo da Taxa Interna de Retorno (TIR) ---\n")
    
    for dim in ['Carteira Total'] + dimensoes:
        df_display = df_tir_summary[df_tir_summary['Dimensão'] == dim]
        if not df_display.empty:
            print(f"--> TIR por '{dim}':")
            # Arredonda os valores para melhor visualização
            display(df_display.sort_values('Valor Presente (M)', ascending=False).drop(columns=['Dimensão']).round(4))
            print("\n")
else:
    print("\nNão foi possível calcular a TIR. Verifique se existem parcelas a vencer na carteira.")

print("="*80)
print("FIM DO CÁLCULO DE TIR")
print("="*80)



#%% 
#%%
# =============================================================================
# Bloco 18: Visualização Gráfica da Distribuição da Carteira (v20)
# =============================================================================
# Descrição: Gera gráficos para analisar a distribuição da carteira.
# v20: Atende à solicitação do usuário para não mostrar nos gráficos os
#      entes cuja TIR calculada resultou em -100%.

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

print("="*80)
print("INICIANDO GERAÇÃO DE GRÁFICOS DE DISTRIBUIÇÃO")
print("="*80)

# --- 1. PREPARAÇÃO DO DATAFRAME DE RESUMO PARA OS GRÁFICOS ---

df_resumo_metricas = df_final2.groupby('Convênio').agg(
    ValorPresente=('ValorPresente', 'sum'),
    ValorLiquido=('_ValorLiquido', 'sum'),
    ValorVencido=('_ValorVencido', 'sum'),
    PDDTotal=('PDDTotal', 'sum')
).reset_index()

df_resumo_metricas['%Vencido'] = (df_resumo_metricas['ValorVencido'] / df_resumo_metricas['ValorPresente']) * 100
df_resumo_metricas['%PDD'] = (df_resumo_metricas['PDDTotal'] / df_resumo_metricas['ValorPresente']) * 100

df_resumo_tir = df_tir_summary[df_tir_summary['Dimensão'] == 'Convênio'].copy()

df_graficos = pd.merge(
    df_resumo_metricas,
    df_resumo_tir.rename(columns={'Segmento': 'Convênio'}),
    on='Convênio',
    how='left'
)


# --- 2. FUNÇÃO DE PLOTAGEM DE RANKING APRIMORADA ---

def plot_ranking_com_outros(df, metric_col, title, n=15, agg_type='sum', is_percent=False):
    df_sorted = df.dropna(subset=[metric_col]).sort_values(metric_col, ascending=False)
    
    if len(df_sorted) > n:
        df_top = df_sorted.head(n)
        df_others = df_sorted.iloc[n:]
        
        if agg_type == 'sum':
            others_value = df_others[metric_col].sum()
        else: # 'mean'
            others_value = df_others[metric_col].mean()
            
        others_name = f'Outros ({len(df_others)} entes)'
        df_outros = pd.DataFrame([{'Convênio': others_name, metric_col: others_value}])
        df_to_plot = pd.concat([df_top, df_outros], ignore_index=True)
    else:
        df_to_plot = df_sorted

    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(12, 8))
    bars = plt.barh(df_to_plot['Convênio'], df_to_plot[metric_col], color=sns.color_palette("viridis", len(df_to_plot)))
    plt.gca().invert_yaxis()
    
    for bar in bars:
        width = bar.get_width()
        if pd.isna(width): continue
        label_format = '{:,.2f}%' if is_percent else 'R$ {:,.2f}M'
        label_value = width if is_percent else width / 1e6
        plt.text(width, bar.get_y() + bar.get_height()/2, f' {label_format.format(label_value)}', va='center')
        
    plt.title(title, fontsize=16, weight='bold')
    plt.xlabel('Valor' + (' (%)' if is_percent else ''), fontsize=12)
    plt.ylabel('Ente (Convênio)', fontsize=12)
    plt.tight_layout()
    plt.show()

# --- 3. GERAÇÃO DOS GRÁFICOS ---

# Gráfico 1: Composição da Carteira (não muda)
print("\nGerando Gráfico 1: Distribuição da Carteira por Valor Líquido...")
plot_ranking_com_outros(
    df_graficos, 'ValorLiquido',
    'Gráfico 1: Distribuição da Carteira por Valor Líquido (Top 15 + Outros)',
    n=15, agg_type='sum', is_percent=False
)

# Gráfico 2: Ranking por Risco (% Vencido) (não muda)
print("\nGerando Gráfico 2: Ranking por Risco...")
plot_ranking_com_outros(
    df_graficos, '%Vencido',
    'Gráfico 2: Top 15 Entes por % de Carteira Vencida',
    n=15, agg_type='mean', is_percent=True
)

# Gráfico 3: Ranking por Retorno (TIR)
# **NOVO**: Filtra os entes com TIR de -100% antes de plotar
df_tir_filtrado = df_graficos[df_graficos['TIR Líquida (PDD & Custos) a.m. (%)'] != -100.0]
plot_ranking_com_outros(
    df_tir_filtrado, 'TIR Líquida (PDD & Custos) a.m. (%)',
    'Gráfico 3: Top 15 Entes por Retorno (TIR Líquida Completa)',
    n=15, agg_type='mean', is_percent=True
)


# --- 4. GRÁFICO 4: ANÁLISE DE RISCO vs. RETORNO (SIMPLIFICADO) ---

print("\nGerando Gráfico 4: Risco vs. Retorno Simplificado...")

# Prepara os dados, tratando valores nulos ou infinitos que podem quebrar o gráfico
df_scatter = df_graficos.dropna(subset=['%Vencido', 'TIR Líquida (PDD & Custos) a.m. (%)']).copy()
df_scatter = df_scatter[
    (df_scatter['TIR Líquida (PDD & Custos) a.m. (%)'] != -100.0) &
    (np.isfinite(df_scatter['%Vencido'])) &
    (np.isfinite(df_scatter['TIR Líquida (PDD & Custos) a.m. (%)']))
]

if not df_scatter.empty:
    plt.figure(figsize=(14, 9))
    
    sns.scatterplot(
        data=df_scatter,
        x='%Vencido',
        y='TIR Líquida (PDD & Custos) a.m. (%)',
        color='darkcyan',
        alpha=0.7
    )

    mean_risk = df_scatter['%Vencido'].mean()
    mean_tir = df_scatter['TIR Líquida (PDD & Custos) a.m. (%)'].mean()
    plt.axvline(mean_risk, color='red', linestyle='--', lw=1)
    plt.axhline(mean_tir, color='red', linestyle='--', lw=1)

    plt.text(plt.xlim()[1], mean_tir, ' Média TIR', va='center', ha='right', backgroundcolor='white', color='red')
    plt.text(mean_risk, plt.ylim()[1], ' Média Risco', va='top', ha='left', backgroundcolor='white', color='red')
    
    # Anotações nos pontos para os maiores entes (ainda útil para identificar os pontos chave)
    for i, row in df_scatter.nlargest(5, 'ValorLiquido').iterrows():
        plt.text(row['%Vencido'] + 0.1, row['TIR Líquida (PDD & Custos) a.m. (%)'], row['Convênio'], fontsize=9, ha='left')

    plt.title('Gráfico 4: Análise de Risco (% Vencido) vs. Retorno (TIR)', fontsize=16, weight='bold')
    plt.xlabel('Risco (% da Carteira Vencida)', fontsize=12)
    plt.ylabel('Retorno (TIR Líquida Completa % a.m.)', fontsize=12)
    plt.grid(True)
    plt.show()
else:
    print("Não foi possível gerar o gráfico de Risco vs. Retorno pois não há dados suficientes (ou todos foram filtrados).")

print("="*80)
print("FIM DA GERAÇÃO DE GRÁFICOS")
print("="*80)