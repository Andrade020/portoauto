# -*- coding: utf-8 -*-
"""
## <span style="color:#AEE5F9;">Análise de Carteira de Originadores

Este notebook realiza uma análise completa da carteira de recebíveis, consolidando dados de diferentes fontes, realizando verificações de qualidade, calculando métricas de risco e, por fim, gerando um relatório HTML interativo. Feito por Lucas Andrade, com auxílio de Felipe Bastos

### <span style="color:#AEE5F9;"> Bibliotecas e Configurações Iniciais
"""

# =============================================================================
# Bibliotecas   ===============================================================
# =============================================================================
import pandas as pd
import numpy as np
import os
from scipy.optimize import brentq
import base64
import os

from IPython.display import display

pd.options.display.max_columns = 100
pd.options.display.max_rows = 200

"""##### <span style="color:#CFFFE5;">NOVIDADE versão 1.02</span>"""

DIAS_ATRASO_DEFINICAO_VENCIDO = 60

"""### <span style="color:#AEE5F9;">  Leitura e Preparação dos Dados
<span style="color: #FFB3B3; font-size: 15px; font-weight: bold;">
   ATENÇÃO: REDEFINIR AQUI OS PATHS</span>

Defino os caminhos dos arquivos de entrada, carrego os dados, uno as duas fontes (`StarCard.xlsx` e `Originadores.xlsx`) usando a coluna `CCB` como chave e realizamos uma limpeza inicial, tratando colunas monetárias e de data.
"""

# =============================================================================
# LER DADOS   =================================================================
# =============================================================================

#! PATHS ----------------------------------------------------------------------
#! ----------------------------------------------------------------------------
# Dados IN :
path_starcard = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\StarCard.xlsx'
path_originadores = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\Originadores.xlsx'
caminho_feriados = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\feriados_nacionais.xls'

logo_path = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\images\logo_inv.png'

#SAÍDA LOCAL:
output_path = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\output'
#! ----------------------------------------------------------------------------

print(f"Lendo arquivo principal: {path_starcard}")
df_starcard = pd.read_excel(path_starcard)

print(f"Lendo arquivo de detalhe: {path_originadores}")

# ?<obs> essas foram deduções próprias, q dei minha própria interpretação
cols_originadores = [
    'CCB', 'Prazo', 'Valor Parcela', 'Valor IOF', 'Valor Liquido Cliente',
    'Data Primeiro Vencimento', 'Data Último Vencimento', 'Data de Inclusão',
    'CET Mensal', 'Taxa CCB', 'Produto', 'Tabela', 'Promotora',
    'Valor Split Originador', 'Valor Split FIDC', 'Valor Split Compra de Divida',
    'Taxa Originador Split', 'Taxa Split FIDC'
]
df_originadores = pd.read_excel(path_originadores, usecols=cols_originadores)

# aproveitei que o CCB parece repetir em ambos os lados, pra dar um LEFT JOIN
#!---------------------------------------------
#TODO :: verificar se essa leitura está correta
# fiz essa verificação que deveria ser sufieciente ao meu entendimento
#!---------------------------------------------
print("Unindo as duas fontes de dados...")
print("Verificando a unicidade da chave 'CCB' em df_originadores...")
if not df_originadores['CCB'].is_unique:
    print("[WARNING] A coluna 'CCB' não é única em Originadores.xlsx. Isso causa duplicação de linhas!")
    #** mostrar os duplicados pra investigr
    duplicados = df_originadores[df_originadores.duplicated(subset='CCB', keep=False)]
    print("CCBs duplicados :")
    display(duplicados.sort_values('CCB'))
else:
    print("'CCB' é uma chave única. A junção tá segura.")



df_merged = pd.merge(df_starcard, df_originadores, on='CCB', how='left', suffixes=('', '_orig'))

print("Iniciando limpeza e preparação dos dados...")

# Aqui eu renomeei para funcionar no script anterior
df_merged = df_merged.rename(columns={
    'Data Referencia': 'DataGeracao',
    'Data Aquisicao': 'DataAquisicao',
    'Data Vencimento': 'DataVencimento',
    'Status': 'Situacao',
    'PDD Total': 'PDDTotal',
    'Valor Nominal': 'ValorNominal',
    'Valor Presente': 'ValorPresente',
    'Valor Aquisicao': 'ValorAquisicao',
    'ID Cliente': 'SacadoID', # obs: nao especifica o doc
    'Pagamento Parcial': 'PagamentoParcial'
})

# remove 'R$ ' -->>> vira float #*(note que os valores vem assim em StarCard)
cols_monetarias = ['ValorAquisicao', 'ValorNominal', 'ValorPresente', 'PDDTotal']
for col in cols_monetarias:
    if df_merged[col].dtype == 'object':
        df_merged[col] = df_merged[col].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip()
        df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')

# cols de data
cols_data = ['DataGeracao', 'DataAquisicao', 'DataVencimento', 'Data de Nascimento']
for col in cols_data:
    df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce')

# ? df_final2 Criado AQUI <<<<
df_final2 = df_merged.copy()
# Libera memória
del df_starcard, df_originadores, df_merged

print("Leitura, junção e limpeza concluídas.")
print(f"DataFrame final com {df_final2.shape[0]} linhas e {df_final2.shape[1]} colunas.")

"""### <span style="color:#AEE5F9;">  Colunas Auxiliares

Crio aqui colunas derivadas que capturam informações importantes, como:
- **_ValorLiquido**: Valor Presente descontado do PDD.
- **_ValorVencido**: Valor Presente de parcelas já vencidas.
- **_MuitosContratos**: Flag para clientes com 3 ou mais contratos.
- **_MuitosEntes**: Flag para clientes com contratos em 3 ou mais convênios.
- **_IdadeCliente**: Idade do cliente calculada na data de geração do relatório.

CHANGELOG: <span style="color:#CFFFE5;">ATUALIZADO na versão 1.02</span>
"""

# =============================================================================
#  Colunas Auxiliares =======================================================
# =============================================================================

df_final2['_ValorLiquido'] = df_final2['ValorPresente'] - df_final2['PDDTotal']
df_final2['_ValorVencido'] = (df_final2['DataVencimento'] <= df_final2['DataGeracao']).astype('int') * df_final2['ValorPresente']

#* [NOVIDADE] ..............................................................................
# ID PARCELAS individuais que estão vencidas conforme a nossa regra.
print(f"Passo 1: Identificando parcelas com {DIAS_ATRASO_DEFINICAO_VENCIDO} ou mais dias de atraso.")
dias_de_atraso = (df_final2['DataGeracao'] - df_final2['DataVencimento']).dt.days
df_final2['_ParcelaVencida_Flag'] = ((dias_de_atraso >= DIAS_ATRASO_DEFINICAO_VENCIDO)).astype(int)
# IDtodos os CCBs  que contêm PELO MENOS UMA parcela vencida.
print("Passo 2: Identificando todos os contratos que possuem ao menos uma parcela vencida.")
contratos_com_parcela_vencida = df_final2.groupby('CCB')['_ParcelaVencida_Flag'].max()
# Filtramos para obter uma lista apenas dos CCBs que de fato estão "contaminados".
lista_ccbs_vencidos = contratos_com_parcela_vencida[contratos_com_parcela_vencida == 1].index
# flag final a nível de CONTRATO.
print("Passo 3: Marcando todas as parcelas de um contrato vencido com a flag final.")
df_final2['_ContratoVencido_Flag'] = df_final2['CCB'].isin(lista_ccbs_vencidos).astype(int)
#*..........................................................................................


# uso 'SacadoID', já que nao tem cpf
sacado_contratos = df_final2.groupby('SacadoID')['CCB'].nunique() # vou usar SacadoID pra achar os sacados com muitos contratos
k = 3
mask_contratos = sacado_contratos >= k
sacado_contratos_alto = sacado_contratos[mask_contratos].index
df_final2['_MuitosContratos'] = df_final2['SacadoID'].isin(sacado_contratos_alto).astype(str)

sacados_entes = df_final2.groupby('SacadoID')['Convênio'].nunique() # muitos entes com sacadoid dnv
k2 = 3
mask_entes = sacados_entes >= k2
sacados_entes_alto = sacados_entes[mask_entes].index
df_final2['_MuitosEntes'] = df_final2['SacadoID'].isin(sacados_entes_alto).astype(str)

#* NOVIDADE: idade do cliente
if 'Data de Nascimento' in df_final2.columns and 'DataGeracao' in df_final2.columns:
    df_final2['_IdadeCliente'] = ((df_final2['DataGeracao'] - df_final2['Data de Nascimento']).dt.days / 365.25).astype(int)
    print("Coluna '_IdadeCliente' criada.")

print("Criação de colunas auxiliares concluída.")

"""### <span style="color:#AEE5F9;"> **NOVO:** Criação de Segmentos Customizados

Adiciono novas colunas ao DataFrame para permitir análises mais aprofundadas por novos segmentos de negócio.
"""

# =============================================================================
# CÉLULA DE CRIAÇÃO DE NOVOS SEGMENTOS PARA ANÁLISE
# =============================================================================
print("\n" + "="*80)
print("--- Iniciando criação de colunas de segmentação customizadas ---")

df_report = df_final2.copy()

# Para evitar erros, garantimos que as colunas de origem sejam do tipo string
df_report['Produto'] = df_report['Produto'].fillna('')
df_report['Convênio'] = df_report['Convênio'].fillna('')

# --- Segmento 1: Tipo de Produto ---
print("1. Criando a coluna '_TipoProduto'...")
condicoes_produto = [
    df_report['Produto'].str.contains('Empréstimo', case=False, na=False),
    df_report['Produto'].str.contains('Cartão RMC', case=False, na=False),
    df_report['Produto'].str.contains('Cartão Benefício', case=False, na=False)
]
opcoes_produto = ['Empréstimo', 'Cartão RMC', 'Cartão Benefício']
df_report['_TipoProduto'] = np.select(condicoes_produto, opcoes_produto, default='Outros')


# --- Segmento 2: Tipo de Empregado ---
print("2. Criando a coluna '_TipoEmpregado'...")
condicoes_empregado = [
    df_report['Produto'].str.contains('Efetivo|Efetivio', case=False, na=False, regex=True),
    df_report['Produto'].str.contains('Temporário', case=False, na=False),
    df_report['Produto'].str.contains('CONTRATADO', case=False, na=False),
    df_report['Produto'].str.contains('Comissionado', case=False, na=False)
]
opcoes_empregado = ['Efetivo', 'Temporário', 'Contratado', 'Comissionado']
df_report['_TipoEmpregado'] = np.select(condicoes_empregado, opcoes_empregado, default='Outros')


# --- Segmento 3: Esfera do Convênio ---
print("3. Criando a coluna '_EsferaConvenio'...")
# Usamos regex para buscar por múltiplas palavras (ex: 'GOV.' ou 'AGN -')
# O caractere '\' antes do '.' é para tratar o ponto como um caractere literal e não um coringa.
condicoes_convenio = [
    df_report['Convênio'].str.contains(r'GOV\.|AGN -', case=False, na=False, regex=True),
    df_report['Convênio'].str.contains(r'PREF\.|PRERF', case=False, na=False, regex=True)
]
opcoes_convenio = ['Estadual', 'Municipal']
df_report['_EsferaConvenio'] = np.select(condicoes_convenio, opcoes_convenio, default='Outros')



# --- Segmento 4: Faixas de Idade ---
print("4. Criando a coluna '_IdadesBins'...")
# Verificamos se a coluna de idade existe antes de prosseguir
if '_IdadeCliente' in df_report.columns:
    # Usamos o seu código para criar os bins
    bins = [
        df_report['_IdadeCliente'].min() - 1,
        37,
        45,
        53,
        df_report['_IdadeCliente'].max()
    ]
    labels = ['Até 37 anos', '38 a 45 anos', '46 a 53 anos', '54 anos ou mais']
    if len(bins) == len(labels) + 1:
        df_report['_IdadesBins'] = pd.cut(df_report['_IdadeCliente'], bins=bins, labels=labels, right=True)
        print("\n_IdadesBins:")
        print(df_report['_IdadesBins'].value_counts())
    else:
        print("[AVISO] Não foi possível criar os bins de idade devido a um problema com os limites.")


# --- Verificação da criação das colunas ---
print("\n--- Contagem de valores para as novas colunas ---")
print("\n_TipoProduto:")
print(df_report['_TipoProduto'].value_counts())
print("\n_TipoEmpregado:")
print(df_report['_TipoEmpregado'].value_counts())
print("\n_EsferaConvenio:")
print(df_report['_EsferaConvenio'].value_counts())
print("\n--- Novas colunas adicionadas com sucesso ao DataFrame 'df_report' ---")
print("="*80)


"""### <span style="color:#AEE5F9;"> Qualidade e Consistência dos Dados

Verifico inconsistências, dados faltantes e características gerais da carteira, armazenando os resultados para exibição no relatório final.
"""

# =============================================================================
# Qualidade e Consistência dos Dados ==========================================
# =============================================================================
# inconsistencias, dados faltantes e caracters gerais da carteira.
#* resultados armazenados

print("\n" + "="*80)
print("INICIANDO VERIFICAÇÕES DE SANIDADE E QUALIDADE DOS DADOS")
print("="*80)

#
checks_results = {}
"""dic para armznr os resltd das verfcc"""

#* char temprario 'x' para fazr a troca de seprdr.
valor_presente_formatado = f"R$ {df_report['ValorPresente'].sum():,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
total_registros_formatado = f"{len(df_report):,}".replace(',', '.')
clientes_unicos_formatado = f"{df_report['SacadoID'].nunique():,}".replace(',', '.')
ccbs_unicos_formatado = f"{df_report['CCB'].nunique():,}".replace(',', '.')

checks_results['Número Total de Registros'] = total_registros_formatado
checks_results['Valor Presente Total da Carteira'] = valor_presente_formatado
checks_results['Período da Carteira (Data de Aquisição)'] = f"{df_report['DataAquisicao'].min().strftime('%d/%m/%Y')} a {df_report['DataAquisicao'].max().strftime('%d/%m/%Y')}"
checks_results['Número de Clientes Únicos'] = clientes_unicos_formatado
checks_results['Número de CCBs Únicos'] = ccbs_unicos_formatado
checks_results['Duplicidade de CCBs'] = f"{df_report.duplicated(subset='CCB').sum()} registros"

#* Verif Valores
checks_results['Valores Monetários Negativos'] = (df_report[['ValorAquisicao', 'ValorNominal', 'ValorPresente', 'PDDTotal']] < 0).any(axis=1).sum()
checks_results['VP > Valor Nominal'] = (df_report['ValorPresente'] > df_report['ValorNominal']).sum()
checks_results['Valor Aquisição > Valor Nominal'] = (df_report['ValorAquisicao'] > df_report['ValorNominal']).sum()

#* Verif Datas
checks_results['Data Aquisição > Data Vencimento'] = (df_report['DataAquisicao'] > df_report['DataVencimento']).sum()

#* ver se faltan dados
critical_cols_nulls = ['DataGeracao', 'DataAquisicao', 'DataVencimento', 'ValorPresente', 'ValorNominal', 'PDDTotal', 'SacadoID', 'Originador', 'Convênio']
null_counts = df_report[critical_cols_nulls].isnull().sum().reset_index()
null_counts.columns = ['Coluna Crítica', 'Registros Faltantes']
null_counts = null_counts[null_counts['Registros Faltantes'] > 0].copy() #  apenas cols com dados faltantes
if not null_counts.empty:
    null_counts['% Faltante'] = (null_counts['Registros Faltantes'] / len(df_report) * 100).map('{:,.2f}%'.format)
    # Convert a tabela de nulos para um HTML que eh inserido diretamente
    checks_results['[TABELA] Dados Faltantes em Colunas Críticas'] = null_counts.to_html(index=False, classes='dataframe dataframe-checks')
else:
    checks_results['Dados Faltantes em Colunas Críticas'] = "Nenhum dado faltante encontrado."

#* ver idades (novidade)
if '_IdadeCliente' in df_report.columns:
    checks_results['Idade Mínima do Cliente'] = f"{df_report['_IdadeCliente'].min()} anos"
    checks_results['Idade Máxima do Cliente'] = f"{df_report['_IdadeCliente'].max()} anos"
    checks_results['Clientes com Idade < 18 ou > 95'] = ((df_report['_IdadeCliente'] < 18) | (df_report['_IdadeCliente'] > 95)).sum()

print("Verificações de sanidade concluídas. Os resultados foram armazenados.")


"""### <span style="color:#AEE5F9;"> Geração do Relatório Final

Consolido todas as análises anteriores em um único  HTML. O processo é dividido em subpartes.

#### <span style="color:#FFDAC1;"> Cálculo de Métricas

Aqui preparo as tabelas de métricas (PDD, Vencido, Ticket Médio) que serão exibidas no relatório. Agrupo os dados pelas dimensões de análise e calculamos o PDD, o % Vencido e o Ticket Médio Ponderado para cada segmento.
<br> CHANGELOG: <span style="color:#CFFFE5;">ATUALIZADO na versão 1.02</span>
"""

# =============================================================================
# Geração do Relatório  ======================================================
# =============================================================================

dimensoes_analise = {
    'Cedentes': 'Cedente',
    'Originadores': 'Originador',
    'Promotoras': 'Promotora',
    'Produtos': 'Produto',
    'Convênios': 'Convênio',
    'Situação': 'Situacao',
    'UF': 'UF',
    'CAPAG': 'CAPAG',
    'Pagamento Parcial': 'PagamentoParcial',
    'Tem Muitos Contratos':'_MuitosContratos',
    'Tem Muitos Entes':'_MuitosEntes',
    # NOVAS DIMENSÕES ADICIONADAS
    'Tipo de Produto': '_TipoProduto',
    'Tipo de Empregado': '_TipoEmpregado',
    'Esfera do Convênio': '_EsferaConvenio',
    'Faixa de Idade': '_IdadesBins'
}
dimensoes_analise = {k: v for k, v in dimensoes_analise.items() if v in df_report.columns} # prova se colunas tao no df

# DIC DE EXEMPLO
COST_DICT = {
    'GOV. GOIAS': [0.035, 5.92],
    'PREF. COTIA': [0.03, 2.14],
}
DEFAULT_COST = [0.035, 5.92]

os.makedirs(output_path, exist_ok=True)


#***********************
#* CÁLCULO MÉTRICAS ( NOVIDADE: contagem de CONTRATOS vencidos)
#***********************
print("\n" + "="*80)
print("INICIANDO CÁLCULO UNIFICADO DAS MÉTRICAS")
print("="*80)

# Adicionando colunas de vencimento
dias_de_atraso = (df_report['DataGeracao'] - df_report['DataVencimento']).dt.days
df_report['_ValorVencido_1d'] = ((dias_de_atraso >= 1).astype(int) * df_report['ValorPresente'])
df_report['_ValorVencido_30d'] = ((dias_de_atraso >= 30).astype(int) * df_report['ValorPresente'])
df_report['_ValorVencido_60d'] = ((dias_de_atraso >= 60).astype(int) * df_report['ValorPresente'])

total_vencido_1d_carteira = df_report['_ValorVencido_1d'].sum()
total_vencido_30d_carteira = df_report['_ValorVencido_30d'].sum()
total_vencido_60d_carteira = df_report['_ValorVencido_60d'].sum()

vp_col_name = 'Valor Presente \n(R$ MM)'
vl_col_name = 'Valor Líquido \n(R$ MM)'
tabelas_metricas = {}

for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_report.columns: continue
    print(f"Calculando métricas para a dimensão: '{nome_analise}'...")

    grouped = df_report.groupby(coluna, observed=False)

    # [NOVIDADE] a contagem agora eh baseada em CCBs unicos.
    #
    total_contratos_unicos = grouped['CCB'].nunique()

    # agg os ccbs unicos depois de filtrar os vencidos:
    contratos_vencidos_unicos = df_report[df_report['_ContratoVencido_Flag'] == 1].groupby(coluna, observed=False)['CCB'].nunique()

    df_metricas = pd.DataFrame({'Nº Contratos Únicos': total_contratos_unicos}) # < aqui comeco a base da tabela
    
    df_metricas = pd.DataFrame({'Nº Contratos Únicos': total_contratos_unicos})  # < aqui comeco a base da tabela
    somas_financeiras = grouped[['_ValorLiquido', 'ValorPresente', '_ValorVencido', '_ValorVencido_1d', '_ValorVencido_30d', '_ValorVencido_60d']].sum()
    df_metricas = df_metricas.join(somas_financeiras)

    # calc % :
    df_metricas['%PDD'] = (1 - df_metricas['_ValorLiquido'] / df_metricas['ValorPresente']) * 100
    df_metricas['% Contratos Vencidos'] = (contratos_vencidos_unicos / df_metricas['Nº Contratos Únicos']) * 100 # Agora a fórmula está correta
    df_metricas['vencido 1d / presente'] = (df_metricas['_ValorVencido_1d'] / df_metricas['ValorPresente']) * 100
    df_metricas['vencido 1d / presente'] = df_metricas.apply(
        lambda row: (row['_ValorVencido_1d'] / row['ValorPresente'] * 100) if row['ValorPresente'] > 0 else 0,
        axis=1
    )
    df_metricas['vencido 30d / presente'] = df_metricas.apply(
        lambda row: (row['_ValorVencido_30d'] / row['ValorPresente'] * 100) if row['ValorPresente'] > 0 else 0,
        axis=1
    )
    df_metricas['vencido 60d / presente'] = df_metricas.apply(
        lambda row: (row['_ValorVencido_60d'] / row['ValorPresente'] * 100) if row['ValorPresente'] > 0 else 0,
        axis=1
    )
    df_metricas['vencido 1d / vencidos carteira'] = df_metricas.apply(
        lambda row: (row['_ValorVencido_1d'] / total_vencido_1d_carteira * 100) if total_vencido_1d_carteira > 0 else 0,
        axis=1
    )
    df_metricas['vencido 30d / vencidos carteira'] = df_metricas.apply(
        lambda row: (row['_ValorVencido_30d'] / total_vencido_30d_carteira * 100) if total_vencido_30d_carteira > 0 else 0,
        axis=1
    )
    df_metricas['vencido 60d / vencidos carteira'] = df_metricas.apply(
        lambda row: (row['_ValorVencido_60d'] / total_vencido_60d_carteira * 100) if total_vencido_60d_carteira > 0 else 0,
        axis=1
    )    # organizo as colunas
    df_metricas = df_metricas.rename(columns={
        'ValorPresente': vp_col_name, 
        '_ValorLiquido': vl_col_name, 
        'Nº Contratos Únicos': 'Nº Contratos',
        'vencido 1d / presente': 'Venc. 1d / VP Total',
        'vencido 30d / presente': 'Venc. 30d / VP Total',
        'vencido 60d / presente': 'Venc. 60d / VP Total',
        'vencido 1d / vencidos carteira': 'Venc. 1d / Venc. Carteira',
        'vencido 30d / vencidos carteira': 'Venc. 30d / Venc. Carteira',
        'vencido 60d / vencidos carteira': 'Venc. 60d / Venc. Carteira'
        })
    df_metricas[[vp_col_name, vl_col_name]] /= 1e6
    df_metricas = df_metricas.drop(columns=['_ValorLiquido', '_ValorVencido', '_ValorVencido_1d', '_ValorVencido_30d', '_ValorVencido_60d'], errors='ignore')

    tabelas_metricas[nome_analise] = df_metricas

print("Cálculo unificado de métricas concluído.")


#***********************
#* TICKET MÉDIO
#***********************
tabelas_ticket = {}
for nome_analise, coluna in dimensoes_analise.items():
    df_temp = df_report.dropna(subset=[coluna, 'ValorPresente', 'ValorNominal'])
    if df_temp.empty: continue
    grouped = df_temp.groupby(coluna, observed=False)
    numerador = grouped.apply(lambda g: (g['ValorNominal'] * g['ValorPresente']).sum(), include_groups=False)
    denominador = grouped['ValorPresente'].sum()
    ticket_ponderado = (numerador / denominador).replace([np.inf, -np.inf], 0)
    ticket_ponderado.name = "Ticket Ponderado (R$)"
    tabelas_ticket[nome_analise] = pd.DataFrame(ticket_ponderado)

print("Ticket médio calculado.")

#***********************
#* PRAZO MÉDIO (NOVO)
#***********************
tabelas_prazo_medio = {}
for nome_analise, coluna in dimensoes_analise.items():
    df_temp = df_report.dropna(subset=[coluna, 'ValorPresente', 'Prazo'])
    if df_temp.empty: continue
    grouped = df_temp.groupby(coluna, observed=False)

    # Média Ponderada: somatório(Prazo * VP) / somatório(VP)
    def weighted_avg(g, col_name, weight_name):
        return (g[col_name] * g[weight_name]).sum() / g[weight_name].sum()

    prazo_ponderado = grouped.apply(weighted_avg, 'Prazo', 'ValorPresente', include_groups=False)
    prazo_ponderado.name = "Prazo Médio (meses)"
    tabelas_prazo_medio[nome_analise] = pd.DataFrame(prazo_ponderado)

print("Prazo médio ponderado calculado.")


"""#### <span style="color:#FFDAC1;"> Cálculo da TIR

Implemento a lógica para o cálculo da TIR (XIRR). Uso uma função para encontrar a taxa que zera o VPL do fluxo de caixa e a aplico para cada segmento da carteira. TIR em diferentes cenários: Bruta, Líquida de PDD, Líquida de Custos e Completa.
"""

#******************
#* TIR com brentq
#*******************
def calculate_xirr(cash_flows, days):
    cash_flows = np.array(cash_flows)
    days = np.array(days)
    def npv(rate):
        if rate <= -1: return float('inf')
        with np.errstate(divide='ignore', over='ignore'):
            return np.sum(cash_flows / (1 + rate) ** (days / 21.0))
    try:
        return brentq(npv, 0, 1.0)
    except ValueError:
        try:
            return brentq(npv, -0.9999, 0)
        except (RuntimeError, ValueError):
            return np.nan


print("\n" + "="*80)
print("INICIANDO CÁLCULO DA TAXA INTERNA DE RETORNO (TIR)")
print("="*80)

ref_date = df_report['DataGeracao'].max()
print(f"Data de Referência para o cálculo da TIR: {ref_date.strftime('%d/%m/%Y')}")
try:
    df_feriados = pd.read_excel(caminho_feriados)
    holidays = pd.to_datetime(df_feriados['Data']).values.astype('datetime64[D]')
    print(f"Sucesso: {len(holidays)} feriados carregados.")
except Exception as e:
    print(f"[AVISO] Não foi possível carregar feriados: {e}")
    holidays = []

df_avencer = df_report[df_report['DataVencimento'] > ref_date].copy()
try:
    start_dates = np.datetime64(ref_date.date())
    end_dates = df_avencer['DataVencimento'].values.astype('datetime64[D]')
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.busday_count(start_dates, end_dates, holidays=holidays)
    df_avencer = df_avencer[df_avencer['_DIAS_UTEIS_'] > 0]
except Exception as e:
    print(f"[ERRO] Falha ao calcular dias úteis: {e}")
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.nan

df_avencer['CustoVariavel'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[0])
df_avencer['CustoFixo'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[1])
# Receita líquida
df_avencer['ReceitaLiquida'] = df_avencer['ValorNominal'] - (df_avencer['CustoFixo'] + (df_avencer['CustoVariavel'] * df_avencer['ValorNominal']))


all_tirs = []
# Construir lista de segmentos a partir de todas as dimensões, incluindo as novas
cat_cols = list(dimensoes_analise.values())
segmentos_para_analise = [('Carteira Total', 'Todos')] + \
                         [(col, seg) for col in cat_cols if col in df_avencer.columns for seg in df_avencer[col].dropna().unique()]


# ==============================================================================
# LOOP DA TIR ================
# ==============================================================================
for tipo_dimensao, segmento in segmentos_para_analise:
    if tipo_dimensao == 'Carteira Total':
        df_segmento = df_avencer.copy() # Usar .copy() para evitar SettingWithCopyWarning
    else:
        df_segmento = df_avencer[df_avencer[tipo_dimensao] == segmento].copy()

    # fallback herdado (provavelmente lixo)
    if df_segmento.empty or df_segmento['_DIAS_UTEIS_'].isnull().all():
        continue

    vp_bruto = df_segmento['ValorPresente'].sum()
    tir_bruta, tir_pdd, tir_custos, tir_completa = np.nan, np.nan, np.nan, np.nan

    if vp_bruto > 0:
        # =Calculo TIR Bruta
        fluxos_brutos = df_segmento.groupby('_DIAS_UTEIS_')['ValorNominal'].sum()
        tir_bruta = calculate_xirr([-vp_bruto] + fluxos_brutos.values.tolist(), [0] + fluxos_brutos.index.tolist())

        # taxa de PDD robusta
        pdd_total_segmento = df_segmento['PDDTotal'].sum()
        if pd.notna(pdd_total_segmento) and vp_bruto > 0:
            pdd_rate = pdd_total_segmento / vp_bruto
        else:
            pdd_rate = 0.0 #  0 se não houver PDD ou VP

        df_segmento['Fluxo_PDD'] = df_segmento['ValorNominal'] * (1 - pdd_rate)
        df_segmento['Fluxo_Custos'] = df_segmento['ReceitaLiquida'] #  calculado antes do loop

        # subtrai os custos do fluxo já líquido de PDD.
        df_segmento['Fluxo_Completo'] = df_segmento['Fluxo_PDD'] - (df_segmento['CustoFixo'] + (df_segmento['CustoVariavel'] * df_segmento['ValorNominal']))


        # fluxos de caixa anuais
        fluxos_pdd = df_segmento.groupby('_DIAS_UTEIS_')['Fluxo_PDD'].sum()
        fluxos_custos = df_segmento.groupby('_DIAS_UTEIS_')['Fluxo_Custos'].sum()
        fluxos_completos = df_segmento.groupby('_DIAS_UTEIS_')['Fluxo_Completo'].sum()

        # TIRs líquidas com os fluxos de caixa
        tir_pdd = calculate_xirr([-vp_bruto] + fluxos_pdd.values.tolist(), [0] + fluxos_pdd.index.tolist())
        tir_custos = calculate_xirr([-vp_bruto] + fluxos_custos.values.tolist(), [0] + fluxos_custos.index.tolist())
        tir_completa = calculate_xirr([-vp_bruto] + fluxos_completos.values.tolist(), [0] + fluxos_completos.index.tolist())

    # A lógica de anexar os resultados permanece a mesma
    all_tirs.append({
        'DimensaoColuna': tipo_dimensao,
        'Segmento': segmento,
        'Valor Presente TIR (M)': vp_bruto / 1e6,
        'TIR Bruta \n(% a.m. )': tir_bruta * 100 if pd.notna(tir_bruta) else np.nan,
        'TIR Líquida de PDD \n(% a.m. )': tir_pdd * 100 if pd.notna(tir_pdd) else np.nan,
        'TIR Líquida de custos \n(% a.m. )': tir_custos * 100 if pd.notna(tir_custos) else np.nan,
        'TIR Líquida Final \n(% a.m. )': tir_completa * 100 if pd.notna(tir_completa) else np.nan,
    })

df_tir_summary = pd.DataFrame(all_tirs)
tir_cols_to_fill = [col for col in df_tir_summary.columns if 'TIR' in col]
df_tir_summary[tir_cols_to_fill] = df_tir_summary[tir_cols_to_fill].fillna(-100.0)
print("Cálculo de TIR concluído.")



"""#### <span style="color:#FFDAC1;">  Montagem do HTML

Uno todos os elementos: as verificações de sanidade, as tabelas de métricas e os resultados da TIR. Boto um estilo CSS para formatação, codo a logo em base64 e monto a estrutura HTML, que é salva em um arquivo local.
<br> CHANGELOG: <span style="color:#CFFFE5;">ATUALIZADO na versão 1.02</span>
"""

#***********************
#* GERAÇÃO DO RELATÓRIO HTML
#***********************

print("\n" + "="*80)
print("GERANDO RELATÓRIO HTML FINAL COM AJUSTES DE ESTILO")
print("="*80)

# LOGO E DATA
def encode_image_to_base64(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except FileNotFoundError:
        print(f"[ATENÇÃO] Arquivo de imagem não encontrado em: {image_path}. A logo não será exibida.")
        return None


logo_base64 = encode_image_to_base64(logo_path)
report_date = ref_date.strftime('%d/%m/%Y')

# CSS - similar ao anterior, com algumas adições apenas
html_css = """
<style>
    /* Estilos para a mensagem de entrada única */
    .single-entry-message {
        background-color: #eef7f7;
        border-left: 5px solid #76c6c5;
        padding: 15px 20px;
        margin: 10px 0;
        font-size: 1.0em;
        color: #313131;
    }
    .single-entry-message strong {
        color: #163f3f;
    }


    .checks-container {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
        gap: 15px;
        margin-bottom: 20px;
    }
    .check-item {
        background-color: #f5f5f5;
        padding: 12px;
        border-radius: 5px;
        border-left: 4px solid #76c6c5;
        font-size: 0.95em;
    }
    .check-item strong {
        color: #163f3f;
    }
    .dataframe-checks th {
        background-color: #5b8c8c; /* Um tom mais claro para diferenciar */
    }
    .check-table-wrapper h4 {
        margin-top: 20px;
        margin-bottom: 10px;
        color: #163f3f;
        border-bottom: 2px solid #eeeeee;
        padding-bottom: 5px;
    }

    /* Configurações Gerais */
    body {
        font-family: "Gill Sans MT", Arial, sans-serif;
        background-color: #f9f9f9;
        color: #313131;
        margin: 0;
        padding: 0;
    }
    .main-content {
        padding: 25px;
    }

    /* --- CABEÇALHO --- */
    header {
        background-color: #163f3f;
        color: #FFFFFF;
        padding: 20px 40px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        border-bottom: 5px solid #76c6c5;
    }
    header .logo img {
        height: 75px;
    }
    header .report-title {
        text-align: left;
        font-family: "Gill Sans MT", Arial, sans-serif;
    }
    header .report-title h1, header .report-title h2, header .report-title h3 {
        margin: 0;
        padding: 0;
        font-weight: normal;
    }
    header .report-title h1 { font-size: 1.6em; }
    header .report-title h2 { font-size: 1.4em; color: #d0d0d0; }
    header .report-title h3 { font-size: 1.1em; color: #a0a0a0; }

    /* Estilos dos Botões e Tabelas (sem alterações) */
    .container-botoes { display: flex; flex-wrap: wrap; gap: 15px; margin-bottom: 25px; }
    .container-botoes > details { flex: 1 1 280px; border: 1px solid #76c6c5; border-radius: 8px; overflow: hidden; }
    .container-botoes > details[open] { flex-basis: 100%; }
    details summary { font-size: 1.1em; font-weight: bold; color: #FFFFFF; background-color: #163f3f; padding: 15px 20px; cursor: pointer; outline: none; list-style-type: none; }
    details summary:hover { background-color: #0e5d5f; }
    details[open] summary { background-color: #76c6c5; color: #313131; }
    details[open] summary:hover { filter: brightness(95%); }
    summary::-webkit-details-marker { display: none; }
    summary::before { content: '► '; margin-right: 8px; font-size: 0.8em;}
    details[open] summary::before { content: '▼ '; }
    
    details .content-wrapper { 
        padding: 20px; 
        background-color: #FFFFFF;
        overflow-x: auto; /* <-- NOVO: Adiciona a barra de rolagem horizontal */
    }

    table.dataframe, th, td { border: 1px solid #bbbbbb; }
    table.dataframe { border-collapse: collapse; width: 100%; }
    th, td { text-align: left; padding: 10px; vertical-align: middle; }
    th { background-color: #163f3f; color: #FFFFFF; }
    tr:nth-child(even) { background-color: #eeeeee; }

    /* --- RODAPÉ --- */
    footer {
        background-color: #f0f0f0;
        color: #555555;
        font-size: 0.8em;
        line-height: 1.6;
        padding: 25px 40px;
        margin-top: 40px;
        border-top: 1px solid #dddddd;
    }
    footer .disclaimer {
        margin-top: 20px;
        font-style: italic;
        border-top: 1px solid #dddddd;
        padding-top: 20px;
    }
</style>
"""

# HTML COMPLETO:
html_parts = []


html_parts.append("<!DOCTYPE html><html lang='pt-BR'><head>")
html_parts.append("<meta charset='UTF-8'><title>Análise de Desempenho - FCT Consignado II</title>")
html_parts.append(html_css)
html_parts.append("</head><body>")

# --- add o Cabeçalho ---
html_parts.append("<header>")
html_parts.append(f"""
<div class="report-title">
    <h1>Análise de desempenho</h1>
    <h2>FCT CONSIGNADO II</h2>
    <h3>{report_date}</h3>
</div>
""")

if logo_base64:
    html_parts.append(f'<div class="logo"><img src="data:image/png;base64,{logo_base64}" alt="Logo"></div>')
html_parts.append("</header>")


html_parts.append("<div class='main-content'>")

# [ADAPTADO] Mapa de descrições atualizado com as novas dimensões
mapa_descricoes = {
    'Cedentes': 'Analisa as métricas de risco e retorno agrupadas por cada Cedente.',
    'Originadores': 'Compara o desempenho da carteira originada por cada parceiro.',
    'Promotoras': 'Métricas agrupadas pela promotora de vendas responsável pela operação.',
    'Produtos': 'Agrupa os dados pelo tipo de produto de crédito (Ex: Cartão Benefício).',
    'Convênios': 'Métricas detalhadas por cada Convênio onde a consignação é feita.',
    'Situação': 'Compara o desempenho dos títulos com base na sua situação atual (Ex: A vencer).',
    'UF': 'Agrega todas as métricas por Unidade Federativa (Estado) do cliente.',
    'CAPAG': 'Métricas baseadas na Capacidade de Pagamento (CAPAG) do município ou estado do convênio.',
    'Pagamento Parcial': 'Verifica se há impacto nas métricas para títulos que aceitam pagamento parcial.',
    'Tem Muitos Contratos': 'Compara clientes com um número baixo vs. alto de contratos ativos.',
    'Tem Muitos Entes': 'Compara clientes que operam em poucos vs. múltiplos convênios.',
    # DESCRIÇÕES PARA AS NOVAS DIMENSÕES
    'Tipo de Produto': 'Análise segmentada pelo tipo de produto principal, como Empréstimo ou Cartão.',
    'Tipo de Empregado': 'Desempenho da carteira agrupado pelo tipo de vínculo do empregado (Efetivo, Temporário, etc.).',
    'Esfera do Convênio': 'Compara as métricas entre convênios de esfera Municipal e Estadual.',
    'Faixa de Idade': 'Análise de risco e retorno distribuída por diferentes faixas de idade dos clientes.'
}

# ==============================================================================
#  Parte nova- verificacao dos dados ==========================
# ==============================================================================
html_parts.append("<details open>") # 'open' ===  comece já expandido
html_parts.append("<summary>Verificações e Sanidade dos Dados</summary>")
html_parts.append("<div class='content-wrapper'>")

# resultados simples
simple_checks_html = "<div class='checks-container'>"
# Estrutura tabelas
table_checks_html = "<div class='check-table-wrapper'>"

for key, value in checks_results.items():
    # ratata tabela html difente por chave
    if '[TABELA]' in key:
        clean_key = key.replace('[TABELA]', '').strip()
        table_checks_html += f"<h4>{clean_key}</h4>"
        table_checks_html += str(value)
    else:
        simple_checks_html += f"<div class='check-item'><strong>{key}:</strong> {value}</div>"

simple_checks_html += "</div>"
table_checks_html += "</div>"

html_parts.append(simple_checks_html)
html_parts.append(table_checks_html)

html_parts.append("</div></details>")

html_parts.append("<div class='container-botoes'>")
dimensoes_ordem_alfabetica = ['CAPAG'] # add outras se necessário

for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_report.columns or df_report[coluna].isnull().all(): continue
    print(f"--> Processando e gerando HTML para: '{nome_analise}'")

    # NOVA LÓGICA: Verificar se o segmento tem apenas uma entrada
    unique_entries = df_report[coluna].dropna().unique()
    if len(unique_entries) <= 1:
        entry_name = unique_entries[0] if len(unique_entries) == 1 else "N/A"
        message_html = f"""
        <details>
            <summary title="Apenas uma entrada neste segmento">{nome_analise}</summary>
            <div class='content-wrapper'>
                <p class='single-entry-message'>
                    Em <strong>'{nome_analise}'</strong>, toda a carteira se concentra em uma única entrada: <strong>{entry_name}</strong>.<br>
                    Portanto, a análise detalhada para este item é idêntica à da carteira consolidada, já apresentada nos resumos gerais.
                </p>
            </div>
        </details>
        """
        html_parts.append(message_html)
        continue # Pula para a próxima iteração do loop

    # table de métricas já pronta.
    df_final = tabelas_metricas.get(nome_analise)

    # Junção de Ticket, Prazo Médio e TIR.
    df_ticket = tabelas_ticket.get(nome_analise, pd.DataFrame())
    df_prazo = tabelas_prazo_medio.get(nome_analise, pd.DataFrame()) # NOVO
    df_tir = df_tir_summary[df_tir_summary['DimensaoColuna'] == coluna].set_index('Segmento')

    df_final = df_final.join(df_ticket, how='outer')
    df_final = df_final.join(df_prazo, how='outer') # NOVO
    df_final = df_final.join(df_tir.drop(columns=['DimensaoColuna', 'Valor Presente TIR (M)']), how='outer')

    df_final.index.name = nome_analise
    df_final.reset_index(inplace=True)

    # Nomes das novas colunas (melhorados)
    novas_colunas_vencimento = [
        'Venc. 1d / VP Total',
        'Venc. 30d / VP Total',
        'Venc. 60d / VP Total',
        'Venc. 1d / Venc. Carteira',
        'Venc. 30d / Venc. Carteira',
        'Venc. 60d / Venc. Carteira'
    ]

    colunas_ordem = [
        nome_analise,
        'Nº Contratos',
        '% Contratos Vencidos',
        vl_col_name,
        vp_col_name,
        '%PDD',
    ] + novas_colunas_vencimento
    
    if 'Ticket Ponderado (R$)' in df_final.columns:
        colunas_ordem.append('Ticket Ponderado (R$)')
    if 'Prazo Médio (meses)' in df_final.columns: # NOVO
        colunas_ordem.append('Prazo Médio (meses)') # NOVO

    ordem_ideal_tir = [
        'TIR Bruta \n(% a.m. )',
        'TIR Líquida de PDD \n(% a.m. )',
        'TIR Líquida de custos \n(% a.m. )',
        'TIR Líquida Final \n(% a.m. )'
    ]

    colunas_tir_ordenadas = [col for col in ordem_ideal_tir if col in df_final.columns]

    colunas_finais = colunas_ordem + colunas_tir_ordenadas
    outras_colunas = [col for col in df_final.columns if col not in colunas_finais]
    df_final = df_final[colunas_finais + outras_colunas]


    if nome_analise in ['CAPAG']:  # Orden
        df_final = df_final.sort_values(nome_analise, ascending=True).reset_index(drop=True)
    else:
        df_final = df_final.sort_values(vp_col_name, ascending=False).reset_index(drop=True)

    # Format
    formatters = {
        vl_col_name: lambda x: f'{x:,.2f}',
        vp_col_name: lambda x: f'{x:,.2f}',
        'Nº Contratos': lambda x: f'{x:,.0f}'.replace(',', '.'),
        'Ticket Ponderado (R$)': lambda x: f'R$ {x:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'),
        'Prazo Médio (meses)': lambda x: f'{x:,.2f}', # NOVO
        '%PDD': lambda x: f'{x:,.2f}%',
        '% Contratos Vencidos': lambda x: f'{x:,.2f}%',
    }

    # Adiciona formatação para as novas colunas
    for col in novas_colunas_vencimento:
        formatters[col] = lambda x: f'{x:,.2f}%'


    for col in colunas_tir_ordenadas:
        formatters[col] = lambda x: f'{x:,.2f}%'

    df_final.columns = [col.replace('\n', '<br>') for col in df_final.columns]

    # GERO HTML para a tabela
    html_parts.append("<details>")
    descricao = mapa_descricoes.get(nome_analise, 'Descrição não disponível.')
    html_parts.append(f'<summary title="{descricao}">{nome_analise}</summary>')
    html_parts.append("<div class='content-wrapper'>")
    html_table = df_final.to_html(index=False, classes='dataframe', formatters=formatters, na_rep='-', escape=False)
    html_parts.append(html_table)
    html_parts.append("</div></details>")

html_parts.append("</div>") # Fim do container-botoes
html_parts.append("</div>") # Fim do main-content

#! obs: herdado do outro código
# (Não sei se é o caso, se aplica aqui)
footer_main_text = """
Este documento tem como objetivo apresentar uma análise de desempenho do fundo FCT Consignado II (CNPJ 52.203.615/0001-19), realizada pelo Porto Real Investimentos na qualidade de cogestora. Os prestadores de serviço do fundo são: FICTOR ASSET (Gestor), Porto Real Investimentos (Cogestor), e VÓRTX DTVM (Administrador e Custodiante).
"""
footer_disclaimer = """
Disclaimer: Este relatório foi preparado pelo Porto Real Investimentos exclusivamente para fins informativos e não constitui uma oferta de venda, solicitação de compra ou recomendação para qualquer investimento. As informações aqui contidas são baseadas em fontes consideradas confiáveis na data de sua publicação, mas não há garantia de sua precisão ou completude. Rentabilidade passada não representa garantia de rentabilidade futura.
"""
html_parts.append("<footer>")
html_parts.append(f'<p>{footer_main_text.strip()}</p>')
html_parts.append(f'<div class="disclaimer">{footer_disclaimer.strip()}</div>')
html_parts.append("</footer>")


html_parts.append("</body></html>")

final_html_content = "\n".join(html_parts)
html_output_filename = os.path.join(output_path, 'analise_originadores.html') # obs: Nome do arquivo novo diferente
try:
    with open(html_output_filename, 'w', encoding='utf-8') as f:
        f.write(final_html_content)
    print("\n" + "="*80)
    print("ANÁLISE CONCLUÍDA COM SUCESSO!")
    print(f"O relatório HTML final foi salvo em: {html_output_filename}")
    print("="*80)
except Exception as e:
    print(f"\n[ERRO GRAVE] Não foi possível salvar o arquivo HTML: {e}")

# Limpar memória
del df_report, df_final2