#%%
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






#%%
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


#%%
# =============================================================================
#  Colunas Auxiliares ========================================================
# =============================================================================
# ```Cria novas colunas para facilitar analse```
#  uso 'SacadoID', já que nao tem cpf

df_final2['_ValorLiquido'] = df_final2['ValorPresente'] - df_final2['PDDTotal']
df_final2['_ValorVencido'] = (df_final2['DataVencimento'] <= df_final2['DataGeracao']).astype('int') * df_final2['ValorPresente']
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


#%%
#%%
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
valor_presente_formatado = f"R$ {df_final2['ValorPresente'].sum():,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
total_registros_formatado = f"{len(df_final2):,}".replace(',', '.')
clientes_unicos_formatado = f"{df_final2['SacadoID'].nunique():,}".replace(',', '.')
ccbs_unicos_formatado = f"{df_final2['CCB'].nunique():,}".replace(',', '.')

checks_results['Número Total de Registros'] = total_registros_formatado
checks_results['Valor Presente Total da Carteira'] = valor_presente_formatado
checks_results['Período da Carteira (Data de Aquisição)'] = f"{df_final2['DataAquisicao'].min().strftime('%d/%m/%Y')} a {df_final2['DataAquisicao'].max().strftime('%d/%m/%Y')}"
checks_results['Número de Clientes Únicos'] = clientes_unicos_formatado
checks_results['Número de CCBs Únicos'] = ccbs_unicos_formatado
checks_results['Duplicidade de CCBs'] = f"{df_final2.duplicated(subset='CCB').sum()} registros"

#* Verif Valores
checks_results['Valores Monetários Negativos'] = (df_final2[['ValorAquisicao', 'ValorNominal', 'ValorPresente', 'PDDTotal']] < 0).any(axis=1).sum()
checks_results['VP > Valor Nominal'] = (df_final2['ValorPresente'] > df_final2['ValorNominal']).sum()
checks_results['Valor Aquisição > Valor Nominal'] = (df_final2['ValorAquisicao'] > df_final2['ValorNominal']).sum()

#* Verif Datas
checks_results['Data Aquisição > Data Vencimento'] = (df_final2['DataAquisicao'] > df_final2['DataVencimento']).sum()

#* ver se faltan dados
critical_cols_nulls = ['DataGeracao', 'DataAquisicao', 'DataVencimento', 'ValorPresente', 'ValorNominal', 'PDDTotal', 'SacadoID', 'Originador', 'Convênio']
null_counts = df_final2[critical_cols_nulls].isnull().sum().reset_index()
null_counts.columns = ['Coluna Crítica', 'Registros Faltantes']
null_counts = null_counts[null_counts['Registros Faltantes'] > 0].copy() #  apenas cols com dados faltantes
if not null_counts.empty:
    null_counts['% Faltante'] = (null_counts['Registros Faltantes'] / len(df_final2) * 100).map('{:,.2f}%'.format)
    # Convert a tabela de nulos para um HTML que eh inserido diretamente
    checks_results['[TABELA] Dados Faltantes em Colunas Críticas'] = null_counts.to_html(index=False, classes='dataframe dataframe-checks')
else:
    checks_results['Dados Faltantes em Colunas Críticas'] = "Nenhum dado faltante encontrado."
    
#* ver idades (novidade)
if '_IdadeCliente' in df_final2.columns:
    checks_results['Idade Mínima do Cliente'] = f"{df_final2['_IdadeCliente'].min()} anos"
    checks_results['Idade Máxima do Cliente'] = f"{df_final2['_IdadeCliente'].max()} anos"
    checks_results['Clientes com Idade < 18 ou > 95'] = ((df_final2['_IdadeCliente'] < 18) | (df_final2['_IdadeCliente'] > 95)).sum()

print("Verificações de sanidade concluídas. Os resultados foram armazenados.")
#%% verificacao
memoria_mb = df_final2.memory_usage(deep=True).sum() / 1024**2
print(f"Uso de memória do DataFrame: {memoria_mb:.2f} MB")

valor_total_estoque = df_final2["ValorPresente"].sum()
print(f"Valor Presente Total do Estoque: R$ {valor_total_estoque:_.2f}".replace('.', ',').replace('_', '.'))


#%% Contagem de Valores para Variáveis categóricas
# Itero sobre as colunas de texto para entender a distribuição.

print("Analisando a contagem de valores para colunas de texto (geral):")
colunas_interesse_texto = ['Situacao', 'Cedente', 'PagamentoParcial', 'Convênio', 'Originador', 'UF', 'CAPAG', 'Produto', 'Promotora']
for col in colunas_interesse_texto:
    if col in df_final2.columns:
        print(f"--- Análise da coluna: {col} ---")
        print(df_final2[col].value_counts(dropna=False).head(20)) #  top 20
        print('*' * 80)


#%%
# =============================================================================
# analise do PDD e Vencidos   ===============================================>>
# (por variável categórica)   ===============================================>>
# =============================================================================
# PDD e a inadimplência por diversas categorias.
cat_cols = [
    'Situacao', 'Cedente', 'PagamentoParcial',
    '_MuitosContratos', '_MuitosEntes', 'Convênio', 'Originador', 'Produto',
    'UF', 'CAPAG', 'Promotora'
]

cat_cols = [col for col in cat_cols if col in df_final2.columns] # tiro cols que nao estejam no df 


# PDD -----------------------------------------------------------------------------------
pdd_ref = (1 - df_final2['_ValorLiquido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"PDD de Referência (Total): {pdd_ref:.2f}%\n")

for col in cat_cols:
    print(f"--- Análise de PDD por '{col}' ---")
    aux_ = df_final2.groupby(col)[['_ValorLiquido', 'ValorPresente']].sum() / 1e6
    aux_['%PDD'] = (1 - aux_['_ValorLiquido'] / aux_['ValorPresente']) * 100
    display(aux_.sort_values('ValorPresente', ascending=False).head(20))
    print("\n" + "="*80 + "\n")

# Vencidos --------------------------------------------------------------------------
venc_ref = (df_final2['_ValorVencido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"Percentual de Vencidos de Referência (Total): {venc_ref:.2f}%\n")

for col in cat_cols:
    print(f"--- Análise de Vencidos por '{col}' ---")
    aux_ = df_final2.groupby(col)[['_ValorVencido', 'ValorPresente']].sum() / 1e6
    aux_['%Vencido'] = (aux_['_ValorVencido'] / aux_['ValorPresente']) * 100
    display(aux_.sort_values('%Vencido', ascending=False).head(20))
    print("\n" + "="*80 + "\n")


#%%
# =============================================================================
# Verificações de Consistência básica  ========================================
# =============================================================================
print("Analisando sacados presentes em múltiplos convênios (usando SacadoID)...")
sacados_multi_entes = df_final2.groupby('SacadoID')['Convênio'].agg(['nunique', pd.unique])
display(sacados_multi_entes.sort_values('nunique', ascending=False).head(20))

print("\nVerificando consistência das datas...")
#* note que tirei um check aqui 
check2 = (df_final2['DataAquisicao'] > df_final2['DataVencimento']).sum()
print(f"Registros com Data de Aquisição > Data de Vencimento: {check2}")

print("\nVerificando consistência dos valores...")
check_v1 = (df_final2['ValorAquisicao'] > df_final2['ValorNominal']).sum()
print(f"Registros com Valor de Aquisição > Valor Nominal: {check_v1}")
check_v2 = (df_final2['ValorAquisicao'] > df_final2['ValorPresente']).sum()
print(f"Registros com Valor de Aquisição > Valor Presente: {check_v2}")
check_v3 = (df_final2['ValorPresente'] > df_final2['ValorNominal']).sum()
print(f"Registros com Valor Presente > Valor Nominal: {check_v3}")

#%% 
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
    'Tem Muitos Entes':'_MuitosEntes'
}
dimensoes_analise = {k: v for k, v in dimensoes_analise.items() if v in df_final2.columns} # prova se colunas tao no df

# DIC DE EXEMPLO
COST_DICT = {
    'GOV. GOIAS': [0.035, 5.92],
    'PREF. COTIA': [0.03, 2.14],
}
DEFAULT_COST = [0.035, 5.92]


os.makedirs(output_path, exist_ok=True)


#***********************
#* CÁLCULO DAS MÉTRICAS
#***********************
print("\n" + "="*80)
print("INICIANDO CÁLCULO DAS MÉTRICAS DE RISCO E INADIMPLÊNCIA")
print("="*80)

# Nomes das colunas para o relatório final (mantido)
vp_col_name = 'Valor Presente \n(R$ MM)'
vl_col_name = 'Valor Líquido \n(R$ MM)'

tabelas_pdd = {}
tabelas_vencido = {}

# Risco: % PDD
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns: continue
    
    aux_pdd = df_final2.groupby(coluna, observed=False)[['_ValorLiquido', 'ValorPresente']].sum()
    """somas brutas a partir dos dados originais"""
    aux_pdd['%PDD'] = (1 - aux_pdd['_ValorLiquido'] / aux_pdd['ValorPresente']) * 100
    """percentual USANDO OS VALORES BRUTOS"""

    #renomeio as colunas para o relatório
    aux_pdd = aux_pdd.rename(columns={'ValorPresente': vp_col_name, '_ValorLiquido': vl_col_name})
    
    # escalar os valores para milhões
    aux_pdd[[vp_col_name, vl_col_name]] /= 1e6
    
    tabelas_pdd[nome_analise] = aux_pdd

# Inadimplencia --- #* % Vencido
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns: continue
    
    # somas brutas
    aux_venc = df_final2.groupby(coluna, observed=False)[['_ValorVencido', 'ValorPresente']].sum()

    # Calcular o percentual USANDO OS VALORES BRUTO
    # Isso garante que a proporção esteja crta
    aux_venc['%Vencido'] = (aux_venc['_ValorVencido'] / aux_venc['ValorPresente']) * 100
    
    # renomear as colunas
    aux_venc = aux_venc.rename(columns={'ValorPresente': vp_col_name, '_ValorVencido': 'ValorVencido (M)'})
    
    # escalar valores para milhões
    aux_venc[[vp_col_name, 'ValorVencido (M)']] /= 1e6
    
    tabelas_vencido[nome_analise] = aux_venc

print("Métricas de PDD e Inadimplência calculadas corretamente.")

#***********************
#* TICKET MÉDIO 
#***********************
tabelas_ticket = {}
for nome_analise, coluna in dimensoes_analise.items():
    df_temp = df_final2.dropna(subset=[coluna, 'ValorPresente', 'ValorNominal'])
    if df_temp.empty: continue
    grouped = df_temp.groupby(coluna, observed=False)
    numerador = grouped.apply(lambda g: (g['ValorNominal'] * g['ValorPresente']).sum(), include_groups=False)
    denominador = grouped['ValorPresente'].sum()
    ticket_ponderado = (numerador / denominador).replace([np.inf, -np.inf], 0)
    ticket_ponderado.name = "Ticket Ponderado (R$)"
    tabelas_ticket[nome_analise] = pd.DataFrame(ticket_ponderado)

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

ref_date = df_final2['DataGeracao'].max()
print(f"Data de Referência para o cálculo da TIR: {ref_date.strftime('%d/%m/%Y')}")
try:
    df_feriados = pd.read_excel(caminho_feriados)
    holidays = pd.to_datetime(df_feriados['Data']).values.astype('datetime64[D]')
    print(f"Sucesso: {len(holidays)} feriados carregados.")
except Exception as e:
    print(f"[AVISO] Não foi possível carregar feriados: {e}")
    holidays = []

df_avencer = df_final2[df_final2['DataVencimento'] > ref_date].copy()
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

#? ==============================================================================
#? ===== FIM DO LOOP DA TIR  ===================
#? ==============================================================================

df_tir_summary = pd.DataFrame(all_tirs)
tir_cols_to_fill = [col for col in df_tir_summary.columns if 'TIR' in col]
df_tir_summary[tir_cols_to_fill] = df_tir_summary[tir_cols_to_fill].fillna(-100.0)
print("Cálculo de TIR concluído.")

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
    /* 3. LOGO MAIOR: Altura da logo aumentada novamente */
    header .logo img {
        height: 75px; /* Aumentado de 65px para 75px */
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
    /* 2. FONTE MENOR: Tamanho do título principal reduzido */
    header .report-title h1 { font-size: 1.6em; /* Reduzido de 1.8em para 1.6em */ }
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
    details .content-wrapper { padding: 20px; background-color: #FFFFFF; }
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
    'Tem Muitos Entes': 'Compara clientes que operam em poucos vs. múltiplos convênios.'
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
    if coluna not in df_final2.columns or df_final2[coluna].isnull().all(): continue
    print(f"--> Processando e gerando HTML para o botão: '{nome_analise}'")
    
    # Junção tabelas de métricas
    df_pdd = tabelas_pdd.get(nome_analise)
    df_venc = tabelas_vencido.get(nome_analise)
    df_ticket = tabelas_ticket.get(nome_analise)
    df_tir = df_tir_summary[df_tir_summary['DimensaoColuna'] == coluna].set_index('Segmento')
    if df_pdd is None: continue

    df_final = df_pdd.join(df_venc.drop(columns=[vp_col_name]), how='outer')
    if df_ticket is not None: df_final = df_final.join(df_ticket, how='outer')
    df_final = df_final.join(df_tir.drop(columns=['DimensaoColuna']), how='outer')
    df_final.index.name = nome_analise
    df_final.reset_index(inplace=True)
    
    df_final = df_final.drop(columns=['ValorVencido (M)', 'Valor Presente TIR (M)'], errors='ignore')

    # Ordenação das cols
    colunas_ordem = [nome_analise, vl_col_name, vp_col_name]
    if 'Ticket Ponderado (R$)' in df_final.columns: colunas_ordem.append('Ticket Ponderado (R$)')
    colunas_ordem.extend(['%PDD', '%Vencido'])
    colunas_tir_existentes = sorted([col for col in df_tir.columns if col in df_final.columns and 'TIR' in col])
    colunas_finais = colunas_ordem + colunas_tir_existentes
    outras_colunas = [col for col in df_final.columns if col not in colunas_finais]
    df_final = df_final[colunas_finais + outras_colunas]

    # Ordenação linhas
    if nome_analise in dimensoes_ordem_alfabetica:
        df_final = df_final.sort_values(nome_analise, ascending=True).reset_index(drop=True)
    else:
        df_final = df_final.sort_values(vp_col_name, ascending=False).reset_index(drop=True)

    # Formatação
    formatters = {
        vl_col_name: lambda x: f'{x:,.2f}',
        vp_col_name: lambda x: f'{x:,.2f}',
        'Ticket Ponderado (R$)': lambda x: f'R$ {x:,.2f}',
        '%PDD': lambda x: f'{x:,.2f}%',
        '%Vencido': lambda x: f'{x:,.2f}%',
    }
    for col in colunas_tir_existentes: formatters[col] = lambda x: f'{x:,.2f}%'
    
    #quebra de linha com tag <br> 
    df_final.columns = [col.replace('\n', '<br>') for col in df_final.columns]
    
    # Cria o HTML pra tabela
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
html_output_filename = os.path.join(output_path, 'analise_originadores_consolidada.html') # obs: Nome do arquivo novo diferente
try:
    with open(html_output_filename, 'w', encoding='utf-8') as f:
        f.write(final_html_content)
    print("\n" + "="*80)
    print("ANÁLISE CONCLUÍDA COM SUCESSO!")
    print(f"O relatório HTML final foi salvo em: {html_output_filename}")
    print("="*80)
except Exception as e:
    print(f"\n[ERRO GRAVE] Não foi possível salvar o arquivo HTML: {e}")
# %%
