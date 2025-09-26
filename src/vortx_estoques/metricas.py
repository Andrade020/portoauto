# -*- coding: utf-8 -*-
"""
## Análise da Carteira de recebíveis

o que eu fiz: 

após diagnóstico dos dados, concluí que 
as seguintes colunas são 100% vazias ou sem variação para a análise:
(NumeroBoleto, CampoAdicional1-5, Registradora) 
e com um único valor : (Coobricacao), 

Achei que CedenteNome poderia substituir Originador. 
Os seguintes mapeamentos foram feitos: 

COLUNA Original  >>>>        Coluna Nova               >>>> Comentário

ID do Contrato   >>>  IdTituloVotxOriginador  >>> Renomeada para CCB para compatibilidade.
ID do Cliente >>>   SacadoCnpjCpf   >>>> Renomeada para SacadoID.
Originador   >>>  CedenteNome   >>> Usada diretamente como a nova dimensão principal.
Valor Líquido >>>>    ValorPresente - PDDTotal     >>>>    Lógica mantida, usando as novas colunas.
Contrato Vencido   >>>   DataGeracao, DataVencimento   >>>>  Lógica mantida, usando as novas colunas.

as colunas Prazo, Data de Nascimento, Produto e Convênio não existiam nos novos dados,
as seguintes análises foram removidas:

<REMOVIDO> Cálculo de Prazo Médio Ponderado.

<REMOVIDO> Segmentação por Tipo de Produto, Empregado, Esfera do Convênio e Faixa Etária.

<REMOVIDO> UF e idade do cliente

Não temos CONVENIO, entao simplifiquei para apenas calcular a TIR liquida de PDD 

Converti o as datas em np.datetime64[ns] 


-------------------------------------------------------------------
olhando a saída: 
a maior parte da concentracao da carteira está no BMP MONEY PLUS, 
enquanto que o banco DIGIMAIS tem uma participacao pequena, porém tem um
pdd maior e indice de vencimento a curto prazo maior

os CT- contratos parecem mais performáticos que os ccb, porque eles tem um pdd menors
e taxas de vencimento mais baixas. 
OBS: o que é estranho é que "CCB" aqui é tratado como um tipo de ativo, 
e não como um código de contrato, achei isso estranho
OBS: nao confundir com a coluna CCB, que renomeamos de "IdTituloVortxOriginador"

> os aditados tiveram um pdd baixo, o que faz sentido, pq se o contrato foi aditado 
é pq o cliente está pagando, e o banco não precisou fazer provisão (eu suponho)

> por algum motivo, o pagamento parcial deu um pdd e uma taxa de vencimento maior, 
eu achei curioso, talvez seja pq o cliente que faz pagamento parcial é mais propenso a atrasos? 

> os caoss de tir inválida pode ser porque é quando toda a carteira nesse segmento venceu;
----------------------------------------------------------------------------------------
questionamento: 
será que o estoque está vindo completo? ou está dando preferencia para vencidos? 

"""


#* ==============>>>>>>>>
#* Bibliotecas   >>>>>>>>
#* ==============>>>>>>>>
import pandas as pd
import numpy as np
import os
from scipy.optimize import brentq
import base64
from IPython.display import display

pd.options.display.max_columns = 100
pd.options.display.max_rows = 200

"""##### <span style="color:#CFFFE5;">Parâmetro de Negócio</span>"""

DIAS_ATRASO_DEFINICAO_VENCIDO = 1

"""### <span style="color:#AEE5F9;"> Leitura e Preparação dos Dados
<span style="color: #FFB3B3; font-size: 15px; font-weight: bold;">
    ATENÇÃO: REDIFINIR AQUI OS PATHS E NOMES DE ARQUIVOS, SE NECESSÁRIO.
</span>
"""

# =============================================================================
# LER DADOS   =================================================================
# =============================================================================

#! PATHS ----------------------------------------------------------------------
DIRETORIO_DADOS = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\data\130184-Estoque'
ARQUIVOS_CSV = [
    'FIDC FCT CONSIGNADO II  - Estoque 29.08.25-Parte1.csv',
    'FIDC FCT CONSIGNADO II  - Estoque 29.08.25-Parte2.csv',
    'FIDC FCT CONSIGNADO II  - Estoque 29.08.25-Parte3.csv'
]
caminho_feriados = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\feriados_nacionais.xls'
logo_path = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\images\logo_inv.png'
output_path = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\output'
#! ----------------------------------------------------------------------------

# --- Carga e unificação dos arquivos ---
lista_dfs = []
caminhos_completos = [os.path.join(DIRETORIO_DADOS, arq) for arq in ARQUIVOS_CSV]
for caminho in caminhos_completos:
    df_temp = pd.read_csv(caminho, sep=';', decimal=',', dayfirst=True, encoding='latin1')
    lista_dfs.append(df_temp)
df_merged = pd.concat(lista_dfs, ignore_index=True)
print(f"Dados unificados com sucesso. Total de {df_merged.shape[0]} registros carregados.")

# --- Limpeza e Preparação dos Dados ---
print("Iniciando limpeza e preparação dos dados...")
colunas_para_remover = [
    'NumeroBoleto', 'CampoAdicional1', 'CampoAdicional2', 'CampoAdicional3', 'CampoAdicional4',
    'CampoAdicional5', 'Registradora', 'IdContratoRegistradora', 'IdTituloRegistradora',
    'Coobricacao', 'PES_TIPO_PESSOA', 'TIT_CEDENTE_ENT_CODIGO', 'Cnae',
    'SecaoCNAEDescricao', 'NotaPdd', 'SAC_TIPO_PESSOA'
]
df_merged.drop(columns=colunas_para_remover, inplace=True, errors='ignore')

# Renomear colunas para compatibilidade
df_merged = df_merged.rename(columns={'IdTituloVortxOriginador': 'CCB', 'SacadoCnpjCpf': 'SacadoID'})

# Conversão de tipos de dados
cols_data = ['DataGeracao', 'DataAquisicao', 'DataVencimento', 'DataEmissao']
for col in cols_data:
    df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce', dayfirst=True)

df_final2 = df_merged.copy()
del df_merged, lista_dfs
print(f"DataFrame pronto para análise com {df_final2.shape[0]} linhas.")


"""### <span style="color:#AEE5F9;"> Colunas Auxiliares e Novas Segmentações

Criação de colunas derivadas para análise de risco e novas segmentações de negócio.
"""

# =============================================================================
#  Colunas Auxiliares =======================================================
# =============================================================================

# * obs: uso a data do próprio relatório como ref
data_referencia_relatorio = df_final2['DataGeracao'].max()
print(f"Usando a data do relatório como referência para inadimplência: {data_referencia_relatorio.strftime('%d/%m/%Y')}")

df_final2['_ValorLiquido'] = df_final2['ValorPresente'] - df_final2['PDDTotal']

# Identificação de contratos vencidos com base na data do relatório
dias_de_atraso = (data_referencia_relatorio - df_final2['DataVencimento']).dt.days
df_final2['_ParcelaVencida_Flag'] = ((dias_de_atraso >= DIAS_ATRASO_DEFINICAO_VENCIDO) & (dias_de_atraso > 0)).astype(int)
contratos_com_parcela_vencida = df_final2.groupby('CCB')['_ParcelaVencida_Flag'].max()
lista_ccbs_vencidos = contratos_com_parcela_vencida[contratos_com_parcela_vencida == 1].index
df_final2['_ContratoVencido_Flag'] = df_final2['CCB'].isin(lista_ccbs_vencidos).astype(int)

# Nova segmentação por Ágio/Deságio
df_final2['_FlagDesagio'] = np.where(df_final2['ValorAquisicao'] > df_final2['ValorNominal'], 'Ágio', 'Deságio')

print("Criação de colunas auxiliares concluída.")
df_report = df_final2.copy()
del df_final2

"""### <span style="color:#AEE5F9;"> Qualidade e Consistência dos Dados

Verifico inconsistências, dados faltantes e características gerais da carteira.
"""
# =============================================================================
# Qualidade e Consistência dos Dados ==========================================
# =============================================================================
print("\n" + "="*80)
print("INICIANDO VERIFICAÇÕES DE SANIDADE E QUALIDADE DOS DADOS")
print("="*80)
checks_results = {}
valor_presente_formatado = f"R$ {df_report['ValorPresente'].sum():,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
total_registros_formatado = f"{len(df_report):,}".replace(',', '.')
clientes_unicos_formatado = f"{df_report['SacadoID'].nunique():,}".replace(',', '.')
ccbs_unicos_formatado = f"{df_report['CCB'].nunique():,}".replace(',', '.')
checks_results['Número Total de Registros'] = total_registros_formatado
checks_results['Valor Presente Total da Carteira'] = valor_presente_formatado
checks_results['Período da Carteira (Data de Aquisição)'] = f"{df_report['DataAquisicao'].min().strftime('%d/%m/%Y')} a {df_report['DataAquisicao'].max().strftime('%d/%m/%Y')}"
checks_results['Número de Clientes Únicos'] = clientes_unicos_formatado
checks_results['Número de Contratos Únicos'] = ccbs_unicos_formatado
checks_results['Valores Monetários Negativos'] = (df_report[['ValorAquisicao', 'ValorNominal', 'ValorPresente', 'PDDTotal']] < 0).any().any()
checks_results['VP > Valor Nominal'] = (df_report['ValorPresente'] > df_report['ValorNominal']).sum()
checks_results['Valor Aquisição > Valor Nominal'] = (df_report['ValorAquisicao'] > df_report['ValorNominal']).sum()
checks_results['Data Aquisição > Data Vencimento'] = (df_report['DataAquisicao'] > df_report['DataVencimento']).sum()
print("Verificações de sanidade concluídas.")


"""### <span style="color:#AEE5F9;"> Geração do Relatório Final

Consolidação de todas as análises em um relatório HTML interativo.
"""
# =============================================================================
# Geração do Relatório  ======================================================
# =============================================================================
# ATUALIZAÇÃO: Dimensões de análise ajustadas
dimensoes_analise = {
    'Cedentes': 'CedenteNome',
    'Tipo de Ativo': 'TipoAtivo',
    'Situação': 'Situacao',
    'Pagamento Parcial': 'PagamentoParcial',
    'Flag Ágio/Deságio': '_FlagDesagio'
}
dimensoes_analise = {k: v for k, v in dimensoes_analise.items() if v in df_report.columns}
os.makedirs(output_path, exist_ok=True)

#***********************
#* CÁLCULO MÉTRICAS
#***********************
print("\n" + "="*80)
print("INICIANDO CÁLCULO UNIFICADO DAS MÉTRICAS")
print("="*80)

dias_de_atraso = (data_referencia_relatorio - df_report['DataVencimento']).dt.days
df_report['_ValorVencido_1d'] = df_report.loc[dias_de_atraso >= 1, 'ValorPresente'].fillna(0)
df_report['_ValorVencido_30d'] = df_report.loc[dias_de_atraso >= 30, 'ValorPresente'].fillna(0)
df_report['_ValorVencido_60d'] = df_report.loc[dias_de_atraso >= 60, 'ValorPresente'].fillna(0)
df_report.fillna({'_ValorVencido_1d': 0, '_ValorVencido_30d': 0, '_ValorVencido_60d': 0}, inplace=True)

vp_col_name = 'Valor Presente \n(R$ MM)'
vl_col_name = 'Valor Líquido \n(R$ MM)'
col_contratos_venc_perc = f"% Contratos Venc. (>{DIAS_ATRASO_DEFINICAO_VENCIDO}d)"
tabelas_metricas = {}

for nome_analise, coluna in dimensoes_analise.items():
    print(f"Calculando métricas para a dimensão: '{nome_analise}'...")
    grouped = df_report.groupby(coluna, observed=True)
    total_contratos_unicos = grouped['CCB'].nunique()
    contratos_vencidos_unicos = df_report[df_report['_ContratoVencido_Flag'] == 1].groupby(coluna, observed=True)['CCB'].nunique()
    df_metricas = pd.DataFrame({'Nº Contratos Únicos': total_contratos_unicos})
    somas_financeiras = grouped[['_ValorLiquido', 'ValorPresente', '_ValorVencido_1d', '_ValorVencido_30d', '_ValorVencido_60d']].sum()
    df_metricas = df_metricas.join(somas_financeiras)
    df_metricas['%PDD'] = (1 - df_metricas['_ValorLiquido'] / df_metricas['ValorPresente']) * 100
    df_metricas[col_contratos_venc_perc] = (contratos_vencidos_unicos.reindex(df_metricas.index).fillna(0) / df_metricas['Nº Contratos Únicos']).fillna(0) * 100
    df_metricas['Venc. 1d / VP'] = (df_metricas['_ValorVencido_1d'] / df_metricas['ValorPresente']) * 100
    df_metricas['Venc. 30d / VP'] = (df_metricas['_ValorVencido_30d'] / df_metricas['ValorPresente']) * 100
    df_metricas['Venc. 60d / VP'] = (df_metricas['_ValorVencido_60d'] / df_metricas['ValorPresente']) * 100
    df_metricas = df_metricas.rename(columns={'ValorPresente': vp_col_name, '_ValorLiquido': vl_col_name, 'Nº Contratos Únicos': 'Nº Contratos'})
    df_metricas[[vp_col_name, vl_col_name]] /= 1e6
    df_metricas = df_metricas.drop(columns=['_ValorLiquido', '_ValorVencido_1d', '_ValorVencido_30d', '_ValorVencido_60d'], errors='ignore')
    tabelas_metricas[nome_analise] = df_metricas
print("Cálculo unificado de métricas concluído.")

#***********************
#* TICKET MÉDIO
#***********************
tabelas_ticket = {}
for nome_analise, coluna in dimensoes_analise.items():
    df_temp = df_report.dropna(subset=[coluna, 'ValorPresente', 'ValorNominal'])
    if df_temp.empty: continue
    grouped = df_temp.groupby(coluna, observed=True)
    numerador = grouped.apply(lambda g: (g['ValorNominal'] * g['ValorPresente']).sum(), include_groups=False)
    denominador = grouped['ValorPresente'].sum()
    ticket_ponderado = (numerador / denominador).replace([np.inf, -np.inf], 0)
    ticket_ponderado.name = "Ticket Ponderado (R$)"
    tabelas_ticket[nome_analise] = pd.DataFrame(ticket_ponderado)
print("Ticket médio calculado.")

"""#### <span style="color:#FFDAC1;"> Cálculo da TIR

Cálculo da TIR (XIRR) para a carteira a vencer. Como não há dados de custo, calculamos a TIR Bruta e a TIR Líquida de PDD.
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
    except (RuntimeError, ValueError):
        return np.nan

print("\n" + "="*80)
print("INICIANDO CÁLCULO DA TAXA INTERNA DE RETORNO (TIR)")
print("="*80)

ref_date = df_report['DataGeracao'].max()
print(f"Data de Referência para o cálculo da TIR: {ref_date.strftime('%d/%m/%Y')}")

try:
    df_feriados = pd.read_excel(caminho_feriados)
    # Limpa os feriados de valores nulos, 
    #  converte para o tipo correto
    holidays = pd.to_datetime(df_feriados['Data'], errors='coerce').dropna().dt.date.values
    print(f"Sucesso: {len(holidays)} feriados carregados.")
except Exception as e:
    print(f"[AVISO] Não foi possível carregar feriados: {e}. O cálculo de dias úteis pode ser impreciso.")
    holidays = []

df_avencer = df_report[df_report['DataVencimento'] > ref_date].copy()

if not df_avencer.empty:
    try:
        # obs: Convertendo explicitamente para os tipos do NumPy
        start_date_np = np.datetime64(ref_date.date(), 'D')
        end_dates_np = df_avencer['DataVencimento'].dt.date.values.astype('datetime64[D]')
        holidays_np = np.array(holidays, dtype='datetime64[D]')

        df_avencer.loc[:, '_DIAS_UTEIS_'] = np.busday_count(start_date_np, end_dates_np, holidays=holidays_np)
        
        df_avencer = df_avencer[df_avencer['_DIAS_UTEIS_'] > 0]
        print("Cálculo de dias úteis realizado com sucesso.")
    except Exception as e:
        print(f"[WGAP] Falha ao calcular dias úteis: {e}")
        df_avencer.loc[:, '_DIAS_UTEIS_'] = np.nan
else:
    print("[AVISO] Não há parcelas futuras ('a vencer') na base de dados. O cálculo de TIR será pulado.")
    df_avencer['_DIAS_UTEIS_'] = np.nan


all_tirs = []
cat_cols = list(dimensoes_analise.values())
segmentos_para_analise = [('Carteira Total', 'Todos')] + \
                          [(col, seg) for col in cat_cols if col in df_avencer.columns for seg in df_avencer[col].dropna().unique()]

for tipo_dimensao, segmento in segmentos_para_analise:
    if tipo_dimensao == 'Carteira Total':
        df_segmento = df_avencer.copy()
    else:
        df_segmento = df_avencer[df_avencer[tipo_dimensao] == segmento].copy()

    if df_segmento.empty or df_segmento['_DIAS_UTEIS_'].isnull().all():
        continue

    vp_bruto = df_segmento['ValorPresente'].sum()
    tir_bruta, tir_pdd = np.nan, np.nan

    if vp_bruto > 0:
        # TIR Bruta
        fluxos_brutos = df_segmento.groupby('_DIAS_UTEIS_')['ValorNominal'].sum()
        tir_bruta = calculate_xirr([-vp_bruto] + fluxos_brutos.values.tolist(), [0] + fluxos_brutos.index.tolist())

        # TIR Líquida de PDD
        pdd_total_segmento = df_segmento['PDDTotal'].sum()
        pdd_rate = (pdd_total_segmento / vp_bruto) if pd.notna(pdd_total_segmento) and vp_bruto > 0 else 0.0
        df_segmento['Fluxo_PDD'] = df_segmento['ValorNominal'] * (1 - pdd_rate)
        fluxos_pdd = df_segmento.groupby('_DIAS_UTEIS_')['Fluxo_PDD'].sum()
        tir_pdd = calculate_xirr([-vp_bruto] + fluxos_pdd.values.tolist(), [0] + fluxos_pdd.index.tolist())

    all_tirs.append({
        'DimensaoColuna': tipo_dimensao, 'Segmento': segmento,
        'Valor Presente TIR (M)': vp_bruto / 1e6,
        'TIR Bruta \n(% a.m. )': tir_bruta * 100 if pd.notna(tir_bruta) else np.nan,
        'TIR Líquida de PDD \n(% a.m. )': tir_pdd * 100 if pd.notna(tir_pdd) else np.nan
    })

df_tir_summary = pd.DataFrame(all_tirs)
if not df_tir_summary.empty:
    tir_cols_to_fill = [col for col in df_tir_summary.columns if 'TIR' in col]
    df_tir_summary[tir_cols_to_fill] = df_tir_summary[tir_cols_to_fill].fillna(-100.0)

print("Cálculo de TIR concluído.")

"""#### <span style="color:#FFDAC1;">  Montagem do HTML

Uno todos os elementos em um relatório HTML interativo.
"""
#***********************
#* GERAÇÃO DO RELATÓRIO HTML
#***********************

print("\n" + "="*80)
print("GERANDO RELATÓRIO HTML FINAL")
print("="*80)

def encode_image_to_base64(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except FileNotFoundError:
        print(f"[ATENÇÃO] Arquivo de imagem não encontrado em: {image_path}. A logo não será exibida.")
        return None

logo_base64 = encode_image_to_base64(logo_path)
report_date = ref_date.strftime('%d/%m/%Y')

html_css = """
<style>
    /* Estilos para a mensagem de entrada única */
    .single-entry-message { background-color: #eef7f7; border-left: 5px solid #76c6c5; padding: 15px 20px; margin: 10px 0; font-size: 1.0em; color: #313131; }
    .single-entry-message strong { color: #163f3f; }
    .summary-title { font-size: 1.4em; color: #163f3f; border-bottom: 2px solid #76c6c5; padding-bottom: 10px; margin-top: 20px; margin-bottom: 20px; }
    .checks-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 15px; margin-bottom: 20px; }
    .check-item { background-color: #f5f5f5; padding: 12px; border-radius: 5px; border-left: 4px solid #76c6c5; font-size: 0.95em; }
    .check-item strong { color: #163f3f; }
    .dataframe-checks th { background-color: #5b8c8c; }
    .check-table-wrapper h4 { margin-top: 20px; margin-bottom: 10px; color: #163f3f; border-bottom: 2px solid #eeeeee; padding-bottom: 5px; }
    body { font-family: "Gill Sans MT", Arial, sans-serif; background-color: #f9f9f9; color: #313131; margin: 0; padding: 0; }
    .main-content { padding: 25px; }
    header { background-color: #163f3f; color: #FFFFFF; padding: 20px 40px; display: flex; justify-content: space-between; align-items: center; border-bottom: 5px solid #76c6c5; }
    header .logo img { height: 75px; }
    header .report-title { text-align: left; font-family: "Gill Sans MT", Arial, sans-serif; }
    header .report-title h1, header .report-title h2, header .report-title h3 { margin: 0; padding: 0; font-weight: normal; }
    header .report-title h1 { font-size: 1.6em; }
    header .report-title h2 { font-size: 1.4em; color: #d0d0d0; }
    header .report-title h3 { font-size: 1.1em; color: #a0a0a0; }
    .container-botoes { display: flex; flex-wrap: wrap; gap: 15px; margin-bottom: 25px; margin-top: 40px; }
    .container-botoes > details { flex: 1 1 280px; border: 1px solid #76c6c5; border-radius: 8px; overflow: hidden; }
    .container-botoes > details[open] { flex-basis: 100%; }
    details summary { font-size: 1.1em; font-weight: bold; color: #FFFFFF; background-color: #163f3f; padding: 15px 20px; cursor: pointer; outline: none; list-style-type: none; }
    details summary:hover { background-color: #0e5d5f; }
    details[open] summary { background-color: #76c6c5; color: #313131; }
    details[open] summary:hover { filter: brightness(95%); }
    summary::-webkit-details-marker { display: none; }
    summary::before { content: '► '; margin-right: 8px; font-size: 0.8em;}
    details[open] summary::before { content: '▼ '; }
    .content-wrapper { padding: 20px; background-color: #FFFFFF; overflow-x: auto; border: 1px solid #ddd; border-radius: 8px; }
    table.dataframe, th, td { border: 1px solid #bbbbbb; }
    table.dataframe { border-collapse: collapse; width: 100%; }
    th, td { text-align: left; padding: 10px; vertical-align: middle; }
    th { background-color: #163f3f; color: #FFFFFF; }
    tr:nth-child(even) { background-color: #eeeeee; }
    footer { background-color: #f0f0f0; color: #555555; font-size: 0.8em; line-height: 1.6; padding: 25px 40px; margin-top: 40px; border-top: 1px solid #dddddd; }
    footer .disclaimer { margin-top: 20px; font-style: italic; border-top: 1px solid #dddddd; padding-top: 20px; }
</style>
"""

html_parts = ["<!DOCTYPE html><html lang='pt-BR'><head>",
              "<meta charset='UTF-8'><title>Análise de Desempenho - FCT Consignado II</title>",
              html_css, "</head><body>"]

html_parts.append(f"<header><div class='report-title'><h1>Análise de desempenho</h1><h2>FCT CONSIGNADO II</h2><h3>{report_date}</h3></div>")
if logo_base64:
    html_parts.append(f'<div class="logo"><img src="data:image/png;base64,{logo_base64}" alt="Logo"></div>')
html_parts.append("</header><div class='main-content'>")

mapa_descricoes = {
    'Cedentes': 'Analisa as métricas de risco e retorno agrupadas por cada Cedente.',
    'Tipo de Ativo': 'Compara o desempenho entre os diferentes tipos de ativos (CCB, CT - Contrato).',
    'Situação': 'Compara o desempenho dos títulos com base na sua situação atual (Ex: Aditado).',
    'Pagamento Parcial': 'Verifica se há impacto nas métricas para títulos que aceitam pagamento parcial.',
    'Flag Ágio/Deságio': 'Compara o desempenho de contratos adquiridos com ágio vs. deságio.'
}

# Verificações de Sanidade dos Dados
html_parts.append("<details open><summary>Verificações e Sanidade dos Dados</summary><div class='content-wrapper'>")
simple_checks_html = "<div class='checks-container'>"
table_checks_html = "<div class='check-table-wrapper'>"
for key, value in checks_results.items():
    if '[TABELA]' in key:
        clean_key = key.replace('[TABELA]', '').strip()
        table_checks_html += f"<h4>{clean_key}</h4>{str(value)}"
    else:
        simple_checks_html += f"<div class='check-item'><strong>{key}:</strong> {value}</div>"
simple_checks_html += "</div>"
table_checks_html += "</div>"
html_parts.append(simple_checks_html)
html_parts.append(table_checks_html)
html_parts.append("</div></details>")

# Resumo da Carteira Total
print("--> Gerando tabela de resumo da carteira total...")
total_metrics = {
    'Nº Contratos': df_report['CCB'].nunique(),
    col_contratos_venc_perc: (df_report[df_report['_ContratoVencido_Flag'] == 1]['CCB'].nunique() / df_report['CCB'].nunique()) * 100,
    vl_col_name: df_report['_ValorLiquido'].sum() / 1e6,
    vp_col_name: df_report['ValorPresente'].sum() / 1e6,
    '%PDD': (1 - df_report['_ValorLiquido'].sum() / df_report['ValorPresente'].sum()) * 100 if df_report['ValorPresente'].sum() > 0 else 0,
    'Venc. 1d / VP': (df_report['_ValorVencido_1d'].sum() / df_report['ValorPresente'].sum()) * 100 if df_report['ValorPresente'].sum() > 0 else 0,
    'Venc. 30d / VP': (df_report['_ValorVencido_30d'].sum() / df_report['ValorPresente'].sum()) * 100 if df_report['ValorPresente'].sum() > 0 else 0,
    'Venc. 60d / VP': (df_report['_ValorVencido_60d'].sum() / df_report['ValorPresente'].sum()) * 100 if df_report['ValorPresente'].sum() > 0 else 0,
    'Ticket Ponderado (R$)': (df_report['ValorNominal'] * df_report['ValorPresente']).sum() / df_report['ValorPresente'].sum() if df_report['ValorPresente'].sum() > 0 else 0,
}
df_total = pd.DataFrame([total_metrics], index=['Total'])
if not df_tir_summary.empty:
    df_tir_total = df_tir_summary[df_tir_summary['DimensaoColuna'] == 'Carteira Total'].drop(columns=['DimensaoColuna', 'Segmento', 'Valor Presente TIR (M)'])
    # CORREÇÃO APLICADA AQUI: Alinha os índices antes de concatenar para garantir uma única linha.
    if not df_tir_total.empty:
        df_tir_total.index = ['Total']
        df_total = pd.concat([df_total, df_tir_total], axis=1)


colunas_vencimento = ['Venc. 1d / VP', 'Venc. 30d / VP', 'Venc. 60d / VP']
colunas_ordem_total = ['Nº Contratos', col_contratos_venc_perc, vl_col_name, vp_col_name, '%PDD'] + colunas_vencimento + ['Ticket Ponderado (R$)']
ordem_ideal_tir = ['TIR Bruta \n(% a.m. )', 'TIR Líquida de PDD \n(% a.m. )']
colunas_tir_ordenadas = [col for col in ordem_ideal_tir if col in df_total.columns]
colunas_finais_total = [col for col in colunas_ordem_total + colunas_tir_ordenadas if col in df_total.columns]
df_total = df_total[colunas_finais_total]

formatters = {
    vl_col_name: '{:,.2f}'.format, vp_col_name: '{:,.2f}'.format,
    'Nº Contratos': lambda x: f'{x:,.0f}'.replace(',', '.'),
    'Ticket Ponderado (R$)': lambda x: f'R$ {x:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'),
    '%PDD': '{:,.2f}%'.format
}
formatters[col_contratos_venc_perc] = '{:,.2f}%'.format
for col in colunas_vencimento: formatters[col] = '{:,.2f}%'.format
for col in colunas_tir_ordenadas: formatters[col] = '{:,.2f}%'.format

df_total.columns = [col.replace('\n', '<br>') for col in df_total.columns]

html_parts.append("<h2 class='summary-title'>Resumo da Carteira Total</h2><div class='content-wrapper'>")
html_parts.append(df_total.to_html(index=False, classes='dataframe', formatters=formatters, na_rep='-', escape=False))
html_parts.append("</div>")

# Início dos botões retráteis
html_parts.append("<div class='container-botoes'>")
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_report.columns or df_report[coluna].isnull().all(): continue
    print(f"--> Processando e gerando HTML para: '{nome_analise}'")

    df_final = tabelas_metricas.get(nome_analise)
    df_ticket = tabelas_ticket.get(nome_analise, pd.DataFrame())

    df_final = df_final.join(df_ticket, how='outer')
    if not df_tir_summary.empty:
        df_tir = df_tir_summary[df_tir_summary['DimensaoColuna'] == coluna].set_index('Segmento')
        if not df_tir.empty:
            df_final = df_final.join(df_tir.drop(columns=['DimensaoColuna', 'Valor Presente TIR (M)']), how='outer')

    df_final.index.name = nome_analise
    df_final.reset_index(inplace=True)

    colunas_ordem = [nome_analise, 'Nº Contratos', col_contratos_venc_perc, vl_col_name, vp_col_name, '%PDD'] + colunas_vencimento
    if 'Ticket Ponderado (R$)' in df_final.columns: colunas_ordem.append('Ticket Ponderado (R$)')

    colunas_finais = [col for col in colunas_ordem + colunas_tir_ordenadas if col in df_final.columns]
    outras_colunas = [col for col in df_final.columns if col not in colunas_finais]
    df_final = df_final[colunas_finais + outras_colunas]

    df_final = df_final.sort_values(vp_col_name, ascending=False).reset_index(drop=True)
    df_final.columns = [col.replace('\n', '<br>') for col in df_final.columns]

    html_parts.append("<details>")
    descricao = mapa_descricoes.get(nome_analise, 'Descrição não disponível.')
    html_parts.append(f'<summary title="{descricao}">{nome_analise}</summary>')
    html_parts.append("<div class='content-wrapper'>")
    html_table = df_final.to_html(index=False, classes='dataframe', formatters=formatters, na_rep='-', escape=False)
    html_parts.append(html_table)
    html_parts.append("</div></details>")

html_parts.append("</div></div>")

footer_main_text = "Este documento tem como objetivo apresentar uma análise de desempenho do fundo FCT Consignado II (CNPJ 52.203.615/0001-19)..."
footer_disclaimer = "Disclaimer: Este relatório foi preparado pelo Porto Real Investimentos exclusivamente para fins informativos..."
html_parts.append(f"<footer><p>{footer_main_text.strip()}</p><div class='disclaimer'>{footer_disclaimer.strip()}</div></footer>")
html_parts.append("</body></html>")

final_html_content = "\n".join(html_parts)
html_output_filename = os.path.join(output_path, 'analise_carteira_estoque.html')
try:
    with open(html_output_filename, 'w', encoding='utf-8') as f:
        f.write(final_html_content)
    print("\n" + "="*80)
    print("ANÁLISE CONCLUÍDA COM SUCESSO!")
    print(f"O relatório HTML final foi salvo em: {html_output_filename}")
    print("="*80)
except Exception as e:
    print(f"\n[ERRO GRAVE] Não foi possível salvar o arquivo HTML: {e}")




#!###############################################
#! BLOCO de DEBUG 
#! ###############################################
# --- debug 1 - vencidos ---

print("--- debug: métrica de contratos vencidos ---")

# pego 1 contrato aleatório p/ testar
ccb_aleatorio = df_report['CCB'].sample(1).iloc[0]
print(f"\nanalisando CCB aleatório: {ccb_aleatorio}")

# filtrando só as parcelas desse contrato
df_contrato_exemplo = df_report[df_report['CCB'] == ccb_aleatorio]

# checar datas de vencimento x geração
print("\ndatas das parcelas:")
display(df_contrato_exemplo[['CCB', 'DataVencimento', 'DataGeracao']])

# verificar se tem alguma vencida
tem_parcela_vencida = (df_contrato_exemplo['DataVencimento'] < data_referencia_relatorio).any()

print(f"\nresultado da verificação:")
if tem_parcela_vencida:
    print(f"--> ok: tem parcela vencida antes de {data_referencia_relatorio.strftime('%d/%m/%Y')}")
    print("    faz sentido esse contrato contar como 'vencido'")
else:
    print("--> erro: não tem parcela vencida. revisar a lógica aí")


# --- debug 2 - TIR ausente por segmento ---

print("\n\n--- debug: TIR ausente p/ segmentos específicos ---")

# segmento: ágio
df_agio = df_report[df_report['_FlagDesagio'] == 'Ágio']
parcelas_futuras_agio = (df_agio['DataVencimento'] > data_referencia_relatorio).sum()

print(f"\nchecando 'Ágio':")
print(f"--> parcelas futuras (> {data_referencia_relatorio.strftime('%d/%m/%Y')}): {parcelas_futuras_agio}")
if parcelas_futuras_agio == 0:
    print("    sem fluxo futuro -> TIR indefinida (correto)")

# segmento: pagamento parcial = SIM
df_pgto_parcial = df_report[df_report['PagamentoParcial'] == 'SIM']
parcelas_futuras_pgto_parcial = (df_pgto_parcial['DataVencimento'] > data_referencia_relatorio).sum()

print(f"\nchecando 'Pagamento Parcial = SIM':")
print(f"--> parcelas futuras (> {data_referencia_relatorio.strftime('%d/%m/%Y')}): {parcelas_futuras_pgto_parcial}")
if parcelas_futuras_pgto_parcial == 0:
    print("    idem: sem fluxo futuro -> TIR não rola mesmo")


del df_report, df_total #! THE END (?)