# /src/fct_consig/flask_dashboard/data_processing.py

import pandas as pd
import numpy as np
import numpy_financial as npf
from scipy.optimize import newton
import os
import glob
from datetime import datetime

# Mapeamentos e constantes (sem alteração)
MAPEAMENTO_COLUNAS = {
    'Convênio': 'Nome do Ente Consignado', 'CedenteCnpjCpf': 'Documento do Cedente',
    'CedenteNome': 'Nome do Cedente', 'SacadoCnpjCpf': 'Documento do Sacado',
    'SacadoNome': 'Nome do Sacado', 'Situacao': 'Situação', 'NotaPdd': 'Risco',
    'CCB': 'Código do Contrato', 'DataEmissao': 'Data de Emissão',
    'DataAquisicao': 'Data de Aquisição', 'DataVencimento': 'Data de Vencimento',
    'ValorAquisicao': 'Valor de Compra', 'ValorPresente': 'Valor Atual',
    'ValorNominal': 'Valor de Vencimento', 'PDDTotal': 'Valor de PDD',
    'TipoAtivo': 'Tipo de Recebível'
}
COST_DICT = {
    'ASSEMBLEIA. MATO GROSSO': [0.03, 2.14], 'GOV. ALAGOAS': [0.035, 5.92],
}
DEFAULT_COST = COST_DICT.get('GOV. ALAGOAS', [0.035, 5.92])

# Funções auxiliares (sem alteração)
def carregar_e_limpar_dados(caminho, padrao):
    lista_arquivos = glob.glob(os.path.join(caminho, padrao))
    if not lista_arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em '{caminho}' com o padrão '{padrao}'")
    
    lista_dfs = []
    for arquivo in lista_arquivos:
        try:
            df_temp = pd.read_csv(arquivo, sep=';', decimal=',', encoding='latin1', dtype=str, low_memory=False)
            lista_dfs.append(df_temp)
        except Exception as e:
            print(f"Erro ao ler {arquivo}: {e}")
            continue
    
    if not lista_dfs:
        raise IOError("Nenhum arquivo pôde ser lido com sucesso.")

    df_final = pd.concat(lista_dfs, ignore_index=True)
    df_final.dropna(how='all', inplace=True)

    colunas_numericas = ['ValorAquisicao', 'ValorNominal', 'ValorPresente', 'PDDTotal']
    colunas_data = ['DataEmissao', 'DataAquisicao', 'DataVencimento']
    for col in colunas_numericas:
        df_final[col] = pd.to_numeric(df_final[col].astype(str).str.replace(',', '.'), errors='coerce')
    for col in colunas_data:
        df_final[col] = pd.to_datetime(df_final[col], dayfirst=True, errors='coerce')
    
    return df_final.dropna(subset=colunas_data + ['ValorPresente'])

def load_holidays(file_path):
    if not os.path.exists(file_path):
        print(f"[AVISO] Arquivo de feriados '{file_path}' não encontrado. Usando lista vazia.")
        return []
    df_feriados = pd.read_excel(file_path)
    return pd.to_datetime(df_feriados['Data'][:-9]).tolist()

def calculate_xirr(cash_flows, days):
    cash_flows, days = np.array(cash_flows), np.array(days)
    def npv(rate):
        return np.sum(cash_flows / (1 + rate) ** (days / 21.0))
    try:
        return newton(npv, 0.015)
    except (RuntimeError, ValueError):
        return np.nan

def add_calculated_columns(df, ref_date, holidays, cost_dict, default_cost):
    df['Data de Vencimento Ajustada'] = df['Data de Vencimento']
    end_dates = pd.to_datetime(df['Data de Vencimento']).values.astype('datetime64[D]')
    start_date = np.datetime64(ref_date, 'D')
    holiday_dates = np.array([np.datetime64(d, 'D') for d in holidays])
    df['_DIAS_UTEIS_'] = np.busday_count(start_date, end_dates, holidays=holiday_dates)
    df['Custo Variável'] = df['Nome do Ente Consignado'].map(lambda x: cost_dict.get(x, default_cost)[0])
    df['Custo Fixo'] = df['Nome do Ente Consignado'].map(lambda x: cost_dict.get(x, default_cost)[1])
    df['Custo Total'] = df['Custo Fixo'] + df['Custo Variável'] * df['Valor de Vencimento']
    df['Receita Líquida'] = df['Valor de Vencimento'] - df['Custo Total']
    return df

# A correção está nesta função
def generate_summary_table(df_final, ref_date):
    print("Iniciando geração da tabela de resumo por ente...")
    lista_entes = ['* CARTEIRA *'] + df_final['Nome do Ente Consignado'].dropna().unique().tolist()
    all_summaries = []
    
    valor_total_carteira = df_final['Valor Atual'].sum() - df_final['Valor de PDD'].sum()

    for ente in lista_entes:
        is_total_portfolio = (ente == '* CARTEIRA *')
        df_ente = df_final if is_total_portfolio else df_final[df_final['Nome do Ente Consignado'] == ente]

        if df_ente.empty:
            continue
            
        mask_vencidos = df_ente['Data de Vencimento Ajustada'] <= ref_date
        df_avencer = df_ente[~mask_vencidos]
        df_vencidos = df_ente[mask_vencidos]
        
        summary = {'Nome do Ente': ente}
        
        summary['# Parcelas'] = len(df_ente)
        # *** GARANTA QUE ESTA LINHA ESTEJA CORRETA ***
        summary['Qtd Contratos'] = df_ente['Código do Contrato'].nunique()
        
        valor_presente_total = df_ente['Valor Atual'].sum()
        valor_pdd = df_ente['Valor de PDD'].sum()
        summary['Valor Presente'] = valor_presente_total
        summary['Valor PDD'] = valor_pdd
        summary['% PDD'] = valor_pdd / (valor_presente_total or 1)
        summary['Valor Líquido'] = valor_presente_total - valor_pdd
        summary['% Carteira'] = summary['Valor Líquido'] / (valor_total_carteira or 1)

        valor_vencido = df_vencidos['Valor Atual'].sum()
        valor_a_vencer = df_avencer['Valor Atual'].sum()
        summary['Valor A Vencer'] = valor_a_vencer
        summary['Valor Vencidos'] = valor_vencido
        summary['% Vencidos'] = valor_vencido / (valor_presente_total or 1)

        if valor_a_vencer > 0:
            prazo_ponderado = (df_avencer['_DIAS_UTEIS_'] * df_avencer['Valor Atual']).sum() / valor_a_vencer
            summary['Prazo Médio (DU)'] = prazo_ponderado
        else:
            summary['Prazo Médio (DU)'] = 0

        if not is_total_portfolio and not df_ente.empty:
            summary['Custo Variável'] = df_ente['Custo Variável'].iloc[0]
            summary['Custo Fixo'] = df_ente['Custo Fixo'].iloc[0]
        
        if valor_a_vencer > 0:
            for tipo, col in {'bruta': 'Valor de Vencimento', 'líquida': 'Receita Líquida'}.items():
                df_parcelas = df_avencer.groupby('_DIAS_UTEIS_')[col].sum()
                dias = [0] + df_parcelas.index.tolist()
                fluxos = [-valor_a_vencer] + df_parcelas.values.tolist()
                
                if sum(fluxos) > 0:
                    summary[f'TIR {tipo} a.m.'] = calculate_xirr(fluxos, dias)
                else:
                    summary[f'TIR {tipo} a.m.'] = np.nan
        else:
            summary['TIR bruta a.m.'] = np.nan
            summary['TIR líquida a.m.'] = np.nan
            
        all_summaries.append(summary)

    print("Geração da tabela de resumo concluída.")
    return pd.DataFrame(all_summaries).set_index('Nome do Ente')
    
def get_summary_data(caminho_dados, padrao_arquivo, ref_date_str, caminho_feriados):
    print("Iniciando processo de ETL...")
    ref_date = datetime.strptime(ref_date_str, '%Y-%m-%d')
    df_inicial = carregar_e_limpar_dados(caminho_dados, padrao_arquivo)
    df_traduzido = df_inicial.rename(columns=MAPEAMENTO_COLUNAS)
    holidays = load_holidays(caminho_feriados)
    df_processed = add_calculated_columns(df_traduzido, ref_date, holidays, COST_DICT, DEFAULT_COST)
    df_summary_com_index = generate_summary_table(df_processed, ref_date)
    df_summary = df_summary_com_index.reset_index()
    print("Processo de ETL concluído.")
    df_limpo = df_summary.replace({np.nan: None})
    return df_limpo