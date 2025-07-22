# -*- coding: utf-8 -*-
"""
Relatorio_Entes_Final.py (com adaptação para relatório simples)

Este script realiza um processo completo de ETL e geração de relatórios:
1. Carrega dados de recebíveis de múltiplos arquivos CSV.
2. Limpa e pré-processa os dados.
3. Traduz os nomes das colunas.
4. Calcula métricas financeiras e de portfólio.
5. Gera três saídas principais:
    - Um arquivo Excel com a tabela de resumo.
    - Um banco de dados SQLite cumulativo.
    - Um relatório HTML SIMPLIFICADO (preto e branco).
    - (A lógica para o relatório HTML complexo original foi mantida na função main())
"""

# ==============================================================================
# 1. IMPORTAR BIBLIOTECAS
# ==============================================================================
import pandas as pd
import numpy as np
import numpy_financial as npf
from scipy.optimize import newton
import os
import glob
from datetime import datetime
import sqlite3
from pathlib import Path
import configparser
# A biblioteca 'base64' não é mais necessária para o relatório simples.

# ==============================================================================
# 2. CONFIGURAÇÃO E CONSTANTES
# ==============================================================================

# Carregar configurações do arquivo config.cfg
config = configparser.ConfigParser()
config_file = 'config.cfg'

if not os.path.exists(config_file):
    raise FileNotFoundError(f"Erro: Arquivo de configuração '{config_file}' não encontrado.")

config.read(config_file)

try:
    ROOT_PATH = config.get('Paths', 'root_path')
    CAMINHO_PASTA_DADOS = os.path.join(ROOT_PATH, config.get('Paths', 'pasta_dados'))
    CAMINHO_RESULTADOS_BASE = os.path.join(ROOT_PATH, config.get('Paths', 'pasta_resultados'))
    CAMINHO_FERIADOS = os.path.join(ROOT_PATH, config.get('Paths', 'arquivo_feriados'))
    
    DATA_REFERENCIA_STR = config.get('Parameters', 'data_referencia')
    PADRAO_ARQUIVO_DADOS = config.get('Parameters', 'padrao_arquivo_dados')
    
    DATA_REFERENCIA = datetime.strptime(DATA_REFERENCIA_STR, '%Y-%m-%d')
    ref_date = DATA_REFERENCIA

except (configparser.NoOptionError, configparser.NoSectionError) as e:
    raise ValueError(f"Erro no arquivo de configuração: {e}")

# Mapeamento de Colunas
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

# Custos por Ente
COST_DICT = {
    'ASSEMBLEIA. MATO GROSSO': [0.03, 2.14], 'GOV. ALAGOAS': [0.035, 5.92],
}
DEFAULT_COST = COST_DICT.get('GOV. ALAGOAS', [0.035, 5.92])

# ==============================================================================
# 3. FUNÇÕES AUXILIARES E DE CARGA (Sem alterações)
# ==============================================================================

def carregar_e_limpar_dados(caminho, padrao):
    lista_arquivos = glob.glob(os.path.join(caminho, padrao))
    if not lista_arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em '{caminho}' com o padrão '{padrao}'")
    
    encodings_para_tentar = ['latin1', 'cp1252', 'utf-8']
    lista_dfs = []
    for arquivo in lista_arquivos:
        for enc in encodings_para_tentar:
            try:
                df_temp = pd.read_csv(arquivo, sep=';', decimal=',', encoding=enc, dtype=str, low_memory=False)
                print(f"Sucesso: Arquivo '{os.path.basename(arquivo)}' lido com encoding '{enc}'.")
                lista_dfs.append(df_temp)
                break
            except Exception:
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
    try:
        df_feriados = pd.read_excel(file_path)
        return pd.to_datetime(df_feriados['Data'][:-9]).tolist()
    except FileNotFoundError:
        print(f"[AVISO] Arquivo de feriados '{file_path}' não encontrado.")
        return []

def calculate_xirr(cash_flows, days):
    cash_flows, days = np.array(cash_flows), np.array(days)
    def npv(rate):
        return np.sum(cash_flows / (1 + rate) ** (days / 21.0))
    try:
        return newton(npv, 0.015)
    except (RuntimeError, ValueError):
        return np.nan

# ==============================================================================
# 4. FUNÇÕES DE ANÁLISE (Sem alterações)
# ==============================================================================

def add_calculated_columns(df, ref_date, holidays, cost_dict, default_cost):
    print("Calculando colunas adicionais...")
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

def generate_summary_table(df_final, ref_date):
    print("Iniciando geração da tabela de resumo por ente...")
    lista_entes = ['* CARTEIRA *'] + df_final['Nome do Ente Consignado'].dropna().unique().tolist()
    all_summaries = []
    
    valor_total_carteira = df_final['Valor Atual'].sum() - df_final['Valor de PDD'].sum()

    for ente in lista_entes:
        is_total_portfolio = (ente == '* CARTEIRA *')
        df_ente = df_final if is_total_portfolio else df_final[df_final['Nome do Ente Consignado'] == ente]

        mask_vencidos = df_ente['Data de Vencimento Ajustada'] <= ref_date
        df_avencer = df_ente[~mask_vencidos]
        df_vencidos = df_ente[mask_vencidos]
        
        summary = {'Nome do Ente': ente}
        
        summary['# Parcelas'] = len(df_ente)
        summary['# Contratos'] = df_ente['Código do Contrato'].nunique()
        summary['# médio de parcelas'] = summary['# Parcelas'] / (summary['# Contratos'] or 1)
        
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
        summary['PDD / Vencidos'] = valor_pdd / (valor_vencido + 1e-9)
        summary['Próxima parcela'] = df_avencer['Data de Vencimento Ajustada'].min()
        summary['Último Vencimento'] = df_avencer['Data de Vencimento Ajustada'].max()

        valor_parcelas_avencer = df_avencer['Valor de Vencimento'].sum()
        custo_total_avencer = df_avencer['Custo Total'].sum()
        summary['Valor Parcelas'] = valor_parcelas_avencer
        summary['Custo Total'] = custo_total_avencer
        summary['Recebimento Líquido'] = valor_parcelas_avencer - custo_total_avencer
        summary['Custo Total / Valor Parcelas'] = custo_total_avencer / (valor_parcelas_avencer or 1)

        if not is_total_portfolio and not df_ente.empty:
            summary['Custo Variável'] = df_ente['Custo Variável'].iloc[0]
            summary['Custo Fixo'] = df_ente['Custo Fixo'].iloc[0]
        
        if valor_a_vencer > 0:
            prazo_medio_du = np.average(df_avencer['_DIAS_UTEIS_'], weights=df_avencer['Valor Atual'])
            summary['Prazo médio (d.u.)'] = prazo_medio_du
            summary['Prazo médio (meses)'] = prazo_medio_du / 21
            summary['Prazo médio (anos)'] = prazo_medio_du / 252
            
            for tipo, col in {'bruta': 'Valor de Vencimento', 'líquida': 'Receita Líquida'}.items():
                df_parcelas = df_avencer.groupby('_DIAS_UTEIS_')[col].sum()
                dias = [0] + df_parcelas.index.tolist()
                fluxos = [-valor_a_vencer] + df_parcelas.values.tolist()
                summary[f'TIR {tipo} a.m.'] = calculate_xirr(fluxos, dias)
        
        all_summaries.append(summary)

    print("Geração da tabela de resumo concluída.")
    return pd.DataFrame(all_summaries).set_index('Nome do Ente')

# ==============================================================================
# 5. <<<< NOVA FUNÇÃO DE EXECUÇÃO PARA RELATÓRIO SIMPLES >>>>
# ==============================================================================
def main_simplificado():
    """
    Executa o fluxo completo e gera saídas padrão (Excel, SQL) e um
    relatório HTML simplificado, em preto e branco, sem interatividade.
    """
    print(f"--- INÍCIO DA ANÁLISE (SAÍDA SIMPLIFICADA) (REF: {DATA_REFERENCIA.strftime('%d/%m/%Y')}) ---")
    
    # 1. Carregar, limpar e traduzir dados
    df_inicial = carregar_e_limpar_dados(CAMINHO_PASTA_DADOS, PADRAO_ARQUIVO_DADOS)
    df_traduzido = df_inicial.rename(columns=MAPEAMENTO_COLUNAS)
    
    # 2. Carregar feriados e adicionar colunas
    holidays = load_holidays(CAMINHO_FERIADOS)
    df_processed = add_calculated_columns(df_traduzido, DATA_REFERENCIA, holidays, COST_DICT, DEFAULT_COST)

    # 3. Gerar a tabela de resumo (ainda com o índice 'Nome do Ente')
    df_summary = generate_summary_table(df_processed, DATA_REFERENCIA)

    # --- SALVAR SAÍDAS PADRÃO (EXCEL, SQL) ---
    # As saídas de dados em Excel e SQL são mantidas pois são úteis
    try:
        # Excel
        excel_output_dir = Path(CAMINHO_RESULTADOS_BASE) / "resumos_em_excel"
        excel_output_dir.mkdir(parents=True, exist_ok=True)
        data_hoje_str = datetime.now().strftime('%Y-%m-%d')
        data_ref_str = DATA_REFERENCIA.strftime('%Y-%m-%d')
        excel_file_name = f"{data_hoje_str}_ref_{data_ref_str}_resumo_entes.xlsx"
        df_summary.to_excel(excel_output_dir / excel_file_name, engine='openpyxl')
        print(f"\n[SUCESSO] Tabela de resumo salva em: {excel_output_dir / excel_file_name}")

        # SQL
        df_sql = df_summary.reset_index().copy() # reset_index para ter a coluna 'Nome do Ente'
        df_sql['data_referencia'] = pd.to_datetime(DATA_REFERENCIA)
        sql_dir = Path(CAMINHO_RESULTADOS_BASE) / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        db_path = sql_dir / "resumo_entes_cumulativo.db"
        conn = sqlite3.connect(db_path)
        df_sql.to_sql("resumo_entes", conn, if_exists='append', index=False)
        conn.close()
        print(f"[SUCESSO] Dados adicionados ao banco '{db_path.name}'.")

    except Exception as e:
        print(f"\n[AVISO] Ocorreu um erro ao salvar os arquivos de dados (Excel/SQL): {e}")


    # --- GERAÇÃO DE RELATÓRIO HTML SIMPLIFICADO ---
    print("\n--- GERANDO RELATÓRIO HTML SIMPLIFICADO ---")
    
    # Formata os números no DataFrame para melhor visualização, mas de forma simples
    df_render = df_summary.copy()
    for col in df_render.columns:
        if pd.api.types.is_float_kind(df_render[col].dtype):
            df_render[col] = df_render[col].map('{:,.4f}'.format)
    
    df_render.reset_index(inplace=True) # Transforma o índice 'Nome do Ente' em uma coluna
    
    # Gera o HTML da tabela diretamente do pandas
    html_table = df_render.to_html(
        classes="simple-report-table", 
        na_rep="—", 
        index=False,
        border=0
    )

    data_relatorio_str = datetime.now().strftime('%d/%m/%Y')
    data_referencia_str = ref_date.strftime('%d/%m/%Y')

    # Monta a página HTML completa com estilo mínimo
    html_content = f"""<!DOCTYPE html>
    <html lang="pt-br">
    <head>
    <meta charset="utf-8">
    <title>Relatório Simplificado de Entes — {ref_date.strftime('%d/%m/%Y')}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #ffffff;
            color: #000000;
        }}
        h1, h2 {{
            color: #333;
        }}
        .simple-report-table {{
            border-collapse: collapse;
            width: 100%;
            font-size: 0.9em;
        }}
        .simple-report-table th, .simple-report-table td {{
            border: 1px solid #cccccc;
            padding: 8px;
            text-align: left;
            white-space: nowrap; /* Evita quebra de linha nas células */
        }}
        .simple-report-table thead th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        .report-header {{
            margin-bottom: 25px;
            padding: 10px;
            border: 1px solid #e0e0e0;
        }}
    </style>
    </head>
    <body>
        <h1>Relatório Simplificado de Resumo de Entes</h1>
        <div class="report-header">
            <p><strong>Fundo:</strong> FCT Consig</p>
            <p><strong>Data do relatório:</strong> {data_relatorio_str}</p>
            <p><strong>Data de referência dos dados:</strong> {data_referencia_str}</p>
        </div>
        
        {html_table}
        
    </body>
    </html>"""

    # Define o nome do arquivo de saída e salva
    output_dir = Path(CAMINHO_RESULTADOS_BASE) / "resumo"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    data_hoje_str = datetime.now().strftime('%Y-%m-%d')
    data_ref_str = ref_date.strftime('%Y-%m-%d')
    
    # NOVO NOME DE ARQUIVO para não estragar o original
    nome_arquivo = f"{data_hoje_str}_{data_ref_str}-resumo_simples.html"
    output_file = output_dir / nome_arquivo

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\n[SUCESSO] Relatório HTML simplificado gerado em: {output_file}")


# ==============================================================================
# X. BLOCO DE EXECUÇÃO PRINCIPAL (MAIN) - ORIGINAL
#    (Mantido aqui para referência ou uso futuro)
# ==============================================================================
# ==============================================================================
# 5. <<<< NOVA FUNÇÃO DE EXECUÇÃO PARA RELATÓRIO SIMPLES >>>>
# ==============================================================================
def main_simplificado():
    """
    Executa o fluxo completo e gera saídas padrão (Excel, SQL) e um
    relatório HTML simplificado, em preto e branco, sem interatividade.
    """
    print(f"--- INÍCIO DA ANÁLISE (SAÍDA SIMPLIFICADA) (REF: {DATA_REFERENCIA.strftime('%d/%m/%Y')}) ---")
    
    # 1. Carregar, limpar e traduzir dados
    df_inicial = carregar_e_limpar_dados(CAMINHO_PASTA_DADOS, PADRAO_ARQUIVO_DADOS)
    df_traduzido = df_inicial.rename(columns=MAPEAMENTO_COLUNAS)
    
    # 2. Carregar feriados e adicionar colunas
    holidays = load_holidays(CAMINHO_FERIADOS)
    df_processed = add_calculated_columns(df_traduzido, DATA_REFERENCIA, holidays, COST_DICT, DEFAULT_COST)

    # 3. Gerar a tabela de resumo (ainda com o índice 'Nome do Ente')
    df_summary = generate_summary_table(df_processed, DATA_REFERENCIA)

    # --- SALVAR SAÍDAS PADRÃO (EXCEL, SQL) ---
    try:
        # Excel
        excel_output_dir = Path(CAMINHO_RESULTADOS_BASE) / "resumos_em_excel"
        excel_output_dir.mkdir(parents=True, exist_ok=True)
        data_hoje_str = datetime.now().strftime('%Y-%m-%d')
        data_ref_str = DATA_REFERENCIA.strftime('%Y-%m-%d')
        excel_file_name = f"{data_hoje_str}_ref_{data_ref_str}_resumo_entes.xlsx"
        df_summary.to_excel(excel_output_dir / excel_file_name, engine='openpyxl')
        print(f"\n[SUCESSO] Tabela de resumo salva em: {excel_output_dir / excel_file_name}")

        # SQL
        df_sql = df_summary.reset_index().copy()
        df_sql['data_referencia'] = pd.to_datetime(DATA_REFERENCIA)
        sql_dir = Path(CAMINHO_RESULTADOS_BASE) / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        db_path = sql_dir / "resumo_entes_cumulativo.db"
        conn = sqlite3.connect(db_path)
        df_sql.to_sql("resumo_entes", conn, if_exists='append', index=False)
        conn.close()
        print(f"[SUCESSO] Dados adicionados ao banco '{db_path.name}'.")

    except Exception as e:
        print(f"\n[AVISO] Ocorreu um erro ao salvar os arquivos de dados (Excel/SQL): {e}")


    # --- GERAÇÃO DE RELATÓRIO HTML SIMPLIFICADO ---
    print("\n--- GERANDO RELATÓRIO HTML SIMPLIFICADO ---")
    
    # Formata os números no DataFrame para melhor visualização, mas de forma simples
    df_render = df_summary.copy()
    for col in df_render.columns:
        # **AQUI ESTÁ A CORREÇÃO**
        if pd.api.types.is_float_dtype(df_render[col].dtype):
            # Formata o número, tratando valores nulos (NaN) para não dar erro
            df_render[col] = df_render[col].apply(lambda x: '{:,.4f}'.format(x) if pd.notnull(x) else '—')
    
    df_render.reset_index(inplace=True) 
    
    # Gera o HTML da tabela diretamente do pandas
    html_table = df_render.to_html(
        classes="simple-report-table", 
        na_rep="—", 
        index=False,
        border=0
    )

    data_relatorio_str = datetime.now().strftime('%d/%m/%Y')
    data_referencia_str = ref_date.strftime('%d/%m/%Y')

    # Monta a página HTML completa com estilo mínimo
    html_content = f"""<!DOCTYPE html>
    <html lang="pt-br">
    <head>
    <meta charset="utf-8">
    <title>Relatório Simplificado de Entes — {ref_date.strftime('%d/%m/%Y')}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #ffffff;
            color: #000000;
        }}
        h1, h2 {{
            color: #333;
        }}
        .simple-report-table {{
            border-collapse: collapse;
            width: 100%;
            font-size: 0.9em;
        }}
        .simple-report-table th, .simple-report-table td {{
            border: 1px solid #cccccc;
            padding: 8px;
            text-align: left;
            white-space: nowrap; /* Evita quebra de linha nas células */
        }}
        .simple-report-table thead th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        .report-header {{
            margin-bottom: 25px;
            padding: 10px;
            border: 1px solid #e0e0e0;
        }}
    </style>
    </head>
    <body>
        <h1>Relatório Simplificado de Resumo de Entes</h1>
        <div class="report-header">
            <p><strong>Fundo:</strong> FCT Consig</p>
            <p><strong>Data do relatório:</strong> {data_relatorio_str}</p>
            <p><strong>Data de referência dos dados:</strong> {data_referencia_str}</p>
        </div>
        
        {html_table}
        
    </body>
    </html>"""

    # Define o nome do arquivo de saída e salva
    output_dir = Path(CAMINHO_RESULTADOS_BASE) / "resumo"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    data_hoje_str = datetime.now().strftime('%Y-%m-%d')
    data_ref_str = ref_date.strftime('%Y-%m-%d')
    
    # NOVO NOME DE ARQUIVO para não estragar o original
    nome_arquivo = f"{data_hoje_str}_{data_ref_str}-resumo_simples.html"
    output_file = output_dir / nome_arquivo

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\n[SUCESSO] Relatório HTML simplificado gerado em: {output_file}")

# ==============================================================================
# 6. PONTO DE ENTRADA DO SCRIPT
# ==============================================================================
if __name__ == "__main__":
    # Para gerar o relatório simples que você pediu:
    main_simplificado()
    
    # Para gerar o relatório complexo original, comente a linha acima
    # e descomente a linha abaixo:
    # main()