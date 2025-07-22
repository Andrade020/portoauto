# -*- coding: utf-8 -*-
"""
Relatorio_Entes_Final.py

Este script realiza um processo completo de ETL e geração de relatórios:
1. Carrega dados de recebíveis de múltiplos arquivos CSV, tratando erros de encoding.
2. Limpa e pré-processa os dados.
3. Traduz os nomes das colunas para um padrão de análise conhecido.
4. Calcula métricas financeiras e de portfólio.
5. Gera três saídas:
    - Um arquivo Excel com a tabela de resumo.
    - Um arquivo CSV com o resumo.
    - Um banco de dados SQLite cumulativo e um arquivo .sql de backup.
    - Um relatório HTML interativo e estilizado com DataTables.
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
import base64
import sqlite3
from pathlib import Path
import configparser # <<< NOVO >>> Biblioteca para ler o arquivo de configuração

# ==============================================================================
# 2. CONFIGURAÇÃO E CONSTANTES
# ==============================================================================

# <<< NOVO BLOCO: Carregar configurações do arquivo config.cfg >>>
config = configparser.ConfigParser()
config_file = 'config.cfg'

if not os.path.exists(config_file):
    raise FileNotFoundError(f"Erro: Arquivo de configuração '{config_file}' não encontrado. Crie-o antes de executar o script.")

config.read(config_file)

# --- Carregar Caminhos do Arquivo de Configuração ---
try:
    ROOT_PATH = config.get('Paths', 'root_path')
    CAMINHO_PASTA_DADOS = os.path.join(ROOT_PATH, config.get('Paths', 'pasta_dados'))
    CAMINHO_RESULTADOS_BASE = os.path.join(ROOT_PATH, config.get('Paths', 'pasta_resultados'))
    CAMINHO_FERIADOS = os.path.join(ROOT_PATH, config.get('Paths', 'arquivo_feriados'))
    CAMINHO_LOGO = os.path.join(ROOT_PATH, config.get('Paths', 'arquivo_logo'))
    
    # --- Carregar Parâmetros do Arquivo de Configuração ---
    DATA_REFERENCIA_STR = config.get('Parameters', 'data_referencia')
    PADRAO_ARQUIVO_DADOS = config.get('Parameters', 'padrao_arquivo_dados')
    
    # Converter a data de string para datetime
    DATA_REFERENCIA = datetime.strptime(DATA_REFERENCIA_STR, '%Y-%m-%d')
    ref_date = DATA_REFERENCIA

except configparser.NoOptionError as e:
    raise ValueError(f"Erro no arquivo de configuração: uma opção esperada não foi encontrada. Detalhe: {e}")
except configparser.NoSectionError as e:
    raise ValueError(f"Erro no arquivo de configuração: uma seção esperada não foi encontrada. Detalhe: {e}")

# --- Constantes que permanecem no script (lógica de negócio) ---
# --- Mapeamento de Colunas (Tradução) ---
MAPEAMENTO_COLUNAS = {
    'Convênio': 'Nome do Ente Consignado',
    'CedenteCnpjCpf': 'Documento do Cedente',
    'CedenteNome': 'Nome do Cedente',
    'SacadoCnpjCpf': 'Documento do Sacado',
    'SacadoNome': 'Nome do Sacado',
    'Situacao': 'Situação',
    'NotaPdd': 'Risco',
    'CCB': 'Código do Contrato',
    'DataEmissao': 'Data de Emissão',
    'DataAquisicao': 'Data de Aquisição',
    'DataVencimento': 'Data de Vencimento',
    'ValorAquisicao': 'Valor de Compra',
    'ValorPresente': 'Valor Atual',
    'ValorNominal': 'Valor de Vencimento',
    'PDDTotal': 'Valor de PDD',
    'TipoAtivo': 'Tipo de Recebível'
}

# --- Custos por Ente ---
COST_DICT = {
    'ASSEMBLEIA. MATO GROSSO': [0.03, 2.14],
    'GOV. ALAGOAS': [0.035, 5.92],
}
DEFAULT_COST = COST_DICT.get('GOV. ALAGOAS', [0.035, 5.92])

# ==============================================================================
# 3. FUNÇÕES AUXILIARES E DE CARGA
# ==============================================================================

# (Nenhuma alteração necessária nesta seção)
def carregar_e_limpar_dados(caminho, padrao):
    lista_arquivos = glob.glob(os.path.join(caminho, padrao))
    if not lista_arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em '{caminho}' com o padrão '{padrao}'")
    
    encodings_para_tentar = ['utf-8', 'cp1252', 'latin1']
    lista_dfs = []
    for arquivo in lista_arquivos:
        df_temp = None
        for enc in encodings_para_tentar:
            try:
                df_temp = pd.read_csv(arquivo, sep=';', decimal=',', encoding=enc, dtype=str, low_memory=False)
                print(f"Sucesso: Arquivo '{os.path.basename(arquivo)}' lido com encoding '{enc}'.")
                lista_dfs.append(df_temp)
                break
            except Exception:
                print(f"Info: Falha ao ler '{os.path.basename(arquivo)}' com encoding '{enc}'. Tentando próximo...")
    
    if not lista_dfs:
        raise IOError("Nenhum arquivo pôde ser lido com sucesso.")

    df_final = pd.concat(lista_dfs, ignore_index=True)
    df_final.dropna(how='all', inplace=True)

    colunas_numericas = ['ValorAquisicao', 'ValorNominal', 'ValorPresente', 'PDDTotal']
    colunas_data = ['DataEmissao', 'DataAquisicao', 'DataVencimento', 'DataGeracao']
    for col in colunas_numericas:
        df_final[col] = pd.to_numeric(df_final[col].astype(str).str.replace(',', '.'), errors='coerce')
    for col in colunas_data:
        df_final[col] = pd.to_datetime(df_final[col], dayfirst=True, errors='coerce')
    
    return df_final.dropna(subset=colunas_data + ['ValorPresente'])

def load_holidays(file_path):
    try:
        print("Lendo feriados nacionais...")
        df_feriados = pd.read_excel(file_path)
        return pd.to_datetime(df_feriados['Data'][:-9]).tolist()
    except FileNotFoundError:
        print(f"[AVISO] Arquivo de feriados '{file_path}' não encontrado. Prazos em dias úteis podem estar incorretos.")
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
# 4. FUNÇÕES DE ANÁLISE
# ==============================================================================

# (Nenhuma alteração necessária nesta seção)
def add_calculated_columns(df, ref_date, holidays, cost_dict, default_cost):
    print("Calculando colunas adicionais (prazos, custos, etc.)...")
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
        print(f"  Processando: {ente}")
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
# 5. BLOCO DE EXECUÇÃO PRINCIPAL (MAIN)
# ==============================================================================
def main():
    print(f"--- INÍCIO DA ANÁLISE DE ESTOQUE (REF: {DATA_REFERENCIA.strftime('%d/%m/%Y')}) ---")
    
    # 1. Carregar, limpar e traduzir dados
    # <<< ALTERADO >>> Usa as variáveis carregadas do config.cfg
    df_inicial = carregar_e_limpar_dados(CAMINHO_PASTA_DADOS, PADRAO_ARQUIVO_DADOS)
    df_traduzido = df_inicial.rename(columns=MAPEAMENTO_COLUNAS)
    print("\nColunas traduzidas com sucesso.")
    
    # 2. Carregar feriados e adicionar colunas
    # <<< ALTERADO >>> Usa a variável CAMINHO_FERIADOS
    holidays = load_holidays(CAMINHO_FERIADOS)
    df_processed = add_calculated_columns(df_traduzido, DATA_REFERENCIA, holidays, COST_DICT, DEFAULT_COST)

    # 3. Gerar a tabela de resumo
    df_summary = generate_summary_table(df_processed, DATA_REFERENCIA).reset_index()

    # --- BLOCO DE SALVAMENTO EM EXCEL ---
    # 4. SALVAR EM EXCEL
    print("\n--- SALVANDO DADOS EM ARQUIVO EXCEL ---")
    try:
        # <<< ALTERADO >>> Usa o caminho base de resultados para criar o diretório de saída
        excel_output_dir = Path(CAMINHO_RESULTADOS_BASE) / "resumos_em_excel"
        excel_output_dir.mkdir(parents=True, exist_ok=True)

        data_hoje_str = datetime.now().strftime('%Y-%m-%d')
        data_ref_str = DATA_REFERENCIA.strftime('%Y-%m-%d')
        
        excel_file_name = f"{data_hoje_str}_ref_{data_ref_str}_resumo_entes.xlsx"
        excel_output_path = excel_output_dir / excel_file_name

        df_summary.to_excel(excel_output_path, index=False, engine='openpyxl')
        print(f"Tabela de resumo salva com sucesso em: {excel_output_path}")

    except Exception as e:
        print(f"\n[AVISO] Ocorreu um erro ao salvar o arquivo Excel: {e}")

    # 5. Salvar em SQL
    try:
        print("\n--- SALVANDO DADOS EM ARQUIVO SQL ---")
        df_sql = df_summary.copy()
        df_sql['data_referencia'] = pd.to_datetime(DATA_REFERENCIA)
        
        # <<< ALTERADO >>> Usa o caminho base de resultados para criar o diretório SQL
        sql_dir = Path(CAMINHO_RESULTADOS_BASE) / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        db_path = sql_dir / "resumo_entes_cumulativo.db"
        
        conn = sqlite3.connect(db_path)
        df_sql.to_sql("resumo_entes", conn, if_exists='append', index=False)
        conn.close()
        print(f"Dados adicionados com sucesso ao banco '{db_path.name}'.")
    except Exception as e:
        print(f"\n[AVISO] Ocorreu um erro ao salvar os dados em SQL: {e}")


    # Ler e codificar a logo em base64
    # <<< ALTERADO >>> Usa a variável CAMINHO_LOGO ao invés de um caminho fixo
    try:
        with open(CAMINHO_LOGO, "rb") as img_f:
            logo_base64 = base64.b64encode(img_f.read()).decode("utf-8")
    except FileNotFoundError:
        print(f"[AVISO] Arquivo de logo não encontrado em '{CAMINHO_LOGO}'. O relatório será gerado sem logo.")
        logo_base64 = ""


    # 6. Formatar DataFrame para HTML
    print("\n--- FORMATANDO DADOS PARA O RELATÓRIO HTML ---")
    # ... (o resto do bloco de formatação permanece o mesmo) ...
    df_render = df_summary.copy()
    
    # Formatação numérica e de texto
    def fmt_num(x, decimals=2):
        if pd.isna(x) or x == '': return "—"
        try:
            s = f"{float(x):,.{decimals}f}"
            return s.replace(",", "X").replace(".", ",").replace("X", ".")
        except (ValueError, TypeError): return "—"

    currency_cols = ['Valor Presente', 'Valor PDD', 'Valor Líquido', 'Valor A Vencer', 'Valor Vencidos', 'Valor Parcelas', 'Custo Total', 'Recebimento Líquido', 'Custo Fixo']
    for col in currency_cols: df_render[col] = df_render[col].apply(lambda x: fmt_num(x, 2))
    percent_cols = ['% PDD', '% Carteira', '% Vencidos', 'Custo Variável', 'Custo Total / Valor Parcelas']
    for col in percent_cols: df_render[col] = df_render[col].apply(lambda x: fmt_num(x * 100, 2))
    tir_cols = ['TIR bruta a.m.', 'TIR líquida a.m.']
    for col in tir_cols: df_render[col] = df_render[col].apply(lambda x: fmt_num(x * 100, 3))
    df_render['PDD / Vencidos'] = df_render['PDD / Vencidos'].apply(lambda x: fmt_num(x, 2))
    df_render['# médio de parcelas'] = df_render['# médio de parcelas'].apply(lambda x: fmt_num(x, 1))
    df_render['# Parcelas'] = df_render['# Parcelas'].apply(lambda x: fmt_num(x, 0))
    df_render['# Contratos'] = df_render['# Contratos'].apply(lambda x: fmt_num(x, 0))
    
    df_render['Nome do Ente'] = df_render['Nome do Ente'].replace({'* CARTEIRA *': 'Total da Carteira'})
    rename_map = {col: f"{col} (R$)" for col in currency_cols}
    rename_map.update({
        '% PDD': 'PDD (%)', '% Carteira': 'Carteira (%)', '% Vencidos': 'Vencidos (%)',
        'Custo Variável': 'Custo Variável (%)', 'Custo Total / Valor Parcelas': 'Custo Total/Valor Parcelas (%)',
        'TIR bruta a.m.': 'TIR bruta a.m. (%)', 'TIR líquida a.m.': 'TIR líquida a.m. (%)'
    })
    df_render.rename(columns=rename_map, inplace=True)
    
    # Ordenar dados para renderização
    total_row = df_render[df_render['Nome do Ente'] == 'Total da Carteira']
    entes_rows = df_render[df_render['Nome do Ente'] != 'Total da Carteira']
    sort_key = pd.to_numeric(entes_rows['Carteira (%)'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce')
    entes_sorted = entes_rows.assign(__sort_key__=sort_key).sort_values(by='__sort_key__', ascending=False).drop(columns=['__sort_key__'])
    df_render = pd.concat([total_row, entes_sorted], ignore_index=True)


    # —————————————————————————————————————————
    # 7. GERAR RELATÓRIO HTML ORDENÁVEL COM DATATABLES
    # —————————————————————————————————————————

    # ... (o resto do script de geração HTML permanece o mesmo) ...
    print("\n--- GERANDO RELATÓRIO HTML ---")
    
    # <<< Removido o bloco de ordenação antigo pois já foi feito acima >>>
    # ...

    # 7.2. Identificar os índices das colunas que precisam de ordenação numérica
    numeric_formatted_cols = [
        '# Parcelas', '# Contratos', '# médio de parcelas', 'Valor Presente (R$)',
        'Valor PDD (R$)', 'PDD (%)', 'Valor Líquido (R$)', 'Carteira (%)',
        'Valor A Vencer (R$)', 'Valor Vencidos (R$)', 'Vencidos (%)',
        'PDD / Vencidos', 'Valor Parcelas (R$)', 'Custo Total (R$)',
        'Recebimento Líquido (R$)', 'Custo Total/Valor Parcelas (%)',
        'Custo Variável (%)', 'Custo Fixo (R$)', 'Prazo médio (d.u.)',
        'Prazo médio (meses)', 'Prazo médio (anos)', 'TIR bruta a.m. (%)',
        'TIR líquida a.m. (%)'
    ]
    all_table_cols = df_render.columns.tolist() 

    numeric_col_indices = [
        i for i, col_name in enumerate(all_table_cols)
        if col_name in numeric_formatted_cols
    ]

    # 7.3. Definir a coluna de ordenação padrão para o DataTables
    try:
        sort_col_name = 'Carteira (%)' # <<< Adicionado para garantir que a variável exista
        default_sort_col_index = all_table_cols.index(sort_col_name)
        default_sort_order = 'desc'
    except (ValueError, NameError):
        default_sort_col_index = 1 
        default_sort_order = 'asc'

    # 7.4. Preparar a coluna 'Nome do Ente' para truncamento e tooltip
    def wrap_for_tooltip(text):
        if isinstance(text, str) and len(text) > 30:
            return f'<span title="{text}">{text}</span>'
        return text

    df_render['Nome do Ente'] = df_render['Nome do Ente'].apply(wrap_for_tooltip)

    # 7.5. Gerar o HTML da tabela
    html_table = df_render.to_html(
        table_id="summary",
        classes="display",
        border=0,
        na_rep="—", 
        index=False,
        escape=False
    )
    
    # ... (o resto do HTML permanece o mesmo) ...
    html_table = html_table.replace(
        '<td>Total da Carteira</td>', 
        '<td class="total-row-first-cell">Total da Carteira</td>',
        1
    )

    data_relatorio_str = datetime.now().strftime('%d-%m-%Y')
    data_referencia_str = ref_date.strftime('%d-%m-%Y')

    # 7.6. Montar a página HTML completa com o JavaScript
    html_content = f"""<!DOCTYPE html>
    <html lang="pt-br">
    <head>
    <meta charset="utf-8">
    <title>Relatório de Resumo de Entes — {ref_date.strftime('%d/%m/%Y')}</title>

    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/fixedcolumns/4.3.0/css/fixedColumns.dataTables.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/fixedheader/3.4.0/css/fixedHeader.dataTables.min.css">


    <style>
        body {{
            background-color: #FFFFFF; color: #313131; font-family: "Gill Sans MT", Arial, sans-serif; margin: 40px;
        }}
        .top-scroll-wrapper {{
            position: sticky; top: 45px; background-color: #ffffff; z-index: 102;
            overflow-x: auto; overflow-y: hidden; height: 20px; width: 100%;
        }}
        .top-scroll-inner {{ height: 20px; }}

        table.dataTable thead th {{
            background-color: #163f3f; color: #FFFFFF; white-space: nowrap; cursor: pointer;
        }}
        table.dataTable thead th.dtfc-fixed-left {{
            background-color: #0e3030;
        }}
        
        .truncate-header {{
            max-width: 25ch; overflow: hidden; text-overflow: ellipsis;
        }}

        .first-column {{
            width: 30ch !important;
            min-width: 30ch !important;
            max-width: 30ch !important;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}

        td.first-column {{
            color: #163f3f; font-weight: bold;
        }}
        
        table.dataTable tbody tr:nth-child(odd) td {{ background-color: #f5f5f5; }}
        table.dataTable tbody tr:nth-child(even) td {{ background-color: #e0e0e0; }}
        table.dataTable tbody td {{ white-space: nowrap; }}
        table.dataTable.dt-has-fixed-columns tbody tr > .dtfc-fixed-left {{ background-color: inherit; }}
        th[title]:hover, td[title]:hover {{ cursor: help; }}
        .total-row td {{ background-color: #FFD8A7 !important; color: #313131 !important; font-weight: bold; border-top: 2px solid #FFA500 !important; border-bottom: 2px solid #FFA500 !important; }}
    </style>

    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/fixedcolumns/4.3.0/js/dataTables.fixedColumns.min.js"></script>
    <script src="https://cdn.datatables.net/fixedheader/3.4.0/js/dataTables.fixedHeader.min.js"></script>


    <script>
        // Plugin de ordenação
        jQuery.extend(jQuery.fn.dataTable.ext.type.order, {{
            "br-numeric-pre": function (a) {{
                if (typeof a !== 'string') {{ return 0; }}
                let x = a.replace(/\\./g, "").replace(/,/, ".");
                return parseFloat(x) || 0;
            }}
        }});

        $(document).ready(function() {{
            var table = $('#summary').DataTable({{
                "columnDefs": [
                    {{ "type": "br-numeric", "targets": {numeric_col_indices} }},
                    {{ "className": "first-column", "targets": 0 }},
                    {{ "className": "truncate-header", "targets": "_all" }}
                ],
                "order": [[ {default_sort_col_index}, '{default_sort_order}' ]],
                "scrollX": true,
                "autoWidth": false,
                "fixedColumns": {{ "left": 1 }},
                "fixedHeader": true,
                "paging": false,
                "info": false,
                "searching": false,
                "initComplete": function(settings, json) {{
                    var tableWrapper = $('#summary_wrapper');
                    var scrollBody = tableWrapper.find('.dataTables_scrollBody');
                    var topScrollWrapper = $('<div class="top-scroll-wrapper"></div>');
                    var topScrollInner = $('<div class="top-scroll-inner"></div>');
                    topScrollWrapper.append(topScrollInner);
                    tableWrapper.prepend(topScrollWrapper);
                    function updateTopScrollWidth() {{
                        var scrollWidth = scrollBody.prop('scrollWidth');
                        topScrollInner.width(scrollWidth);
                    }}
                    topScrollWrapper.on('scroll', function() {{ scrollBody.scrollLeft($(this).scrollLeft()); }});
                    scrollBody.on('scroll', function() {{ topScrollWrapper.scrollLeft($(this).scrollLeft()); }});
                    $(window).on('resize', updateTopScrollWidth);
                    updateTopScrollWidth();
                }}
            }});

            table.on('draw.dt', function() {{
                $('#summary thead th').each(function() {{
                    var $this = $(this);
                    if (!$this.attr('title')) {{ $this.attr('title', $this.text()); }}
                }});
                $("#summary tbody td:contains('Total da Carteira')").parent('tr').addClass('total-row');
            }});
        }});
    </script>
    </head>
    <body>
        <div style="display:flex; align-items:center; margin-bottom:20px;">
            <img src="data:image/png;base64,{logo_base64}" alt="Logo" style="max-height:80px; margin-right:20px;">
            <h1>Relatório de Resumo de Entes — {ref_date.strftime('%d/%m/%Y')}</h1>
        </div>
        <div style="margin-bottom: 25px; padding: 10px; border: 1px solid #e0e0e0; background-color: #f9f9f9;">
            <h2 style="margin-top: 0; margin-bottom: 5px; color: #163f3f;">Fundo FCT Consig</h2>
            <p style="margin: 0; font-size: 1.1em;">
                <strong>Data do relatório:</strong> {data_relatorio_str} | <strong>Data de referência:</strong> {data_referencia_str}
            </p>
        </div>
        {html_table}
    </body>
    </html>"""

    data_hoje_str = datetime.now().strftime('%Y-%m-%d')
    data_ref_str = ref_date.strftime('%Y-%m-%d')

    # <<< ALTERADO >>> Usa o caminho base de resultados e remove a redefinição de 'ROOT'
    output_dir = Path(CAMINHO_RESULTADOS_BASE) / "resumo"
    output_dir.mkdir(parents=True, exist_ok=True)

    nome_arquivo = f"{data_hoje_str}_{data_ref_str}-resumo.html"
    output_file = output_dir / nome_arquivo

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\nRelatório HTML gerado em: {output_file}")


if __name__ == "__main__":
    main()