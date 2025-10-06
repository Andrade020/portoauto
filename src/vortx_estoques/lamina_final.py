# %%
# =============================================================================
# CÉLULA 1: BIBLIOTECAS E CONFIGURAÇÕES GERAIS
# =============================================================================
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os
import glob
from datetime import datetime
from docx import Document
from docx.shared import Cm, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx2pdf import convert
from IPython.display import display
from docx.oxml import OxmlElement
from docx.oxml.ns import qn


# --- PARÂMETROS DE ENTRADA E SAÍDA ---
# ! ATENÇÃO: Configure os caminhos e nomes de arquivos aqui.

# FUNDO PRINCIPAL PARA ANÁLISE
FUNDO_ALVO = 'FIDC FCT II SR2'

# CAMINHOS DOS DADOS DE ENTRADA
PATH_RENTABILIDADE = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\data\135972-Rentabilidade_Sintetica.csv"
PATH_ESTOQUE = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\data\estoque_consolidado_agosto"
PATH_TEMPLATE_DOCX = r"C:\Users\Leo\Desktop\Porto_Real\portoreal\notebooks\template.docx"
PATH_LOGO = r"C:\Users\Leo\Desktop\Porto_Real\portoreal\images\logo.png"

# CAMINHOS DE SAÍDA
PASTA_SAIDA_IMAGENS = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\output\lamina_imagens"
PASTA_SAIDA_RELATORIOS = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\output"

# --- CONFIGURAÇÕES VISUAIS ---
pd.options.display.max_rows = 100
pd.options.display.max_columns = 50

# PALETA DE CORES
COLOR_PALETTE = {
    'primary_light': '#76C6C5',
    'primary_dark': '#0E5D5F',
    'secondary': '#163F3F',
    'header_bg': '#F1F9F9',
    'header_text': '#0E5D5F'
}

# Cria as pastas de saída se não existirem
os.makedirs(PASTA_SAIDA_IMAGENS, exist_ok=True)
os.makedirs(PASTA_SAIDA_RELATORIOS, exist_ok=True)
print(f"Imagens serão salvas em: {PASTA_SAIDA_IMAGENS}")
print(f"Relatórios serão salvos em: {PASTA_SAIDA_RELATORIOS}")


# %%
# =============================================================================
# CÉLULA 2: FUNÇÕES DE PROCESSAMENTO DE DADOS
# =============================================================================

def carregar_dados_rentabilidade(caminho_arquivo):
    """Lê, limpa e pré-processa o arquivo de rentabilidade sintética."""
    print("Carregando dados de rentabilidade...")
    try:
        colunas = [
            'carteira', 'nomeFundo', 'tipoCarteira', 'administrador', 'periodo',
            'emissao', 'data', 'valorCota', 'quantidade', 'numeroCotistas',
            'variacaoDia', 'variacaoPeriodo', 'captacoes', 'resgate', 'eventos',
            'pl', 'coluna_extra'
        ]
        df = pd.read_csv(
            caminho_arquivo, sep=';', encoding='latin1', header=None, names=colunas, skiprows=1
        )
        df.columns = df.columns.str.strip()

        colunas_numericas = ['valorCota', 'pl', 'quantidade', 'numeroCotistas', 'variacaoDia']
        for col in colunas_numericas:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
        df.dropna(subset=['data', 'valorCota'], inplace=True)
        df['nomeFundo'] = df['nomeFundo'].str.strip()

        print("Dados de rentabilidade carregados e limpos com sucesso.")
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo de rentabilidade não encontrado em: {caminho_arquivo}")
        return pd.DataFrame()

def processar_dados_estoque(caminho_pasta):
    """Lê e consolida os arquivos de estoque de uma pasta."""
    print("\nCarregando dados de estoque...")
    arquivos_csv = glob.glob(os.path.join(caminho_pasta, "*.csv"))
    if not arquivos_csv:
        print(f"AVISO: Nenhum arquivo CSV de estoque encontrado em {caminho_pasta}")
        return pd.DataFrame()

    all_dfs = []
    float_cols = [
        'Valor Aquisicao', 'Valor Nominal', 'Valor Presente', 'PDD Vencido',
        'PDD Total', 'Taxa Operada Originador', 'CET Mensal', 'Taxa CCB',
        'Taxa Originador Split', 'Taxa Split FIDC'
    ]
    date_cols = ['Data Aquisicao', 'Data Vencimento', 'Data Referencia', 'Data de Nascimento']

    for file in arquivos_csv:
        try:
            df = pd.read_csv(file, encoding='utf-16', sep='\t', engine='python', on_bad_lines='warn', header=0)
            df.columns = df.columns.str.strip()

            if not df.empty:
                for col in float_cols:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace(',', '.').astype(float)
                for col in date_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                all_dfs.append(df)
        except Exception as e:
            print(f"Erro ao processar o arquivo de estoque {os.path.basename(file)}: {e}")
            continue

    if not all_dfs:
        print("Nenhum dado de estoque foi carregado com sucesso.")
        return pd.DataFrame()

    df_final = pd.concat(all_dfs, ignore_index=True)
    print("Dados de estoque consolidados com sucesso.")
    return df_final

def obter_dados_bacen(codigo_bcb, data_inicio='28/03/2017'):
    """Busca uma série temporal da API do Banco Central do Brasil."""
    try:
        url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_bcb}/dados?formato=json&dataInicial={data_inicio}'
        df = pd.read_json(url)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df.set_index('data', inplace=True)
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        print(f"Série {codigo_bcb} do Bacen carregada com sucesso!")
        return df
    except Exception as e:
        print(f"ERRO ao buscar dados do Bacen (Série {codigo_bcb}): {e}")
        return pd.DataFrame()

def preparar_df_retornos(df_rentabilidade, df_cota_com_cdi):
    """Cria o DataFrame de retornos diários a partir da 'variacaoDia' e adiciona o CDI."""
    df_retornos = df_rentabilidade.pivot_table(
        index='data',
        columns='nomeFundo',
        values='variacaoDia'
    ) / 100
    if 'CDI' in df_cota_com_cdi.index:
        df_retornos['CDI'] = df_cota_com_cdi.loc['CDI'].pct_change()
    return df_retornos


# %%
# =============================================================================
# CÉLULA 3: FUNÇÕES DE GERAÇÃO DE GRÁFICOS E TABELAS
# =============================================================================

def _render_mpl_table(data, ax, col_width=1.5, row_height=0.625, font_size=10):
    """Função auxiliar para renderizar uma tabela Matplotlib com estilo."""
    header_color = COLOR_PALETTE['header_bg']
    header_font_color = COLOR_PALETTE['header_text']
    edge_color = 'w'
    row_colors = ['#FFFFFF', '#FFFFFF']
    
    ax.axis('off')
    mpl_table = ax.table(cellText=data.values, cellLoc='center', bbox=[0, 0, 1, 1],
                         colLabels=data.columns, loc='center', colLoc='center')
    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in mpl_table._cells.items():
        cell.set_edgecolor(edge_color)
        cell.set_linewidth(0)
        if k[0] == 0:  # Linha de cabeçalho
            cell.set_text_props(weight='bold', color=header_font_color)
            cell.set_facecolor(header_color)
        else:
            if (k[0] - 1) % 3 == 2:
                 cell.set_facecolor('#F0F0F0') # Linha % CDI
            else:
                 cell.set_facecolor(row_colors[k[0] % len(row_colors)])

        if k[1] <= 0:  # Colunas de cabeçalho (Ano, Cota)
            cell.set_text_props(weight='bold', color=header_font_color)
            cell.set_facecolor(header_color)
    return ax

def criar_tabela_performance(df_rentabilidade_processada, fundos_a_exibir, df_cota_base, benchmark='CDI'):
    """
    Gera a tabela de performance calculando a rentabilidade mensal a partir da coluna 'variacaoDia',
    agrupada por ano e com estética aprimorada.
    """
    # --- ETAPA 1: CALCULAR PERFORMANCE MENSAL A PARTIR DA 'variacaoDia' ---
    df_calc = df_rentabilidade_processada[['data', 'nomeFundo', 'variacaoDia']].copy()
    df_calc['mes'] = df_calc['data'].dt.to_period('M')
    
    # Calcula o fator diário usando a coluna oficial, exatamente como no seu script de exemplo
    df_calc['fator_diario'] = 1 + (df_calc['variacaoDia'] / 100)
    
    # Agrupa por fundo e mês, e calcula o produto dos fatores diários (rentabilidade geométrica)
    fator_mensal = df_calc.groupby(['nomeFundo', 'mes'])['fator_diario'].prod()
    perf_mensal_fundos = (fator_mensal - 1).unstack('nomeFundo').T

    # Calcula a performance mensal para o benchmark (CDI)
    retornos_cdi = df_cota_base.loc[benchmark].pct_change().dropna()
    perf_mensal_cdi = (retornos_cdi + 1).resample('M').prod() - 1
    
    # --- ETAPA 2: MONTAR A ESTRUTURA DA TABELA ---
    all_rows = []
    years = sorted(df_calc['data'].dt.year.unique())

    def format_pct(x):
        return f'{x:.2%}'.replace('.', ',') if isinstance(x, (int, float)) and pd.notna(x) else '-'
    def format_pct_of_bench(x):
        return f'{x*100:.1f}%'.replace('.', ',') if isinstance(x, (int, float)) and pd.notna(x) else '-'

    for year in years:
        for nome_fundo, nome_exibicao in fundos_a_exibir.items():
            if nome_fundo not in perf_mensal_fundos.index:
                continue

            row_fundo = [year, nome_exibicao]
            row_pct_bench = [year, f"% {benchmark}"]
            
            # Rentabilidade YTD (Ano)
            start_of_year = pd.Timestamp(f'{year}-01-01')
            ret_fundo_ano = (perf_mensal_fundos.loc[nome_fundo][(perf_mensal_fundos.columns.year == year)] + 1).prod() - 1
            ret_bench_ano = (perf_mensal_cdi[perf_mensal_cdi.index.year == year] + 1).prod() - 1
            
            # Rentabilidade Acumulada (Desde o Início)
            start_date = df_cota_base.loc[nome_fundo].dropna().index[0]
            ret_fundo_inicio = (perf_mensal_fundos.loc[nome_fundo][perf_mensal_fundos.columns.to_timestamp() >= start_date] + 1).prod() - 1
            ret_bench_inicio = (perf_mensal_cdi[perf_mensal_cdi.index >= start_date] + 1).prod() - 1

            for month in range(1, 13):
                mes_period = pd.Period(f'{year}-{month}', freq='M')
                if mes_period in perf_mensal_fundos.columns:
                    ret_fundo = perf_mensal_fundos.loc[nome_fundo, mes_period]
                    ret_bench = perf_mensal_cdi.get(mes_period.to_timestamp('M'), pd.NA)
                    pct_cdi = ret_fundo / ret_bench if pd.notna(ret_fundo) and pd.notna(ret_bench) and ret_bench != 0 else '-'
                    row_fundo.append(format_pct(ret_fundo))
                    row_pct_bench.append(format_pct_of_bench(pct_cdi))
                else:
                    row_fundo.append('-')
                    row_pct_bench.append('-')

            pct_cdi_ano = ret_fundo_ano / ret_bench_ano if ret_bench_ano != 0 else '-'
            pct_cdi_inicio = ret_fundo_inicio / ret_bench_inicio if ret_bench_inicio != 0 else '-'
            
            row_fundo.extend([format_pct(ret_fundo_ano), format_pct(ret_fundo_inicio)])
            row_pct_bench.extend([format_pct_of_bench(pct_cdi_ano), format_pct_of_bench(pct_cdi_inicio)])
            all_rows.extend([row_fundo, row_pct_bench])

    # --- ETAPA 3: RENDERIZAR E SALVAR A TABELA (sem alterações na estética) ---
    if not all_rows: return None
    
    table_cols = ['Ano', 'Cota', 'Jan', 'Fev', 'Mar', 'Abr', 'Maio', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'YTD', 'Desde o\nInício']
    df_table = pd.DataFrame(all_rows, columns=table_cols)
    
    num_rows_per_year = len(fundos_a_exibir) * 2
    anos_formatados = [row['Ano'] if i % num_rows_per_year == 0 else '' for i, row in df_table.iterrows()]
    df_table['Ano'] = anos_formatados

    def render_mpl_table_final(data, ax, font_size=8.5):
        # (Esta função interna de renderização permanece a mesma da versão anterior)
        header_color = COLOR_PALETTE['header_bg']; header_font_color = COLOR_PALETTE['header_text']
        font_color = '#4F4F4F'; font_name = 'Gill Sans MT'
        ax.axis('off')
        mpl_table = ax.table(cellText=data.values, cellLoc='center', bbox=[0, 0, 1, 1], colLabels=data.columns, loc='center', colLoc='center')
        mpl_table.auto_set_font_size(False); mpl_table.set_fontsize(font_size)
        for k, cell in mpl_table._cells.items():
            cell.set_edgecolor('w'); cell.set_linewidth(0)
            props = {'fontfamily': font_name}
            if k[0] == 0:
                props.update({'weight': 'bold', 'color': header_font_color}); cell.set_facecolor(header_color)
            else:
                props.update({'color': font_color})
                if k[0] % 2 == 0: cell.set_facecolor('#F0F0F0')
                else: cell.set_facecolor('#FFFFFF')
            if k[1] <= 0:
                props.update({'weight': 'bold', 'color': header_font_color})
                if k[0] != 0: cell.set_facecolor(header_color)
            cell.set_text_props(**props)
        return ax

    fig, ax = plt.subplots(figsize=(12, len(all_rows) * 0.35))
    render_mpl_table_final(df_table, ax)
    
    path_tabela = os.path.join(PASTA_SAIDA_IMAGENS, f"tabela_rentabilidade_{list(fundos_a_exibir.keys())[0].replace(' ', '_')}.png")
    fig.savefig(path_tabela, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    print(f"Tabela de rentabilidade (cálculo via variacaoDia) salva em: {path_tabela}")
    return path_tabela

def plotar_graficos_rentabilidade(df_cota, df_patr, fundos_a_exibir, benchmark='CDI'):
    """
    Cria e salva todos os gráficos de performance, incluindo um
    gráfico de PL empilhado para todos os fundos.
    """
    # Esta primeira parte da função (gráficos de retorno, vol e dd) permanece a mesma.
    fundo_alvo = list(fundos_a_exibir.keys())[0] # Usa o primeiro fundo como principal
    nome_exib_alvo = list(fundos_a_exibir.values())[0]
    nome_arquivo_limpo = fundo_alvo.replace(' ', '_')

    # 1. Gráfico de Retorno Acumulado
    fig, ax = plt.subplots(figsize=(12, 6))
    df_fund_evol = df_cota.loc[fundo_alvo].dropna()
    start = df_fund_evol.index[0]
    df_fund_evol_norm = df_fund_evol / df_fund_evol.iloc[0] * 100
    ax.plot(df_fund_evol_norm, color=COLOR_PALETTE['primary_light'], lw=2.5, label=nome_exib_alvo)
    ax.text(df_fund_evol_norm.index[-1], df_fund_evol_norm.iloc[-1], f' {df_fund_evol_norm.iloc[-1]:.2f}', color=COLOR_PALETTE['primary_light'], fontsize=12, va='center')
    df_bench_evol = df_cota.loc[benchmark][df_cota.loc[benchmark].index >= start].dropna()
    df_bench_evol_norm = df_bench_evol / df_bench_evol.iloc[0] * 100
    ax.plot(df_bench_evol_norm, color='k', linestyle='--', alpha=0.7, lw=2, label=benchmark)
    ax.text(df_bench_evol_norm.index[-1], df_bench_evol_norm.iloc[-1], f' {df_bench_evol_norm.iloc[-1]:.2f}', color='k', alpha=0.8, fontsize=12, va='center')
    ax.spines[['top', 'right']].set_visible(False)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.0f'))
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    ax.set_title(f'Retorno Acumulado - {nome_exib_alvo}', fontsize=16, pad=20)
    ax.legend(loc='upper left', frameon=False, fontsize=12)
    plt.tight_layout()
    path_retorno = os.path.join(PASTA_SAIDA_IMAGENS, f"grafico_retorno_{nome_arquivo_limpo}.png")
    fig.savefig(path_retorno, dpi=300, bbox_inches='tight'); plt.close(fig)
    print(f"Gráfico de retorno salvo em: {path_retorno}")

    # 2. Gráfico de Volatilidade
    # (Esta seção não precisa de alterações)
    fig, ax = plt.subplots(figsize=(12, 6))
    window=22
    ret_fundo = df_cota.loc[fundo_alvo].pct_change(); vol_fundo = ret_fundo.rolling(window=window).std() * np.sqrt(252)
    ax.plot(vol_fundo, color=COLOR_PALETTE['primary_light'], lw=2, label=nome_exib_alvo)
    ret_bench = df_cota.loc[benchmark].pct_change(); vol_bench = ret_bench.rolling(window=window).std() * np.sqrt(252)
    ax.plot(vol_bench, color='k', linestyle='--', alpha=0.7, lw=2, label=benchmark)
    ax.spines[['top', 'right']].set_visible(False); ax.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    ax.set_title(f'Volatilidade Anualizada (Janela Móvel de {window}d) - {nome_exib_alvo}', fontsize=16, pad=20)
    ax.legend(loc='upper left', frameon=False, fontsize=12); plt.tight_layout()
    path_vol = os.path.join(PASTA_SAIDA_IMAGENS, f"grafico_volatilidade_{nome_arquivo_limpo}.png")
    fig.savefig(path_vol, dpi=300, bbox_inches='tight'); plt.close(fig)
    print(f"Gráfico de volatilidade salvo em: {path_vol}")

    # 3. Gráfico de Drawdown
    # (Esta seção não precisa de alterações)
    fig, ax = plt.subplots(figsize=(12, 6))
    df_fund_dd = df_cota.loc[fundo_alvo].dropna()
    daily_dd = df_fund_dd / df_fund_dd.cummax() - 1.0
    ax.plot(daily_dd, color=COLOR_PALETTE['primary_light'], lw=1)
    ax.fill_between(daily_dd.index, daily_dd, 0, color=COLOR_PALETTE['primary_light'], alpha=0.3)
    ax.spines[['top', 'right']].set_visible(False); ax.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    ax.set_title(f'Drawdown Histórico - {nome_exib_alvo}', fontsize=16, pad=20); plt.tight_layout()
    path_dd = os.path.join(PASTA_SAIDA_IMAGENS, f"grafico_drawdown_{nome_arquivo_limpo}.png")
    fig.savefig(path_dd, dpi=300, bbox_inches='tight'); plt.close(fig)
    print(f"Gráfico de drawdown salvo em: {path_dd}")

    # --- 4. NOVO GRÁFICO DE EVOLUÇÃO DO PL (EMPILHADO) ---
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Prepara os dados para o gráfico empilhado
    fundos_originais = list(fundos_a_exibir.keys())
    nomes_exibicao = list(fundos_a_exibir.values())
    
    df_plot_pl = df_patr.loc[fundos_originais].T.dropna(how='all').fillna(0)
    # Garante que a ordem de empilhamento seja Subordinada na base e Sênior no topo
    df_plot_pl = df_plot_pl[['FIDC FCT II', 'FIDC FCT II SR2']]

    # Cores para cada camada do gráfico
    cores_pl = [COLOR_PALETTE['primary_dark'], COLOR_PALETTE['primary_light']]
    
    # Plota o gráfico de área empilhada
    ax.stackplot(df_plot_pl.index, df_plot_pl.T.values,
                 labels=['Subordinada', 'Sênior'], # Garante a ordem correta na legenda
                 colors=cores_pl, alpha=0.8)

    ax.spines[['top', 'right']].set_visible(False)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'R$ {x/1e6:.1f}M'))
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    ax.set_title(f'Evolução do Patrimônio Líquido (PL)', fontsize=16, pad=20)
    ax.legend(loc='upper left', frameon=False, fontsize=12)

    # Adiciona um rótulo com o PL Total no final do gráfico
    pl_total_final = df_plot_pl.iloc[-1].sum()
    ax.text(df_plot_pl.index[-1], pl_total_final, f' Total R$ {pl_total_final/1e6:.2f}M',
            color=COLOR_PALETTE['secondary'], fontsize=12, va='center', ha='left')
    
    plt.tight_layout()
    path_pl = os.path.join(PASTA_SAIDA_IMAGENS, f"grafico_pl_{nome_arquivo_limpo}.png")
    fig.savefig(path_pl, dpi=300, bbox_inches='tight'); plt.close(fig)
    print(f"Gráfico de PL empilhado salvo em: {path_pl}")
    
    return {
        'retorno': path_retorno, 'volatilidade': path_vol,
        'drawdown': path_dd, 'pl': path_pl
    }

def plotar_graficos_estoque(df_estoque):
    """Cria e salva todos os gráficos relacionados ao estoque."""
    if df_estoque.empty:
        print("DataFrame de estoque vazio. Pulando geração de gráficos de estoque.")
        return {}
    
    print("\n--- Gerando imagens de Estoque ---")
    df_avencer = df_estoque[df_estoque['Status'] == 'A vencer'].copy()
    paths = {}
    
    # Gerar e salvar cada gráfico de estoque
    paths['venc_mensal'] = plotar_vencimento_mensal(df_avencer, col_valor='Valor Presente', col_vcto='VCTO_MES')
    paths['venc_anual'] = plotar_vencimento_anual(df_avencer, col_valor='Valor Presente', col_vcto='VCTO_ANO')
    paths['conc_uf'] = plotar_concentracao_uf(df_avencer, col_valor='Valor Presente', col_class='UF')
    paths['dist_capag'] = plotar_distribuicao_capag(df_estoque, class_col='CAPAG', col_valor='Valor Presente')
    paths['aging_vencidos'] = plotar_aging_com_acumulado(df_estoque, col_valor='Valor Presente')
    paths['conc_cumulativa_sacado'] = plotar_concentracao_cumulativa_sacado(df_avencer, col_valor='Valor Presente', col_class='CPF Cliente')
    print("\nGráficos de estoque gerados com sucesso.")
    return paths

def plotar_vencimento_mensal(df_aberto, col_valor, col_vcto, cutoff=10):
    """Gera e salva o gráfico de vencimento mensal com a estética original."""
    df_aberto = df_aberto.copy()
    df_aberto[col_vcto] = df_aberto['Data Vencimento'].dt.strftime('%Y-%m')
    df2 = df_aberto.groupby(col_vcto)[col_valor].sum()
    df3 = (df2 / df2.sum()).cumsum()
    df2 = df2[:cutoff]
    df3 = df3[:cutoff]

    if df2.empty:
        print("Não há dados para gerar o gráfico de vencimento mensal.")
        return None

    # Restaura a lógica original de limites dinâmicos para espaçamento visual
    ymin1 = 0
    ymax1 = df2.max() * 2
    ymax2 = 1.3
    ymin2 = df3.min() * 2.5 - 1.5 * ymax2

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax2 = ax.twinx()

    # Restaura o np.clip para garantir que barras pequenas sejam visíveis
    bars = ax.bar(df2.index, np.clip(df2.values, 0.01 * df2.max(), df2.max()), color=COLOR_PALETTE['primary_dark'])
    ax2.plot(df3.index, df3.values, color=COLOR_PALETTE['secondary'], lw=2, marker='o')

    # Lógica de formatação dos eixos e rótulos do script original
    month_map = {'01':'jan','02':'fev','03':'mar','04':'abr','05':'mai','06':'jun','07':'jul','08':'ago','09':'set','10':'out','11':'nov','12':'dez'}
    ax.set_xticks(df2.index)
    ax.set_xticklabels([f"{month_map[x[-2:]]}/{x[2:4]}" for x in df2.index])
    ax.set_ylim(ymin1, ymax1)
    ax2.set_ylim(ymin2, ymax2)
    ax.set_yticks([])
    ax2.set_yticks([])
    ax.tick_params(bottom=False)
    ax.set_frame_on(False)
    ax2.spines[:].set_visible(False)

    max_height = max([bar.get_height() for bar in bars])
    for i, bar in enumerate(bars):
        h = bar.get_height() + max_height * 0.04
        value = df2.values[i] / 1000
        ax.text(bar.get_x() + bar.get_width()/2, h, f'{value:,.0f}'.replace(',', '.'), ha='center')

    for x, y in df3.items():
        value = f'{y:.0%}' if ((y < .990) or (y == 1)) else '>99%'
        ax2.text(x, y + (ymax2 - ymin2) * 0.04, value, ha='center')

    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "vencimento_mensal.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de vencimento mensal (estilo corrigido) salvo em: {path_saida}")
    return path_saida

def plotar_vencimento_anual(df_aberto, col_valor, col_vcto):
    """Gera e salva o gráfico de vencimento anual com a estética original."""
    df_aberto = df_aberto.copy()
    df_aberto[col_vcto] = df_aberto['Data Vencimento'].dt.year
    df2 = df_aberto.groupby(col_vcto)[col_valor].sum()
    df3 = df2.cumsum() / df2.sum()

    if df2.empty:
        print("Não há dados para gerar o gráfico de vencimento anual.")
        return None

    # Restaura a lógica original de limites dinâmicos para espaçamento visual
    ymin1 = 0
    ymax1 = df2.max() * 2
    ymax2 = 1.15
    ymin2 = df3.min() * 2.5 - 1.5 * ymax2

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax2 = ax.twinx()
    
    # Restaura o np.clip para garantir que barras pequenas sejam visíveis
    bars = ax.bar(df2.index, np.clip(df2.values, 0.01 * df2.max(), df2.max()), color=COLOR_PALETTE['primary_dark'])
    ax2.plot(df3.index, df3.values, color=COLOR_PALETTE['secondary'], lw=2, marker='o')
    
    # Lógica de formatação dos eixos e rótulos do script original
    ax.set_xticks(df2.index.tolist())
    ax.set_ylim(ymin1, ymax1)
    ax2.set_ylim(ymin2, ymax2)
    ax.set_yticks([])
    ax2.set_yticks([])
    ax.tick_params(bottom=False)
    ax.set_frame_on(False)
    ax2.spines[:].set_visible(False)

    max_height = max([bar.get_height() for bar in bars])
    for i, bar in enumerate(bars):
        h = bar.get_height() + max_height * 0.04
        value = df2.values[i] / 1000
        ax.text(bar.get_x() + bar.get_width()/2, h, f'{value:,.0f}'.replace(',', '.'), ha='center')

    for x, y in df3.items():
        value = f'{y:.0%}' if ((y < .990) or np.isclose(y, 1)) else '>99%'
        ax2.text(x, y + 0.06, value, ha='center')

    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "vencimento_anual.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de vencimento anual (estilo corrigido) salvo em: {path_saida}")
    return path_saida

def plotar_concentracao_uf(df_aberto, col_valor, col_class='UF', cutoff=15):
    """Gera e salva o gráfico de concentração por UF."""
    df11 = df_aberto.groupby(col_class)[col_valor].sum().sort_values(ascending=False)
    ncut = min(len(df11), cutoff)
    
    if ncut >= cutoff:
        x = df11[:cutoff].index.tolist() + ['Outros']
        y = list(df11[:cutoff].values) + [df11.iloc[cutoff:].sum()]
    else:
        x = df11.index.tolist()
        y = list(df11.values)
        
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.barh(x, y, color=COLOR_PALETTE['primary_dark'])
    plt.gca().invert_yaxis()
    ax.set_xticks([])
    ax.tick_params(left=False, bottom=False)
    ax.set_frame_on(False)

    for i, bar in enumerate(bars):
        value = y[i] / 1000
        ax.text(bar.get_width() * 1.01, bar.get_y() + bar.get_height()/2, f'{value:,.0f}'.replace(',', '.'), va='center', ha='left')

    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "concentracao_uf.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de concentração por UF salvo em: {path_saida}")
    return path_saida

def plotar_distribuicao_capag(df_in, class_col, col_valor):
    """Gera e salva o gráfico de distribuição por CAPAG."""
    df_capag = df_in.dropna(subset=[class_col]).groupby(class_col)[col_valor].sum()
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(df_capag.index, df_capag.values, color=COLOR_PALETTE['primary_dark'])

    ax.set_xticks(df_capag.index.tolist())
    ax.set_yticks([])
    ax.tick_params(bottom=False)
    ax.set_frame_on(False)

    for i, bar in enumerate(bars):
        value = df_capag.values[i] / 1000
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.05, f'{value:,.0f}'.replace(',', '.'), ha='center')

    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "distribuicao_capag.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de distribuição por CAPAG salvo em: {path_saida}")
    return path_saida

def plotar_concentracao_cumulativa_sacado(df_aberto, col_valor='Valor Presente', col_class='CPF Cliente'):
    """
    Gera um gráfico de barras horizontal mostrando a concentração CUMULATIVA de valor
    em faixas de devedores (Top 10, Top 20, Top 50, etc.), como na imagem de exemplo.
    """
    df_agrupado = df_aberto.groupby(col_class)[col_valor].sum().sort_values(ascending=False)
    
    if df_agrupado.empty:
        print(f"Não há dados para gerar o gráfico de concentração cumulativa por '{col_class}'.")
        return None

    valor_total = df_agrupado.sum()
    num_sacados = len(df_agrupado)

    # Define as faixas cumulativas, como na imagem
    bins = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
    
    resultados = {}
    # Calcula o percentual para cada faixa
    for n in bins:
        if n < num_sacados:
            soma_top_n = df_agrupado.iloc[0:n].sum()
            pct_top_n = (soma_top_n / valor_total) * 100
            resultados[f'{n} maiores'] = pct_top_n
            
    # Adiciona a linha final com o total de sacados
    resultados[f'{num_sacados} maiores'] = 100.0
    
    df_plot = pd.Series(resultados)
    
    # --- PLOTAGEM ---
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.barh(df_plot.index, df_plot.values, color=COLOR_PALETTE['primary_dark'])
    
    # Inverte o eixo para ter o '10 maiores' no topo
    ax.invert_yaxis()
    
    # Formatação dos eixos
    ax.set_xticks([])
    ax.set_frame_on(False)
    ax.tick_params(left=False) # Remove os tracinhos do eixo Y
    
    # Adiciona os rótulos de percentual no final das barras
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 1, bar.get_y() + bar.get_height()/2, f'{width:.0f}%', 
                va='center', ha='left', fontsize=9)

    ax.set_title("Concentração Cumulativa por Sacado", fontsize=12)
    
    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "concentracao_cumulativa_sacado.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de concentração cumulativa por sacado salvo em: {path_saida}")
    return path_saida


def plotar_aging_com_acumulado(df_estoque, col_valor='Valor Presente'):
    """
    Gera um gráfico de Aging aprimorado, com mais faixas de atraso e
    rótulos de dados na linha de percentual acumulado.
    """
    df_vencidos = df_estoque[df_estoque['Status'] == 'Vencido'].copy()

    if df_vencidos.empty:
        print("Não há dados de carteira vencida para gerar o gráfico de Aging.")
        return None

    hoje = datetime.now()
    df_vencidos['dias_atraso'] = (hoje - df_vencidos['Data Vencimento']).dt.days

    # 1. MELHORIA: Mais faixas de atraso para uma análise mais detalhada
    bins = [-1, 14, 30, 60, 90, 120, 150, 180, np.inf]
    labels = ['0-14 D', '15-30 D', '31-60 D', '61-90 D', '91-120 D', '121-150 D', '151-180 D', '180+ D']
    df_vencidos['Faixa Atraso'] = pd.cut(df_vencidos['dias_atraso'], bins=bins, labels=labels, right=True)
    
    df_aging = df_vencidos.groupby('Faixa Atraso', observed=False)[col_valor].sum()
    total_em_atraso = df_aging.sum()

    df_aging_pct_acum = df_aging.cumsum() / total_em_atraso
    df_aging_pct_acum['Total'] = 1.0
    df_aging['Total'] = total_em_atraso
    
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax2 = ax.twinx()

    bars = ax.bar(df_aging.index, df_aging.values, color=COLOR_PALETTE['primary_dark'], width=0.6)
    bars[-1].set_color(COLOR_PALETTE['secondary'])
    
    line = ax2.plot(df_aging_pct_acum.index, df_aging_pct_acum.values, color=COLOR_PALETTE['secondary'], marker='o', lw=2)

    ax.set_yticks([]); ax.set_frame_on(False)
    ax.set_ylim(0, total_em_atraso * 1.35) # Aumenta o espaço para o rótulo da linha
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height, f'R$ {height/1e6:.2f}M'.replace('.',','),
                ha='center', va='bottom', fontsize=9, color='#333333')

    ax2.set_ylim(0, 1.15)
    ax2.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
    ax2.spines[:].set_visible(False)
    
    # 2. MELHORIA: Adiciona os rótulos na linha de percentual acumulado
    for i, pct in enumerate(df_aging_pct_acum):
        if i < len(df_aging_pct_acum) - 1: # Não adiciona rótulo no ponto "Total"
             ax2.text(i, pct + 0.05, f'{pct:.0%}', ha='center', fontsize=9, color=COLOR_PALETTE['secondary'])
    
    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "aging_com_acumulado.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de Aging (versão aprimorada) salvo em: {path_saida}")
    return path_saida
# %%
# =============================================================================
# CÉLULA 4: FUNÇÕES DE MONTAGEM DO RELATÓRIO (DOCX)
# =============================================================================

### FUNÇÃO AUXILIAR ADICIONADA ###
def adicionar_linha_abaixo_paragrafo(paragraph, color="76C6C5", size="4", space="1"):
    """
    Adiciona uma borda na parte inferior de um parágrafo.
    """
    pPr = paragraph._p.get_or_add_pPr()
    pBdr = OxmlElement('w:pBdr')
    borda_inferior = OxmlElement('w:bottom')
    borda_inferior.set(qn('w:val'), 'single')
    borda_inferior.set(qn('w:sz'), size)
    borda_inferior.set(qn('w:space'), space)
    borda_inferior.set(qn('w:color'), color)
    pBdr.append(borda_inferior)
    pPr.append(pBdr)


def configurar_cabecalho_rodape(doc, nome_relatorio, data_referencia, path_logo):
    """Configura o cabeçalho e as margens do documento."""
    section = doc.sections[0]
    section.top_margin, section.bottom_margin = Cm(1), Cm(1)
    section.left_margin, section.right_margin = Cm(1.6), Cm(1)
    
    header = section.header
    header_table = header.tables[0] if header.tables else header.add_table(rows=1, cols=2, width=Cm(18))
    
    cell_left = header_table.cell(0, 0)
    p_left = cell_left.paragraphs[0]
    p_left.text = ""
    run_name = p_left.add_run(nome_relatorio.upper())
    run_name.font.name = "Gill Sans MT"
    run_name.font.size, run_name.bold = Pt(14), True
    run_name.font.color.rgb = RGBColor(16, 112, 130)
    
    p_left.add_run("\n")
    meses_pt = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    data_formatada = f"{meses_pt[data_referencia.month - 1]} {data_referencia.year}"
    run_month = p_left.add_run(data_formatada)
    run_month.font.name, run_month.font.size, run_month.italic = "Arial", Pt(12), True
    run_month.font.color.rgb = RGBColor(38, 38, 38)
    
    cell_right = header_table.cell(0, 1)
    p_right = cell_right.paragraphs[0]
    p_right.text = ""
    p_right.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p_right.add_run().add_picture(path_logo, width=Cm(4.93))

def montar_corpo_relatorio(doc, paths_imagens):
    """Adiciona a tabela e os gráficos ao corpo do documento."""
    
    ### FUNÇÃO INTERNA MODIFICADA ###
    def add_section_title(cell, text):
        p = cell.add_paragraph()
        p.paragraph_format.space_before, p.paragraph_format.space_after = Pt(6), Pt(2)
        run = p.add_run(text)
        run.font.name, run.font.size, run.bold = "Gill Sans MT", Pt(10), True
        run.font.color.rgb = RGBColor(89, 89, 89)
        # --- Chama a função para adicionar a linha ---
        adicionar_linha_abaixo_paragrafo(p, color="76C6C5", space="2")

    # Tabela de Rentabilidade
    title_p = doc.add_paragraph()
    title_p.paragraph_format.space_before = Pt(12)
    run = title_p.add_run("\tRentabilidade Mensal")
    run.font.name, run.font.size, run.bold = "Gill Sans MT", Pt(11), True
    
    ### LINHA ADICIONADA AQUI ###
    # --- Adiciona a linha para o título principal da rentabilidade ---
    adicionar_linha_abaixo_paragrafo(title_p, color="76C6C5", space="4")
    
    doc.add_picture(paths_imagens['tabela_perf'], width=Cm(17.8))

    # Tabela de Gráficos
    graphs_table = doc.add_table(rows=5, cols=2)
    graphs_table.autofit = False
    for col in graphs_table.columns:
        col.width = Cm(8.9)
    img_width = Cm(8.5)

    # Mapeamento de células e títulos
    mapa_graficos = {
        (0, 0): ("Retorno Acumulado", paths_imagens.get('retorno')),
        (0, 1): ("Evolução do Patrimônio Líquido (PL)", paths_imagens.get('pl')),
        (1, 0): ("Volatilidade Anualizada", paths_imagens.get('volatilidade')),
        (1, 1): ("Drawdown Histórico", paths_imagens.get('drawdown')),
        (2, 0): ("Vencimento Mensal", paths_imagens.get('venc_mensal')),
        (2, 1): ("Vencimento Anual", paths_imagens.get('venc_anual')),
        (3, 0): ("Concentração por UF", paths_imagens.get('conc_uf')),
        (3, 1): ("Distribuição por CAPAG", paths_imagens.get('dist_capag')),
        (4, 0): ("Concentração Cumulativa por Sacado", paths_imagens.get('conc_cumulativa_sacado')),
        (4, 1): ("Aging da Carteira Vencida", paths_imagens.get('aging_vencidos')),
    }
    
    for (row, col), (titulo, path) in mapa_graficos.items():
        if path and os.path.exists(path):
            cell = graphs_table.cell(row, col)
            # A função add_section_title agora cuida de tudo: cria o texto E a linha
            add_section_title(cell, titulo)
            cell.add_paragraph().add_run().add_picture(path, width=img_width)
# %%
# =============================================================================
# CÉLULA 5: ORQUESTRADOR PRINCIPAL - EXECUÇÃO DO FLUXO
# =============================================================================

def gerar_relatorio_completo():
    """Função principal que orquestra todo o processo."""
    
    # --- ETAPA 1: PROCESSAMENTO DE DADOS DE RENTABILIDADE ---
    df_rentabilidade = carregar_dados_rentabilidade(PATH_RENTABILIDADE)
    if df_rentabilidade.empty:
        print("Processo interrompido: falha ao carregar dados de rentabilidade.")
        return

    df_cota = df_rentabilidade.pivot_table(index='data', columns='nomeFundo', values='valorCota').ffill().T
    df_patr = df_rentabilidade.pivot_table(index='data', columns='nomeFundo', values='pl').ffill().T
    
    df_cdi = obter_dados_bacen(12)
    if not df_cdi.empty:
        df_cota.loc['CDI'] = (1 + df_cdi['valor'] / 100).cumprod().ffill()
    
    df_retornos = preparar_df_retornos(df_rentabilidade, df_cota)
    
    # --- ETAPA 2: GERAÇÃO DAS IMAGENS ---
    print("\n--- Gerando imagens de Rentabilidade ---")
    paths_imagens = {}
    # Define os fundos a serem exibidos na tabela e seus novos nomes
    fundos_para_tabela = {
        'FIDC FCT II SR2': 'Sênior',
        'FIDC FCT II': 'Subordinada'
    }
    # Passa o DataFrame original processado para a função, que agora faz o cálculo internamente
    paths_imagens['tabela_perf'] = criar_tabela_performance(df_rentabilidade, fundos_para_tabela, df_cota)
    paths_rentabilidade = plotar_graficos_rentabilidade(df_cota, df_patr, fundos_para_tabela)
    paths_imagens.update(paths_rentabilidade)
    
    df_estoque = processar_dados_estoque(PATH_ESTOQUE)
    paths_estoque = plotar_graficos_estoque(df_estoque)
    paths_imagens.update(paths_estoque)
    
    # --- ETAPA 3: MONTAGEM E SALVAMENTO DO RELATÓRIO ---
    print("\n--- Montando relatório ---")
    ref_date = datetime.now()
    doc = Document(PATH_TEMPLATE_DOCX)
    
    configurar_cabecalho_rodape(doc, 'Lâmina de Performance e Estoque', ref_date, PATH_LOGO)
    montar_corpo_relatorio(doc, paths_imagens)
    
    nome_arquivo = f"Lamina_Completa_{ref_date.strftime('%Y-%m-%d')}.docx"
    path_docx = os.path.join(PASTA_SAIDA_RELATORIOS, nome_arquivo)
    
    try:
        doc.save(path_docx)
        print(f"Relatório DOCX gerado com sucesso em: {path_docx}")
        
        print("Iniciando conversão para PDF...")
        path_pdf = path_docx.replace(".docx", ".pdf")
        convert(path_docx, path_pdf)
        print(f"Relatório PDF gerado com sucesso em: {path_pdf}")
    except Exception as e:
        print(f"Ocorreu um erro ao salvar o relatório: {e}")

# --- Ponto de Entrada para Execução do Script ---
if __name__ == "__main__":
    gerar_relatorio_completo()