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

from dateutil.parser import parse

import matplotlib.dates as mdates
import locale

PATH_RENTABILIDADE = r"C:\Users\LucasRafaeldeAndrade\Desktop\portoauto\src\vortx_estoques\data\Rentabilidades\154168-Rentabilidade_Sintetica.csv"
PATH_ESTOQUE = r"C:\Users\LucasRafaeldeAndrade\Desktop\portoauto\src\vortx_estoques\data\Estoques_Consolidados\2025-10-14"


# =============================================================================
# CONFIGURAÇÕES GLOBAIS DE TAMANHO E ESTILO
# =============================================================================

# Conversão: 4.77 cm x 8.31 cm para polegadas (1 polegada = 2.54 cm)
FIG_WIDTH_CM = 8.31
FIG_HEIGHT_CM = 4.77
FIG_WIDTH_INCHES = FIG_WIDTH_CM / 2.54  # ≈ 3.27 polegadas
FIG_HEIGHT_INCHES = FIG_HEIGHT_CM / 2.54  # ≈ 1.88 polegadas
FIGSIZE_PADRAO = (FIG_WIDTH_INCHES, FIG_HEIGHT_INCHES)
DPI_PADRAO = 300

# Tamanhos de fonte padronizados (ajustados para o tamanho pequeno)
FONTSIZE_TITLE = 7
FONTSIZE_LABEL = 6
FONTSIZE_TICK = 5.5
FONTSIZE_LEGEND = 5.5
FONTSIZE_ANNOTATION = 5.5


COLOR_PALETTE = {
    'primary_light': '#76C6C5',
    'primary_dark': '#0E5D5F',
    'secondary': '#163F3F',
    'header_bg': '#F1F9F9',
    'header_text': '#0E5D5F'
    ## a unica cor que eu nao tenho certeza que deveria estar aqui é a #313131
}

PASTA_SAIDA_IMAGENS = r"C:\Users\LucasRafaeldeAndrade\Desktop\portoauto\src\vortx_estoques\imagens"
pd.options.display.max_rows = 100
pd.options.display.max_columns = 50

os.makedirs(PASTA_SAIDA_IMAGENS, exist_ok=True)
def plotar_grafico_subordinacao(df_patrimonio, pasta_saida):
    """
    Calcula e plota o histórico do índice de subordinação do fundo.
    MODIFICADO: Adicionada anotação de "Limite Mínimo" na linha pontilhada.
    """
    print("Gerando gráfico de subordinação...")
    try:
        # ... (código de cálculo) ...
        pl_subordinado = df_patrimonio.loc['FIDC FCT II']
        pl_senior = df_patrimonio.loc['FIDC FCT II SR2']
        pl_total = pl_senior + pl_subordinado
        indice_subordinacao = (pl_subordinado / pl_total.replace(0, np.nan)).dropna() * 100
        if indice_subordinacao.empty: return None

        # --- PLOTAGEM ---
# Define o tamanho customizado para este gráfico
        largura_fixa_pol = FIG_WIDTH_CM / 2.54 
        altura_desejada_pol = 7.6 / 2.54 

        fig, ax = plt.subplots(
            figsize=(largura_fixa_pol, altura_desejada_pol), 
            dpi=DPI_PADRAO
        )
        ax.plot(indice_subordinacao.index, indice_subordinacao.values, color=COLOR_PALETTE['primary_dark'], lw=2.5)
        ax.axhline(y=20, color='gray', linestyle='--', linewidth=1.2, alpha=0.7)
        
        # --- Formatação do Gráfico ---
        ax.axhline(0, color='lightgray', linestyle='-', linewidth=1, zorder=0)
        ax.spines[['top', 'right', 'left', 'bottom']].set_visible(False)
        ax.tick_params(
            axis='both', which='both', length=0, 
            labelsize=FONTSIZE_TICK, labelcolor='#333333'
        )
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=5, maxticks=7))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b/%y'))
        fig.autofmt_xdate()
        ax.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=100.0))
        ax.set_ylim(bottom=0)
        
        # --- Rótulos e Anotações ---
        ultimo_valor = indice_subordinacao.iloc[-1]
        ax.text(indice_subordinacao.index[-1], ultimo_valor, f' {ultimo_valor:.2f}%'.replace('.',','), 
                color=COLOR_PALETTE['primary_dark'], fontsize=FONTSIZE_ANNOTATION, va='bottom')#, ha='right')
        
        # ALTERAÇÃO: Anotação para a linha de limite mínimo
        ax.annotate(
            'Limite Mínimo',
            xy=(indice_subordinacao.index[-1], 20), # Posição da seta
            xytext=(8, 0),                          # Deslocamento do texto
            textcoords='offset points',
           # ha='right',
            va='bottom',
            fontsize=FONTSIZE_ANNOTATION,
            color='gray'
        )

        path_saida_grafico = os.path.join(pasta_saida, "grafico_subordinacao.png")
        fig.savefig(path_saida_grafico, dpi=DPI_PADRAO, bbox_inches='tight', pad_inches=0.05)
        plt.close(fig)
        
        print(f"Gráfico de subordinação (versão final) salvo em: {path_saida_grafico}")
        return path_saida_grafico

    except KeyError as e:
        print(f"AVISO: Não foi possível gerar o gráfico de subordinação. Cota não encontrada: {e}")
        return None
    except Exception as e:
        print(f"ERRO inesperado ao gerar gráfico de subordinação: {e}")
        return None


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
                # SUBSTITUA O LOOP ANTIGO POR ESTE AQUI:
                for col in date_cols:
                    if col in df.columns:
                        # Tenta converter cada valor na coluna de data, lidando com nulos (NaN, NaT)
                        # A mágica está no `parse(x, dayfirst=True)`
                        df[col] = df[col].apply(
                            lambda x: parse(str(x), dayfirst=True) if pd.notna(x) else pd.NaT
                        )
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
                # SUBSTITUA O LOOP ANTIGO POR ESTE AQUI:
                for col in date_cols:
                    if col in df.columns:
                        # Tenta converter cada valor na coluna de data, lidando com nulos (NaN, NaT)
                        # A mágica está no `parse(x, dayfirst=True)`
                        df[col] = df[col].apply(
                            lambda x: parse(str(x), dayfirst=True) if pd.notna(x) else pd.NaT
                        )
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




# =============================================================================
# FUNÇÃO: TABELA DE PERFORMANCE COMPLETA E CORRIGIDA
# =============================================================================

def criar_tabela_performance(df_rentabilidade_processada, fundos_a_exibir, df_cota_base, benchmark='CDI'):
    """
    Gera a tabela de performance com a lógica de comparação do benchmark (CDI) corrigida
    para sempre usar o mesmo período de dias do fundo.
    """
    # --- ETAPA 1: PREPARAR DADOS DIÁRIOS ---
    df_calc = df_rentabilidade_processada[['data', 'nomeFundo', 'variacaoDia']].copy()
    df_calc['retorno_diario'] = df_calc['variacaoDia'] / 100
    
    cdi_disponivel = benchmark in df_cota_base.index
    retornos_cdi_diarios = pd.Series(dtype=float)
    if cdi_disponivel:
        retornos_cdi_diarios = df_cota_base.loc[benchmark].pct_change().dropna()
    else:
        print(f"AVISO: Benchmark '{benchmark}' não encontrado. Tabela será gerada sem essa comparação.")

    # --- ETAPA 2: MONTAR A ESTRUTURA DA TABELA ---
    all_rows = []
    years = sorted(df_calc['data'].dt.year.dropna().unique())

    def format_pct(x):
        return f'{x:.2%}'.replace('.', ',') if isinstance(x, (int, float)) and pd.notna(x) else '-'
    def format_pct_of_bench(x):
        return f'{x*100:.1f}%'.replace('.', ',') if isinstance(x, (int, float)) and pd.notna(x) else '-'

    for year in years:
        for nome_fundo, nome_exibicao in fundos_a_exibir.items():
            df_fundo_ano = df_calc[(df_calc['nomeFundo'] == nome_fundo) & (df_calc['data'].dt.year == year)]
            if df_fundo_ano.empty:
                continue

            row_fundo = [year, nome_exibicao]
            if cdi_disponivel:
                row_pct_bench = [year, f"% {benchmark}"]

            # --- CÁLCULO MENSAL CORRIGIDO ---
            for month in range(1, 13):
                df_fundo_mes = df_fundo_ano[df_fundo_ano['data'].dt.month == month]
                
                if df_fundo_mes.empty:
                    row_fundo.append('-')
                    if cdi_disponivel: row_pct_bench.append('-')
                else:
                    # Calcula o retorno do fundo para o mês (parcial ou completo)
                    ret_fundo = (1 + df_fundo_mes['retorno_diario']).prod() - 1
                    row_fundo.append(format_pct(ret_fundo))

                    if cdi_disponivel:
                        # Calcula o retorno do CDI nos mesmos dias do fundo
                        datas_do_fundo_no_mes = df_fundo_mes['data']
                        ret_bench = (1 + retornos_cdi_diarios.reindex(datas_do_fundo_no_mes).dropna()).prod() - 1
                        pct_cdi = ret_fundo / ret_bench if ret_bench != 0 else pd.NA
                        row_pct_bench.append(format_pct_of_bench(pct_cdi))

            # --- CÁLCULO YTD (ANO) CORRIGIDO ---
            ret_fundo_ano = (1 + df_fundo_ano['retorno_diario']).prod() - 1
            ret_bench_ano = pd.NA
            if cdi_disponivel:
                datas_do_fundo_no_ano = df_fundo_ano['data']
                ret_bench_ano = (1 + retornos_cdi_diarios.reindex(datas_do_fundo_no_ano).dropna()).prod() - 1
            
            # --- CÁLCULO DESDE O INÍCIO CORRIGIDO ---
            df_fundo_total = df_calc[(df_calc['nomeFundo'] == nome_fundo) & (df_calc['data'].dt.year <= year)]
            ret_fundo_inicio = (1 + df_fundo_total['retorno_diario']).prod() - 1
            ret_bench_inicio = pd.NA
            if cdi_disponivel:
                datas_do_fundo_total = df_fundo_total['data']
                ret_bench_inicio = (1 + retornos_cdi_diarios.reindex(datas_do_fundo_total).dropna()).prod() - 1
            
            # Adiciona os valores calculados na linha
            row_fundo.extend([format_pct(ret_fundo_ano), format_pct(ret_fundo_inicio)])
            all_rows.append(row_fundo)
            
            if cdi_disponivel:
                pct_cdi_ano = ret_fundo_ano / ret_bench_ano if ret_bench_ano != 0 else pd.NA
                pct_cdi_inicio = ret_fundo_inicio / ret_bench_inicio if ret_bench_inicio != 0 else pd.NA
                row_pct_bench.extend([format_pct_of_bench(pct_cdi_ano), format_pct_of_bench(pct_cdi_inicio)])
                all_rows.append(row_pct_bench)

    # --- ETAPA 3: RENDERIZAR E SALVAR A TABELA (sem alterações) ---
    if not all_rows: return None
    
    table_cols = ['Ano', 'Cota', 'Jan', 'Fev', 'Mar', 'Abr', 'Maio', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'YTD', 'Desde o\nInício']
    df_table = pd.DataFrame(all_rows, columns=table_cols)
    
    num_rows_per_year = len(all_rows) // len(years)
    anos_formatados = [row['Ano'] if i % num_rows_per_year == 0 else '' for i, row in df_table.iterrows()]
    df_table['Ano'] = anos_formatados

    def render_mpl_table_final(data, ax, font_size=8.5):
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
                is_pct_cdi_row = cdi_disponivel and (k[0] % 2 == 0)
                if is_pct_cdi_row: cell.set_facecolor('#F0F0F0')
                else: cell.set_facecolor('#FFFFFF')
            if k[1] <= 0:
                props.update({'weight': 'bold', 'color': header_font_color})
                if k[0] != 0: cell.set_facecolor(header_color)
            cell.set_text_props(**props)
        return ax

    fig, ax = plt.subplots(figsize=(12, len(all_rows) * 0.35))
    render_mpl_table_final(df_table, ax)
    
    # Usa o primeiro fundo da lista para nomear o arquivo
    primeiro_fundo_nome = list(fundos_a_exibir.keys())[0].replace(' ', '_')
    path_tabela = os.path.join(PASTA_SAIDA_IMAGENS, f"tabela_rentabilidade_{primeiro_fundo_nome}.png")
    fig.savefig(path_tabela, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    print(f"Tabela de rentabilidade (lógica do CDI corrigida) salva em: {path_tabela}")
    return path_tabela


# =============================================================================
# FUNÇÃO: GRÁFICOS DE RENTABILIDADE PADRONIZADOS
# =============================================================================

def plotar_graficos_rentabilidade(df_cota, df_patr, df_retornos, fundos_a_exibir, benchmark='CDI'):
    """
    Cria e salva todos os gráficos de performance com tamanho e fontes padronizados.
    """
    fundo_alvo_principal = list(fundos_a_exibir.keys())[0]
    nome_exib_principal = list(fundos_a_exibir.values())[0]
    nome_arquivo_limpo = fundo_alvo_principal.replace(' ', '_')

    # --- 1. GRÁFICO DE RETORNO ACUMULADO ---
    fig, ax = plt.subplots(figsize=FIGSIZE_PADRAO, dpi=DPI_PADRAO)
    
    retornos_diarios = df_retornos[fundo_alvo_principal].dropna()
    common_start_date = retornos_diarios.index.min()
    
    df_fund_evol_pct = ((1 + retornos_diarios).cumprod() - 1) * 100
    ax.plot(df_fund_evol_pct, color=COLOR_PALETTE['primary_light'], lw=1.2, label=nome_exib_principal)
    
    last_val = df_fund_evol_pct.iloc[-1]
    label_text_br = f' +{last_val:.2f}%'.replace('.', ',')
    ax.text(df_fund_evol_pct.index[-1], last_val, label_text_br, 
            color=COLOR_PALETTE['primary_light'], fontsize=FONTSIZE_ANNOTATION, va='center')
    
    if benchmark in df_retornos.columns:
        retornos_cdi = df_retornos[benchmark].dropna()[common_start_date:]
        if not retornos_cdi.empty:
            df_bench_evol_pct = ((1 + retornos_cdi).cumprod() - 1) * 100
            ax.plot(df_bench_evol_pct, color='k', linestyle='--', alpha=0.7, lw=1, label=benchmark)
            
            last_val_bench = df_bench_evol_pct.iloc[-1]
            label_text_bench_br = f' +{last_val_bench:.2f}%'.replace('.', ',')
            ax.text(df_bench_evol_pct.index[-1], last_val_bench, label_text_bench_br, 
                    color='k', alpha=0.8, fontsize=FONTSIZE_ANNOTATION, va='center')
    
    ax.axhline(0, color='lightgray', linestyle='-', linewidth=0.5, zorder=0)
    ax.spines[['top', 'right', 'left', 'bottom']].set_visible(False)
    ax.tick_params(axis='both', which='both', length=0, labelsize=FONTSIZE_TICK, labelcolor='#333333')
    ax.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=100, decimals=0))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b/%y'))
    fig.autofmt_xdate()
    ax.legend(loc='upper left', frameon=False, fontsize=FONTSIZE_LEGEND)
    
    plt.tight_layout(pad=0.3)
    path_retorno = os.path.join(PASTA_SAIDA_IMAGENS, f"grafico_retorno_{nome_arquivo_limpo}.png")
    fig.savefig(path_retorno, dpi=DPI_PADRAO, bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    print(f"✓ Gráfico de retorno salvo")

    # --- 2. VOLATILIDADE ---
    fig, ax = plt.subplots(figsize=FIGSIZE_PADRAO, dpi=DPI_PADRAO)
    window = 22
    ret_fundo = df_cota.loc[fundo_alvo_principal].pct_change()
    vol_fundo = ret_fundo.rolling(window=window).std() * np.sqrt(252)
    ax.plot(vol_fundo, color=COLOR_PALETTE['primary_light'], lw=1.2, label=nome_exib_principal)
    
    if benchmark in df_cota.index:
        ret_bench = df_cota.loc[benchmark].pct_change()
        vol_bench = ret_bench.rolling(window=window).std() * np.sqrt(252)
        ax.plot(vol_bench, color='k', linestyle='--', alpha=0.7, lw=1, label=benchmark)
    
    ax.spines[['top', 'right']].set_visible(False)
    ax.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
    ax.grid(axis='y', linestyle='--', alpha=0.5, linewidth=0.3)
    ax.set_title(f'Volatilidade Anualizada', fontsize=FONTSIZE_TITLE, pad=4)
    ax.tick_params(labelsize=FONTSIZE_TICK, length=2, labelcolor='#333333')
    ax.legend(loc='upper left', frameon=False, fontsize=FONTSIZE_LEGEND)
    
    plt.tight_layout(pad=0.3)
    path_vol = os.path.join(PASTA_SAIDA_IMAGENS, f"grafico_volatilidade_{nome_arquivo_limpo}.png")
    fig.savefig(path_vol, dpi=DPI_PADRAO, bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    print(f"✓ Gráfico de volatilidade salvo")

    # --- 3. DRAWDOWN ---
    fig, ax = plt.subplots(figsize=FIGSIZE_PADRAO, dpi=DPI_PADRAO)
    df_fund_dd = df_cota.loc[fundo_alvo_principal].dropna()
    daily_dd = df_fund_dd / df_fund_dd.cummax() - 1.0
    ax.plot(daily_dd, color=COLOR_PALETTE['primary_light'], lw=0.8)
    ax.fill_between(daily_dd.index, daily_dd, 0, color=COLOR_PALETTE['primary_light'], alpha=0.3)
    
    ax.spines[['top', 'right']].set_visible(False)
    ax.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
    ax.grid(axis='y', linestyle='--', alpha=0.5, linewidth=0.3)
    ax.set_title(f'Drawdown Histórico', fontsize=FONTSIZE_TITLE, pad=4)
    ax.tick_params(labelsize=FONTSIZE_TICK, length=2, labelcolor='#333333')
    
    plt.tight_layout(pad=0.3)
    path_dd = os.path.join(PASTA_SAIDA_IMAGENS, f"grafico_drawdown_{nome_arquivo_limpo}.png")
    fig.savefig(path_dd, dpi=DPI_PADRAO, bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    print(f"✓ Gráfico de drawdown salvo")

    # --- 4. PL EMPILHADO (SE TIVER SUBORDINADA) ---
    fig, ax = plt.subplots(figsize=FIGSIZE_PADRAO, dpi=DPI_PADRAO)
    
    # Verifica se há dados de ambas as cotas para fazer o stackplot
    if 'FIDC FCT II' in df_patr.index and 'FIDC FCT II SR2' in df_patr.index:
        df_plot_pl = df_patr.loc[['FIDC FCT II', 'FIDC FCT II SR2']].T.fillna(0)
        start_date = df_plot_pl[(df_plot_pl['FIDC FCT II'] > 0) & (df_plot_pl['FIDC FCT II SR2'] > 0)].index.min()
        df_plot_pl = df_plot_pl.loc[start_date:]
        
        cores_pl = [COLOR_PALETTE['primary_dark'], COLOR_PALETTE['primary_light']]
        handles = ax.stackplot(df_plot_pl.index, df_plot_pl['FIDC FCT II'], df_plot_pl['FIDC FCT II SR2'],
                               labels=['Subordinada', 'SÊNIOR II'], colors=cores_pl, alpha=0.8)
        
        ax.legend(handles=handles[::-1], labels=['SÊNIOR II', 'Subordinada'], 
                  loc='upper left', frameon=False, fontsize=FONTSIZE_LEGEND)
        
        pl_total_final = df_plot_pl.iloc[-1].sum()
        pl_sub_final = df_plot_pl.iloc[-1]['FIDC FCT II']
        
        texto_total = f'R$ {pl_total_final/1e6:,.1f}M'.replace(',', 'v').replace('.', ',').replace('v', '.')
        ax.text(df_plot_pl.index[-1], pl_total_final, f' {texto_total}',
                color=COLOR_PALETTE['secondary'], fontsize=FONTSIZE_ANNOTATION, va='center', ha='left')
        
        texto_sub = f'Sub R$ {pl_sub_final/1e6:,.1f}M'.replace(',', 'v').replace('.', ',').replace('v', '.')
        ax.text(df_plot_pl.index[-1], pl_sub_final, f' {texto_sub}',
                color=COLOR_PALETTE['primary_dark'], fontsize=FONTSIZE_ANNOTATION-0.5, va='center', ha='left')
    else:
        # Se só tiver a Sênior, faz gráfico simples
        df_plot_pl = df_patr.loc[fundo_alvo_principal].dropna()
        ax.plot(df_plot_pl.index, df_plot_pl.values, color=COLOR_PALETTE['primary_light'], lw=1.5, label=nome_exib_principal)
        ax.fill_between(df_plot_pl.index, df_plot_pl.values, 0, color=COLOR_PALETTE['primary_light'], alpha=0.3)
        ax.legend(loc='upper left', frameon=False, fontsize=FONTSIZE_LEGEND)
        
        pl_final = df_plot_pl.iloc[-1]
        texto_pl = f' R$ {pl_final/1e6:.2f}M'.replace('.', ',')
        ax.text(df_plot_pl.index[-1], pl_final, texto_pl,
                color=COLOR_PALETTE['secondary'], fontsize=FONTSIZE_ANNOTATION, va='center', ha='left')
    
    ax.spines[['top', 'right', 'left', 'bottom']].set_visible(False)
    ax.axhline(0, color='lightgray', linewidth=0.5, zorder=0)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1000:,.0f}'.replace(',', '.')))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b/%y'))
    ax.tick_params(axis='y', length=0, labelsize=FONTSIZE_TICK, labelcolor='#333333')
    ax.tick_params(axis='x', labelsize=FONTSIZE_TICK, rotation=30, length=0, labelcolor='#333333')
    
    plt.tight_layout(pad=0.3)
    path_pl = os.path.join(PASTA_SAIDA_IMAGENS, f"grafico_pl_{nome_arquivo_limpo}.png")
    fig.savefig(path_pl, dpi=DPI_PADRAO, bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    print(f"✓ Gráfico de PL salvo")
    
    return {
        'retorno': path_retorno, 'volatilidade': path_vol,
        'drawdown': path_dd, 'pl': path_pl
    }

def plotar_graficos_estoque(df_estoque):
    """Cria e salva todos os gráficos relacionados ao estoque, incluindo os novos."""
    if df_estoque.empty:
        print("DataFrame de estoque vazio. Pulando geração de gráficos de estoque.")
        return {}
    
    print("\n--- Gerando imagens de Estoque ---")
    df_avencer = df_estoque[df_estoque['Status'] == 'A vencer'].copy()
    paths = {}
    
    # --- Gráficos Originais ---
    paths['venc_mensal'] = plotar_vencimento_mensal(df_avencer, col_valor='Valor Presente', col_vcto='VCTO_MES')
    paths['venc_anual'] = plotar_vencimento_anual(df_avencer, col_valor='Valor Presente', col_vcto='VCTO_ANO')
    paths['conc_uf'] = plotar_concentracao_uf(df_avencer, col_valor='Valor Presente', col_class='UF')
    paths['dist_capag'] = plotar_distribuicao_capag(df_estoque, class_col='CAPAG', col_valor='Valor Presente')
    paths['aging_vencidos'] = plotar_aging_com_acumulado(df_estoque, col_valor='Valor Presente')
    paths['conc_cumulativa_sacado'] = plotar_concentracao_cumulativa_sacado(df_avencer, col_valor='Valor Presente', col_class='CPF Cliente')
    
    # --- NOVOS GRÁFICOS ---
    paths['conc_convenio'] = plotar_concentracao_convenio(df_avencer)
    paths['dist_produto'] = plotar_distribuicao_produto(df_avencer)
    paths['dist_idade'] = plotar_distribuicao_idade(df_estoque) # Usa df_estoque completo
    
    print("\nGráficos de estoque gerados com sucesso.")
    return paths

def plotar_vencimento_mensal(df_aberto, col_valor, col_vcto, cutoff=10):
    """Gráfico de vencimento mensal com tamanho padronizado."""
    df_aberto = df_aberto.copy()
    df_aberto[col_vcto] = df_aberto['Data Vencimento'].dt.strftime('%Y-%m')
    df2 = df_aberto.groupby(col_vcto)[col_valor].sum()
    df3 = (df2 / df2.sum()).cumsum()
    df2 = df2[:cutoff]
    df3 = df3[:cutoff]

    if df2.empty: return None

    ymin1, ymax1 = 0, df2.max() * 2
    ymax2, ymin2 = 1.3, df3.min() * 2.5 - 1.5 * 1.3

    fig, ax = plt.subplots(figsize=FIGSIZE_PADRAO, dpi=DPI_PADRAO)
    ax2 = ax.twinx()

    bars = ax.bar(df2.index, np.clip(df2.values, 0.01 * df2.max(), df2.max()), 
                  color=COLOR_PALETTE['primary_dark'], width=0.6)
    ax2.plot(df3.index, df3.values, color=COLOR_PALETTE['secondary'], lw=1.2, marker='o', markersize=3)

    month_map = {'01':'jan','02':'fev','03':'mar','04':'abr','05':'mai','06':'jun',
                 '07':'jul','08':'ago','09':'set','10':'out','11':'nov','12':'dez'}
    ax.set_xticks(df2.index)
    ax.set_xticklabels([f"{month_map[x[-2:]]}/{x[2:4]}" for x in df2.index], fontsize=FONTSIZE_TICK)
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
        ax.text(bar.get_x() + bar.get_width()/2, h, f'{value:,.0f}'.replace(',', '.'), 
                ha='center', fontsize=FONTSIZE_ANNOTATION)

    for x, y in df3.items():
        value = f'{y:.0%}' if ((y < .990) or (y == 1)) else '>99%'
        ax2.text(x, y + (ymax2 - ymin2) * 0.04, value, ha='center', fontsize=FONTSIZE_ANNOTATION)

    plt.tight_layout(pad=0.3)
    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "vencimento_mensal.png")
    fig.savefig(path_saida, dpi=DPI_PADRAO, bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    print(f"✓ Gráfico de vencimento mensal padronizado salvo")
    return path_saida

def plotar_vencimento_anual(df_aberto, col_valor, col_vcto):
    """Gráfico de vencimento anual com tamanho padronizado."""
    df_aberto = df_aberto.copy()
    df_aberto[col_vcto] = df_aberto['Data Vencimento'].dt.year
    df2 = df_aberto.groupby(col_vcto)[col_valor].sum()
    df3 = df2.cumsum() / df2.sum()

    if df2.empty: return None

    ymin1, ymax1 = 0, df2.max() * 2
    ymax2, ymin2 = 1.15, df3.min() * 2.5 - 1.5 * 1.15

    fig, ax = plt.subplots(figsize=FIGSIZE_PADRAO, dpi=DPI_PADRAO)
    ax2 = ax.twinx()
    
    bars = ax.bar(df2.index, np.clip(df2.values, 0.01 * df2.max(), df2.max()), 
                  color=COLOR_PALETTE['primary_dark'], width=0.6)
    ax2.plot(df3.index, df3.values, color=COLOR_PALETTE['secondary'], lw=1.2, marker='o', markersize=3)
    
    ax.set_xticks(df2.index.tolist())
    ax.tick_params(labelsize=FONTSIZE_TICK)
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
        ax.text(bar.get_x() + bar.get_width()/2, h, f'{value:,.0f}'.replace(',', '.'), 
                ha='center', fontsize=FONTSIZE_ANNOTATION)

    for x, y in df3.items():
        value = f'{y:.0%}' if ((y < .990) or np.isclose(y, 1)) else '>99%'
        ax2.text(x, y + 0.06, value, ha='center', fontsize=FONTSIZE_ANNOTATION)

    plt.tight_layout(pad=0.3)
    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "vencimento_anual.png")
    fig.savefig(path_saida, dpi=DPI_PADRAO, bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    print(f"✓ Gráfico de vencimento anual padronizado salvo")
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
    """Gera e salva o gráfico de concentração por UF (vertical)."""
    df11 = df_aberto.groupby(col_class)[col_valor].sum().sort_values(ascending=False)
    
    if len(df11) > cutoff:
        x = df11.head(cutoff).index.tolist() + ['Outros']
        y = df11.head(cutoff).values.tolist() + [df11.iloc[cutoff:].sum()]
    else:
        x = df11.index.tolist()
        y = df11.values.tolist()
        
    df_plot = pd.Series(y, index=x)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(df_plot.index, df_plot.values, color=COLOR_PALETTE['primary_dark'])
    
    ax.set_yticks([])
    ax.tick_params(bottom=False)
    ax.set_frame_on(False)

    max_height = df_plot.max()
    ax.set_ylim(0, max_height * 1.15) # Espaço para rótulos

    for bar in bars:
        height = bar.get_height()
        value = height / 1000
        ax.text(bar.get_x() + bar.get_width()/2, height, f'{value:,.0f}'.replace(',', '.'), 
                ha='center', va='bottom', fontsize=9)

    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "concentracao_uf.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de concentração por UF salvo em: {path_saida}")
    return path_saida

def plotar_distribuicao_capag(df_in, class_col, col_valor):
    """Gera e salva o gráfico de distribuição por CAPAG com rótulos e percentuais ajustados."""
    df_capag = df_in.dropna(subset=[class_col]).groupby(class_col)[col_valor].sum()
    
    if df_capag.empty:
        print("Não há dados para gerar o gráfico de CAPAG.")
        return None
        
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(df_capag.index, df_capag.values, color=COLOR_PALETTE['primary_dark'])

    ax.set_xticks(df_capag.index.tolist())
    ax.set_yticks([])
    ax.tick_params(bottom=False)
    ax.set_frame_on(False)
    
    # Define o limite do eixo Y para dar espaço para o rótulo de duas linhas
    ax.set_ylim(0, df_capag.max() * 1.3)
    
    total_geral = df_capag.sum()

    for bar in bars:
        height = bar.get_height()
        percentual = (height / total_geral) * 100
        
        # Formata o valor em milhares (k)
        valor_k = f'{height/1000:,.0f}'.replace(',', '.')
        # Formata o percentual
        percentual_str = f'({percentual:.1f}%)'.replace('.', ',')
        # Cria o rótulo com quebra de linha
        label = f'{valor_k}\n{percentual_str}'
        
        # Posiciona o texto logo acima da barra
        ax.text(bar.get_x() + bar.get_width()/2, height, label, 
                ha='center', va='bottom', fontsize=9)

    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "distribuicao_capag.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de distribuição por CAPAG (com %) salvo em: {path_saida}")
    return path_saida

def plotar_concentracao_cumulativa_sacado(df_aberto, col_valor='Valor Presente', col_class='CPF Cliente'):
    """Gera um gráfico de barras vertical mostrando a concentração CUMULATIVA de valor."""
    df_agrupado = df_aberto.groupby(col_class)[col_valor].sum().sort_values(ascending=False)
    if df_agrupado.empty: return None

    valor_total = df_agrupado.sum()
    num_sacados = len(df_agrupado)
    bins = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
    resultados = {}

    for n in bins:
        if n < num_sacados:
            soma_top_n = df_agrupado.iloc[0:n].sum()
            pct_top_n = (soma_top_n / valor_total) * 100
            resultados[f'{n} maiores'] = pct_top_n
            
    resultados[f'Total ({num_sacados})'] = 100.0
    df_plot = pd.Series(resultados)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(df_plot.index, df_plot.values, color=COLOR_PALETTE['primary_dark'])
    
    ax.set_yticks([])
    ax.set_frame_on(False)
    ax.tick_params(bottom=False)
    
    # Rotaciona os rótulos do eixo X
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    
    ax.set_ylim(0, 110) # Garante espaço para o rótulo

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height + 1, f'{height:.0f}%', 
                ha='center', va='bottom', fontsize=9)

    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "concentracao_cumulativa_sacado.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de concentração cumulativa por sacado salvo em: {path_saida}")
    return path_saida

def plotar_aging_com_acumulado(df_estoque, col_valor='Valor Presente'):
    """Gera um gráfico de Aging com rótulos em milhares e sem ticks no eixo Y."""
    df_vencidos = df_estoque[df_estoque['Status'] == 'Vencido'].copy()
    if df_vencidos.empty:
        print("AVISO: Nenhum dado 'Vencido' para gerar gráfico de Aging.")
        return None

    hoje = df_estoque['Data Referencia'].max() # Usa a data de referência para consistência
    df_vencidos['dias_atraso'] = (hoje - df_vencidos['Data Vencimento']).dt.days
    bins = [-1, 14, 30, 60, 90, 120, 150, 180, np.inf]
    labels = ['0-14\nDias', '15-30\nDias', '31-60\nDias', '61-90\nDias', '91-120\nDias', '121-150\nDias', '151-180\nDias', '180+\nDias']
    df_vencidos['Faixa Atraso'] = pd.cut(df_vencidos['dias_atraso'], bins=bins, labels=labels, right=True)
    
    df_aging = df_vencidos.groupby('Faixa Atraso', observed=False)[col_valor].sum()
    total_em_atraso = df_aging.sum()

    # Se não houver nada em atraso, não plote
    if total_em_atraso == 0:
        print("AVISO: Valor total em atraso é R$0. Pulando gráfico de Aging.")
        return None

    # 1. Salva o pct_acum (partes) e o max_val (partes)
    df_aging_pct_acum = df_aging.cumsum() / total_em_atraso
    max_val_parte = df_aging.max()

    # 2. Adiciona o "Total" ao df_aging (para as barras)
    df_aging.loc['Total em\nAtraso'] = total_em_atraso
    
    # 3. Setup do Gráfico
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax2 = ax.twinx()

    # 4. Lógica de Limites (Y-Axis) - 50% partes, 50% linha
    min_val_linha = df_aging_pct_acum.min()

    ymin1 = 0
    # CHAVE: O limite do gráfico (eixo da barra) é 2x a maior PARTE
    ymax1 = max_val_parte * 2  
    
    # Y-lim da LINHA (ax2) flutua na metade de cima
    # ALTERAÇÃO AQUI: 1.3 -> 1.1 (para "subir" o 100%)
    ymax2 = 1.1 
    ymin2 = min_val_linha * 2.5 - 1.5 * ymax2 
    
    ax.set_ylim(ymin1, ymax1)
    ax2.set_ylim(ymin2, ymax2)

    # 5. Plota a Linha (só as partes, sem o "Total")
    line = ax2.plot(df_aging_pct_acum.index, df_aging_pct_acum.values, color=COLOR_PALETTE['secondary'], marker='o', lw=2)
    
    # 6. Prepara valores "clipados" para as barras
    # A barra "Total" será visualmente limitada se for > ymax1
    plot_values = df_aging.values.clip(0, ymax1)

    # 7. Plota as Barras (com valores clipados)
    bars = ax.bar(df_aging.index, plot_values, color=COLOR_PALETTE['primary_dark'], width=0.6)
    
    # 8. Colore a última barra (Total)
    bars[-1].set_facecolor(COLOR_PALETTE['secondary'])
        
    # 9. Formatação dos Eixos
    ax.set_yticks([])
    ax.tick_params(axis='y', length=0) 
    ax.set_frame_on(False)
    ax2.spines[:].set_visible(False)
    ax2.set_yticks([])
    
    # 10. Rótulos das Barras (Lógica Simplificada)
    # ALTERAÇÃO AQUI: Removida a lógica 'if visual_height < real_value'
    for i, bar in enumerate(bars):
        visual_height = bar.get_height()  # Altura visual (clipada)
        real_value = df_aging.values[i]   # Valor real (não clipado)

        if real_value == 0: continue 

        label = f'{real_value/1000:,.0f}'.replace(',', '.')
        
        # Comportamento normal: rótulo ACIMA da barra
        # O label da barra "Total" ficará no topo da barra clipada (no limite ymax1).
        ax.text(bar.get_x() + bar.get_width() / 2, visual_height, label,
                ha='center', va='bottom', fontsize=9, color='#333333')
                    
    # 11. Rótulos da Linha
    for i, pct in enumerate(df_aging_pct_acum):
        # Ajusta a posição do rótulo da linha (usa o novo ymax2)
        ax2.text(i, pct + (ymax2 - ymin2) * 0.04, f'{pct:.0%}', ha='center', fontsize=9, color=COLOR_PALETTE['secondary'])
    
    # 12. Salvar e Fechar
    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "aging_com_acumulado.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de Aging (versão aprimorada) salvo em: {path_saida}")
    return path_saida

def plotar_concentracao_convenio(df_aberto, col_valor='Valor Presente', col_class='Convênio Formatado', cutoff=10):
    """Gera e salva o gráfico de concentração por Convênio (vertical)."""
    df_aberto[col_class] = df_aberto[col_class].str.strip()
    df_agrupado = df_aberto.groupby(col_class)[col_valor].sum().sort_values(ascending=False)
    
    if len(df_agrupado) > cutoff:
        df_top = df_agrupado.head(cutoff)
        outros_sum = df_agrupado.iloc[cutoff:].sum()
        df_top['Outros'] = outros_sum
        df_plot = df_top
    else:
        df_plot = df_agrupado

    if df_plot.empty: return None

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(df_plot.index, df_plot.values, color=COLOR_PALETTE['primary_dark'])
    
    ax.set_yticks([])
    ax.tick_params(bottom=False)
    ax.set_frame_on(False)
    
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_ylim(0, df_plot.max() * 1.25) # Espaço para rótulos
    total_geral = df_agrupado.sum()

    for bar in bars:
        height = bar.get_height()
        percentual = (height / total_geral) * 100
        valor_k = f'{height/1000:,.0f}'.replace(',', '.')
        percentual_str = f'({percentual:.1f}%)'.replace('.', ',')
        label = f'{valor_k}\n{percentual_str}'
        ax.text(bar.get_x() + bar.get_width()/2, height, label, 
                ha='center', va='bottom', fontsize=9)
    
    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "concentracao_convenio.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de concentração por convênio (Top 10) salvo em: {path_saida}")
    return path_saida
# 2. FUNÇÃO DE PRODUTO ATUALIZADA
def plotar_distribuicao_produto(df_aberto, col_valor='Valor Presente', col_class='Produto', cutoff=8):
    """Gera e salva o gráfico de distribuição por Produto, com formatação de valor em milhares."""
    df_agrupado = df_aberto.groupby(col_class)[col_valor].sum().sort_values(ascending=False)

    if len(df_agrupado) > cutoff:
        df_top = df_agrupado.head(cutoff)
        outros_sum = df_agrupado.iloc[cutoff:].sum()
        df_top['Outros'] = outros_sum
        df_plot = df_top
    else:
        df_plot = df_agrupado

    if df_plot.empty:
        print("Não há dados para gerar o gráfico de distribuição por produto.")
        return None
        
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(df_plot.index, df_plot.values, color=COLOR_PALETTE['primary_dark'], width=0.5)

    ax.set_yticks([])
    ax.tick_params(bottom=False)
    ax.set_frame_on(False)
    
    total_geral = df_plot.sum()
    for bar in bars:
        height = bar.get_height()
        percentual = (height / total_geral) * 100
        # *** NOVA FORMATAÇÃO DO RÓTULO ***
        valor_k = f'{height/1000:,.0f}'.replace(',', '.')
        percentual_str = f'({percentual:.1f}%)'.replace('.', ',')
        label = f'{valor_k}\n{percentual_str}'
        
        ax.text(bar.get_x() + bar.get_width()/2, height, label, 
                ha='center', va='bottom', fontsize=9, color='#333333')
                
    plt.setp(ax.get_xticklabels(), rotation=15, ha="right")
    ax.set_ylim(0, df_plot.max() * 1.3) # Aumenta um pouco mais o espaço para o rótulo com quebra de linha
    
    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "distribuicao_produto.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de distribuição por produto salvo em: {path_saida}")
    return path_saida

# 3. FUNÇÃO DE IDADE ATUALIZADA
def plotar_distribuicao_idade(df_estoque, col_valor='Valor Presente', col_data_nasc='Data de Nascimento', col_data_ref='Data Referencia'):
    """Gera e salva o gráfico de distribuição por faixa de idade, com quebra de linha no rótulo."""
    df_temp = df_estoque.dropna(subset=[col_data_nasc, col_data_ref]).copy()
    df_temp['idade'] = (df_temp[col_data_ref] - df_temp[col_data_nasc]).dt.days / 365.25
    bins = [0, 25, 35, 45, 55, 65, 75, np.inf]
    labels = ['Até 25', '26-35', '36-45', '46-55', '56-65', '66-75', '75+']
    df_temp['faixa_idade'] = pd.cut(df_temp['idade'], bins=bins, labels=labels, right=True)
    df_agrupado = df_temp.groupby('faixa_idade', observed=False)[col_valor].sum()

    if df_agrupado.empty: return None

    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(df_agrupado.index, df_agrupado.values, color=COLOR_PALETTE['primary_dark'], width=0.6)
    ax.set_yticks([])
    ax.tick_params(bottom=False)
    ax.set_frame_on(False)
    total_geral = df_agrupado.sum()

    for bar in bars:
        height = bar.get_height()
        percentual = (height / total_geral) * 100
        valor_k = f'{height/1000:,.0f}'.replace(',', '.')
        percentual_str = f'({percentual:.1f}%)'.replace('.', ',')
        # Adiciona a quebra de linha '\n'
        label = f'{valor_k}\n{percentual_str}' 
        ax.text(bar.get_x() + bar.get_width()/2, height, label, ha='center', va='bottom', fontsize=9)

    ax.set_ylim(0, df_agrupado.max() * 1.25)
    path_saida = os.path.join(PASTA_SAIDA_IMAGENS, "distribuicao_idade.png")
    fig.savefig(path_saida, bbox_inches='tight')
    plt.close(fig)
    print(f"Gráfico de distribuição por idade salvo em: {path_saida}")
    return path_saida
#%% 
# --- ETAPA 1: PROCESSAMENTO DE DADOS DE RENTABILIDADE ---
df_rentabilidade = carregar_dados_rentabilidade(PATH_RENTABILIDADE)
if df_rentabilidade.empty:
    print("Processo interrompido: falha ao carregar dados de rentabilidade.")

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
    'FIDC FCT II SR2': 'SÊNIOR II'#, # <-- Mude aqui
   # 'FIDC FCT II': 'Subordinada'
}
# Passa o DataFrame original processado para a função, que agora faz o cálculo internamente
paths_imagens['tabela_perf'] = criar_tabela_performance(df_rentabilidade, fundos_para_tabela, df_cota)
paths_rentabilidade = plotar_graficos_rentabilidade(df_cota, df_patr, df_retornos, fundos_para_tabela)

paths_imagens['subordinacao'] = plotar_grafico_subordinacao(df_patr, PASTA_SAIDA_IMAGENS)
paths_imagens.update(paths_rentabilidade)

df_estoque = processar_dados_estoque(PATH_ESTOQUE)
paths_estoque = plotar_graficos_estoque(df_estoque)
paths_imagens.update(paths_estoque)