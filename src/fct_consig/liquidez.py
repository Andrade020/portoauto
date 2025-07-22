#%% [markdown]
# # Relatório de Liquidez - Porto Real
# Este script processa os arquivos de estoque, calcula as projeções de liquidez
# e gera um relatório final em formato HTML.

# %% [markdown]
# ## 1. Importação de Bibliotecas e Definição de Funções

import os
import re
import glob
import shutil
import subprocess
from datetime import datetime, date
from typing import Tuple, Dict, List, Any

import polars as pl
import pandas as pd
import numpy as np

# %%
def process_data(
    downloads_base: str,
    target_date: date,
    str_cols: list[str],
    date_cols: list[str],
    float_cols: list[str],
    int_cols: list[str],
    one_data_cols: list[str],
    no_data_cols: list[str],
    cat_cols: list[str]
) -> pd.DataFrame:
    """
    Recebe a DATA TARGET(string), BASE DE DOWNLOADS(path-string) e LISTA DE COLUNAS(lista), 
    retorna df final.
    """
    print(f"[DEBUG] Procurando .tar.zst em: {downloads_base!r}")
    archives: list[tuple[date, date, str]] = []
    for fname in os.listdir(downloads_base):
        if not fname.endswith('.tar.zst') or '_' not in fname:
            continue
        base = fname[:-len('.tar.zst')]
        parts = base.split('_', 1)
        if len(parts) != 2:
            continue
        down_str, ref_str = parts
        try:
            down_dt = datetime.strptime(down_str, '%Y-%m-%d').date()
            ref_dt  = datetime.strptime(ref_str, '%Y-%m-%d').date()
        except ValueError:
            continue
        archives.append((down_dt, ref_dt, fname))
    print(f"[DEBUG] Encontrados {len(archives)} arquivos .tar.zst válidos")

    if not archives:
        raise FileNotFoundError(
            f"[ERROR] Nenhum .tar.zst no formato YYYY-MM-DD_YYYY-MM-DD em {downloads_base!r}"
        )

    print(f"[DEBUG] Filtrando arquivos com ref_date = {target_date}")
    candidates = [a for a in archives if a[1] == target_date]
    print(f"[DEBUG] {len(candidates)} candidatos após filtro por ref_date")
    if not candidates:
        raise FileNotFoundError(f"[ERROR] Nenhum .tar.zst para reference_date = {target_date}")
    
    latest_down, latest_ref, latest_file = max(candidates, key=lambda x: x[0])
    archive_path = os.path.join(downloads_base, latest_file)
    print(f"[DEBUG] Selecionado arquivo:\n"
          f"        nome: {latest_file}\n"
          f"   download: {latest_down}\n"
          f"   referência: {latest_ref}")

    tmp_dir = os.path.join(downloads_base, latest_file[:-len('.tar.zst')])
    print(f"[DEBUG] Extraindo para: {tmp_dir}")
    os.makedirs(tmp_dir, exist_ok=True)
    subprocess.run([
        'tar', '-I', 'zstd', '-xvf', archive_path, '-C', tmp_dir
    ], check=True, capture_output=True) # capture_output para evitar poluir o console

    csv_files = glob.glob(os.path.join(tmp_dir, '**', '*.csv'), recursive=True)
    print(f"[DEBUG] {len(csv_files)} arquivos CSV encontrados em {tmp_dir}")
    if not csv_files:
        shutil.rmtree(tmp_dir)
        raise ValueError(f"[ERROR] Nenhum CSV em {tmp_dir}")

    schema_overrides = {c: pl.Utf8 for c in str_cols}
    dfs = []
    for fn in csv_files:
        try:
            df_pl = pl.read_csv(
                fn,
                separator=';',
                encoding='latin1',
                schema_overrides=schema_overrides
            )
            cols_to_parse = [c for c in date_cols if c in df_pl.columns]
            if cols_to_parse:
                df_pl = df_pl.with_columns([
                    pl.col(c)
                      .str.strptime(pl.Date, '%d/%m/%Y', strict=False)
                      .alias(c)
                    for c in cols_to_parse
                ])

            to_drop = [c for c in one_data_cols + no_data_cols if c in df_pl.columns]
            if to_drop:
                df_pl = df_pl.drop(to_drop)

            cast_floats = [c for c in float_cols if c in df_pl.columns]
            if cast_floats:
                df_pl = df_pl.with_columns([
                    pl.col(c)
                      .str.replace_all(',', '.')
                      .cast(pl.Float64)
                      .alias(c)
                    for c in cast_floats
                ])
                
            cast_ints = [c for c in int_cols if c in df_pl.columns]
            if cast_ints:
                df_pl = df_pl.with_columns([
                    pl.col(c).cast(pl.Int64).alias(c)
                    for c in cast_ints
                ])
            dfs.append(df_pl)
        except Exception as e:
            print(f"[WARNING] Falha ao ler ou processar o arquivo {fn}: {e}")


    print(f"[DEBUG] Concatenando {len(dfs)} DataFrames Polars")
    df_all = pl.concat(dfs, how='vertical').to_pandas()
    print(f"[DEBUG] DataFrame Pandas resultante: {df_all.shape[0]} linhas x {df_all.shape[1]} colunas")

    cats = [c for c in cat_cols if c in df_all.columns]
    if cats:
        for c in cats:
            df_all[c] = df_all[c].astype('category')

    subset = ['Número do Participante', 'Nosso Número Bancário']
    if all(col in df_all.columns for col in subset):
        if df_all[subset].duplicated().any():
            print("[WARNING] Entradas duplicadas encontradas e removidas no subset ['Número do Participante', 'Nosso Número Bancário']")
            df_all.drop_duplicates(subset=subset, inplace=True)


    print(f"[DEBUG] Removendo diretório temporário: {tmp_dir}")
    shutil.rmtree(tmp_dir)

    print(f"[DEBUG] Processo concluído com sucesso usando {latest_file}")
    return df_all


def get_latest_archive_dates(downloads_base: str) -> Tuple[date, date]:
    """
    Varre `downloads_base` e retorna as datas do arquivo mais recente.
    """
    pattern = re.compile(r'^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.tar\.zst$')
    archives: list[Tuple[date, date]] = []

    for fname in os.listdir(downloads_base):
        m = pattern.match(fname)
        if not m:
            continue
        down_str, ref_str = m.groups()
        try:
            down_dt = datetime.strptime(down_str, '%Y-%m-%d').date()
            ref_dt  = datetime.strptime(ref_str,  '%Y-%m-%d').date()
        except ValueError:
            continue
        archives.append((down_dt, ref_dt))

    if not archives:
        raise FileNotFoundError(
            f"Nenhum .tar.zst no formato esperado em {downloads_base!r}"
        )

    # retorna (download_date, reference_date) máximo por (ref_dt, down_dt)
    download_date, reference_date = max(archives, key=lambda x: (x[1], x[0]))
    return download_date, reference_date

# NOVA FUNÇÃO PARA GERAR O RELATÓRIO HTML
def gerar_relatorio_html(
    df: pd.DataFrame, 
    empresa: str, 
    fundo: str, 
    ref_date: date, 
    output_dir: str
):
    """
    Gera um relatório HTML a partir de um DataFrame com o design especificado.
    
    Args:
        df (pd.DataFrame): O DataFrame a ser exibido no relatório.
        empresa (str): Nome da empresa para o cabeçalho.
        fundo (str): Nome do fundo para o cabeçalho.
        ref_date (date): A data de referência dos dados.
        output_dir (str): O diretório onde o arquivo HTML será salvo.
    """
    # Formata as datas para o nome do arquivo e para o título do relatório
    today_str = datetime.now().strftime('%Y_%m_%d')
    ref_date_str_file = ref_date.strftime('%Y_%m_%d')
    
    today_display = datetime.now().strftime('%d/%m/%Y')
    ref_date_display = ref_date.strftime('%d/%m/%Y')
    
    # Define o caminho completo e o nome do arquivo
    filename = f"{today_str}-{ref_date_str_file}.html"
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)

    # Converte o DataFrame para HTML com formatação
    # Adiciona a classe 'styled-table' para ser estilizada pelo CSS
    # Formata os números float com 2 casas decimais e separador de milhar
    table_html = df.to_html(
        classes='styled-table', 
        index=True, 
        float_format=lambda x: f'{x:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'),
        border=0
    )
    
    # Define o template HTML com o CSS
    html_template = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Relatório de Liquidez - {fundo}</title>
        <style>
            body {{
                font-family: "Gill Sans MT", Arial, sans-serif;
                background-color: #f4f4f4; /* Fundo um pouco mais claro que branco puro */
                color: #313131;
                margin: 0;
                padding: 20px;
            }}
            .container {{
                max-width: 1200px;
                margin: auto;
                background-color: #FFFFFF;
                padding: 20px 40px;
                border-radius: 8px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }}
            .header {{
                border-bottom: 4px solid #0e5d5f;
                padding-bottom: 15px;
                margin-bottom: 25px;
                text-align: center;
            }}
            .header h1 {{
                color: #163f3f;
                font-size: 28px;
                margin: 0;
            }}
            .header h2 {{
                color: #0e5d5f;
                font-size: 22px;
                margin: 5px 0;
            }}
            .header p {{
                color: #313131;
                font-size: 16px;
                margin: 5px 0;
            }}
            .styled-table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 14px;
            }}
            .styled-table thead {{
                background-color: #0e5d5f;
                color: #FFFFFF;
            }}
            .styled-table th, .styled-table td {{
                padding: 12px 15px;
                text-align: right;
                border: 1px solid #ddd;
            }}
            .styled-table th:first-child, .styled-table td:first-child {{
                text-align: left;
                font-weight: bold;
            }}
            .styled-table tbody tr {{
                border-bottom: 1px solid #dddddd;
            }}
            .styled-table tbody tr:nth-of-type(even) {{
                background-color: #f3f3f3;
            }}
            .styled-table tbody tr:hover {{
                background-color: #76c6c5;
                color: #163f3f;
                cursor: pointer;
            }}
            .footer {{
                text-align: center;
                margin-top: 25px;
                padding-top: 15px;
                border-top: 1px solid #ccc;
                font-size: 12px;
                color: #888;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>{empresa}</h1>
                <h2>Relatório de Liquidez do Fundo</h2>
                <p><strong>Fundo:</strong> {fundo}</p>
                <p><strong>Data de Referência:</strong> {ref_date_display} &nbsp;&nbsp;&nbsp; <strong>Data de Emissão:</strong> {today_display}</p>
            </div>

            {table_html}

            <div class="footer">
                <p>Este é um relatório gerado automaticamente. &copy; {datetime.now().year} {empresa}. Todos os direitos reservados.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Salva o conteúdo no arquivo
    try:
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(html_template)
        print(f"\n[SUCESSO] Relatório salvo em: {full_path}")
    except Exception as e:
        print(f"\n[ERRO] Falha ao salvar o relatório HTML: {e}")


# %% [markdown]
# ## 2. Definição de Parâmetros e Variáveis

no_data_cols = ['Código da Parcela', 'Código na Registradora', 'Documento da Registradora',
                'Nome da Registradora', 'Chave NFE/CTE', 'Código de Averbação']

one_data_cols = ['Fundo', 'CNPJ Fundo', 'Data do Movimento', 'Tipo da Operação']

cat_cols = ['Tipo de Recebível', 'Nome do Recebível', 'Código do Contrato',
            'Documento do Cedente', 'Nome do Cedente', 'Documento do Sacado',
            'Nome do Sacado', 'Situação', 'Risco', 'Aval',
            'Classificação do Sacado', 'Classificação da Parcela',
            'Nome do Ente Consignado', 'Documento do Ente Consignado',
            'UF Ente', 'CAPAG Ente', 'Nome do Originador', 'Documento do Originador']

date_cols = ['Data de Emissão', 'Data de Aquisição', 'Data de Vencimento',
             'Data de Vencimento Ajustada']

float_cols = ['Valor de Contrato', 'Valor de Compra', 'Valor Atual',
              'Valor de Vencimento', 'Valor de PDD', 'Taxa do Arquivo',
              'Taxa da Operação por DU', 'Valor de Vencimento Atualizado',
              'Taxa DU EQ Mês', 'Taxa DU EQ Ano', 'TIR']

int_cols = ['Numero da Parcela', 'Quantidade de Parcelas',
            'Prazo Dias Úteis', 'Dias Corridos Vencidos']

str_cols = ['Classificação da Parcela', 'Número do Participante',
            'Nosso Número Bancário', 'Nome da Registradora',
            'Nome do Recebível', 'Código do Contrato', 'UF Ente']

# --- PONTOS DE ATENÇÃO: Preencha as variáveis abaixo ---

# PREENCHA com o seu dicionário de custos. Exemplo:
cost_dict: Dict[str, Tuple[float, float]] = {
    'ESTADO DO MARANHAO': (0.01, 5.0),
    'GOVERNO DO ESTADO DE GOIAS': (0.012, 4.5),
    'PREFEITURA DE SAO PAULO': (0.009, 5.5),
    # Adicione outros entes aqui
}
# Custo padrão para entes não encontrados no dicionário
default_cost = cost_dict.get('ESTADO DO MARANHAO', (0.01, 5.0)) # Usando Maranhão como padrão


# PREENCHA com sua lista de feriados. Exemplo:
holidays: List[str] = [
    '2025-01-01', '2025-03-03', '2025-03-04', '2025-04-18', '2025-04-21',
    '2025-05-01', '2025-06-19', '2025-09-07', '2025-10-12', '2025-11-02',
    '2025-11-15', '2025-12-25'
]

# %% [markdown]
# ## 3. Execução do Processamento e Geração do Relatório

# Definições para o relatório
NOME_EMPRESA = "Porto Real"
NOME_FUNDO = "MIDIAN RESPONSABILIDADE LIMITADA"
DIRETORIO_SAIDA = "/home/felipe/portoreal/results/liquidez"
BASE_DOWNLOADS = "/home/felipe/portoreal/downloads/estoques"

# 1) Obtenha as duas datas
try:
    download_dt, ref_dt = get_latest_archive_dates(BASE_DOWNLOADS)
    print("Último download:", download_dt)
    print("Última referência:", ref_dt)

    # 2) Chama process_data passando ref_dt como target_date
    df_final = process_data(
        downloads_base=BASE_DOWNLOADS,
        target_date=ref_dt,
        str_cols=str_cols,
        date_cols=date_cols,
        float_cols=float_cols,
        int_cols=int_cols,
        one_data_cols=one_data_cols,
        no_data_cols=no_data_cols,
        cat_cols=cat_cols
    )

    # 3) Cálculos de negócio
    df_final['% PDD'] = df_final['Valor de PDD'] / df_final['Valor Atual']
    df_final["CUSTO_VAR"] = df_final['Nome do Ente Consignado'].apply(lambda x: cost_dict.get(x, default_cost)[0])
    df_final["CUSTO_FIXO"] = df_final['Nome do Ente Consignado'].apply(lambda x: cost_dict.get(x, default_cost)[1])
    df_final["CUSTO_TOTAL"] = df_final["Valor de Vencimento Atualizado"] * (1 - df_final['% PDD']) * df_final["CUSTO_VAR"] \
                               + df_final["CUSTO_FIXO"]

    group_col = 'Data de Vencimento Ajustada'
    agg_col = ['Valor de Vencimento', 'Valor Atual', 'Valor de PDD', 'CUSTO_TOTAL']
    df_in = df_final.groupby(group_col)[agg_col].sum()

    df_in['% PDD'] = df_in['Valor de PDD'] / df_in['Valor Atual']
    df_in['Valor Líquido'] = df_in['Valor de Vencimento'] * (1 - df_in['% PDD'])
    df_in['Recebimento Líquido'] = df_in['Valor de Vencimento'] * (1 - df_in['% PDD']) - df_in['CUSTO_TOTAL']

    # Garante que o index seja datetime para os próximos passos
    df_in.index = pd.to_datetime(df_in.index)
    
    end_date3 = [np.datetime64(d.date(), 'D') for d in df_in.index]
    holidays3 = [np.datetime64(d, 'D') for d in holidays]
    df_in['Dias Úteis'] = np.busday_count(np.datetime64(ref_dt, 'D'), end_date3, holidays=holidays3)

    df_in = df_in[df_in['Dias Úteis'] > 0]
    
    # Caixa inicial
    caixa = 254_252.97 + 3_433_040.33

    # Reindex para ter todos os dias úteis na sequência
    df_in2 = df_in.set_index('Dias Úteis').reindex(np.arange(0, df_in['Dias Úteis'].max() + 1)).fillna(0).copy()

    cash_cols = ['Valor de Vencimento', 'Valor Atual', 'Valor Líquido', 'Recebimento Líquido']
    for col in cash_cols:
        df_in2.loc[0, col] = caixa if col == 'Valor Líquido' else 0 # Caixa entra como valor líquido inicial

    # Ajustando colunas para o cumulativo
    cumulative_cols = ['Valor de Vencimento', 'Valor Atual', 'Valor de PDD', 'CUSTO_TOTAL', 'Valor Líquido', 'Recebimento Líquido']
    for col in cumulative_cols:
        df_in2[f'[CUM] {col}'] = df_in2[col].cumsum()
        
    # Cálculo final do % PL
    # Evitando divisão por zero se o máximo for 0
    max_liquido_cum = df_in2['[CUM] Valor Líquido'].max()
    if max_liquido_cum > 0:
        df_in2['% PL'] = df_in2['[CUM] Valor Líquido'] / max_liquido_cum
    else:
        df_in2['% PL'] = 0


    # 4) Geração do Relatório HTML
    gerar_relatorio_html(
        df=df_in2,
        empresa=NOME_EMPRESA,
        fundo=NOME_FUNDO,
        ref_date=ref_dt,
        output_dir=DIRETORIO_SAIDA
    )

except FileNotFoundError as e:
    print(f"\n[ERRO FATAL] {e}")
except Exception as e:
    print(f"\n[ERRO INESPERADO] Ocorreu um erro durante a execução: {e}")