## versao antes de incluir o PL
#>><<<>>><<<>><<><<<<><><<<><<<><><>
#* MbyLRdA
# %% bibliotecas
import pandas as pd
import glob
from pathlib import Path
import os
from datetime import datetime, date
import numpy as np
import configparser
import re # Importado para usar expressões regulares

# ==============================================================================
# <<< NOVO BLOCO >>> Carregar configurações do arquivo config.cfg
# ==============================================================================
config = configparser.ConfigParser()
config_file = 'config.cfg'

if not os.path.exists(config_file):
    raise FileNotFoundError(f"Erro: Arquivo de configuração '{config_file}' não encontrado.")

config.read(config_file)

try:
    # Carrega o caminho raiz principal
    ROOT_PATH = Path(config.get('Paths', 'root_path'))

    # Carrega caminhos específicos para este script
    PASTA_DADOS_VENCIDOS = ROOT_PATH / config.get('PathsVencidos', 'pasta_dados_vencidos')
    PASTA_RESULTADOS_VENCIDOS = ROOT_PATH / config.get('PathsVencidos', 'pasta_resultados_vencidos')

    # Carrega parâmetros específicos para este script
    PADRAO_ARQUIVO_ESTOQUE = config.get('ParametersVencidos', 'padrao_arquivo_estoque')
    ENCODING_LEITURA = config.get('ParametersVencidos', 'encoding_leitura')

except (configparser.NoSectionError, configparser.NoOptionError) as e:
    raise ValueError(f"Erro ao ler o arquivo de configuração. Verifique se as seções '[PathsVencidos]' e '[ParametersVencidos]' e suas chaves estão corretas. Detalhe: {e}")


# ==============================================================================
# ETAPA 1: CÓDIGO PARA CRIAR O df_final (AGORA USANDO CONFIGS)
# ==============================================================================

# <<< BLOCO ALTERADO COM DIAGNÓSTICO FINAL >>>
caminho_pasta = str(PASTA_DADOS_VENCIDOS)
padrao_config = PADRAO_ARQUIVO_ESTOQUE
lista_arquivos = []

try:
    # Lógica para criar o padrão regex
    partes = padrao_config.split()
    partes_processadas = [re.escape(p).replace('\\*', '.*') for p in partes]
    padrao_regex_final_str = r'\s+'.join(partes_processadas)
    regex = re.compile(padrao_regex_final_str, re.IGNORECASE)

    # --- NOVO BLOCO DE DIAGNÓSTICO APRIMORADO ---
    print("\n--- INICIANDO DIAGNÓSTICO FINAL DE ARQUIVOS ---")
    print(f"Pasta de dados configurada: {caminho_pasta}")
    print(f"Padrão regex utilizado: {regex.pattern}")

    # 1. VERIFICA SE A PASTA EXISTE
    if not os.path.isdir(caminho_pasta):
        print(f"\nERRO CRÍTICO: O caminho '{caminho_pasta}' não existe ou não é uma pasta.")
        print("Por favor, verifique as chaves 'root_path' e 'pasta_dados_vencidos' no seu arquivo config.cfg.")
    else:
        print("\nSUCESSO: A pasta foi encontrada.")
        
        # 2. LISTA TODO O CONTEÚDO DA PASTA
        try:
            conteudo_da_pasta = os.listdir(caminho_pasta)
            if not conteudo_da_pasta:
                print("AVISO: A pasta está vazia.")
            else:
                print("\nConteúdo encontrado na pasta:")
                for item in conteudo_da_pasta:
                    print(f"  - {item}")

                # 3. FILTRA APENAS OS ARQUIVOS .CSV E APLICA O REGEX
                print("\nAnalisando arquivos .csv e aplicando o filtro:")
                arquivos_csv_encontrados = [f for f in conteudo_da_pasta if f.lower().endswith('.csv')]
                
                if not arquivos_csv_encontrados:
                    print("Nenhum arquivo com extensão .csv foi encontrado na pasta.")
                else:
                    for basename in arquivos_csv_encontrados:
                        match_result = "MATCH" if regex.search(basename) else "NO MATCH"
                        print(f"  - Arquivo: '{basename}' -> Resultado: {match_result}")
                        if regex.search(basename):
                            lista_arquivos.append(os.path.join(caminho_pasta, basename))

        except Exception as e:
            print(f"\nERRO ao tentar ler o conteúdo da pasta: {e}")

    print("\n--- FIM DO DIAGNÓSTICO ---\n")
    # --- FIM DO BLOCO DE DIAGNÓSTICO ---

except Exception as e:
    print(f"Erro ao construir o padrão de busca ou listar arquivos: {e}")
    lista_arquivos = []
# --- FIM DO BLOCO ALTERADO ---


if not lista_arquivos:
    print("---------------------------------------------------------------------")
    print(f"Nenhum arquivo correspondente ao padrão foi encontrado no caminho '{caminho_pasta}'")
    df_final = pd.DataFrame()
else:
    print("Arquivos que serão lidos:")
    for arquivo in lista_arquivos:
        print(os.path.basename(arquivo))

    # %% Listas de colunas e leitura (lógica do script, permanece aqui)
    colunas_data = ['DataEmissao', 'DataAquisicao', 'DataVencimento', 'DataGeracao']
    colunas_texto = [
        'Situacao', 'PES_TIPO_PESSOA', 'CedenteCnpjCpf', 'TIT_CEDENTE_ENT_CODIGO',
        'CedenteNome', 'Cnae', 'SecaoCNAEDescricao', 'NotaPdd', 'SAC_TIPO_PESSOA',
        'SacadoCnpjCpf', 'SacadoNome', 'IdTituloVortx', 'TipoAtivo', 'NumeroBoleto',
        'NumeroTitulo', 'CampoChave', 'PagamentoParcial', 'Coobricacao',
        'CampoAdicional1', 'CampoAdicional2', 'CampoAdicional3', 'CampoAdicional4',
        'CampoAdicional5', 'IdTituloVortxOriginador', 'Registradora',
        'IdContratoRegistradora', 'IdTituloRegistradora', 'CCB', 'Convênio'
    ]
    dtype_texto = {col: str for col in colunas_texto}

    lista_dfs = []
    for arquivo in lista_arquivos:
        try:
            df_temp = pd.read_csv(
                arquivo,
                sep=';',
                decimal=',',
                encoding=ENCODING_LEITURA,
                parse_dates=colunas_data,
                dayfirst=True,
                dtype=dtype_texto
            )
            lista_dfs.append(df_temp)
            print(f"Sucesso: Arquivo '{os.path.basename(arquivo)}' lido e adicionado.")
        except Exception as e:
            print(f"ERRO ao processar o arquivo '{os.path.basename(arquivo)}': {e}")

    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True)
        print("\n------------------------------------------------------")
        print("DataFrame `df_final` criado com sucesso!")
    else:
        print("\nNenhum arquivo foi lido.")
        df_final = pd.DataFrame()

# ==============================================================================
# ETAPA 2: GERAÇÃO DO RELATÓRIO COM A LÓGICA CORRIGIDA
# ==============================================================================

# %% Variaveis fixas e caminhos para o relatório
TARGET_DATE = date.today()

# %% Funções de processamento e geração do relatório
def overdue_evolution_pandas(df_input: pd.DataFrame, ref_date: date) -> pd.DataFrame:
    if df_input.empty:
        print("[ALERTA] DataFrame de entrada vazio.")
        return pd.DataFrame()

    rename_map = {
        "Convênio": "Nome do Ente Consignado",
        "DataVencimento": "Data de Vencimento",
        "Situacao": "Situação",
        "ValorPresente": "Valor Atual"
    }
    cols_to_use = list(rename_map.keys())
    if not all(col in df_input.columns for col in cols_to_use):
        print(f"[ERRO] Colunas essenciais não encontradas.")
        return pd.DataFrame()

    full = df_input[cols_to_use].copy()
    full.rename(columns=rename_map, inplace=True)

    full["Data de Vencimento"] = pd.to_datetime(full["Data de Vencimento"], errors='coerce')
    full['Valor Atual'] = pd.to_numeric(full['Valor Atual'], errors='coerce')

    full.dropna(subset=["Nome do Ente Consignado", "Data de Vencimento", "Valor Atual"], inplace=True)

    print("\nCalculando vencidos com base na data de referência...")
    ref_timestamp = pd.to_datetime(ref_date)

    is_overdue = full['Data de Vencimento'].dt.normalize() < ref_timestamp
    venc = full[is_overdue].copy()
    print(f"Encontrados {len(venc)} títulos vencidos.")

    if venc.empty:
        print("[INFO] Nenhum item vencido encontrado para a data de referência.")
        return pd.DataFrame()

    # <<< ALTERAÇÃO AQUI: Agrupamento por Mês >>>
    # 1. Cria uma nova coluna que representa o período (Mês/Ano) do vencimento.
    #    Usar `dt.to_period('M')` garante que os meses serão ordenados cronologicamente.
    venc['MesVencimento'] = venc['Data de Vencimento'].dt.to_period('M')
    
    # 2. Agrupa os dados pelo novo campo 'MesVencimento' em vez da data exata.
    grp = venc.groupby(["Nome do Ente Consignado", "MesVencimento"])["Valor Atual"].sum().reset_index()
    
    # 3. Cria a tabela dinâmica (pivot) usando os meses como colunas.
    report = grp.pivot_table(
        index="Nome do Ente Consignado",
        columns="MesVencimento",
        values="Valor Atual",
        fill_value=0
    )
    # <<< FIM DA ALTERAÇÃO >>>

    if report.empty:
        return pd.DataFrame()

    report["Total"] = report.sum(axis=1)
    report = report[report["Total"] >= 20].copy()

    total_final = report["Total"].sum()
    if total_final > 0:
        report["Total (%)"] = ((report["Total"] / total_final) * 100).round(2)
    else:
        report["Total (%)"] = 0.0

    report.sort_values(by="Total", ascending=False, inplace=True)
    report.reset_index(inplace=True)

    coluna_de_texto = "Nome do Ente Consignado"
    extras = ["Total", "Total (%)"]
    # As colunas de data agora são períodos, então a lógica para encontrá-las permanece a mesma.
    outras_cols = [c for c in report.columns if c not in extras and c != coluna_de_texto]
    report = report[[coluna_de_texto] + outras_cols + extras]

    if not report.empty:
        total_geral_row = report.sum(axis=0, numeric_only=True)
        total_geral_row[coluna_de_texto] = "Total Geral"
        total_geral_row["Total (%)"] = 100.00
        report.loc["Total Geral"] = total_geral_row

    return report


#? ——— Bloco Principal de Geração do HTML ———

if 'df_final' in locals() and not df_final.empty:
    if 'DataGeracao' in df_final.columns and not df_final['DataGeracao'].isna().all():
        ref_date_obj = df_final['DataGeracao'].max().date()
        print(f"\nData de referência definida pela 'DataGeracao' mais recente: {ref_date_obj.strftime('%d/%m/%Y')}")
    else:
        ref_date_obj = date.today()
        print(f"\n[ALERTA] Usando a data de hoje como referência: {ref_date_obj.strftime('%d/%m/%Y')}")

    report = overdue_evolution_pandas(df_final, ref_date_obj)

    if not report.empty:
        print("\nRelatório de Vencidos (baseado em datas) processado.")

        #? --- Formatação e estilização para HTML ---
        ref_date_str = ref_date_obj.strftime("%Y-%m-%d")
        ref_date_display = ref_date_obj.strftime("%d/%m/%Y")
        data_relatorio_display = TARGET_DATE.strftime('%d/%m/%Y')

        # <<< ALTERAÇÃO AQUI: Formatação das colunas de Mês/Ano >>>
        # Converte as colunas, que são objetos de Período (ex: Period('2024-04', 'M')),
        # para o formato de string 'MM/YYYY'.
        new_cols = [c.strftime("%m/%Y") if isinstance(c, pd.Period) else c for c in report.columns]
        report.columns = new_cols
        # <<< FIM DA ALTERAÇÃO >>>

        coluna_de_texto = "Nome do Ente Consignado"
        val_cols = [c for c in report.columns if "%" not in str(c) and "Total" not in str(c) and c != coluna_de_texto]
        pct_col = ["Total (%)"]
        
        all_values = report.loc[report.index != 'Total Geral', val_cols].to_numpy().flatten()
        all_values = all_values[~np.isnan(all_values) & (all_values > 0)]

        if all_values.size > 0:
            vmin = 0
            vmax = np.percentile(all_values, 95)
            if vmax == 0: vmax = 1
        else:
            vmin, vmax = 0, 1
            
        global_gmap = (report[val_cols] - vmin) / (vmax - vmin)
        global_gmap = global_gmap.clip(0, 1)
        global_gmap = global_gmap.mask(report[val_cols].isna() | np.isclose(report[val_cols], 0))
        
        def fmt_val(x):
            if pd.isna(x) or np.isclose(x, 0): return ""
            return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        def fmt_pct(x):
            if pd.isna(x) or np.isclose(x, 0): return ""
            return f"{x:.2f}%".replace(".", ",")

        def highlight_total_geral(row):
            if row.name == 'Total Geral':
                return ['background-color: #e0e0e0; color: #000000; font-weight: bold;'] * len(row)
            return [''] * len(row)

        styler = (
            report.style
            .set_table_attributes('class="dataframe" style="border-collapse: collapse; border-spacing: 0; width: 100%; font-family: Arial, sans-serif;"')
            .set_properties(**{"padding": "6px", "text-align": "right"})
            .set_table_styles([
                # --- Estilos Originais (mantidos) ---
                {"selector": "th, td", "props": [("border", "1px solid #999")]},
                {"selector": "th", "props": [("background-color", "#163f3f"), ("color", "#FFFFFF"), ("text-align", "center")]},
                {"selector": "caption", "props": [("caption-side", "bottom"), ("padding", "8px"), ("font-size", "1.1em"), ("color", "#0e5d5f")]},
                {"selector": ".dataframe td.na", "props": [("background-color", "transparent"), ("color", "#000000")]},

                # --- NOVOS ESTILOS PARA FIXAÇÃO ---
                # 1. Fixa todo o cabeçalho (thead) no topo
                {"selector": "thead th", "props": [("position", "sticky"), ("top", "0"), ("z-index", "1")]},

                # 2. Estiliza e fixa a primeira coluna ('Nome do Ente Consignado') à esquerda
                {"selector": "tbody td:first-child", "props": [
                    ("position", "sticky"),
                    ("left", "0"),
                    ("background-color", "#ffffff"), 
                    ("text-align", "left"),         
                    ("font-weight", "bold"),
                    # --- NOVAS PROPRIEDADES ---
                    ("white-space", "nowrap"),      # Impede a quebra de linha
                    ("overflow", "hidden"),         # Esconde o texto que transborda
                    ("text-overflow", "ellipsis"),  # Adiciona "..." no final
                    ("max-width", "30ch")           # Define a largura máxima
                ]},

                # 3. Garante que o canto superior esquerdo (cabeçalho da primeira coluna) fique fixo e sobreposto corretamente
                {"selector": "thead th:first-child", "props": [("position", "sticky"), ("left", "0"), ("z-index", "2")]},

            ], overwrite=False)
            .format({col: fmt_val for col in val_cols + ["Total"]} | {col: fmt_pct for col in pct_col})
            .background_gradient(cmap="RdYlGn_r", subset=val_cols, gmap=global_gmap, axis=None)
            .background_gradient(cmap="Blues",  subset=pct_col,  axis=None)
            .background_gradient(cmap="Greys", subset=["Total"], axis=None)
            .applymap(
                lambda v: 'background-color: transparent'
                if (pd.isna(v) or v == 0) else '',
                subset=val_cols + pct_col + ["Total"]
            )
            .apply(highlight_total_geral, axis=1)
            .hide(axis="index")
        )
        
        header_html = f"""
        <div style="font-family: Arial, sans-serif; margin-bottom: 20px;">
            <h2 style="color: #163f3f;">Relatório de Evolução de Vencidos</h2>
            <p><strong>Data do relatório:</strong> {data_relatorio_display} | <strong>Data de referência dos dados:</strong> {ref_date_display}</p>
        </div>
        """
        table_html = styler.to_html()
        
        # Ocultado para brevidade, pois a lógica não muda
        style_script_html = """"""

        final_html = header_html + table_html + style_script_html

        output_dir = PASTA_RESULTADOS_VENCIDOS
        output_dir.mkdir(parents=True, exist_ok=True)
        today_str = TARGET_DATE.strftime("%Y-%m-%d")
        file_name = f"{today_str}_{ref_date_str}-relatorio-de-vencidos.html"
        html_path = output_dir / file_name
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(final_html)

        print(f"\nRelatório HTML gerado com sucesso em: {html_path}")

    else:
        print("\nO relatório final está vazio. Nenhum arquivo HTML foi gerado.")

else:
    print("\nO script não pôde continuar porque o `df_final` não foi criado ou está vazio.")
