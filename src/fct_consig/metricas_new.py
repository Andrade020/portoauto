
#%%
# =============================================================================
# Bloco 1: Importação de Bibliotecas e Configurações Iniciais
# =============================================================================
# Descrição: Importa as bibliotecas necessárias para a análise (pandas, glob, numpy, matplotlib)
# e define configurações de exibição para o pandas, garantindo que mais colunas e
# linhas sejam mostradas nas saídas.

import pandas as pd
from glob import glob
import numpy as np
import matplotlib.pyplot as plt

# Configurações de exibição do Pandas
pd.options.display.max_columns = 100
pd.options.display.max_rows = 200


#%%
# =============================================================================
# Bloco 2: Carregamento e Leitura dos Dados de Estoque
# =============================================================================
# Descrição: Localiza todos os arquivos CSV de estoque, define os tipos de dados
# para colunas específicas (data, texto, numérico) para otimizar a leitura e,
# em seguida, carrega e concatena todos os arquivos em um único DataFrame.

# Padrão para encontrar os arquivos de estoque
caminho_feriados = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\feriados_nacionais.xls'
patt = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\data_temp\fct_2025-07-14\*Estoque*.csv'  # Usando o caminho mais específico do primeiro script
list_files = glob(patt)
print("Arquivos encontrados:", list_files)

# Definição das colunas por tipo de dado
colunas_data = [
    'DataEmissao', 'DataAquisicao', 'DataVencimento', 'DataGeracao'
]

colunas_texto = [
    'Situacao', 'PES_TIPO_PESSOA', 'CedenteCnpjCpf', 'TIT_CEDENTE_ENT_CODIGO',
    'CedenteNome', 'Cnae', 'SecaoCNAEDescricao', 'NotaPdd', 'SAC_TIPO_PESSOA',
    'SacadoCnpjCpf', 'SacadoNome', 'IdTituloVortx', 'TipoAtivo', 'NumeroBoleto',
    'NumeroTitulo', 'CampoChave', 'PagamentoParcial', 'Coobricacao',
    'CampoAdicional1', 'CampoAdicional2', 'CampoAdicional3', 'CampoAdicional4',
    'CampoAdicional5', 'IdTituloVortxOriginador', 'Registradora',
    'IdContratoRegistradora', 'IdTituloRegistradora', 'CCB', 'Convênio'
]

# Dicionário de tipos para colunas de texto
dtype_texto = {col: str for col in colunas_texto}

# Leitura e concatenação dos arquivos CSV
dfs = []
for file in list_files:
    print(f"Lendo o arquivo: {file}")
    df_ = pd.read_csv(file, sep=';', encoding='latin1', dtype=dtype_texto,
                      decimal=',', parse_dates=colunas_data, dayfirst=True)
    dfs.append(df_)

df_final = pd.concat(dfs, ignore_index=True)
print("Leitura e concatenação concluídas.")


#%%
# =============================================================================
# Bloco 3: Engenharia de Atributos (Criação de Colunas Auxiliares)
# =============================================================================
# Descrição: Cria novas colunas no DataFrame para facilitar análises futuras.
# - _ValorLiquido: Valor Presente ajustado pelo PDD.
# - _ValorVencido: Identifica o valor de títulos já vencidos.
# - _MuitosContratos: Flag para sacados com um número elevado de contratos (CCB).
# - _MuitosEntes: Flag para sacados associados a múltiplos convênios.

# Criar coluna de Valor Líquido
df_final['_ValorLiquido'] = df_final['ValorPresente'] - df_final['PDDTotal']

# Criar coluna de Valor Vencido
df_final['_ValorVencido'] = (df_final['DataVencimento'] <= df_final['DataGeracao']).astype('int') * df_final['ValorPresente']

# Identificar sacados com muitos contratos (CCB)
sacado_contratos = df_final.groupby('SacadoNome')['CCB'].nunique()
k = 3
mask_contratos = sacado_contratos >= k
sacado_contratos_alto = sacado_contratos[mask_contratos].index
df_final['_MuitosContratos'] = df_final['SacadoNome'].isin(sacado_contratos_alto).astype(str)

# Identificar sacados com muitos entes (Convênios)
sacados_entes = df_final.groupby('SacadoCnpjCpf')['Convênio'].nunique()
k2 = 3
mask_entes = sacados_entes >= k2
sacados_entes_alto = sacados_entes[mask_entes].index
df_final['_MuitosEntes'] = df_final['SacadoCnpjCpf'].isin(sacados_entes_alto).astype(str)

print("Criação de colunas auxiliares concluída.")


#%%
# =============================================================================
# Bloco 4: Verificação Inicial do DataFrame
# =============================================================================
# Descrição: Realiza uma verificação rápida do DataFrame, mostrando o uso de
# memória e o valor total do estoque (Valor Presente).

memoria_mb = df_final.memory_usage(deep=True).sum() / 1024**2
print(f"Uso de memória do DataFrame: {memoria_mb:.2f} MB")

valor_total_estoque = df_final["ValorPresente"].sum()
print(f"Valor Presente Total do Estoque: R$ {valor_total_estoque:_.2f}".replace('.', ',').replace('_', '.'))


#%%
# =============================================================================
# Bloco 5: Análise Exploratória Geral com value_counts()
# =============================================================================
# Descrição: Itera sobre as colunas de texto para entender a distribuição de suas
# categorias. As perguntas no comentário guiam esta exploração.

# Comentários / dúvidas do segundo script:
# 1. [Situacao] - diferença entre sem cobrança e aditado
# 2. [SAC_TIPO_PESSOA] - tipo J = jurídico?? tem isso?
# 3. [SacadoCnpjCpf] - por que tem CNPJ?
# 4. [SacadoNome] 'BMP SOCIEDADE DE CREDITO DIRETO S.A'
# 5. [SacadoCnpjCpf'] - verificar consistência dos CPFs
# 6. [TipoAtivo] - CCB e Contrato. Por que tem contrato?
# 7. [DataGeracao] - data de referência ou data de processamento?
# 8. [SacadoCnpjCpf] - tem sacado com 1040 linhas (!)
# 9. [Convênio] - sacados com muitos convênios (3)

df_final2 = df_final[~df_final['Situacao'].isna()].copy()

print("Analisando a contagem de valores para colunas de texto (geral):")
for col in df_final2.select_dtypes(include=['object']).columns:
    print(f"--- Análise da coluna: {col} ---")
    print(df_final2[col].value_counts(dropna=False))
    print('*' * 80)


#%%
# =============================================================================
# Bloco 6: Verificação do Impacto de Linhas com 'Situacao' Nula
# =============================================================================
# Descrição: Analisa se as linhas com 'Situacao' nula têm impacto significativo
# no cálculo do Valor Presente total.

print("Verificando o impacto das linhas com 'Situacao' nula...")
v1_ = df_final['ValorPresente'].sum()
v2_ = df_final[~df_final['Situacao'].isna()]['ValorPresente'].sum()
print(f"Valores são próximos? {np.isclose(v1_, v2_)}")

# Libera memória
del df_final


#%%
# =============================================================================
# Bloco 7: Análise de Sacados com SAC_TIPO_PESSOA == 'J'
# =============================================================================
# Descrição: Isola e analisa registros onde o tipo de sacado é 'J'.

print("Analisando sacados com tipo 'J'...")
df_sacado_J = df_final2[df_final2['SAC_TIPO_PESSOA'] == 'J']
if not df_sacado_J.empty:
    print("Tamanhos de 'SacadoCnpjCpf' para tipo 'J':", df_sacado_J['SacadoCnpjCpf'].map(len).unique())
    print(df_sacado_J.sample(min(5, len(df_sacado_J))))
    #! DISPLAY, NO JUPYTER
else:
    print("Nenhum sacado do tipo 'J' encontrado.")


#%%
# =============================================================================
# Bloco 8: Análise Específica de Sacados com CNPJ
# =============================================================================
# Descrição: Filtra sacados com CNPJ e realiza uma análise detalhada sobre eles,
# incluindo uma nova verificação de value_counts() para todas as colunas
# categóricas dentro deste subconjunto.

print("Analisando sacados com CNPJ...")
df_final2_cnpj = df_final2[df_final2['SacadoCnpjCpf'].map(len) == 18].copy()

if not df_final2_cnpj.empty:
    print("\nNomes únicos de sacados com CNPJ:")
    print(df_final2_cnpj['SacadoNome'].unique())

    # >>> VERIFICAÇÃO EXTRA ADICIONADA AQUI <<<
    print("\n--- value_counts() para o subconjunto de Sacados com CNPJ ---")
    for col in df_final2_cnpj.select_dtypes(include=['object']).columns:
        print(f"--- Análise da coluna: {col} (Apenas CNPJ) ---")
        print(df_final2_cnpj[col].value_counts(dropna=False))
        print('*'*80)

    print("\nEstatísticas descritivas para dados numéricos de sacados com CNPJ:")

    print(df_final2_cnpj.describe(include=[np.number]))
    #! DISPLAY, NO JUPYTER
    print("\nSoma dos valores numéricos:")

    print(df_final2_cnpj.select_dtypes(include=[np.number]).sum())
    #! DISPLAY, NO JUPYTER

    # Exportar para Excel
    # df_final2_cnpj.to_excel('df_final2_cnpj.xlsx')
else:
    print("Nenhum sacado com formato de CNPJ (18 caracteres) encontrado.")


#%%
# =============================================================================
# Bloco 9: Validação de CPFs na Coluna 'SacadoCnpjCpf'
# =============================================================================
# Descrição: Aplica uma função para validar a estrutura matemática de CPFs.

def validar_cpf(cpf):
    """Função para validar um número de CPF."""
    cpf = ''.join(filter(str.isdigit, str(cpf)))
    if len(cpf) != 11 or cpf == cpf[0] * 11: return False
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    resto = (soma * 10) % 11
    if resto == 10: resto = 0
    if resto != int(cpf[9]): return False
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    resto = (soma * 10) % 11
    if resto == 10: resto = 0
    if resto != int(cpf[10]): return False
    return True

mask_cpf = df_final2['SacadoCnpjCpf'].map(len) == 14
df_final2['CPF_válido'] = False
df_final2.loc[mask_cpf, 'CPF_válido'] = df_final2.loc[mask_cpf, 'SacadoCnpjCpf'].apply(validar_cpf)
cpfs_invalidos = df_final2[(mask_cpf) & (~df_final2['CPF_válido'])]['SacadoCnpjCpf'].unique()
print(f"Encontrados {len(cpfs_invalidos)} CPFs com estrutura inválida.")


#%%
# =============================================================================
# Bloco 10: Análise do Percentual de PDD por Variável Categórica
# =============================================================================
# Descrição: Calcula o percentual de PDD agrupado por diversas variáveis
# categóricas para identificar categorias de maior risco.

cat_cols = [
    'Situacao', 'CedenteNome', 'SAC_TIPO_PESSOA', 'PagamentoParcial',
    'TipoAtivo', '_MuitosContratos', '_MuitosEntes', 'Convênio'
]

pdd_ref = (1 - df_final2['_ValorLiquido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"PDD de Referência (Total): {pdd_ref:.2f}%\n")

for col in cat_cols:
    print(f"--- Análise de PDD por '{col}' ---")
    aux_ = df_final2.groupby(col)[['_ValorLiquido', 'ValorPresente']].sum() / 1e6
    aux_['%PDD'] = (1 - aux_['_ValorLiquido'] / aux_['ValorPresente']) * 100
    if col == 'Convênio':
        aux_ = aux_.sort_values('ValorPresente', ascending=False)
    print(aux_.head(20))
    #! DISPLAY, NO JUPYTER
    print("\n" + "="*80 + "\n")


#%%
# =============================================================================
# Bloco 11: Análise do Percentual de Títulos Vencidos por Variável Categórica
# =============================================================================
# Descrição: Calcula a proporção do valor vencido em relação ao Valor Presente
# para cada categoria, identificando os grupos com maior inadimplência.

venc_ref = (df_final2['_ValorVencido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"Percentual de Vencidos de Referência (Total): {venc_ref:.2f}%\n")

for col in cat_cols:
    print(f"--- Análise de Vencidos por '{col}' ---")
    aux_ = df_final2.groupby(col)[['_ValorVencido', 'ValorPresente']].sum() / 1e6
    aux_['%Vencido'] = (aux_['_ValorVencido'] / aux_['ValorPresente']) * 100
    if col == 'Convênio':
        aux_ = aux_.sort_values('ValorPresente', ascending=False)
    else:
        aux_ = aux_.sort_values('%Vencido', ascending=False)
    print(aux_.head(20))
    #! DISPLAY, NO JUPYTER
    print("\n" + "="*80 + "\n")


#%%
# =============================================================================
# Bloco 12: Verificação de Sacados Presentes em Múltiplos Convênios ('Entes')
# =============================================================================
# Descrição: Identifica sacados com pulverização entre diferentes parceiros.

print("Analisando sacados presentes em múltiplos convênios...")
sacados_multi_entes = df_final2.groupby('SacadoCnpjCpf')['Convênio'].agg(['nunique', pd.unique])
sacados_multi_entes = sacados_multi_entes.sort_values('nunique', ascending=False)

print(sacados_multi_entes.head(35))
#! DISPLAY, NO JUPYTER

#%%
# =============================================================================
# Bloco 13: Verificação de Consistência das Datas
# =============================================================================
# Descrição: Realiza verificações de sanidade nas colunas de data.

print("Verificando consistência das datas...")
check1 = (df_final2['DataEmissao'] > df_final2['DataAquisicao']).sum()
print(f"Registros com Data de Emissão > Data de Aquisição: {check1}")
check2 = (df_final2['DataAquisicao'] > df_final2['DataVencimento']).sum()
print(f"Registros com Data de Aquisição > Data de Vencimento: {check2}")


#%%
# =============================================================================
# Bloco 14: Verificação de Consistência dos Valores Monetários
# =============================================================================
# Descrição: Realiza verificações de sanidade nos valores financeiros.

print("\nVerificando consistência dos valores...")
check_v1 = (df_final2['ValorAquisicao'] > df_final2['ValorNominal']).sum()
print(f"Registros com Valor de Aquisição > Valor Nominal: {check_v1}")
check_v2 = (df_final2['ValorAquisicao'] > df_final2['ValorPresente']).sum()
print(f"Registros com Valor de Aquisição > Valor Presente: {check_v2}")
check_v3 = (df_final2['ValorPresente'] > df_final2['ValorNominal']).sum()
print(f"Registros com Valor Presente > Valor Nominal: {check_v3}")


#%%
# =============================================================================
# Bloco 15: Análise Focada de Métricas de Desempenho (PDD e Vencidos)
# =============================================================================
# Descrição: Este bloco executa uma análise direcionada das métricas de PDD
# e Vencidos, segmentando os resultados por Cedente, Tipo de Contrato (TipoAtivo)
# e Ente Consignado (Convênio), conforme solicitado.

# *****************************************************************************
# * BLOCO DE EXPORTAÇÃO (FINAL, COM AJUSTES FINAIS DE ESTILO)
# *****************************************************************************
import base64
import os

print("\n" + "="*80)
print("GERANDO RELATÓRIO HTML FINAL COM AJUSTES DE ESTILO")
print("="*80)

# --- 1. PREPARAÇÃO DOS ATIVOS (LOGO E DATA) ---
def encode_image_to_base64(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except FileNotFoundError:
        print(f"[AVISO] Arquivo de imagem não encontrado em: {image_path}. A logo não será exibida.")
        return None

logo_path = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\images\logo_inv.png'
logo_base64 = encode_image_to_base64(logo_path)
report_date = ref_date.strftime('%d/%m/%Y')

# --- 2. DEFINIÇÃO DO CSS COMPLETO ---

# MODIFICADO AQUI: Ajustes finais no CSS do cabeçalho
html_css = """
<style>
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

# --- 3. CONSTRUÇÃO DO HTML COMPLETO ---

html_parts = []
html_parts.append("<!DOCTYPE html><html lang='pt-BR'><head>")
html_parts.append("<meta charset='UTF-8'><title>Análise de Desempenho - FCT Consignado II</title>")
html_parts.append(html_css)
html_parts.append("</head><body>")

# --- Adiciona o Cabeçalho ---
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
mapa_descricoes = {
    'Cedentes': 'Analisa as métricas de risco e retorno agrupadas por cada Cedente (originador) dos títulos.', 'Tipo de Contrato': 'Agrupa os dados por Tipo de Ativo (CCB, Contrato) para comparar o desempenho de cada um.', 'Ente Consignado': 'Métricas detalhadas por cada Convênio (ente público ou privado) onde a consignação é feita.', 'Situação': 'Compara o desempenho dos títulos com base na sua situação atual (ex: Aditado, Liquidado).', 'Tipo de Pessoa Sacado': 'Diferencia a análise entre sacados Pessoa Física (F) e Jurídica (J).', 'Pagamento Parcial': 'Verifica se há impacto nas métricas para títulos que aceitam pagamento parcial.', 'Tem Muitos Contratos': 'Compara sacados com um número baixo vs. alto de contratos (CCBs) ativos.', 'Tem Muitos Entes': 'Compara sacados que operam em poucos vs. múltiplos convênios (entes).', 'Sacado é BMP': 'Isola e analisa especificamente as operações com o sacado BMP S.A.', 'Nível do Ente': 'Agrupa os convênios por nível governamental (Municipal, Estadual, Federal).', 'Previdência': 'Identifica e analisa separadamente os convênios que são de regimes de previdência.', 'Ente Genérico': 'Agrupa convênios que não se encaixam em uma categoria específica.', 'CAPAG': 'Métricas baseadas na Capacidade de Pagamento (CAPAG) do município ou estado, uma nota do Tesouro Nacional.', 'Faixa Pop. Municipal': 'Agrupa os convênios municipais por faixas de população.', 'Faixa Pop. Estadual': 'Agrupa os convênios estaduais por faixas de população.', 'UF': 'Agrega todas as métricas por Unidade Federativa (Estado).'
}
html_parts.append("<div class='container-botoes'>")
dimensoes_ordem_alfabetica = ['Faixa Pop. Municipal', 'Faixa Pop. Estadual', 'CAPAG']
vp_col_name = 'Valor Presente \n(R$ MM)'
vl_col_name = 'Valor Líquido \n(R$ MM)'
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns or df_final2[coluna].isnull().all(): continue
    print(f"--> Processando e gerando HTML para o botão: '{nome_analise}'")
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
    colunas_ordem = [nome_analise, vl_col_name, vp_col_name]
    if 'Ticket Ponderado (R$)' in df_final.columns: colunas_ordem.append('Ticket Ponderado (R$)')
    colunas_ordem.extend(['%PDD', '%Vencido'])
    colunas_tir_existentes = sorted([col for col in df_tir.columns if col in df_final.columns and 'TIR' in col])
    colunas_finais = colunas_ordem + colunas_tir_existentes
    outras_colunas = [col for col in df_final.columns if col not in colunas_finais]
    df_final = df_final[colunas_finais + outras_colunas]
    if nome_analise in dimensoes_ordem_alfabetica:
        df_final = df_final.sort_values(nome_analise, ascending=True).reset_index(drop=True)
    else:
        df_final = df_final.sort_values(vp_col_name, ascending=False).reset_index(drop=True)
    formatters = { vl_col_name: lambda x: f'{x:,.2f}', vp_col_name: lambda x: f'{x:,.2f}', 'Ticket Ponderado (R$)': lambda x: f'R$ {x:,.2f}', '%PDD': lambda x: f'{x:,.2f}%', '%Vencido': lambda x: f'{x:,.2f}%', }
    for col in colunas_tir_existentes: formatters[col] = lambda x: f'{x:,.2f}%'
    df_final.columns = [col.replace('\n', '<br>') for col in df_final.columns]
    html_parts.append("<details>")
    descricao = mapa_descricoes.get(nome_analise, 'Descrição não disponível.')
    html_parts.append(f'<summary title="{descricao}">{nome_analise}</summary>')
    html_parts.append("<div class='content-wrapper'>")
    html_table = df_final.to_html(index=False, classes='dataframe', formatters=formatters, na_rep='-', escape=False)
    html_parts.append(html_table)
    html_parts.append("</div></details>")
html_parts.append("</div>")
html_parts.append("</div>")

# --- Adiciona o Rodapé ---
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

# --- 4. SALVA O ARQUIVO FINAL ---
final_html_content = "\n".join(html_parts)
html_output_filename = os.path.join(output_path, 'analise_metricas_consolidadas.html')
try:
    with open(html_output_filename, 'w', encoding='utf-8') as f:
        f.write(final_html_content)
    print("\n" + "="*80)
    print("ANÁLISE CONCLUÍDA COM SUCESSO!")
    print(f"O relatório HTML final foi salvo em: {html_output_filename}")
    print("="*80)
except Exception as e:
    print(f"\n[ERRO GRAVE] Não foi possível salvar o arquivo HTML: {e}")