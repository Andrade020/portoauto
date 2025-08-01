# =============================================================================
# Bloco 1: Importações e Configurações
# =============================================================================
# Descrição: Importa as bibliotecas necessárias e define os caminhos dos
# arquivos de entrada (Excel) e saída (XLSX).
import pandas as pd
import re
from thefuzz import fuzz # %pip install thefuzz

# --- ARQUIVOS (Altere se necessário) ---
# Caminho para o arquivo Excel com os nomes de referência
caminho_excel_referencia = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\nomes_municipios.xls'
# Nome do arquivo Excel que será gerado com os resultados
arquivo_saida_xlsx = 'correspondencia_convenios.xlsx'


# =============================================================================
# Bloco 2: Carregamento dos Dados de Referência
# =============================================================================
# Descrição: Carrega as abas 'MUNICÍPIOS' e 'BRASIL E UFs' do arquivo Excel
# para DataFrames do pandas, lendo todas as colunas como texto (str).
try:
    # Carrega a lista de municípios, forçando todas as colunas a serem lidas como string
    df_municipios = pd.read_excel(caminho_excel_referencia, sheet_name='MUNICÍPIOS', header=1, dtype=str)
    print(f"Sucesso: Carregados {len(df_municipios)} municípios da aba 'MUNICÍPIOS' (todas as colunas como texto).")

    # Carrega a lista de UFs, forçando todas as colunas a serem lidas como string
    df_ufs = pd.read_excel(caminho_excel_referencia, sheet_name='BRASIL E UFs', header=1, dtype=str)
    # Renomeia a primeira coluna para um nome fixo
    nome_coluna_uf = df_ufs.columns[0]
    df_ufs = df_ufs.rename(columns={nome_coluna_uf: 'NOME DA UF'})
    print(f"Sucesso: Carregadas {len(df_ufs)} UFs da aba 'BRASIL E UFs' (todas as colunas como texto).")

except FileNotFoundError:
    print(f"[ERRO] O arquivo de referência não foi encontrado em: {caminho_excel_referencia}")
    # Encerra a execução se o arquivo não for encontrado
    df_municipios = pd.DataFrame() 
    df_ufs = pd.DataFrame()


# =============================================================================
# Bloco 3: Funções de Apoio (Categorização e Busca)
# =============================================================================
# Descrição: Define as funções que irão limpar e categorizar os nomes dos
# convênios e a função que encontrará os nomes mais próximos usando Levenshtein.

# Listas de prefixos para identificação
prefixos_prefeitura = ["PREFEITURA MUNICIPAL DE", "PREFEITURA MUNICIPAL", "PREFEITURA", "PREF.", "PREV."]
prefixos_governo = ["GOV.", "GOVERNO DE", "GOVERNO DO ESTADO"]

def categorizar_e_limpar_convenio(nome):
    """
    Categoriza um convênio e remove prefixos conhecidos.
    Retorna uma tupla com (nome limpo, categoria).
    """
    nome_upper = nome.upper().strip()

    for prefixo in prefixos_prefeitura:
        if nome_upper.startswith(prefixo):
            nome_limpo = nome_upper.replace(prefixo, "", 1).strip()
            return nome_limpo, "Prefeitura"

    for prefixo in prefixos_governo:
        if nome_upper.startswith(prefixo):
            nome_limpo = nome_upper.replace(prefixo, "", 1).strip()
            return nome_limpo, "Governo"

    return nome_upper, "Outros"


def encontrar_top_5_matches(nome_a_procurar, df_referencia, coluna_referencia):
    """
    Calcula a similaridade Levenshtein entre um nome e uma coluna de um DataFrame.
    Retorna um DataFrame com as 5 linhas mais similares.
    """
    if df_referencia.empty:
        return pd.DataFrame()

    df_referencia['similaridade'] = df_referencia[coluna_referencia].apply(
        lambda x: fuzz.ratio(nome_a_procurar, x.upper()) if pd.notna(x) else 0
    )

    top_5 = df_referencia.nlargest(5, 'similaridade')
    return top_5


# =============================================================================
# Bloco 4: Processamento Principal e Geração do Arquivo Excel
# =============================================================================
# Descrição: Itera sobre os convênios, busca as correspondências, armazena os
# resultados em listas e, ao final, escreve tudo em um único arquivo .xlsx
# com abas separadas para cada categoria.

if not df_municipios.empty:

    # 1. Obter e categorizar todos os convênios únicos
    convenios_unicos = df_final2["Convênio"].dropna().unique()
    convenios_categorizados = {
        "Prefeitura": [], "Governo": [], "Outros": []
    }
    for nome_original in convenios_unicos:
        nome_limpo, categoria = categorizar_e_limpar_convenio(nome_original)
        convenios_categorizados[categoria].append({'original': nome_original, 'limpo': nome_limpo})

    # 2. Listas para armazenar os DataFrames de resultado de cada categoria
    resultados_prefeituras = []
    resultados_governos = []
    resultados_outros = []

    print("\nIniciando busca por correspondências...")
    # --- Processamento das Prefeituras ---
    for item in convenios_categorizados["Prefeitura"]:
        matches = encontrar_top_5_matches(item['limpo'], df_municipios.copy(), 'NOME DO MUNICÍPIO')
        matches.insert(0, 'Nome_Processado', item['limpo'])
        matches.insert(0, 'Convenio_Original', item['original'])
        resultados_prefeituras.append(matches)

    # --- Processamento dos Governos ---
    for item in convenios_categorizados["Governo"]:
        matches = encontrar_top_5_matches(item['limpo'], df_ufs.copy(), 'NOME DA UF')
        matches.insert(0, 'Nome_Processado', item['limpo'])
        matches.insert(0, 'Convenio_Original', item['original'])
        resultados_governos.append(matches)

    # --- Processamento dos Outros (busca em ambas as fontes) ---
    for item in convenios_categorizados["Outros"]:
        # Busca em Municípios
        matches_mun = encontrar_top_5_matches(item['limpo'], df_municipios.copy(), 'NOME DO MUNICÍPIO')
        matches_mun.insert(0, 'Fonte_da_Busca', 'Municípios')
        matches_mun.insert(0, 'Nome_Processado', item['limpo'])
        matches_mun.insert(0, 'Convenio_Original', item['original'])
        resultados_outros.append(matches_mun)
        # Busca em UFs
        matches_uf = encontrar_top_5_matches(item['limpo'], df_ufs.copy(), 'NOME DA UF')
        matches_uf.insert(0, 'Fonte_da_Busca', 'UFs')
        matches_uf.insert(0, 'Nome_Processado', item['limpo'])
        matches_uf.insert(0, 'Convenio_Original', item['original'])
        resultados_outros.append(matches_uf)
    
    print("Busca finalizada. Gerando arquivo Excel...")

    # 3. Escrever os resultados consolidados no arquivo Excel
    with pd.ExcelWriter(arquivo_saida_xlsx, engine='openpyxl') as writer:
        if resultados_prefeituras:
            df_final_prefeituras = pd.concat(resultados_prefeituras, ignore_index=True)
            df_final_prefeituras.to_excel(writer, sheet_name='Prefeituras', index=False)

        if resultados_governos:
            df_final_governos = pd.concat(resultados_governos, ignore_index=True)
            df_final_governos.to_excel(writer, sheet_name='Governos', index=False)
            
        if resultados_outros:
            df_final_outros = pd.concat(resultados_outros, ignore_index=True)
            df_final_outros.to_excel(writer, sheet_name='Outros', index=False)

    print(f"\nProcesso concluído! O arquivo '{arquivo_saida_xlsx}' foi gerado com sucesso.")