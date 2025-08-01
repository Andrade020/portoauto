#%%
import pandas as pd

# --- PARTE 1: PROCESSAMENTO DE MUNICÍPIOS (TIPO == 1) ---
print("--- INICIANDO PROCESSAMENTO DE MUNICÍPIOS (TIPO 1) ---")

# 0- Leia o arquivo CSV principal
caminho_relacao_municipios = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\relacao_municipios_populacao.csv"
try:
    df_mun = pd.read_csv(caminho_relacao_municipios)
    print("DataFrame 'df_mun' carregado com sucesso.")
except FileNotFoundError:
    print(f"Erro: O arquivo '{caminho_relacao_municipios}' não foi encontrado.")
    exit()

# 1- Preencha a coluna "codigo" para os municípios
def criar_codigo_municipio(row):
    if row['tipo'] != 1:
        return None
    try:
        cod_uf_str = str(int(row['cod_uf'])).zfill(2)
        cod_ente_str = str(int(row['cod_ente'])).zfill(5)
        return f"{cod_uf_str}{cod_ente_str}"
    except (ValueError, TypeError):
        return None

df_mun['codigo'] = df_mun.apply(criar_codigo_municipio, axis=1)
print("\nColuna 'codigo' preenchida para municípios.")

# 2- Leia o arquivo Excel com os dados da CAPAG dos municípios
caminho_capag_mun = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\capag_municipios.xlsx"
sheet_name = "Prévia da CAPAG"
try:
    df_capag_mun = pd.read_excel(
        caminho_capag_mun,
        sheet_name=sheet_name,
        header=2,
        dtype={'Código Município Completo': str}
    )
    df_capag_mun.rename(columns={'Código Município Completo': 'codigo'}, inplace=True)
    print("DataFrame 'df_capag_mun' carregado com sucesso.")
except Exception as e:
    print(f"Erro ao carregar o arquivo CAPAG de municípios: {e}")
    exit()

# 3- Filtra e enriquece os dados dos municípios
df_municipios = df_mun[df_mun['tipo'] == 1].copy()
df_capag_mun_subset = df_capag_mun[['codigo', 'CAPAG']] # Pegamos apenas as colunas necessárias
df_final_municipios = pd.merge(df_municipios, df_capag_mun_subset, on='codigo', how='left')
print("Dados dos municípios enriquecidos com a CAPAG.")


# --- PARTE 2: PROCESSAMENTO DE ESTADOS (TIPO == 2) ---
print("\n--- INICIANDO PROCESSAMENTO DE ESTADOS (TIPO 2) ---")

# 1- Filtra o DataFrame original para obter apenas os estados
df_estados = df_mun[df_mun['tipo'] == 2].copy()

# 2- Leia o arquivo CSV com os dados da CAPAG dos estados
caminho_capag_estados = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\capag_estados.csv"
try:
    # O separador é ponto e vírgula (;)
    df_capag_estados = pd.read_csv(caminho_capag_estados, sep=';')
    print("DataFrame 'df_capag_estados' carregado com sucesso.")

    # Limpeza para garantir a junção correta (remove espaços em branco antes/depois da sigla)
    df_estados['UF'] = df_estados['UF'].str.strip()
    df_capag_estados['UF'] = df_capag_estados['UF'].str.strip()

except FileNotFoundError:
    print(f"Erro: O arquivo '{caminho_capag_estados}' não foi encontrado.")
    exit()
except Exception as e:
    print(f"Erro ao carregar o arquivo CAPAG de estados: {e}")
    exit()


# 3- Enriquece os dados dos estados
df_capag_estados_subset = df_capag_estados[['UF', 'Classificação da CAPAG']]
df_final_estados = pd.merge(df_estados, df_capag_estados_subset, on='UF', how='left')

# Renomeia a coluna para consistência com o DataFrame de municípios
df_final_estados.rename(columns={'Classificação da CAPAG': 'CAPAG'}, inplace=True)
print("Dados dos estados enriquecidos com a Classificação da CAPAG.")


# --- PARTE 3: COMBINAÇÃO E EXPORTAÇÃO FINAL ---
print("\n--- COMBINANDO RESULTADOS E EXPORTANDO ---")

# Concatena os DataFrames de municípios e estados
df_final_combinado = pd.concat([df_final_municipios, df_final_estados], ignore_index=True)

# Garante a ordem original das colunas (adicionando CAPAG no final se ela não existia)
colunas_originais = list(df_mun.columns)
colunas_finais = colunas_originais + ['CAPAG'] if 'CAPAG' not in colunas_originais else colunas_originais
df_final_combinado = df_final_combinado.reindex(columns=colunas_finais)


# 4- Exporte o resultado final combinado para um novo CSV
caminho_saida = "relacao_completa_enriquecida.csv"
df_final_combinado.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')

print(f"\nProcesso completo finalizado com sucesso!")
print(f"O arquivo final combinado foi salvo em: '{caminho_saida}'")

# Mostra uma prévia do resultado final
print("\nAmostra do resultado final combinado:")
print(df_final_combinado.head())
print("...")
print(df_final_combinado.tail())