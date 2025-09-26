# -*- coding: utf-8 -*-
"""
Script de Depuração para Análise de Contratos Vencidos
"""
#%%
# =============================================================================
# Bibliotecas e Configurações Essenciais
# =============================================================================
import pandas as pd
import numpy as np
from IPython.display import display


pd.options.display.max_columns = 100
pd.options.display.max_rows = 200
pd.options.display.float_format = '{:.2f}'.format # Formatação para facilitar a leitura

# =============================================================================
# 1. Leitura e Preparação dos Dados (Parte Mínima Necessária)
# =============================================================================
# ATENÇÃO: Verifique se os caminhos abaixo estão corretos
path_starcard = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\StarCard.xlsx'
path_originadores = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\Originadores.xlsx'

print("Lendo e preparando os dados...")
try:
    df_starcard = pd.read_excel(path_starcard)
    df_originadores = pd.read_excel(path_originadores, usecols=['CCB']) # Lendo apenas o CCB para unir
except FileNotFoundError as e:
    print(f"[ERRO] Arquivo não encontrado. Verifique o caminho: {e}")
    # Encerra o script se os arquivos não forem encontrados
    exit()

# Unindo as fontes de dados
df_merged = pd.merge(df_starcard, df_originadores.drop_duplicates(subset='CCB'), on='CCB', how='left')

# Renomeando colunas essenciais
df_merged = df_merged.rename(columns={
    'Data Referencia': 'DataGeracao',
    'Data Vencimento': 'DataVencimento',
})

# Limpeza das colunas de data (passo crucial para a depuração)
# Usamos 'coerce' para transformar datas inválidas em NaT (Not a Time)
df_merged['DataGeracao'] = pd.to_datetime(df_merged['DataGeracao'], errors='coerce')
df_merged['DataVencimento'] = pd.to_datetime(df_merged['DataVencimento'], errors='coerce')

# Vamos chamar nosso dataframe de trabalho de 'df_debug'
df_debug = df_merged[['CCB', 'DataGeracao', 'DataVencimento']].copy()

print("Dados carregados com sucesso.")
print("-" * 50)

# =============================================================================
# 2. VERIFICAÇÕES DE SANIDADE DOS DADOS (Ponto Chave da Depuração)
# =============================================================================
print("Iniciando verificações de sanidade das datas...")

# V2.1: Verificar se há valores nulos ou inválidos nas datas
null_geracao = df_debug['DataGeracao'].isnull().sum()
null_vencimento = df_debug['DataVencimento'].isnull().sum()

if null_geracao > 0 or null_vencimento > 0:
    print(f"\n[ALERTA] Foram encontrados valores nulos/inválidos nas colunas de data:")
    print(f" - Datas de Geração Nulas: {null_geracao}")
    print(f" - Datas de Vencimento Nulas: {null_vencimento}")
    # Removemos as linhas com datas nulas para não afetar o cálculo
    df_debug.dropna(subset=['DataGeracao', 'DataVencimento'], inplace=True)
    print(" -> Linhas com datas nulas foram removidas para a análise.\n")
else:
    print("Nenhum valor nulo encontrado nas colunas de data. Ótimo!")

# V2.2: Verificar o intervalo das datas para ver se fazem sentido
print("Intervalo de datas no arquivo:")
print(f" - Data de Geração (Referência): de {df_debug['DataGeracao'].min().strftime('%Y-%m-%d')} a {df_debug['DataGeracao'].max().strftime('%Y-%m-%d')}")
print(f" - Data de Vencimento: de {df_debug['DataVencimento'].min().strftime('%Y-%m-%d')} a {df_debug['DataVencimento'].max().strftime('%Y-%m-%d')}")

# V2.3: Verificar se a 'DataGeracao' é uma data única (MUITO IMPORTANTE)
datas_geracao_unicas = df_debug['DataGeracao'].unique()
if len(datas_geracao_unicas) > 1:
    print(f"\n[ALERTA IMPORTANTE] Existem {len(datas_geracao_unicas)} datas de referência (DataGeracao) diferentes no arquivo.")
    print("Isso pode distorcer a análise de dias de atraso se não for esperado.")
    print("A análise continuará usando a data de referência de cada linha.")
else:
    print(f"\nO arquivo possui uma única data de referência: {pd.to_datetime(datas_geracao_unicas[0]).strftime('%Y-%m-%d')}. Perfeito!")

print("-" * 50)

# =============================================================================
# 3. Cálculo dos Dias de Atraso e Flags de Vencimento
# =============================================================================
print("Calculando dias de atraso...")

# O cálculo é feito linha a linha, respeitando a DataGeracao de cada registro
df_debug['dias_de_atraso'] = (df_debug['DataGeracao'] - df_debug['DataVencimento']).dt.days

# V3.1: Exibir um resumo estatístico dos dias de atraso para entender a distribuição
print("\nResumo dos 'dias_de_atraso' calculados:")
print(df_debug['dias_de_atraso'].describe())
print("\nValores negativos significam parcelas 'a vencer'.")
print("Valores positivos significam parcelas 'vencidas'.")

# V3.2: Criar flags para cada faixa de vencimento solicitada
# Nota: dias_de_atraso >= 1 já significa vencido.
df_debug['_ParcelaVencida_1d'] = (df_debug['dias_de_atraso'] >= 1).astype(int)
df_debug['_ParcelaVencida_30d'] = (df_debug['dias_de_atraso'] >= 30).astype(int)
df_debug['_ParcelaVencida_60d'] = (df_debug['dias_de_atraso'] >= 60).astype(int)

# V3.3: Exibir uma amostra para verificação manual
print("\nAmostra dos dados com dias de atraso e flags (5 vencidas e 5 a vencer):")
vencidas_sample = df_debug[df_debug['dias_de_atraso'] > 0].head()
avencer_sample = df_debug[df_debug['dias_de_atraso'] <= 0].head()
display(pd.concat([vencidas_sample, avencer_sample]))
print("-" * 50)

# =============================================================================
# 4. Lógica de Contaminação e Cálculo Final dos Percentuais
# =============================================================================
print("Aplicando a lógica de contaminação por contrato (CCB)...")

# Dicionário para armazenar os resultados
resultados = {}

# Número total de contratos únicos na base
total_contratos_unicos = df_debug['CCB'].nunique()
resultados['Total de Contratos Únicos'] = total_contratos_unicos

# Loop para calcular para cada faixa de dias
for dias in [1, 30, 60]:
    flag_parcela = f'_ParcelaVencida_{dias}d'
    
    # 1. Agrupar por CCB e verificar se ALGUMA parcela está vencida
    contratos_com_parcela_vencida = df_debug.groupby('CCB')[flag_parcela].max()
    
    # 2. Filtrar para obter a lista de CCBs "contaminados"
    ccbs_vencidos = contratos_com_parcela_vencida[contratos_com_parcela_vencida == 1].index
    
    # 3. Contar quantos CCBs únicos estão nessa condição
    num_contratos_vencidos = len(ccbs_vencidos)
    
    # 4. Calcular o percentual
    percentual_vencido = (num_contratos_vencidos / total_contratos_unicos) * 100 if total_contratos_unicos > 0 else 0
    
    # Armazenar resultados
    resultados[f'Nº Contratos Vencidos (>{dias}d)'] = num_contratos_vencidos
    resultados[f'% Contratos Vencidos (>{dias}d)'] = percentual_vencido

print("\nCálculo finalizado.")
print("=" * 60)
print("RESULTADOS DA DEPURAÇÃO")
print("=" * 60)
print(f"  Total de Contratos Únicos na Carteira: {resultados['Total de Contratos Únicos']:,}")
print("-" * 60)
print(f"  Contratos com pelo menos uma parcela vencida há > 1 dia:")
print(f"    - Nº de Contratos: {resultados['Nº Contratos Vencidos (>1d)']:,}")
print(f"    - Percentual: {resultados['% Contratos Vencidos (>1d)']:.2f}%")
print("-" * 60)
print(f"  Contratos com pelo menos uma parcela vencida há > 30 dias:")
print(f"    - Nº de Contratos: {resultados['Nº Contratos Vencidos (>30d)']:,}")
print(f"    - Percentual: {resultados['% Contratos Vencidos (>30d)']:.2f}%")
print("-" * 60)
print(f"  Contratos com pelo menos uma parcela vencida há > 60 dias:")
print(f"    - Nº de Contratos: {resultados['Nº Contratos Vencidos (>60d)']:,}")
print(f"    - Percentual: {resultados['% Contratos Vencidos (>60d)']:.2f}%")
print("=" * 60)

#%%

# -*- coding: utf-8 -*-
"""
Script de Depuração para Análise de Contratos Vencidos
Versão 2.0 - com Geração de Evidências em .txt
"""

# =============================================================================
# Bibliotecas e Configurações Essenciais
# =============================================================================
import pandas as pd
import numpy as np
import os # Para lidar com o caminho do arquivo de saída

pd.options.display.max_columns = 100
pd.options.display.max_rows = 200
pd.options.display.float_format = '{:.2f}'.format

# =============================================================================
# 1. Leitura e Preparação dos Dados (Parte Mínima Necessária)
# =============================================================================
# ATENÇÃO: Verifique se os caminhos abaixo estão corretos
path_starcard = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\StarCard.xlsx'
path_originadores = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\Originadores.xlsx'

print("Lendo e preparando os dados...")
try:
    df_starcard = pd.read_excel(path_starcard)
    df_originadores = pd.read_excel(path_originadores, usecols=['CCB']) # Lendo apenas o CCB para unir
except FileNotFoundError as e:
    print(f"[ERRO] Arquivo não encontrado. Verifique o caminho: {e}")
    exit()

df_merged = pd.merge(df_starcard, df_originadores.drop_duplicates(subset='CCB'), on='CCB', how='left')

# Renomeando colunas essenciais para a análise de vencimento
df_merged = df_merged.rename(columns={
    'Data Referencia': 'DataGeracao',
    'Data Vencimento': 'DataVencimento',
    'Valor Presente': 'ValorPresente' # Adicionando Valor Presente para contexto
})

# Limpeza das colunas de data
df_merged['DataGeracao'] = pd.to_datetime(df_merged['DataGeracao'], errors='coerce')
df_merged['DataVencimento'] = pd.to_datetime(df_merged['DataVencimento'], errors='coerce')

# Vamos chamar nosso dataframe de trabalho de 'df_debug'
df_debug = df_merged[['CCB', 'DataGeracao', 'DataVencimento', 'ValorPresente']].copy()
df_debug.dropna(subset=['CCB', 'DataGeracao', 'DataVencimento'], inplace=True)
print("Dados carregados com sucesso.")
print("-" * 50)


# =============================================================================
# 2. VERIFICAÇÕES DE SANIDADE DOS DADOS (Mantido da versão anterior)
# =============================================================================
print("Iniciando verificações de sanidade das datas...")
# (As verificações de sanidade são as mesmas e serão impressas no console)
datas_geracao_unicas = df_debug['DataGeracao'].unique()
if len(datas_geracao_unicas) > 1:
    print(f"\n[ALERTA IMPORTANTE] Existem {len(datas_geracao_unicas)} datas de referência (DataGeracao) diferentes.")
else:
    print(f"\nO arquivo possui uma única data de referência: {pd.to_datetime(datas_geracao_unicas[0]).strftime('%Y-%m-%d')}. Perfeito!")
print("-" * 50)


# =============================================================================
# 3. Cálculo dos Dias de Atraso e Flags de Vencimento
# =============================================================================
print("Calculando dias de atraso e flags de vencimento...")
df_debug['dias_de_atraso'] = (df_debug['DataGeracao'] - df_debug['DataVencimento']).dt.days

df_debug['_ParcelaVencida_1d'] = (df_debug['dias_de_atraso'] >= 1).astype(int)
df_debug['_ParcelaVencida_30d'] = (df_debug['dias_de_atraso'] >= 30).astype(int)
df_debug['_ParcelaVencida_60d'] = (df_debug['dias_de_atraso'] >= 60).astype(int)
print("Cálculos concluídos.")
print("-" * 50)

# =============================================================================
# 4. Lógica de Contaminação e Cálculo Final dos Percentuais
# =============================================================================
print("Calculando os percentuais agregados...")
resultados = {}
listas_ccbs_vencidos = {}
total_contratos_unicos = df_debug['CCB'].nunique()
resultados['Total de Contratos Únicos'] = total_contratos_unicos

for dias in [1, 30, 60]:
    flag_parcela = f'_ParcelaVencida_{dias}d'
    contratos_com_parcela_vencida = df_debug.groupby('CCB')[flag_parcela].max()
    ccbs_vencidos = contratos_com_parcela_vencida[contratos_com_parcela_vencida == 1].index
    listas_ccbs_vencidos[dias] = ccbs_vencidos
    num_contratos_vencidos = len(ccbs_vencidos)
    percentual_vencido = (num_contratos_vencidos / total_contratos_unicos) * 100 if total_contratos_unicos > 0 else 0
    resultados[f'Nº Contratos Vencidos (>{dias}d)'] = num_contratos_vencidos
    resultados[f'% Contratos Vencidos (>{dias}d)'] = percentual_vencido
print("Cálculo de percentuais finalizado.")
print("-" * 50)


# =============================================================================
# 5. Geração de Evidências e Relatório (.txt) -- NOVIDADE
# =============================================================================
print("Gerando relatório de evidências em arquivo de texto...")

# Define o nome do arquivo e o número de exemplos a serem mostrados
NOME_ARQUIVO_SAIDA = "relatorio_depuracao_vencidos.txt"
N_EXEMPLOS = 3

# Abre o arquivo em modo de escrita ('w') com codificação UTF-8
with open(NOME_ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("RELATÓRIO DE DEPURACAO - ANÁLISE DE CONTRATOS VENCIDOS\n")
    f.write(f"Data da Análise: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("=" * 80 + "\n\n")

    # --- Escreve o Resumo Geral no arquivo ---
    f.write("1. RESUMO GERAL DA CARTEIRA\n")
    f.write("-" * 30 + "\n")
    f.write(f"Total de Contratos Únicos na Carteira: {resultados['Total de Contratos Únicos']:,}\n\n")
    
    for dias in [1, 30, 60]:
        f.write(f"  - Contratos Vencidos há > {dias} dias:\n")
        f.write(f"    - Nº de Contratos: {resultados[f'Nº Contratos Vencidos (>{dias}d)']:,}\n")
        f.write(f"    - Percentual da Carteira: {resultados[f'% Contratos Vencidos (>{dias}d)']:.2f}%\n\n")
        
    f.write("\n" + "=" * 80 + "\n")
    f.write("2. EVIDÊNCIAS - EXEMPLOS DE CONTRATOS\n")
    f.write(f"A seguir, são listados até {N_EXEMPLOS} exemplos de CCBs para cada categoria, mostrando todas as suas parcelas.\n")
    f.write("A(s) parcela(s) que causam a classificação de 'vencido' são marcadas.\n")
    f.write("=" * 80 + "\n\n")

    # --- Loop para gerar os exemplos de cada categoria ---
    for dias in [60, 30, 1]: # Começando do mais crítico
        f.write("-" * 80 + "\n")
        f.write(f"Exemplos de Contratos Vencidos há mais de {dias} dias\n")
        f.write("-" * 80 + "\n\n")
        
        ccbs_para_amostra = listas_ccbs_vencidos[dias]
        
        if len(ccbs_para_amostra) == 0:
            f.write("Nenhum contrato encontrado nesta categoria.\n\n")
            continue
            
        # Seleciona os primeiros N_EXEMPLOS
        exemplos_selecionados = ccbs_para_amostra[:N_EXEMPLOS]
        
        for i, ccb_exemplo in enumerate(exemplos_selecionados):
            f.write(f"EXEMPLO {i+1}: CCB = {ccb_exemplo}\n")
            f.write(f"Motivo da Flag: Possui pelo menos uma parcela com {dias} ou mais dias de atraso.\n\n")
            
            # Filtra o dataframe para pegar todas as parcelas deste CCB
            df_exemplo = df_debug[df_debug['CCB'] == ccb_exemplo].copy()
            
            # Adiciona uma coluna 'EVIDENCIA' para marcar a(s) parcela(s) problemática(s)
            df_exemplo['EVIDENCIA'] = np.where(df_exemplo['dias_de_atraso'] >= dias, '<<<<< PARCELA VENCIDA QUE CAUSOU A FLAG', '')
            
            # Formata as datas para melhor leitura no .txt
            df_exemplo['DataGeracao'] = df_exemplo['DataGeracao'].dt.strftime('%Y-%m-%d')
            df_exemplo['DataVencimento'] = df_exemplo['DataVencimento'].dt.strftime('%Y-%m-%d')
            
            # Define as colunas que queremos mostrar no relatório
            colunas_para_mostrar = ['DataGeracao', 'DataVencimento', 'dias_de_atraso', 'ValorPresente', 'EVIDENCIA']
            
            # Converte o dataframe do exemplo para uma string formatada e escreve no arquivo
            f.write(df_exemplo[colunas_para_mostrar].to_string(index=False))
            f.write("\n\n" + "-"*40 + "\n\n")

# Mensagem final no console
caminho_completo = os.path.abspath(NOME_ARQUIVO_SAIDA)
print("Relatório de depuração foi gerado com sucesso!")
print(f"Por favor, verifique o arquivo: {caminho_completo}")
print("="*60)

#%%

# =============================================================================
# 6. VALIDAÇÃO CRUZADA com a Coluna "Status"
# =============================================================================
print("\n" + "=" * 80)
print("INICIANDO VALIDAÇÃO CRUZADA COM A COLUNA 'Status' ORIGINAL")
print("=" * 80)

# Verificamos se a coluna 'Status' existe no dataframe original
if 'Status' in df_merged.columns:
    
    print("Analisando a coluna 'Status' para uma verificação independente...\n")
    
    # 1. Identificar todas as parcelas que estão com o status "Vencido"
    df_status_vencido = df_merged[df_merged['Status'] == 'Vencido'].copy()
    
    if not df_status_vencido.empty:
        # 2. Contar quantos contratos (CCBs) únicos possuem pelo menos uma parcela "Vencido"
        ccbs_com_status_vencido = df_status_vencido['CCB'].nunique()
        
        # 3. Contar o total de contratos únicos na base de dados para calcular o percentual
        total_de_contratos = df_merged['CCB'].nunique()
        
        # 4. Calcular o percentual
        percentual_status_vencido = (ccbs_com_status_vencido / total_de_contratos) * 100
        
        print("Resultados da Análise da Coluna 'Status':")
        print("-" * 50)
        print(f"  - Total de Contratos Únicos na Carteira: {total_de_contratos:,}")
        print(f"  - Nº de Contratos com pelo menos uma parcela 'Vencido': {ccbs_com_status_vencido:,}")
        print(f"  - Percentual de Contratos 'Vencido' (pelo Status): {percentual_status_vencido:.2f}%\n")
        
        print("--- Comparativo ---")
        print(f"Percentual calculado pelo Status: {percentual_status_vencido:.2f}%")
        if 'resultados' in locals() and '% Contratos Vencidos (>1d)' in resultados:
            print(f"Percentual calculado por Data (> 1 dia de atraso): {resultados['% Contratos Vencidos (>1d)']:.2f}%")
        print("-" * 50)
        print("\nInterpretação:")
        print("Compare os dois valores acima. Se forem muito próximos, isso fortalece a consistência dos dados.")
        print("Uma pequena diferença é esperada, pois a regra para o 'Status' ser 'Vencido' pode ser, por exemplo, D+1 (1 dia de atraso).")
        print("Uma grande diferença pode indicar que a data de referência ('DataGeracao') usada no cálculo de dias de atraso não está sincronizada com a data em que o 'Status' foi gerado.")

    else:
        print("[AVISO] Não foram encontradas parcelas com o status 'Vencido' na coluna 'Status'.")

else:
    print("[ERRO] A coluna 'Status' não foi encontrada no DataFrame 'df_merged'. A verificação não pode ser concluída.")

print("\n" + "=" * 80)

#%% 
import pandas as pd
import numpy as np

# --- Carga e preparação dos dados ---
# (Se o caminho estiver errado, o script vai quebrar. É o esperado.)
df1 = pd.read_excel(r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\StarCard.xlsx')
df2 = pd.read_excel(r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\Originadores.xlsx', usecols=['CCB'])

df = pd.merge(df1, df2.drop_duplicates(), on='CCB', how='left')

# Converte as datas importantes, o que for inválido vira NaT e será removido
df['ref_dt'] = pd.to_datetime(df['Data Referencia'], errors='coerce')
df['venc_dt'] = pd.to_datetime(df['Data Vencimento'], errors='coerce')
df.dropna(subset=['ref_dt', 'venc_dt', 'CCB'], inplace=True)

# Calcula o atraso de uma vez
df['atraso'] = (df['ref_dt'] - df['venc_dt']).dt.days

# --- Análise de Vencidos por Dias de Atraso ---
total_ccbs = df['CCB'].nunique()
dias_analise = [1, 30, 60]
resultados = []

print("--- % de Contratos Vencidos (pelo menos 1 parcela) ---")

for dias in dias_analise:
    # Contratos com atraso MAIOR OU IGUAL (>=)
    ccbs_venc_ge = df[df['atraso'] >= dias]['CCB'].nunique()
    
    # Contratos com atraso ESTRITAMENTE MAIOR (>)
    ccbs_venc_gt = df[df['atraso'] > dias]['CCB'].nunique()
    
    resultados.append({
        'Faixa': f"{dias} dias",
        '% Vencidos (>=': (ccbs_venc_ge / total_ccbs) * 100,
        '% Vencidos (>)': (ccbs_venc_gt / total_ccbs) * 100
    })

# Mostra o resultado numa tabela limpa
tabela_resultados = pd.DataFrame(resultados)
print(tabela_resultados.to_string(index=False))


# --- Validação Rápida com a Coluna 'Status' ---
print("\n--- Validação Cruzada com a Coluna 'Status' ---")

# Calcula o % de CCBs que a base já diz que estão "Vencido"
venc_status = df[df['Status'] == 'Vencido']['CCB'].nunique()
perc_status = (venc_status / total_ccbs) * 100

# Pega o resultado de >= 1 dia da nossa tabela para comparar
perc_calculado_1d = tabela_resultados.loc[tabela_resultados['Faixa'] == '1 dias', '% Vencidos (>='].iloc[0]

print(f"Pela coluna 'Status': {perc_status:.2f}% de contratos vencidos.")
print(f"Pelo nosso cálculo (>= 1 dia): {perc_calculado_1d:.2f}% de contratos vencidos.")


# --- Exemplo prático para provar o ponto ---
print("\n--- Exemplo de Contrato Vencido (>= 60 dias) ---")

# Pega o primeiro CCB que encontrar com atraso de 60 dias ou mais
ccbs_vencidos_60d = df[df['atraso'] >= 60]['CCB'].unique()

if len(ccbs_vencidos_60d) > 0:
    ccb_exemplo = ccbs_vencidos_60d[0]
    df_exemplo = df[df['CCB'] == ccb_exemplo][['CCB', 'ref_dt', 'venc_dt', 'atraso']]
    
    # Marca a linha que "contaminou" o contrato
    df_exemplo['EVIDENCIA'] = np.where(df_exemplo['atraso'] >= 60, '<<<<<', '')
    
    print(f"Analisando todas as parcelas do CCB: {ccb_exemplo}")
    print(df_exemplo.to_string(index=False))
else:
    print("Nenhum contrato encontrado com 60 dias ou mais de atraso para usar como exemplo.")