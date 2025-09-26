#%% 

import pandas as pd
import numpy as np
from IPython.display import display

pd.options.display.max_columns = 100
pd.options.display.max_rows = 200
pd.options.display.float_format = '{:.2f}'.format

#paths
path_starcard = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\StarCard.xlsx'
path_originadores = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\Originadores.xlsx'

# merge
df_starcard = pd.read_excel(path_starcard)
df_originadores = pd.read_excel(path_originadores, usecols=['CCB'])
df_merged = pd.merge(df_starcard, df_originadores.drop_duplicates(subset='CCB'), on='CCB', how='left')

# limpo as colunas de data
df_merged = df_merged.rename(columns={
    'Data Referencia': 'DataGeracao',
    'Data Vencimento': 'DataVencimento',
})
df_merged['DataGeracao'] = pd.to_datetime(df_merged['DataGeracao'], errors='coerce')
df_merged['DataVencimento'] = pd.to_datetime(df_merged['DataVencimento'], errors='coerce')

df_debug = df_merged[['CCB', 'DataGeracao', 'DataVencimento']].copy()
df_debug.dropna(subset=['DataGeracao', 'DataVencimento'], inplace=True)

# Verif de múltiplas datas de referência
datas_geracao_unicas = df_debug['DataGeracao'].unique()
if len(datas_geracao_unicas) > 1:
    print(f"\n[ALERTA] Existem {len(datas_geracao_unicas)} datas de referência diferentes no arquivo.")
else:
    print(f"\nArquivo com data de referência única.")

# Cálc dos dias de atraso e exibição de amostra
df_debug['dias_de_atraso'] = (df_debug['DataGeracao'] - df_debug['DataVencimento']).dt.days

print("\nAmostra dos dados com dias de atraso calculados:")
vencidas_sample = df_debug[df_debug['dias_de_atraso'] > 0].head()
avencer_sample = df_debug[df_debug['dias_de_atraso'] <= 0].head()
display(pd.concat([vencidas_sample, avencer_sample]))

# contaminação por contrato
resultados = {}
total_contratos_unicos = df_debug['CCB'].nunique()
resultados['Total de Contratos Únicos'] = total_contratos_unicos

for dias in [1, 30, 60]:
    df_debug[f'_ParcelaVencida_{dias}d'] = (df_debug['dias_de_atraso'] >= dias).astype(int)
    

    contratos_com_parcela_vencida = df_debug.groupby('CCB')[f'_ParcelaVencida_{dias}d'].max()
    ccbs_vencidos = contratos_com_parcela_vencida[contratos_com_parcela_vencida == 1].index
    
    num_contratos_vencidos = len(ccbs_vencidos)
    percentual_vencido = (num_contratos_vencidos / total_contratos_unicos) * 100
    
    resultados[f'Nº Contratos Vencidos (>{dias}d)'] = num_contratos_vencidos
    resultados[f'% Contratos Vencidos (>{dias}d)'] = percentual_vencido

print("\n" + "=" * 60)
print("RESULTADOS DA DEPURAÇÃO")
print(f"Total de Contratos Únicos na Carteira: {resultados['Total de Contratos Únicos']:,}")
print("=" * 60)
for dias in [1, 30, 60]:
    print(f"Contratos com parcela vencida há > {dias} dias:")
    print(f"  - Nº de Contratos: {resultados[f'Nº Contratos Vencidos (>{dias}d)']:,}")
    print(f"  - Percentual: {resultados[f'% Contratos Vencidos (>{dias}d)']:.2f}%")
    print("-" * 60)