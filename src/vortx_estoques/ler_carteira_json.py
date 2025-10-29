#%% 
import pandas as pd
import json
import os
import glob
from datetime import datetime

#! #######################     PATHS    #############################################################
PATH_CARTEIRAS_HISTORICO = r"C:\Users\LucasRafaeldeAndrade\Desktop\portoauto\src\vortx_estoques\data\Carteiras"
#! #################################################################################################

# ---  encontro e carrego o arquivo json mais recente ---
mapa_carteiras = {}
#? itera sobre todos os arquivos .json na pasta
for arquivo in glob.glob(os.path.join(PATH_CARTEIRAS_HISTORICO, "*.json")):
    try:
        with open(arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)
            #? extrai a data do primeiro objeto da carteira
            data_str = dados[0]['carteiras'][0]['dataAtual'].split('T')[0]
            data_obj = datetime.strptime(data_str, '%Y-%m-%d')
            mapa_carteiras[data_obj] = arquivo
    except Exception:
        # ignora arquivos que nao seguem o padrão esperado
        continue

if not mapa_carteiras:
    raise FileNotFoundError("nenhum arquivo de carteira .json válido foi encontrado.")

#? encontra a data mais recente e o arquivo correspondente
data_mais_recente = max(mapa_carteiras.keys())
path_carteira_final = mapa_carteiras[data_mais_recente]

print(f"carregando o arquivo de carteira mais recente: {os.path.basename(path_carteira_final)}")

#? carrega os dados do arquivo json encontrado
with open(path_carteira_final, 'r', encoding='utf-8') as f:
    dados_carteira_recente = json.load(f)[0]

# %%
#* extração e processamento dos dados da carteira json

lista_de_fluxos_df = [] #! lista para armazenar os dataframes de cada ativo

#? extrai a data base do relatório
data_base = datetime.strptime(dados_carteira_recente['carteiras'][0]['dataAtual'].split('T')[0], '%Y-%m-%d')

#? process a disponibilidade
disponibilidade = next((a.get('Disponibilidade') for a in dados_carteira_recente['ativos'] if a.get('Disponibilidade')), None)
if disponibilidade:
    # cria um dataframe para o saldo inicial
    df_disponibilidade = pd.DataFrame([{'data': data_base,
                                        'categoria': 'Conta Corr.',
                                        'valor': disponibilidade['ativos'][0]['saldo'],
                                        'descricao': 'Saldo Inicial'}])
    lista_de_fluxos_df.append(df_disponibilidade)

#? processo as cotas
cotas = next((a.get('Cotas') for a in dados_carteira_recente['ativos'] if a.get('Cotas')), None)
if cotas:
    df_fundos = pd.DataFrame(cotas['ativos'])
    df_fundos['data'] = data_base #? a data de resgate é a data base
    df_fundos['categoria'] = 'Fundo inv.'
    df_fundos['valor'] = df_fundos['valorBruto']
    df_fundos['descricao'] = "Resgate " + df_fundos['titulo']
    lista_de_fluxos_df.append(df_fundos[['data', 'categoria', 'valor', 'descricao']])

#? process a rendafixa
renda_fixa = next((a.get('RendaFixa') for a in dados_carteira_recente['ativos'] if a.get('RendaFixa')), None)
if renda_fixa:
    df_rf = pd.DataFrame(renda_fixa['ativos'])
    df_rf['data'] = pd.to_datetime(df_rf['dataVencimento']) #? a data é o vencimento do título
    df_rf['categoria'] = 'Títulos Renda Fixa'
    df_rf['valor'] = df_rf['mercadoAtual']
    df_rf['descricao'] = "Venc. " + df_rf['codigoCustodia']
    lista_de_fluxos_df.append(df_rf[['data', 'categoria', 'valor', 'descricao']])

#* consolida todos os dataframes em um soh
if lista_de_fluxos_df:
    df_carteira_json = pd.concat(lista_de_fluxos_df, ignore_index=True)
else:
    df_carteira_json = pd.DataFrame()

#? printa o resultado da leitura da carteira
print(df_carteira_json)
# %%
