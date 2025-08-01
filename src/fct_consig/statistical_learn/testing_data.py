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
# config.cfg  #* aqui estao os paths, etc
# ==============================================================================
config = configparser.ConfigParser()
config_file = 'config.cfg'

if not os.path.exists(config_file):
    raise FileNotFoundError(f"Erro: Arquivo de configuração '{config_file}' não encontrado.")

config.read(config_file)

ROOT_PATH = Path(config.get('Paths', 'root_path'))
PASTA_DADOS_VENCIDOS = ROOT_PATH / config.get('PathsVencidos', 'pasta_dados_vencidos')
PASTA_RESULTADOS_VENCIDOS = ROOT_PATH / config.get('PathsVencidos', 'pasta_resultados_vencidos')
PADRAO_ARQUIVO_ESTOQUE = config.get('ParametersVencidos', 'padrao_arquivo_estoque')
ENCODING_LEITURA = config.get('ParametersVencidos', 'encoding_leitura')

# ==============================================================================
#* ler df final
# ==============================================================================
caminho_pasta = str(PASTA_DADOS_VENCIDOS)
padrao_config = PADRAO_ARQUIVO_ESTOQUE
lista_arquivos = []

partes = padrao_config.split()
partes_processadas = [re.escape(p).replace('\\*', '.*') for p in partes]
padrao_regex_final_str = r'\s+'.join(partes_processadas)
regex = re.compile(padrao_regex_final_str, re.IGNORECASE)

# --- NOVO BLOCO DE DIAGNÓSTICO APRIMORADO ---
print("\n iniciando leitura...")
print(f"dir de dados configurada: {caminho_pasta}")
print(f"padrão de <<regex>> utilizado: {regex.pattern}")

conteudo_da_pasta = os.listdir(caminho_pasta)
if not conteudo_da_pasta:
    print("AVISO: A pasta está vazia.")
else:
    print("\nConteúdo encontrado na pasta:")
    for item in conteudo_da_pasta:
        print(f"   - {item}")

    # 3. FILTRA APENAS OS ARQUIVOS .CSV E APLICA O REGEX
    print("\nAnalisando arquivos .csv e aplicando o filtro:")
    arquivos_csv_encontrados = [f for f in conteudo_da_pasta if f.lower().endswith('.csv')]
    
    if not arquivos_csv_encontrados:
        print("Nenhum arquivo com extensão .csv foi encontrado na pasta.")
    else:
        for basename in arquivos_csv_encontrados:
            match_result = "MATCH" if regex.search(basename) else "NO MATCH"
            print(f"   - Arquivo: '{basename}' -> Resultado: {match_result}")
            if regex.search(basename):
                lista_arquivos.append(os.path.join(caminho_pasta, basename))


if not lista_arquivos:
    print("#################################################################################")
    print(f"não foi encontrado nenhum arquivo com esse padrão no caminho '{caminho_pasta}'")
    df_final = pd.DataFrame()
else:
    print("files que serão lidos:")
    for arquivo in lista_arquivos:
        print(os.path.basename(arquivo))

    # %% def colunas e leitura 
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
        print("\n##############################################")
        print("<DEBUG> `df_final` criado com sucesso!")
    else:
        print("<DEBUG> \nNenhum arquivo foi lido.")
        df_final = pd.DataFrame()


#%% ANALISE EXPLORATORIA DOS DADOS 
# ==============================================================================
# blco unco para analise exloratoria de dads (eda)

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from IPython.display import display


# Verifica se a variável df_final existe e não está vazia antes de prosseguir
if 'df_final' in locals() and not df_final.empty:
    print("=====================================================================")
    print("== INICIANDO ANÁLISE EXPLORATÓRIA COMPLETA DO DATAFRAME `df_final` ==")
    print("=====================================================================")

    print("\n\n---  INFO GERAIS (df_final.info) ---")
    df_final.info(verbose=True, show_counts=True)
    print("\n\n--- ESTATÍSTICAS DESCR (COLS NUMÉRICAS) ---")
    display(df_final.describe(include=np.number).T)
    print("\n\n--- ESTATÍSTICAS DESCR (COLS CATEGÓRICAS) <<---")
    desc_cat = df_final.describe(include=['object', 'category'])
    display(desc_cat.T)
    print("\n\n--- MISSING VALUES ---")
    missing_values = df_final.isnull().sum()
    missing_percentage = (missing_values / len(df_final)) * 100
    missing_df = pd.DataFrame({'Contagem de Ausentes': missing_values, 'Percentual de Ausentes (%)': missing_percentage})
    missing_df = missing_df[missing_df['Contagem de Ausentes'] > 0]
    display(missing_df.sort_values(by='Percentual de Ausentes (%)', ascending=False))
    print(">>>> VALORES ÚNICOS <<<---")
    categorical_cols = df_final.select_dtypes(include=['object', 'category']).columns
    cardinality = df_final[categorical_cols].nunique().sort_values(ascending=False)
    cardinality_df = cardinality.to_frame(name='Contagem de Valores Únicos')
    print("Contagem de valores únicos para colunas categóricas:")
    display(cardinality_df)        
    coluna_exemplo = 'Situacao' # pode ser outra coluna q tiver interesse
    print(f"\n Ex de distribuição de valores para a coluna '{coluna_exemplo}':")
    display(df_final[coluna_exemplo].value_counts(dropna=False, normalize=True).head(10).to_frame(name='Percentual'))

    # (USANDO COLUNAS NUMÉRICAS)
    print("\n\n  MATRIZ DE CORRELAÇÃO ---")
    numeric_df = df_final.select_dtypes(include=np.number)
    correlation_matrix = numeric_df.corr()
    plt.figure(figsize=(16, 12))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=.5)
    plt.title('Matriz de Correlação das Variáveis Numéricas', fontsize=16)
    plt.show()
        


#%% MACHINE LEARNING 
#* ==============================================================================>>>>>>>>>
#* BLOCO UNICO: PREPARAÇÃO, MODELAGEM E AVALIAÇÃO (coloque # + %% se quiser separar )
#* ==============================================================================>>>>>>>>>

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, classification_report

#!   Modelos que estou usando: 
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from lightgbm import LGBMClassifier
LGBM_AVAILABLE = True


############################ 
df_model = df_final.copy()
"""MESMA COISA QUE O DF FINAL, apenas copiei"""

#*******************************************************************
#? LIMPEZA, ORGANIZAR  FEATURES E CRIAÇ DA VARIÁVEL ALVO
#********************************************************************
print("TEINANDO MODELO: INICIANDO LIMPEZA E PREPARAÇÃO DOS DADOS ---")

############*  remoção das colunas inúteis
cols_to_drop = [
    #! 100% vazios
    'Registradora', 'NumeroBoleto', 'CampoAdicional1', 'CampoAdicional2', 
    'CampoAdicional3', 'CampoAdicional4', 'CampoAdicional5', 'IdContratoRegistradora', 
    'IdTituloRegistradora',
    #! ids e alta cardnl
    'IdTituloVortx', 'NumeroTitulo', 'CampoChave', 'SacadoCnpjCpf', 
    'SacadoNome', 'IdTituloVortxOriginador', 'CCB',
    #! baxa varnc
    'PES_TIPO_PESSOA', 'NotaPdd',
    #! data leakage
    'PDDNota', 'PDDVencido', 'PDDTotal', 'PDDEfeitoVagao', 'PercentagemEfeitoVagao'
]


df_model.drop(columns=cols_to_drop, inplace=True, errors='ignore')
print(f"Colunas removidas: {len(cols_to_drop)}")


############*  removendo linhas missing 
#? colunas essenciais, que nao vou aceitar valores nan
essential_cols = [
    'DataVencimento', 'DataGeracao', 'DataEmissao', 'ValorNominal', 'Situacao'
]
rows_before = len(df_model)
df_model.dropna(subset=essential_cols, inplace=True)
rows_after = len(df_model)
print(f"Linhas com dados ausentes removidas: {rows_before - rows_after}")

### ! Organizando as Features >>>>>>>>>>>>>>>>>>>>
# crio aqui o prazo original do título em dias
df_model['PrazoEmissaoVencimento'] = (df_model['DataVencimento'] - df_model['DataEmissao']).dt.days
#  dias de atraso na data da foto
df_model['dias_atraso'] = (df_model['DataGeracao'] - df_model['DataVencimento']).dt.days

##### >>>>>>>>>>>>>>>
print("<DEBUG>  Features 'PrazoEmissaoVencimento' e 'dias_atraso' criadas.")
##### >>>>>>>>>>>>>

# ?? filtrando apenas o que o nmodelo deve ver
#* vou querer que o medelo aprenda com  títulos que já venceram na DataGeracao. #*
#* (títulos a vencer no futuro não são nem adimplentes nem inadimplentes ainda.)
df_model = df_model[df_model['dias_atraso'] >= 0].copy()
print(f"Universo de análise filtrado para títulos vencidos. Total de registros: {len(df_model)}")


#! Variável Alvo (Y) <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
DIAS_INADIMPLENCIA = 30  # qttd de dias para ser considerado inadimplente
df_model['Y_inadimplente'] = (df_model['dias_atraso'] > DIAS_INADIMPLENCIA).astype(int)
#! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

print(f"\nVariável Alvo 'Y_inadimplente' criada com limite de {DIAS_INADIMPLENCIA} dias.")
print("Distribuição da variável alvo:")
print(df_model['Y_inadimplente'].value_counts(normalize=True))


#******************************************************************************
#*                     PREPARAÇÃO PARA MODELAGEM
#******************************************************************************
print("\n--- FASE 3: PREPARANDO DADOS PARA OS MODELOS ---")

#?  Features (X) vs  Alvo (y)
features_num = ['ValorNominal', 'PrazoEmissaoVencimento']
features_cat = [
    'Situacao', 'CedenteCnpjCpf', 'CedenteNome', 'Cnae', 'SecaoCNAEDescricao', 
    'SAC_TIPO_PESSOA', 'TipoAtivo', 'PagamentoParcial', 'Coobricacao', 'Convênio'
]

#* features que realmente existem no df_model após as limpezas
features_cat = [col for col in features_cat if col in df_model.columns]

X = df_model[features_num + features_cat]
y = df_model['Y_inadimplente']


# duv em Treino e Teste

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y  # stratify=y é para problemas desbalanceados
)
print(f"Dados divididos em treino ({len(X_train)} registros) e teste ({len(X_test)} registros).")


# Pipe de Pré-proc   ________________________________________________________________________
# numéricas: uso o Standard scaler
numeric_transformer = StandardScaler() #features numéricas: padronização #*isso torna prox de 1 para ele generalizar
# categóricas: one-hot encoding
categorical_transformer = OneHotEncoder(handle_unknown='ignore')  # handle_unknown='ignore' evita erros se uma categoria aparecer no teste mas não no treino
# ___________________________________________________________________________________________


preprocessor = ColumnTransformer(  #* Junta os transformadores
    transformers=[
        ('num', numeric_transformer, features_num),
        ('cat', categorical_transformer, features_cat)
    ],
    remainder='passthrough' # Mantenho as colunas n especific (se houver)
)


#*******************************************************************************
#*                       TREINAMENTO DO MODELO
#*******************************************************************************
print("\n TREINANDO E AVALIANDO OS MODELOS ---")

#* MODELOS A TESTAR: 
models = {
    'Regressão Logística': LogisticRegression(random_state=42, class_weight='balanced', max_iter=1000),
    'Random Forest': RandomForestClassifier(random_state=42, class_weight='balanced', n_jobs=-1),
    'LightGBM':LGBMClassifier(random_state=42, n_jobs=-1)
}

#? LOOP DE TREINAMENTO DE CADA MODELO ACIMA
results = {}

for model_name, model in models.items():
    print(f"\n==================== TREINANDO: {model_name} ====================")
    
    # Cria o pipeline final juntando o pré-processador e o modelo
    pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                               ('classifier', model)])
    

    pipeline.fit(X_train, y_train)     #* Treina o modelo    
    y_pred_proba = pipeline.predict_proba(X_test)[:, 1] #* Faz predições de probabilidade para calcular a AUC
    auc_score = roc_auc_score(y_test, y_pred_proba) #* CALC auc 
    results[model_name] = auc_score    
    print(f"AUC no conj de teste: {auc_score:.4f}")
    print("\n Relatório da Classificação:")
    y_pred_class = pipeline.predict(X_test)
    print(classification_report(y_test, y_pred_class))


#?______________________________________________________________________________
#? Comparando modelos
#?______________________________________________________________________________
print("\n\n--- FASE 5: RANKING FINAL DOS MODELOS POR AUC ---")
sorted_results = sorted(results.items(), key=lambda item: item[1], reverse=True)
##
for model, score in sorted_results:
    print(f"- {model}: {score:.4f}")
##
print("\nAnálise concluída. ")


#%% vendo features mais significativas: 
best_model_name = max(results, key=results.get)
print(f"Analisando o (melhor) modelo: {best_model_name}")

# Aqui estou recriando o melhor modelo (agora é o RF) 
best_model_pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                                      ('classifier', models[best_model_name])])
best_model_pipeline.fit(X_train, y_train)
feature_names = best_model_pipeline.named_steps['preprocessor'].get_feature_names_out()
importances = best_model_pipeline.named_steps['classifier'].feature_importances_   # impotancia de cada feature
importance_df = pd.DataFrame({
    'Feature': feature_names,
    'Importance': importances
}).sort_values(by='Importance', ascending=False)

###################################
#! quantidade de features >>>>>>>>
n_feat = 5
#! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
####################################
print(f"\nAs {n_feat} features mais importantes para o modelo:")
display(importance_df.head(n_feat))

# histogr:
plt.figure(figsize=(10, 8))
sns.barplot(x='Importance', y='Feature', data=importance_df.head(n_feat), palette='viridis')
plt.title(f'Top {n_feat} Features - {best_model_name}')
plt.show()



#%% 
#? ************************************************************
#?           HYPER TUNING
#? ************************************************************
from sklearn.model_selection import RandomizedSearchCV

best_model_name = max(results, key=results.get)  # vai pegar o Random Forest hoje

if best_model_name != 'Random Forest':
    print(f"Erro: O melhor modelo foi '{best_model_name}'. O código abaixo está otimizando um 'Random Forest'.")
    print("Considere ajustar os parâmetros para o seu melhor modelo.")

print(f"--- INICIANDO AJUSTE FINO PARA O MODELO: Random Forest ---")
print("Este processo pode levar vários minutos...")

pipeline_for_tuning = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', RandomForestClassifier(random_state=42, class_weight='balanced', n_jobs=-1))
])

# grid de hiper parâmetros que quero testar
param_dist = {
    'classifier__n_estimators': [100, 200, 300, 400],       # numro de arvrs na florst
    'classifier__max_depth': [10, 20, 30, None],            # profnd maxma de cada arvre
    'classifier__min_samples_split': [2, 5, 10],            # minmo de amostr para divdr um no
    'classifier__min_samples_leaf': [1, 2, 4],              # minmo de amostr em um no folha
    'classifier__bootstrap': [True, False]                  # se amostr sao sortds com ou sem repsc
}
# Random Search
# n_iter: Número de combinacoes a serem testadas
# cv: no de "folds" da cross validation . acho que 3 deve bastar
# scoring: A metrica que quero otimizar.
random_search = RandomizedSearchCV(
    estimator=pipeline_for_tuning,
    param_distributions=param_dist,
    n_iter=15,  #  15 combinac
    cv=3,
    scoring='roc_auc',  ## <- vou otimizar isso
    verbose=2,
    random_state=42,
    n_jobs=-1  # aumenta no de cores do proc
)

#! comeca busca
random_search.fit(X_train, y_train)


print("\n\n--- RESULTADOS DO TUNING ---")
print(f"Melhor pontuação AUC (validação cruzada): {random_search.best_score_:.4f}")
print("Melhores parms encontrados:")
print(random_search.best_params_)


print("\n--- AVALIAÇÃO FINAL DO MODELO OTIMO NO CONJUNTO DE TESTE ---")
best_model_tuned = random_search.best_estimator_
y_pred_proba_tuned = best_model_tuned.predict_proba(X_test)[:, 1]
auc_score_tuned = roc_auc_score(y_test, y_pred_proba_tuned)

print(f"AUC do modelo original no teste: {results['Random Forest']:.4f}")
print(f"AUC do modelo AJUSTADO no teste: {auc_score_tuned:.4f}")

print("\nRelatório de Classificação do Modelo Ajustado:")
y_pred_class_tuned = best_model_tuned.predict(X_test)
print(classification_report(y_test, y_pred_class_tuned))


# Salvar o modelo final- pra nao ter que ficar treinando de novo e de novo

import joblib    ### sim... eu pensei a mesma coisa kkk

joblib.dump(best_model_tuned, 'modelo_inadimplencia_rf_OTIMIZADO.pkl')
print("\n Modelo final OTIMIZADO salvo em 'modelo_inadimplencia_rf_OTIMIZADO.pkl'  !!!  ")




#%% testando com o proprio "df_final"

#* ==============================================================================
#* Testando o Modelo Treinado
#* ==============================================================================
import pandas as pd
import numpy as np
import joblib
import os
from datetime import datetime

MODEL_FILE = 'modelo_inadimplencia_rf_OTIMIZADO.pkl'

CAPACIDADE_COBRANCA = 500


# -(RE)CARREGAR O MODELO TREINADO --- >> -- >> 
print(f"--- ETAPA 1: Carregando o modelo '{MODEL_FILE}' ---")
if not os.path.exists(MODEL_FILE):
    raise FileNotFoundError(f"ERRO: O arquivo do modelo '{MODEL_FILE}' não foi encontrado.")
production_model = joblib.load(MODEL_FILE)
print("Modelo carregado com sucesso.")


#* ==============================================================================
#* >>> ANÁLISE: O RISCO AUMENTA COM O No DE PARCELAS?
#* ==============================================================================
## to deixando ele robusto a colcoar novos dados. 
## agora, paradoxalmente, stou usando o próprio df_final
# PRe PROC
def preprocess_new_data(df_new):
    print("Iniciando pré-processamento dos novos dados...")
    df_processed = df_new.copy()
    date_cols = ['DataEmissao', 'DataAquisicao', 'DataVencimento', 'DataGeracao']
    for col in date_cols:
        df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')
    df_processed['PrazoEmissaoVencimento'] = (df_processed['DataVencimento'] - df_processed['DataEmissao']).dt.days
    df_processed['dias_atraso'] = (df_processed['DataGeracao'] - df_processed['DataVencimento']).dt.days
    df_processed = df_processed[df_processed['dias_atraso'] >= 0].copy()
    print(f"Pré-processamento concluído. {len(df_processed)} títulos válidos para análise.")
    return df_processed

df_predict = preprocess_new_data(df_final)
# prob para CADA PARCELA
probabilities = production_model.predict_proba(df_predict)[:, 1]
df_predict['Probabilidade_Inadimplencia'] = probabilities
# Agrupo por nome do sacado
df_agregado_cliente = df_predict.groupby('SacadoNome').agg(
    Risco_Maximo_Cliente=('Probabilidade_Inadimplencia', 'max'),
    Valor_Total_Devido=('ValorNominal', 'sum'),
    Qtd_Parcelas_Abertas=('ValorNominal', 'count'),
    Maior_Atraso_Dias=('dias_atraso', 'max')
).reset_index()



#### a parte de cima serve para colocar novos dados 
# agg clientes pela quantidade de parcelas abertas
risco_por_qtd_parcelas = df_agregado_cliente.groupby('Qtd_Parcelas_Abertas').agg(
    Risco_Medio_Grupo=('Risco_Maximo_Cliente', 'mean'),
    Qtd_Clientes_Grupo=('SacadoNome', 'count')
).reset_index()

risco_por_qtd_parcelas_filtrado = risco_por_qtd_parcelas[risco_por_qtd_parcelas['Qtd_Clientes_Grupo'] >= 10]

print("\nRisco Médio por Quantidade de Parcelas em Aberto:")
display(risco_por_qtd_parcelas_filtrado.head(15))

plt.figure(figsize=(14, 7))
sns.barplot(
    data=risco_por_qtd_parcelas_filtrado.head(25), # Limita a 25 para melhor visualização
    x='Qtd_Parcelas_Abertas',
    y='Risco_Medio_Grupo',
    palette='coolwarm'
)
plt.title('Risco Médio de Inadimplência vs. Quantidade de Parcelas em Aberto', fontsize=16)
plt.xlabel('Quantidade de Parcelas em Aberto por Cliente')
plt.ylabel('Probabilidade Média de Inadimplência (Risco)')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
