# =============================================================================
# Bloco 1: Importação de Bibliotecas
# =============================================================================
# Descrição: Importa bibliotecas para manipulação de dados, machine learning e visualização.
import pandas as pd
import numpy as np
from glob import glob
import matplotlib.pyplot as plt
import seaborn as sns
import joblib

# Pré-processamento e Modelagem
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import (classification_report, roc_auc_score, confusion_matrix, 
                             mean_squared_error, r2_score, mean_absolute_error)

# Modelos
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
import lightgbm as lgb

# Configurações de exibição
pd.options.display.max_columns = 100
pd.options.display.max_rows = 200

# =============================================================================
# Bloco 2: Carregamento e Engenharia de Atributos (Baseado no script original)
# =============================================================================
# Descrição: Reutiliza a lógica de carregamento e criação de features do script
# de análise para garantir consistência e aproveitar o trabalho já feito.

print("Iniciando carregamento e engenharia de atributos...")

# Caminhos (ajuste conforme necessário)
patt = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\data_temp\fct_2025-07-14\*Estoque*.csv'
list_files = glob(patt)

if not list_files:
    raise FileNotFoundError(f"Nenhum arquivo encontrado no padrão: {patt}. Verifique o caminho.")

# Definição das colunas
colunas_data = ['DataEmissao', 'DataAquisicao', 'DataVencimento', 'DataGeracao']
colunas_texto = ['Situacao', 'CedenteNome', 'SacadoCnpjCpf', 'TipoAtivo', 'Convênio']
dtype_texto = {col: str for col in colunas_texto}

# Leitura e concatenação
dfs = [pd.read_csv(file, sep=';', encoding='latin1', dtype=dtype_texto,
                   decimal=',', parse_dates=colunas_data, dayfirst=True)
       for file in list_files]
df_full = pd.concat(dfs, ignore_index=True)

# Limpeza inicial (removendo linhas com dados essenciais nulos)
df_full.dropna(subset=['Situacao', 'DataGeracao', 'ValorPresente', 'PDDTotal'], inplace=True)
df_full = df_full[df_full['ValorPresente'] > 0] # Evita divisão por zero

# --- Engenharia de Atributos ---
# Coluna de Prazo do Título (feature importante para ML)
df_full['_PrazoDias'] = (df_full['DataVencimento'] - df_full['DataAquisicao']).dt.days

# Sacados com muitos contratos (CCB)
sacado_contratos = df_full.groupby('SacadoCnpjCpf')['CCB'].nunique()
sacado_contratos_alto = sacado_contratos[sacado_contratos >= 3].index
df_full['_MuitosContratos'] = df_full['SacadoCnpjCpf'].isin(sacado_contratos_alto).astype(int)

# Sacados com muitos entes (Convênios)
sacados_entes = df_full.groupby('SacadoCnpjCpf')['Convênio'].nunique()
sacados_entes_alto = sacados_entes[sacados_entes >= 3].index
df_full['_MuitosEntes'] = df_full['SacadoCnpjCpf'].isin(sacados_entes_alto).astype(int)

print("Carregamento e engenharia de atributos concluídos.")
print(f"Total de registros para modelagem: {len(df_full)}")


# =============================================================================
# Bloco 3: Definição das Variáveis-Alvo (Target)
# =============================================================================
# Descrição: Cria as duas variáveis que queremos prever.

# --- Target 1: Inadimplência (Problema de Classificação) ---
# Alvo é 1 se o título estiver vencido, 0 caso contrário.
df_full['Target_Inadimplente'] = (df_full['DataVencimento'] <= df_full['DataGeracao']).astype(int)

# --- Target 2: Risco de PDD (Problema de Regressão) ---
# Alvo é o percentual do PDD em relação ao Valor Presente.
df_full['Target_PDD_Percent'] = (df_full['PDDTotal'] / df_full['ValorPresente']).fillna(0)

print("\nDistribuição do Alvo de Inadimplência:")
print(df_full['Target_Inadimplente'].value_counts(normalize=True))


# =============================================================================
# Bloco 4: Seleção de Features e Preparação dos Dados
# =============================================================================
# Descrição: Seleciona as colunas que serão usadas como features (X) e os
# alvos (y). Define o pipeline de pré-processamento para tratar dados
# numéricos e categóricos.

# Seleção de Features: colunas que o modelo usará para prever
features = [
    'ValorPresente',
    'ValorNominal',
    'Situacao',
    'CedenteNome',
    'TipoAtivo',
    'Convênio',
    '_PrazoDias',
    '_MuitosContratos',
    '_MuitosEntes'
]

# Features numéricas e categóricas
numeric_features = df_full[features].select_dtypes(include=np.number).columns.tolist()
categorical_features = df_full[features].select_dtypes(include=['object', 'category']).columns.tolist()

print(f"\nFeatures Numéricas: {numeric_features}")
print(f"Features Categóricas: {categorical_features}")

# Definição do Pré-processador com ColumnTransformer
# - Numéricas: Padroniza a escala (importante para Regressão Logística/Linear)
# - Categóricas: Transforma em variáveis numéricas (One-Hot Encoding)
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numeric_features),
        ('cat', OneHotEncoder(handle_unknown='ignore', drop='first'), categorical_features)
    ],
    remainder='passthrough' # Mantém outras colunas (se houver)
)


# =============================================================================
# Bloco 5: Modelo de Classificação - Previsão de Inadimplência
# =============================================================================
print("\n" + "="*80)
print("INICIANDO MODELAGEM DE CLASSIFICAÇÃO (PREVISÃO DE INADIMPLÊNCIA)")
print("="*80)

# Separando dados para o modelo de classificação
X_class = df_full[features]
y_class = df_full['Target_Inadimplente']

# Divisão em treino e teste (estratificado para manter a proporção de inadimplentes)
X_train_c, X_test_c, y_train_c, y_test_c = train_test_split(
    X_class, y_class, test_size=0.25, random_state=42, stratify=y_class
)

# Definição dos modelos a serem testados
models_class = {
    "Regressão Logística": LogisticRegression(random_state=42, class_weight='balanced', max_iter=1000),
    "Random Forest": RandomForestClassifier(random_state=42, class_weight='balanced'),
    "LightGBM": lgb.LGBMClassifier(random_state=42)
}

best_model_c = None
best_auc = -1

for name, model in models_class.items():
    print(f"\n--- Treinando e avaliando: {name} ---")
    
    # Criação do pipeline completo
    pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                               ('classifier', model)])
    
    # Treinamento
    pipeline.fit(X_train_c, y_train_c)
    
    # Predições
    y_pred_c = pipeline.predict(X_test_c)
    y_proba_c = pipeline.predict_proba(X_test_c)[:, 1]
    
    # Avaliação
    auc = roc_auc_score(y_test_c, y_proba_c)
    print(f"AUC Score: {auc:.4f}")
    print("Relatório de Classificação:")
    print(classification_report(y_test_c, y_pred_c))
    
    if auc > best_auc:
        best_auc = auc
        best_model_c = pipeline

print(f"\nMelhor modelo de classificação foi '{type(best_model_c.named_steps['classifier']).__name__}' com AUC de {best_auc:.4f}")

# =============================================================================
# Bloco 6: Importância das Features (Classificação)
# =============================================================================
# Descrição: Extrai e exibe as features mais importantes do melhor modelo.

print("\n--- Importância das Features (Melhor Modelo de Classificação) ---")

# Acessa os nomes das features após o OneHotEncoding
try:
    feature_names_raw = best_model_c.named_steps['preprocessor'].get_feature_names_out()
    
    # Acessa as importâncias do modelo dentro do pipeline
    if hasattr(best_model_c.named_steps['classifier'], 'feature_importances_'):
        importances = best_model_c.named_steps['classifier'].feature_importances_
        
        feature_importance_df = pd.DataFrame({
            'feature': feature_names_raw,
            'importance': importances
        }).sort_values('importance', ascending=False)

        print(feature_importance_df.head(15))

        # Plotar as top 15 features
        plt.figure(figsize=(10, 8))
        sns.barplot(x='importance', y='feature', data=feature_importance_df.head(15))
        plt.title('Top 15 Features Mais Importantes para Prever Inadimplência')
        plt.tight_layout()
        plt.show()

    else:
        print("Este modelo (Regressão Logística) não possui 'feature_importances_'. Use 'coef_' para interpretação.")
        
except Exception as e:
    print(f"Não foi possível extrair a importância das features: {e}")


# =============================================================================
# Bloco 7: Modelo de Regressão - Previsão do Percentual de PDD
# =============================================================================
print("\n" + "="*80)
print("INICIANDO MODELAGEM DE REGRESSÃO (PREVISÃO DE PERCENTUAL DE PDD)")
print("="*80)

# Separando dados para o modelo de regressão
X_reg = df_full[features]
y_reg = df_full['Target_PDD_Percent']

# Divisão em treino e teste
X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(
    X_reg, y_reg, test_size=0.25, random_state=42
)

# Definição dos modelos
models_reg = {
    "Regressão Linear": LinearRegression(),
    "Random Forest": RandomForestRegressor(random_state=42, n_jobs=-1),
    "LightGBM": lgb.LGBMRegressor(random_state=42, n_jobs=-1)
}

for name, model in models_reg.items():
    print(f"\n--- Treinando e avaliando: {name} ---")
    
    pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                               ('regressor', model)])
    
    pipeline.fit(X_train_r, y_train_r)
    y_pred_r = pipeline.predict(X_test_r)
    
    r2 = r2_score(y_test_r, y_pred_r)
    rmse = np.sqrt(mean_squared_error(y_test_r, y_pred_r))
    mae = mean_absolute_error(y_test_r, y_pred_r)
    
    print(f"R² Score: {r2:.4f}")
    print(f"Mean Absolute Error (MAE): {mae:.4f}")
    print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")


# =============================================================================
# Bloco 8: Salvando o Melhor Modelo
# =============================================================================
# Descrição: Salva o pipeline completo do melhor modelo de classificação para uso futuro.

output_path = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\modelos'
os.makedirs(output_path, exist_ok=True) # Cria a pasta se não existir

model_filename = os.path.join(output_path, 'modelo_inadimplencia_v1.joblib')

print(f"\nSalvando o melhor modelo de classificação em: {model_filename}")
joblib.dump(best_model_c, model_filename)

# Demonstração de como carregar e usar o modelo salvo
print("\n--- Demonstração de Carga e Predição ---")
loaded_model = joblib.load(model_filename)
sample = X_test_c.head(1)
prediction = loaded_model.predict(sample)
prediction_proba = loaded_model.predict_proba(sample)

print("Exemplo de Título para Predição:")
print(sample)
print(f"\nPredição (0=Adimplente, 1=Inadimplente): {prediction[0]}")
print(f"Probabilidade de ser Inadimplente: {prediction_proba[0][1]:.2%}")

print("\n" + "="*80)
print("SCRIPT DE MACHINE LEARNING CONCLUÍDO!")
print("="*80)