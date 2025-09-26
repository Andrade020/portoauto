import pandas as pd
import numpy as np
import os
import warnings
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.impute import SimpleImputer
import glob
import unicodedata
import re
import pprint
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, RobustScaler, PowerTransformer
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from lightgbm.callback import early_stopping as lgb_early_stopping
import shap
import optuna
warnings.filterwarnings('ignore')

def carregar_e_preparar_dados(): 
    print("="*88)
    print("ETAPA 1: Carregando dados (VERSÃO PARA ML V3 - COM PRÓXIMA PARCELA)...")
    print("="*88)

    path_starcard = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\StarCard.xlsx'
    path_originadores = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\originadores\data\Originadores.xlsx'
    path_relacoes = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\relacoes.csv'
    path_contas_regionais_folder = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\data_raw\dados_macro_adicionais\ibge\contas_regionais'

    def normalizar_nome(texto):
        if not isinstance(texto, str): return ""
        texto = re.sub(r'^(PREF\.\s*|PRERF\.\s*|GOV\.\s*|AGN\s*-\s*)', '', texto, flags=re.IGNORECASE)
        texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
        texto = re.sub(r'[^A-Z0-9\s]', '', texto.upper())
        texto = re.sub(r'\s+', ' ', texto).strip()
        return texto

    df_starcard = pd.read_excel(path_starcard)
    cols_originadores = ['CCB', 'Data de Inclusão', 'Prazo', 'Valor Parcela', 'Produto', 'Convênio', 'CAPAG']
    try:
        df_originadores = pd.read_excel(path_originadores, usecols=cols_originadores)
    except ValueError:
        print("[ALERTA] Coluna 'CAPAG' não encontrada.")
        cols_originadores = [c for c in cols_originadores if c != 'CAPAG']
        df_originadores = pd.read_excel(path_originadores, usecols=cols_originadores)
    
    df_pib_ibge = pd.DataFrame()
    try:
        all_xlsx_files = glob.glob(os.path.join(path_contas_regionais_folder, '*.xlsx'))
        valid_files = [f for f in all_xlsx_files if not os.path.basename(f).startswith('~$')]
        if not valid_files: raise FileNotFoundError("Nenhum arquivo .xlsx válido foi encontrado.")
        latest_file = max(valid_files, key=os.path.getctime)
        print(f"[INFO] Lendo arquivo de Contas Regionais: {os.path.basename(latest_file)}")
        df_pib_ibge_full = pd.read_excel(latest_file, header=0)
        df_pib_ibge_full.columns = [str(col).replace('\n', ' ').strip() for col in df_pib_ibge_full.columns]
        ano_col_name = [col for col in df_pib_ibge_full.columns if "Ano" in col][0]
        pib_col_name = [col for col in df_pib_ibge_full.columns if "Produto Interno Bruto" in col][0]
        cod_uf_col_name = [col for col in df_pib_ibge_full.columns if "Unidade da Federação" in col][0]
        cod_mun_col_name = [col for col in df_pib_ibge_full.columns if "do Município" in col][0]
        cols_to_use = [ano_col_name, cod_uf_col_name, cod_mun_col_name, pib_col_name]
        df_pib_ibge_full = df_pib_ibge_full[cols_to_use]
        df_pib_ibge_full.columns = ['Ano', 'Codigo_UF', 'Codigo_Municipio', 'PIB']
        ano_recente = df_pib_ibge_full['Ano'].max()
        print(f"[INFO] Ano mais recente encontrado na base do IBGE: {ano_recente}. Filtrando dados...")
        df_pib_ibge = df_pib_ibge_full[df_pib_ibge_full['Ano'] == ano_recente].copy()
        for col in ['Codigo_UF', 'Codigo_Municipio']:
            df_pib_ibge[col] = pd.to_numeric(df_pib_ibge[col], errors='coerce')
        df_pib_ibge.dropna(subset=['Codigo_UF', 'Codigo_Municipio'], inplace=True)
        df_pib_ibge['Codigo_UF'] = df_pib_ibge['Codigo_UF'].astype(int)
        df_pib_ibge['Codigo_Municipio'] = df_pib_ibge['Codigo_Municipio'].astype(int)
        df_pib_estados = df_pib_ibge.groupby("Codigo_UF", as_index=False)['PIB'].sum().rename(columns={'PIB': 'PIB_Estadual'})
    except Exception as e:
        print(f"[ERRO] Falha ao carregar ou processar o arquivo de PIB: {e}.")

    df_relacoes = pd.DataFrame()
    try:
        cols_relacoes = ['Convênio', 'populacao', 'tipo', 'cod_uf', 'codigo', 'Nome']
        df_relacoes = pd.read_csv(path_relacoes, sep=';', usecols=cols_relacoes)
        df_relacoes['cod_uf'] = pd.to_numeric(df_relacoes['cod_uf'], errors='coerce').fillna(0).astype(int)
        df_relacoes['codigo'] = pd.to_numeric(df_relacoes['codigo'], errors='coerce').fillna(0).astype(int)
        mapa_uf = { 'ALAGOAS': 27, 'AMAZONAS': 13, 'ESPIRITO SANTO': 32, 'GOIAS': 52, 'MARANHAO': 21, 'PARAIBA': 25, 'PARANA': 41, 'PIAUI': 22, 'RIO DE JANEIRO': 33, 'RIO GRANDE DO NORTE': 24, 'SANTA CATARINA': 42, 'SAO PAULO': 35, 'TOCANTINS': 17, 'CEARA': 23, 'MATO GROSSO': 51 }
        print("[INFO] Corrigindo 'cod_uf' ausente para convênios de estado...")
        for nome_estado, codigo_uf in mapa_uf.items():
            mask = (df_relacoes['Convênio'].str.contains(nome_estado, case=False, na=False)) & (df_relacoes['tipo'] == 2)
            df_relacoes.loc[mask, 'cod_uf'] = codigo_uf
        if not df_pib_ibge.empty:
            df_relacoes = pd.merge(df_relacoes, df_pib_ibge[['Codigo_Municipio', 'PIB']], left_on='codigo', right_on='Codigo_Municipio', how='left')
            df_relacoes = pd.merge(df_relacoes, df_pib_estados, left_on='cod_uf', right_on='Codigo_UF', how='left')
            df_relacoes['PIB_Consolidado'] = np.where(df_relacoes['tipo'] == 1, df_relacoes['PIB'], df_relacoes['PIB_Estadual'])
        df_relacoes['Nome_Normalizado'] = df_relacoes['Nome'].apply(normalizar_nome)
    except FileNotFoundError:
        print(f"[ERRO] 'relacoes.csv' não encontrado.")

    if 'Data de Inclusão' in df_originadores.columns:
        df_originadores['Data de Inclusão'] = pd.to_datetime(df_originadores['Data de Inclusão'], errors='coerce')
        df_originadores = df_originadores.sort_values(['CCB', 'Data de Inclusão']).drop_duplicates(subset='CCB', keep='first')
    
    df_merged = pd.merge(df_starcard, df_originadores, on='CCB', how='left', suffixes=('', '_orig'))
    
    if 'Convênio' in df_merged.columns and not df_relacoes.empty:
        df_merged_original_convenio = df_merged['Convênio'].copy()
        df_merged['Convênio'] = df_merged['Convênio'].astype(str).str.upper().str.strip()
        df_relacoes_lookup = df_relacoes.drop_duplicates(subset='Convênio', keep='first').copy()
        df_relacoes_lookup['Convênio'] = df_relacoes_lookup['Convênio'].astype(str).str.upper().str.strip()
        features_externas = ['Convênio', 'populacao', 'PIB_Consolidado', 'tipo']
        features_existentes = [f for f in features_externas if f in df_relacoes_lookup.columns]
        df_merged = pd.merge(df_merged, df_relacoes_lookup[features_existentes], on='Convênio', how='left')
        df_merged['Convênio'] = df_merged_original_convenio

    if 'PIB_Consolidado' in df_merged.columns:
        nao_informados_mask = df_merged['PIB_Consolidado'].isnull()
        if nao_informados_mask.any():
            print(f"[INFO] {nao_informados_mask.sum()} registros não encontrados. Iniciando busca por fallback...")
            counts = df_relacoes['Nome_Normalizado'].value_counts()
            nomes_ambiguos = counts[counts > 1].index.tolist()
            if nomes_ambiguos:
                print(f"[ALERTA] Nomes de municípios ambíguos encontrados. Resolvendo pela maior população.")
            df_relacoes_sorted = df_relacoes.sort_values('populacao', ascending=False)
            df_relacoes_deduped = df_relacoes_sorted.drop_duplicates(subset='Nome_Normalizado', keep='first')
            mapa_fallback = df_relacoes_deduped.set_index('Nome_Normalizado')
            convenios_a_tratar = df_merged.loc[nao_informados_mask, 'Convênio'].unique()
            for conv in convenios_a_tratar:
                nome_normalizado = normalizar_nome(conv)
                if 'AGN - RIO GRANDE DO NORTE' in conv: nome_normalizado = 'RIO GRANDE DO NORTE'
                if nome_normalizado in mapa_fallback.index:
                    dados_encontrados = mapa_fallback.loc[nome_normalizado]
                    mask_update = df_merged['Convênio'] == conv
                    df_merged.loc[mask_update, 'populacao'] = dados_encontrados['populacao']
                    df_merged.loc[mask_update, 'PIB_Consolidado'] = dados_encontrados['PIB_Consolidado']
                    df_merged.loc[mask_update, 'tipo'] = dados_encontrados['tipo']
    
    df_base = df_merged.copy()
    del df_starcard, df_originadores, df_merged, df_relacoes, df_pib_ibge
    df_base = df_base.rename(columns={'Data Referencia': 'DataGeracao', 'Data Vencimento': 'DataVencimento', 'ID Cliente': 'SacadoID'})
    for col in ['DataGeracao', 'DataVencimento', 'Data de Nascimento']:
        if col in df_base.columns: df_base[col] = pd.to_datetime(df_base[col], errors='coerce')
    
    if 'Valor Parcela' in df_base.columns and df_base['Valor Parcela'].dtype == 'object':
        s = df_base['Valor Parcela'].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip()
        df_base['Valor Parcela'] = pd.to_numeric(s, errors='coerce')
        
    if 'Produto' in df_base.columns: df_base['Produto'] = df_base['Produto'].fillna('')
    if 'Convênio' in df_base.columns: df_base['Convênio'] = df_base['Convênio'].fillna('')
    
    if 'DataGeracao' in df_base.columns and 'DataVencimento' in df_base.columns:
        df_base['dias_de_atraso'] = (df_base['DataGeracao'] - df_base['DataVencimento']).dt.days
        
    condicoes_produto = [df_base['Produto'].str.contains('Empréstimo', case=False, na=False), df_base['Produto'].str.contains('Cartão RMC', case=False, na=False), df_base['Produto'].str.contains('Cartão Benefício', case=False, na=False)]
    opcoes_produto = ['Empréstimo', 'Cartão RMC', 'Cartão Benefício']
    df_base['_TipoProduto'] = np.select(condicoes_produto, opcoes_produto, default='Outros')
    
    condicoes_empregado = [df_base['Produto'].str.contains('Efetivo|Efetivio', case=False, na=False, regex=True), df_base['Produto'].str.contains('Temporário', case=False, na=False), df_base['Produto'].str.contains('CONTRATADO', case=False, na=False), df_base['Produto'].str.contains('Comissionado', case=False, na=False)]
    opcoes_empregado = ['Efetivo', 'Temporário', 'Contratado', 'Comissionado']
    df_base['_TipoEmpregado'] = np.select(condicoes_empregado, opcoes_empregado, default='Outros')
    
    condicoes_convenio = [df_base['Convênio'].str.contains(r'GOV\.|AGN -', case=False, na=False, regex=True), df_base['Convênio'].str.contains(r'PREF\.|PRERF', case=False, na=False, regex=True)]
    opcoes_convenio = ['Estadual', 'Municipal']
    df_base['_EsferaConvenio'] = np.select(condicoes_convenio, opcoes_convenio, default='Outros')
    
    if 'Data de Inclusão' not in df_base.columns: raise ValueError("Coluna 'Data de Inclusão' não encontrada.")
    df_base = df_base.sort_values(['SacadoID', 'Data de Inclusão', 'CCB'])
    df_base['_NumContratosAnteriores'] = df_base.groupby('SacadoID').cumcount()
    df_base['_MuitosContratos'] = (df_base['_NumContratosAnteriores'] >= 2).astype(int)
    
    if 'Data de Nascimento' in df_base.columns:
        base_idade = df_base['Data de Inclusão'].fillna(df_base.get('DataGeracao'))
        df_base['_IdadeCliente'] = (base_idade - df_base['Data de Nascimento']).dt.days / 365.25
        
    df_base['populacao_municipal'] = np.where(df_base['tipo'] == 1, df_base['populacao'].fillna(0), 0)
    df_base['PIB_municipal'] = np.where(df_base['tipo'] == 1, df_base['PIB_Consolidado'].fillna(0), 0)
    df_base['populacao_estadual'] = np.where(df_base['tipo'] == 2, df_base['populacao'].fillna(0), 0)
    df_base['PIB_estadual'] = np.where(df_base['tipo'] == 2, df_base['PIB_Consolidado'].fillna(0), 0)

    if 'CAPAG' in df_base.columns:
        print("[INFO] Mapeando CAPAG para variável numérica ordinal...")
        capag_mapa = {
            'A+': 6, 'A': 5, 'A-': 4,
            'B+': 3, 'B': 2, 'B-': 1,
            'C': 0, 'D': -1
        }
        df_base['CAPAG_Ordinal'] = df_base['CAPAG'].map(capag_mapa).fillna(-2)

    print("[INFO] Calculando o número da próxima parcela a vencer para cada contrato...")
    if 'DataGeracao' in df_base.columns and 'DataVencimento' in df_base.columns and 'PARCELA' in df_base.columns:
        df_futuras = df_base[df_base['DataVencimento'] >= df_base['DataGeracao']].copy()
        df_futuras = df_futuras.sort_values('DataVencimento')
        df_proxima_parcela = df_futuras.drop_duplicates(subset='CCB', keep='first')
        df_proxima_parcela = df_proxima_parcela[['CCB', 'PARCELA']].rename(columns={'PARCELA': 'Proxima_Parcela_Num'})
        df_base = pd.merge(df_base, df_proxima_parcela, on='CCB', how='left')
        df_base['Proxima_Parcela_Num'] = df_base['Proxima_Parcela_Num'].fillna(0).astype(int)
    else:
        print("[ALERTA] Colunas necessárias para calcular a próxima parcela não encontradas. Feature não criada.")


    atraso_por_ccb = df_base.groupby('CCB', as_index=False)['dias_de_atraso'].max().rename(columns={'dias_de_atraso': 'max_dias_atraso'})
    for k in [1, 30, 60, 90]: atraso_por_ccb[f'Vencido_{k}d_Flag'] = (atraso_por_ccb['max_dias_atraso'] >= k).astype(int)
    
    df_base['DataInclRef'] = df_base['Data de Inclusão'].fillna(df_base.get('DataGeracao'))
    snapshots = df_base.sort_values(['CCB', 'DataInclRef', 'DataVencimento'])
    primeiro_snap = snapshots.groupby('CCB', as_index=False).first()
    
    features_para_modelo = [
        'CCB', 'Prazo', 'Valor Parcela', 
        '_TipoProduto', '_TipoEmpregado', '_EsferaConvenio', 
        '_MuitosContratos', 
        '_IdadeCliente',
        'populacao_municipal', 'PIB_municipal',
        'populacao_estadual', 'PIB_estadual',
        'CAPAG_Ordinal' #,    'Proxima_Parcela_Num' # <-- RUIM!! ele dá previsao quase perfeita
    ]
    
    ausentes = sorted(list(set(features_para_modelo) - set(primeiro_snap.columns)))
    if ausentes: print(f"[AVISO] Features ausentes e removidas: {ausentes}")
    
    features_existentes = [f for f in features_para_modelo if f in primeiro_snap.columns]
    df_features = primeiro_snap[features_existentes]
    
    df_ml = pd.merge(df_features, atraso_por_ccb, on='CCB', how='inner').rename(columns={
        'Prazo': 'Total_Parcelas', 'Valor Parcela': 'Valor_Parcela', 
        '_EsferaConvenio': 'Esfera_convenio', '_MuitosContratos': 'Muitos_Contratos',
        '_TipoProduto': 'Tipo_Produto', '_TipoEmpregado': 'Tipo_Empregado',
        '_IdadeCliente': 'Idade_Cliente',
        'Proxima_Parcela_Num': 'Proxima_Parcela_Num'
    })
    
    print(f"\n<INFO> Dataset para ML criado com {df_ml.shape[0]} linhas e {df_ml.shape[1]} colunas.")
    print("\n<INFO> Verificando as primeiras linhas e os tipos de dados do dataframe final:")
    print(df_ml.head())
    print("\n")
    df_ml.info()
        
    return df_ml

def _mostrar_feature_importance(pipeline, X_train_cols):
    try:
        classifier = pipeline.named_steps['classifier']
        preprocessor = pipeline.named_steps['preprocessor']
        cat_features = preprocessor.transformers_[1][2]
        num_features = preprocessor.transformers_[0][2]
        cat_features_out = preprocessor.named_transformers_['cat'].get_feature_names_out(cat_features)
        feature_names = num_features + list(cat_features_out)

        if hasattr(classifier, 'feature_importances_'):
            importances = pd.Series(classifier.feature_importances_, index=feature_names)
        elif hasattr(classifier, 'coef_'):
            importances = pd.Series(classifier.coef_[0], index=feature_names)
        else:
            print("[INFO] Modelo não possui um atributo de importância de features padrão.")
            return

        print("Top 10 importâncias:")
        print(importances.abs().sort_values(ascending=False).head(20))

    except Exception as e:
        print(f"[AVISO] Não foi possível extrair feature importance: {e}")

def treinar_e_avaliar(df_ml, target):
    print("\n" + "="*88)
    print(f"   ETAPA 1: TREINANDO MODELOS BASE PARA O ALVO: {target}")
    print("="*88)
    targets_all = ['Vencido_1d_Flag', 'Vencido_30d_Flag', 'Vencido_60d_Flag', 'Vencido_90d_Flag']
    X = df_ml.drop(columns=targets_all + ['CCB', 'max_dias_atraso'])
    y = df_ml[target]
    X_train_val, X_test, y_train_val, y_test = train_test_split(X, y, test_size=0.20, random_state=42, stratify=y)
    X_train, X_val, y_train, y_val = train_test_split(X_train_val, y_train_val, test_size=0.25, random_state=42, stratify=y_train_val)

    numerical_features = X.select_dtypes(include=np.number).columns.tolist()
    categorical_features = X.select_dtypes(include=['object', 'category']).columns.tolist()
    numeric_transformer = Pipeline(steps=[('imputer', SimpleImputer(strategy='median')), ('power_transform', PowerTransformer()), ('scaler', RobustScaler())])
    categorical_transformer = Pipeline(steps=[('onehot', OneHotEncoder(handle_unknown='ignore', drop='first'))])
    preprocessor = ColumnTransformer(transformers=[('num', numeric_transformer, numerical_features), ('cat', categorical_transformer, categorical_features)], remainder='passthrough')

    pos = int((y_train == 1).sum())
    neg = int((y_train == 0).sum())
    scale_pos_weight = neg / pos if pos > 0 else 1.0
    
    
    models = {
        "Regressão Logística": LogisticRegression(random_state=42, class_weight='balanced', max_iter=2000, n_jobs=-1),
        "Random Forest":       RandomForestClassifier(random_state=42, class_weight='balanced', n_jobs=-1),
        "XGBoost":             xgb.XGBClassifier(random_state=42, eval_metric='logloss', scale_pos_weight=scale_pos_weight, n_jobs=-1, early_stopping_rounds=20),
        "LightGBM":            lgb.LGBMClassifier(random_state=42, scale_pos_weight=scale_pos_weight, n_jobs=-1, verbosity=-1),
        "Rede Neural (MLP)":   MLPClassifier(random_state=42, max_iter=500, early_stopping=True, hidden_layer_sizes=(100, 50))
    }
    
    modelos_para_otimizar = {}
    preprocessor.fit(X_train)

    for name, model in models.items():
        print(f"\n--- Treinando {name} ---")
        full_pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('classifier', model)])
        
        fit_params = {}
        if name in ["XGBoost", "LightGBM", "Rede Neural (MLP)"]:
            X_val_transformed = preprocessor.transform(X_val)
            if name == "XGBoost":
                fit_params = {'classifier__eval_set': [(X_val_transformed, y_val)]}
            elif name == "LightGBM":
                fit_params = {
                    'classifier__eval_set': [(X_val_transformed, y_val)],
                    'classifier__callbacks': [lgb_early_stopping(20, verbose=False)]
                }
        
        full_pipeline.fit(X_train, y_train, **fit_params)
        y_pred_proba = full_pipeline.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_pred_proba)
        
        print(f"Performance no CONJUNTO DE TESTE -> ROC-AUC: {auc:.4f}")
        
        # NOVA PARTE: Mostra a importância das features para o modelo atual
        _mostrar_feature_importance(full_pipeline, X.columns)
        
        # Salva os principais competidores para a próxima fase
        if name in ["Random Forest", "XGBoost", "LightGBM"]:
            modelos_para_otimizar[name] = {'pipeline': full_pipeline, 'score_base': auc}
            
    return modelos_para_otimizar, preprocessor, X_train, y_train, X_val, y_val, X_test, y_test

def otimizar_modelos_selecionados(modelos_para_otimizar, preprocessor, X_train, y_train, X_val, y_val):
    print("\n" + "="*88)
    print("   ETAPA 2: OTIMIZANDO HIPERPARÂMETROS DOS PRINCIPAIS MODELOS")
    print("="*88)

    todos_params_otimizados = {}
    
    for nome_modelo, info in modelos_para_otimizar.items():
        pipeline_base = info['pipeline']
        print(f"\n--- Otimizando {nome_modelo} ---")
        
        X_train_transformed = preprocessor.transform(X_train)
        X_val_transformed = preprocessor.transform(X_val)

        def objective(trial):
            # (A função 'objective' interna permanece a mesma da versão anterior)
            modelo_classe = pipeline_base.steps[-1][1].__class__.__name__
            if "XGB" in modelo_classe:
                param = {'n_estimators': trial.suggest_int('n_estimators', 200, 1000), 'max_depth': trial.suggest_int('max_depth', 4, 10), 'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.2)}
                model = xgb.XGBClassifier(**param, random_state=42, eval_metric='logloss')
            elif "LGBM" in modelo_classe:
                param = {'n_estimators': trial.suggest_int('n_estimators', 200, 1000), 'max_depth': trial.suggest_int('max_depth', 4, 10), 'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.2), 'num_leaves': trial.suggest_int('num_leaves', 20, 100)}
                model = lgb.LGBMClassifier(**param, random_state=42, verbosity=-1)

            elif "Forest" in modelo_classe:
                param = {
                    'n_estimators': trial.suggest_int('n_estimators', 100, 700),
                    # --- MUDANÇA AQUI ---
                    # Forçamos o Optuna a testar árvores com profundidade de no mínimo 8
                    'max_depth': trial.suggest_int('max_depth', 8, 30), 
                    'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
                    'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 20),
                }
                model = RandomForestClassifier(**param, random_state=42, n_jobs=-1)
            
            model.fit(X_train_transformed, y_train)
            preds = model.predict_proba(X_val_transformed)[:, 1]
            return roc_auc_score(y_val, preds)

        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=30, show_progress_bar=True)

        print(f"Melhor ROC-AUC na validação para {nome_modelo}: {study.best_value:.4f}")
        todos_params_otimizados[nome_modelo] = study.best_params

    return todos_params_otimizados

def explicar_modelo_com_shap(pipeline_final, X_train, X_test):
    print("\n" + "="*88)
    print("   ETAPA 3: GERANDO EXPLICAÇÕES DO MODELO COM SHAP")
    print("="*88)
    
    preprocessor = pipeline_final.named_steps['preprocessor']
    classifier = pipeline_final.named_steps['classifier']
    modelo_classe = classifier.__class__.__name__

    X_test_transformed = preprocessor.transform(X_test)
    
    if "XGB" in modelo_classe or "LGBM" in modelo_classe or "Forest" in modelo_classe:
        print("Usando TreeExplainer para modelo baseado em árvore...")
        explainer = shap.TreeExplainer(classifier)
        shap_values = explainer.shap_values(X_test_transformed)
    else:
        print(f"Usando KernelExplainer para {modelo_classe}... (Isso pode ser mais lento)")
        # KernelExplainer é mais lento, então usamos uma amostra do background (treino)
        X_train_summary = shap.sample(preprocessor.transform(X_train), 100)
        explainer = shap.KernelExplainer(classifier.predict_proba, X_train_summary)
        shap_values = explainer.shap_values(X_test_transformed, nsamples=50)

    try:
        cat_features_out = preprocessor.named_transformers_['cat'].get_feature_names_out(preprocessor.transformers_[1][2])
        feature_names = preprocessor.transformers_[0][2] + list(cat_features_out)
        X_test_transformed_df = pd.DataFrame(X_test_transformed, columns=feature_names)
    except:
        X_test_transformed_df = pd.DataFrame(X_test_transformed)

    print("Gerando gráfico SHAP...")
    plot_values = shap_values[1] if isinstance(shap_values, list) else shap_values
    
    shap.summary_plot(plot_values, X_test_transformed_df, show=True, max_display=15)

df_ml = carregar_e_preparar_dados()

alvo_principal = 'Vencido_60d_Flag'

(modelos_para_otimizar, preprocessor, 
    X_train, y_train, X_val, y_val, X_test, y_test) = treinar_e_avaliar(df_ml, target=alvo_principal)

params_otimizados_dict = otimizar_modelos_selecionados(
    modelos_para_otimizar, preprocessor, X_train, y_train, X_val, y_val
)

X_train_val = pd.concat([X_train, X_val])
y_train_val = pd.concat([y_train, y_val])

print("\n\n" + "#"*88)
print("#   RESULTADOS FINAIS PARA OS MODELOS OTIMIZADOS   #")
print("#"*88)

for nome_modelo, melhores_params in params_otimizados_dict.items():
    print("\n" + "="*88)
    print(f"   PROCESSANDO MODELO FINAL: {nome_modelo}")
    print("="*88)

    pipeline_base = modelos_para_otimizar[nome_modelo]['pipeline']
    
    modelo_final_sem_pipe = pipeline_base.steps[-1][1].__class__(**melhores_params, random_state=42)
    
    pipeline_final = Pipeline(steps=[('preprocessor', preprocessor), ('classifier', modelo_final_sem_pipe)])
    
    print(f"Treinando {nome_modelo} otimizado...")
    pipeline_final.fit(X_train_val, y_train_val)
    
    y_pred_final = pipeline_final.predict_proba(X_test)[:, 1]
    auc_final = roc_auc_score(y_test, y_pred_final)
    print(f"\n🚀 Performance FINAL de '{nome_modelo}' no conjunto de teste: ROC-AUC = {auc_final:.4f}")

    explicar_modelo_com_shap(pipeline_final, X_train, X_test)

