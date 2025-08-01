# /src/fct_consig/flask_dashboard/app.py

import pandas as pd
import configparser
import os
from flask import Flask, render_template, jsonify, request
import numpy as np

# Importa a função de processamento de dados
from data_processing import get_summary_data

# --- CONFIGURAÇÃO E CARGA INICIAL DOS DADOS -- # 

# Configura o Flask
app = Flask(__name__)

# Carrega as configurações do config.cfg
config = configparser.ConfigParser()
config.read('config.cfg')

ROOT_PATH = config.get('Paths', 'root_path')
CAMINHO_PASTA_DADOS = os.path.join(ROOT_PATH, config.get('Paths', 'pasta_dados'))
CAMINHO_FERIADOS = os.path.join(ROOT_PATH, config.get('Paths', 'arquivo_feriados'))
DATA_REFERENCIA_STR = config.get('Parameters', 'data_referencia')
PADRAO_ARQUIVO_DADOS = config.get('Parameters', 'padrao_arquivo_dados')

# Carrega e processa os dados UMA VEZ ao iniciar a aplicação
try:
    df_summary = get_summary_data(
        CAMINHO_PASTA_DADOS,
        PADRAO_ARQUIVO_DADOS,
        DATA_REFERENCIA_STR,
        CAMINHO_FERIADOS
    )
except (FileNotFoundError, IOError) as e:
    print(f"ERRO CRÍTICO: {e}")
    df_summary = pd.DataFrame()

# --- ROTAS DA APLICAÇÃO ---

@app.route('/')
def index():
    if df_summary.empty:
        return "<h1>Erro Crítico</h1><p>Os dados da carteira não puderam ser carregados. Verifique os logs do servidor.</p>"
    
    metricas = [
        'Valor Líquido', '% Vencidos', '% PDD', 
        'TIR líquida a.m.', 'Prazo Médio (DU)', 'Qtd Contratos'
    ]
    return render_template(
        'index.html',
        data_referencia=DATA_REFERENCIA_STR,
        metricas=metricas
    )

@app.route('/api/dashboard_data')
def get_dashboard_data():
    if df_summary.empty:
        return jsonify({"error": "Dados não disponíveis"}), 500

    metric = request.args.get('metric', 'Valor Líquido')
    top_n = int(request.args.get('top_n', 20))

    if metric not in df_summary.columns:
        return jsonify({"error": f"Métrica '{metric}' não encontrada"}), 400

    df_entes = df_summary[df_summary['Nome do Ente'] != '* CARTEIRA *'].copy()
    df_entes[metric] = pd.to_numeric(df_entes[metric], errors='coerce').fillna(0)

    # --- CÁLCULO DOS KPIs (Já implementado) ---
    carteira_total_row = df_summary[df_summary['Nome do Ente'] == '* CARTEIRA *'].iloc[0]
    
    if not df_entes.empty:
        max_ente_row = df_entes.loc[df_entes[metric].idxmax()]
        min_ente_row = df_entes.loc[df_entes[metric].idxmin()]
        std_dev_val = df_entes[metric].std()
        
        kpi_data = {
            'total': float(carteira_total_row[metric]),
            'mean': float(df_entes[metric].mean()),
            'max_value': float(max_ente_row[metric]),
            'max_ente': max_ente_row['Nome do Ente'],
            'min_value': float(min_ente_row[metric]),
            'min_ente': min_ente_row['Nome do Ente'],
            'std_dev': float(0 if pd.isna(std_dev_val) else std_dev_val),
        }
    else: 
        kpi_data = {key: 0 for key in ['total', 'mean', 'max_value', 'min_value', 'std_dev']}
        kpi_data.update({'max_ente': 'N/A', 'min_ente': 'N/A'})

    # --- LÓGICA DO GRÁFICO (Já implementada) ---
    df_sorted = df_entes.sort_values(by=metric, ascending=False)
    
    if top_n == -1 or len(df_sorted) <= top_n:
        chart_labels = df_sorted['Nome do Ente'].tolist()
        chart_values = df_sorted[metric].tolist()
    else:
        df_top = df_sorted.head(top_n)
        df_others = df_sorted.iloc[top_n:]
        
        if '%' in metric or 'TIR' in metric or 'Prazo' in metric:
            others_value = df_others[metric].mean()
        else:
            others_value = df_others[metric].sum()
        
        chart_labels = df_top['Nome do Ente'].tolist()
        chart_values = df_top[metric].tolist()

        if others_value > 0:
            chart_labels.append('Outros')
            chart_values.append(float(others_value))

    chart_data = {
        'labels': chart_labels,
        'values': [float(v) for v in chart_values]
    }

    #* NOVO: ADICIONANDO DADOS DA TABELA À RESPOSTA DA API 
    table_data = df_entes.to_dict(orient='records')

    response_data = {
        'kpi_data': kpi_data,
        'chart_data': chart_data,
        'table_data': table_data # Nova chave na resposta
    }

    return jsonify(response_data)


if __name__ == '__main__':
    app.run(debug=True, port=5001, reloader_type='stat')
