# /src/fct_consig/flask_dashboard/app.py

import pandas as pd
import configparser
import os
from flask import Flask, render_template, jsonify, request
import numpy as np

# Importa a função de processamento de dados
from data_processing import get_summary_data

# --- CONFIGURAÇÃO E CARGA INICIAL DOS DADOS ---
app = Flask(__name__)
config = configparser.ConfigParser()
config.read('config.cfg')

ROOT_PATH = config.get('Paths', 'root_path')
CAMINHO_PASTA_DADOS = os.path.join(ROOT_PATH, config.get('Paths', 'pasta_dados'))
CAMINHO_FERIADOS = os.path.join(ROOT_PATH, config.get('Paths', 'arquivo_feriados'))
DATA_REFERENCIA_STR = config.get('Parameters', 'data_referencia')
PADRAO_ARQUIVO_DADOS = config.get('Parameters', 'padrao_arquivo_dados')

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

# --- FUNÇÕES AUXILIARES ---

def format_value(value, format_type):
    """Formata valores para exibição."""
    if pd.isna(value) or value is None: return "N/A"
    if format_type == 'currency': return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    if format_type == 'percent': return f"{value:.1%}".replace(".",",")
    if format_type == 'integer': return f"{int(value):,}".replace(",",".")
    if format_type == 'days': return f"{int(value)} DU"
    return value

def prepare_classic_charts(df):
    """Prepara os dados para os 4 gráficos da visão clássica."""
    df_entes = df[df['Nome do Ente'] != '* CARTEIRA *'].copy()
    
    # Gráfico de Pizza
    df_pie_sorted = df_entes.sort_values('% Carteira', ascending=False)
    df_top_10 = df_pie_sorted.head(10)
    outros_sum = df_pie_sorted.iloc[10:]['% Carteira'].sum()
    outros_data = pd.DataFrame([{'Nome do Ente': 'Outros', '% Carteira': outros_sum}])
    df_pie_final = pd.concat([df_top_10, outros_data], ignore_index=True)
    
    # Gráficos de Barras
    df_risk_sorted = df_entes.sort_values('% Vencidos', ascending=False)
    df_tir_sorted = df_entes.sort_values('TIR líquida a.m.', ascending=False).fillna(0)

    return {
        'composition': {'labels': df_pie_final['Nome do Ente'].tolist(), 'datasets': [{'data': df_pie_final['% Carteira'].tolist()}]},
        'risk': {'labels': df_risk_sorted['Nome do Ente'].tolist(), 'datasets': [{'label': '% Vencidos', 'data': df_risk_sorted['% Vencidos'].tolist(), 'backgroundColor': 'rgba(255, 99, 132, 0.5)'}]},
        'tir': {'labels': df_tir_sorted['Nome do Ente'].tolist(), 'datasets': [{'label': 'TIR líquida a.m.', 'data': df_tir_sorted['TIR líquida a.m.'].tolist(), 'backgroundColor': 'rgba(54, 162, 235, 0.5)'}]},
        'scatter': {'datasets': [{'label': 'Risco vs. Retorno', 'data': [{'x': row['% Vencidos'], 'y': row['TIR líquida a.m.'], 'ente': row['Nome do Ente']} for _, row in df_entes.iterrows()], 'backgroundColor': 'rgba(75, 192, 192, 0.5)'}]}
    }

# --- ROTAS DA APLICAÇÃO ---

@app.route('/')
def index():
    """Renderiza o 'shell' da aplicação que conterá os dois dashboards."""
    if df_summary.empty:
        return "<h1>Erro Crítico</h1><p>Os dados da carteira não puderam ser carregados. Verifique os logs do servidor.</p>"
    
    metricas_avancadas = ['Valor Líquido', '% Vencidos', '% PDD', 'TIR líquida a.m.', 'Prazo Médio (DU)', 'Qtd Contratos']
    return render_template('index.html', data_referencia=DATA_REFERENCIA_STR, metricas=metricas_avancadas)

@app.route('/api/classic_data')
def get_classic_data():
    """Fornece todos os dados necessários para a 'Visão Geral'."""
    if df_summary.empty: return jsonify({"error": "Dados não disponíveis"}), 500
    
    carteira_total = df_summary[df_summary['Nome do Ente'] == '* CARTEIRA *'].iloc[0]
    
    kpis = {
        'valor_liquido': format_value(carteira_total['Valor Líquido'], 'currency'),
        'contratos': format_value(carteira_total['Qtd Contratos'], 'integer'),
        'inadimplencia': format_value(carteira_total['% Vencidos'], 'percent'),
        'provisionamento': format_value(carteira_total['% PDD'], 'percent'),
        'rentabilidade': format_value(carteira_total['TIR líquida a.m.'], 'percent'),
        'prazo_medio': format_value(carteira_total['Prazo Médio (DU)'], 'days'),
    }
    
    chart_data = prepare_classic_charts(df_summary)
    table_data = df_summary[df_summary['Nome do Ente'] != '* CARTEIRA *'].to_dict(orient='records')
    
    return jsonify({'kpis': kpis, 'chart_data': chart_data, 'table_data': table_data})


@app.route('/api/advanced_data')
def get_advanced_data():
    """Fornece os dados para a 'Análise Detalhada'."""
    if df_summary.empty: return jsonify({"error": "Dados não disponíveis"}), 500

    metric = request.args.get('metric', 'Valor Líquido')
    top_n = int(request.args.get('top_n', 20))

    if metric not in df_summary.columns: return jsonify({"error": f"Métrica '{metric}' não encontrada"}), 400

    df_entes = df_summary[df_summary['Nome do Ente'] != '* CARTEIRA *'].copy()
    df_entes[metric] = pd.to_numeric(df_entes[metric], errors='coerce').fillna(0)

    carteira_total_row = df_summary[df_summary['Nome do Ente'] == '* CARTEIRA *'].iloc[0]
    
    if not df_entes.empty:
        max_ente_row = df_entes.loc[df_entes[metric].idxmax()]
        min_ente_row = df_entes.loc[df_entes[metric].idxmin()]
        std_dev_val = df_entes[metric].std()
        kpi_data = {
            'total': float(carteira_total_row[metric]), 'mean': float(df_entes[metric].mean()),
            'max_value': float(max_ente_row[metric]), 'max_ente': max_ente_row['Nome do Ente'],
            'min_value': float(min_ente_row[metric]), 'min_ente': min_ente_row['Nome do Ente'],
            'std_dev': float(0 if pd.isna(std_dev_val) else std_dev_val),
        }
    else: 
        kpi_data = {key: 0 for key in ['total', 'mean', 'max_value', 'min_value', 'std_dev']}
        kpi_data.update({'max_ente': 'N/A', 'min_ente': 'N/A'})

    df_sorted = df_entes.sort_values(by=metric, ascending=False)
    
    if top_n == -1 or len(df_sorted) <= top_n:
        chart_labels = df_sorted['Nome do Ente'].tolist()
        chart_values = df_sorted[metric].tolist()
    else:
        df_top = df_sorted.head(top_n)
        df_others = df_sorted.iloc[top_n:]
        others_value = df_others[metric].mean() if '%' in metric or 'TIR' in metric or 'Prazo' in metric else df_others[metric].sum()
        chart_labels = df_top['Nome do Ente'].tolist()
        chart_values = df_top[metric].tolist()
        if others_value > 0:
            chart_labels.append('Outros')
            chart_values.append(float(others_value))

    chart_data = {'labels': chart_labels, 'values': [float(v) for v in chart_values]}
    table_data = df_entes.to_dict(orient='records')

    return jsonify({'kpi_data': kpi_data, 'chart_data': chart_data, 'table_data': table_data})

if __name__ == '__main__':
    app.run(debug=True, port=5001, reloader_type='stat')
