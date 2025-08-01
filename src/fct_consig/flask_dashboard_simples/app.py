# /src/fct_consig/flask_dashboard/app.py
#* MbyLRdA
import pandas as pd
import configparser
import os
from flask import Flask, render_template, jsonify, request

# Importa a função de processamento de dados
from data_processing import get_summary_data

# --- CONFIGURAÇÃO E CARGA INICIAL DOS DADOS --- #

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
except FileNotFoundError as e:
    print(f"ERRO CRÍTICO: {e}")
    df_summary = pd.DataFrame()

# --- FUNÇÕES AUXILIARES DE FORMATAÇÃO E GRÁFICOS ---

def format_value(value, format_type):
    """Formata valores para exibição nos cards."""
    if pd.isna(value) or value is None:
        return "N/A"
    if format_type == 'currency':
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    if format_type == 'percent':
        return f"{value:.2%}".replace(".",",")
    if format_type == 'integer':
        return f"{int(value):,}".replace(",",".")
    if format_type == 'days':
        return f"{int(value)} DU"
    return value


def prepare_chartjs_data(df):
    """Prepara os dados no formato que o Chart.js espera."""
    df_entes = df[df['Nome do Ente'] != '* CARTEIRA *'].copy()

    # --- LÓGICA DO GRÁFICO DE PIZZA (TOP 10 + OUTROS) ---
    df_pie_sorted = df_entes.sort_values('% Carteira', ascending=False)
    
    if len(df_pie_sorted) > 10:
        df_top_10 = df_pie_sorted.head(10)
        outros_sum = df_pie_sorted.iloc[10:]['% Carteira'].sum()
        
        outros_data = pd.DataFrame([{'Nome do Ente': 'Outros', '% Carteira': outros_sum}])
        
        df_pie_final = pd.concat([df_top_10, outros_data], ignore_index=True)
    else:
        df_pie_final = df_pie_sorted

    # Ordena os dataframes para os gráficos de ranking
    df_risk_sorted = df_entes.sort_values('% Vencidos', ascending=False)
    df_tir_sorted = df_entes.sort_values('TIR líquida a.m.', ascending=False).fillna(0)

    chart_data = {
        # Gráfico 1: Composição da Carteira (Pizza) - USA OS DADOS TRATADOS
        'composition': {
            'labels': df_pie_final['Nome do Ente'].tolist(),
            'datasets': [{
                'data': df_pie_final['% Carteira'].tolist(),
            }]
        },
        # Gráfico 2: Ranking de Risco (Barras)
        'risk': {
            'labels': df_risk_sorted['Nome do Ente'].tolist(),
            'datasets': [{'label': '% Vencidos', 'data': df_risk_sorted['% Vencidos'].tolist(), 'backgroundColor': 'rgba(255, 99, 132, 0.5)', 'borderColor': 'rgba(255, 99, 132, 1)', 'borderWidth': 1}]
        },
        # Gráfico 3: Ranking de Rentabilidade (Barras) $ 
        'tir': {
            'labels': df_tir_sorted['Nome do Ente'].tolist(),
            'datasets': [{'label': 'TIR líquida a.m.', 'data': df_tir_sorted['TIR líquida a.m.'].tolist(), 'backgroundColor': 'rgba(54, 162, 235, 0.5)', 'borderColor': 'rgba(54, 162, 235, 1)', 'borderWidth': 1}]
        },
        # Gráfico 4: Risco vs. Retorno (Dispersão)
        'scatter': {
            'datasets': [{'label': 'Risco vs. Retorno por Ente', 'data': [{'x': row['% Vencidos'], 'y': row['TIR líquida a.m.'], 'ente': row['Nome do Ente']} for index, row in df_entes.iterrows()], 'backgroundColor': 'rgba(75, 192, 192, 0.5)',}]
        }
    }
    return chart_data

# --- ROTAS DA APLICAÇÃO ---

@app.route('/')
def index():
    """Renderiza a página principal do dashboard."""
    if df_summary.empty:
        return "<h1>Erro ao carregar os dados. Verifique os logs do servidor.</h1>"

    carteira_total = df_summary[df_summary['Nome do Ente'] == '* CARTEIRA *'].iloc[0]
    
    # *** NOVO: Adicionado 'prazo_medio' aos KPIs ***
    kpis = {
        'valor_liquido': format_value(carteira_total['Valor Líquido'], 'currency'),
        'contratos': format_value(carteira_total['# Contratos'], 'integer'),
        'inadimplencia': format_value(carteira_total['% Vencidos'], 'percent'),
        'provisionamento': format_value(carteira_total['% PDD'], 'percent'),
        'rentabilidade': format_value(carteira_total['TIR líquida a.m.'], 'percent'),
        'prazo_medio': format_value(carteira_total['Prazo Médio (DU)'], 'days'), # NOVO KPI
    }

    # Gera os dados para os gráficos do Chart.js
    chart_data = prepare_chartjs_data(df_summary)

    table_data = df_summary[df_summary['Nome do Ente'] != '* CARTEIRA *'].to_dict(orient='records')

    return render_template(
        'index.html',
        kpis=kpis,
        chart_data=chart_data,
        table_data=table_data,
        data_referencia=DATA_REFERENCIA_STR
    )

@app.route('/api/data')
def get_data_for_ente():
    """API para fornecer dados filtrados."""
    ente_selecionado = request.args.get('ente', '* CARTEIRA *')
    
    if ente_selecionado not in df_summary['Nome do Ente'].values:
        return jsonify({"error": "Ente não encontrado"}), 404

    data = df_summary[df_summary['Nome do Ente'] == ente_selecionado].iloc[0]

    # *** NOVO: Adicionado 'prazo_medio' à resposta da API ***
    kpis = {
        'valor_liquido': format_value(data['Valor Líquido'], 'currency'),
        'contratos': format_value(data['# Contratos'], 'integer'),
        'inadimplencia': format_value(data['% Vencidos'], 'percent'),
        'provisionamento': format_value(data['% PDD'], 'percent'),
        'rentabilidade': format_value(data.get('TIR líquida a.m.'), 'percent'),
        'prazo_medio': format_value(data.get('Prazo Médio (DU)'), 'days'), # NOVO KPI NA API
    }
    
    if ente_selecionado == '* CARTEIRA *':
        table_data = df_summary[df_summary['Nome do Ente'] != '* CARTEIRA *'].to_dict(orient='records')
    else:
        table_data = df_summary[df_summary['Nome do Ente'] == ente_selecionado].to_dict(orient='records')

    return jsonify({
        "kpis": kpis,
        "table_data": table_data,
        "ente_selecionado": ente_selecionado
    })

##
if __name__ == '__main__':
    app.run(debug=True, port=5001, reloader_type='stat')