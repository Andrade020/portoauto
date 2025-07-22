# app.py

import pandas as pd
import configparser
import os
from flask import Flask, render_template, jsonify, request
import plotly.express as px
import plotly.io as pio

# Importa a função de processamento de dados
from data_processing import get_summary_data

# --- CONFIGURAÇÃO E CARGA INICIAL DOS DADOS ---

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
    print("A aplicação não pode iniciar sem os dados. Verifique o config.cfg e os caminhos.")
    df_summary = pd.DataFrame() # Cria um dataframe vazio para evitar que a app quebre

# Define um tema padrão para os gráficos
pio.templates.default = "plotly_white"

# --- FUNÇÕES AUXILIARES DE FORMATAÇÃO E GRÁFICOS ---

def format_value(value, format_type):
    """Formata valores para exibição nos cards."""
    if pd.isna(value):
        return "N/A"
    if format_type == 'currency':
        return f"R$ {value:,.2f}"
    if format_type == 'percent':
        return f"{value:.2%}"
    if format_type == 'integer':
        return f"{int(value):,}"
    return value

def create_charts(df):
    """Cria todos os gráficos necessários a partir do DataFrame."""
    df_entes = df[df['Nome do Ente'] != '* CARTEIRA *'].copy()
    
    # 1. Gráfico de Composição da Carteira
    fig_pie = px.pie(
        df_entes,
        names='Nome do Ente',
        values='% Carteira',
        title='Composição da Carteira (% Valor Líquido)',
        hole=0.3
    )
    fig_pie.update_traces(textposition='inside', textinfo='percent+label')

    # 2. Gráfico de Ranking de Risco
    fig_risk = px.bar(
        df_entes.sort_values('% Vencidos', ascending=False),
        x='Nome do Ente',
        y='% Vencidos',
        title='Ranking de Risco (% Vencidos)',
        labels={'% Vencidos': '% Vencidos', 'Nome do Ente': 'Ente'},
        text_auto='.2%'
    )
    fig_risk.update_layout(yaxis_tickformat=".0%")

    # 3. Gráfico de Ranking de Rentabilidade
    fig_tir = px.bar(
        df_entes.sort_values('TIR líquida a.m.', ascending=False),
        x='Nome do Ente',
        y='TIR líquida a.m.',
        title='Ranking de Rentabilidade (TIR Líquida a.m.)',
        labels={'TIR líquida a.m.': 'TIR Líquida a.m.', 'Nome do Ente': 'Ente'},
        text_auto='.2%'
    )
    fig_tir.update_layout(yaxis_tickformat=".2%")

    # 4. Gráfico de Relação Risco vs. Retorno
    fig_scatter = px.scatter(
        df_entes,
        x='% Vencidos',
        y='TIR líquida a.m.',
        text='Nome do Ente',
        title='Risco (% Vencidos) vs. Retorno (TIR Líquida)',
        labels={'% Vencidos': 'Risco (% Vencidos)', 'TIR líquida a.m.': 'Retorno (TIR Líquida a.m.)'}
    )
    fig_scatter.update_traces(textposition='top center')
    fig_scatter.update_layout(xaxis_tickformat=".0%", yaxis_tickformat=".2%")

    return {
        'pie': fig_pie.to_json(),
        'risk': fig_risk.to_json(),
        'tir': fig_tir.to_json(),
        'scatter': fig_scatter.to_json(),
    }


# --- ROTAS DA APLICAÇÃO ---

@app.route('/')
def index():
    """Renderiza a página principal do dashboard."""
    if df_summary.empty:
        return "<h1>Erro ao carregar os dados. Verifique os logs do servidor.</h1>"

    # Pega os dados da carteira total para os KPIs iniciais
    carteira_total = df_summary[df_summary['Nome do Ente'] == '* CARTEIRA *'].iloc[0]
    
    kpis = {
        'valor_liquido': format_value(carteira_total['Valor Líquido'], 'currency'),
        'contratos': format_value(carteira_total['# Contratos'], 'integer'),
        'inadimplencia': format_value(carteira_total['% Vencidos'], 'percent'),
        'provisionamento': format_value(carteira_total['% PDD'], 'percent'),
        'rentabilidade': format_value(carteira_total['TIR líquida a.m.'], 'percent'),
    }

    # Gera os gráficos
    charts_json = create_charts(df_summary)

    # Prepara os dados da tabela (sem a carteira total)
    table_data = df_summary[df_summary['Nome do Ente'] != '* CARTEIRA *'].to_dict(orient='records')

    return render_template(
        'index.html',
        kpis=kpis,
        charts=charts_json,
        table_data=table_data,
        data_referencia=DATA_REFERENCIA_STR
    )

@app.route('/api/data')
def get_data_for_ente():
    """API para fornecer dados filtrados para um ente específico."""
    ente_selecionado = request.args.get('ente', '* CARTEIRA *')
    
    if ente_selecionado not in df_summary['Nome do Ente'].values:
        return jsonify({"error": "Ente não encontrado"}), 404

    data = df_summary[df_summary['Nome do Ente'] == ente_selecionado].iloc[0]

    # Prepara os KPIs para o ente selecionado
    kpis = {
        'valor_liquido': format_value(data['Valor Líquido'], 'currency'),
        'contratos': format_value(data['# Contratos'], 'integer'),
        'inadimplencia': format_value(data['% Vencidos'], 'percent'),
        'provisionamento': format_value(data['% PDD'], 'percent'),
        'rentabilidade': format_value(data.get('TIR líquida a.m.'), 'percent'), # .get para segurança
    }
    
    # Se o ente selecionado for a carteira, a tabela mostra todos. Senão, mostra só o ente.
    if ente_selecionado == '* CARTEIRA *':
        table_data = df_summary[df_summary['Nome do Ente'] != '* CARTEIRA *'].to_dict(orient='records')
    else:
        table_data = df_summary[df_summary['Nome do Ente'] == ente_selecionado].to_dict(orient='records')

    return jsonify({
        "kpis": kpis,
        "table_data": table_data,
        "ente_selecionado": ente_selecionado
    })


if __name__ == '__main__':
    app.run(debug=True, port=5001) 