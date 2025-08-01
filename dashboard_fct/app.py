# app.py

from flask import Flask, render_template
from analysis_logic import obter_dados_dashboard
import os

# --- Configurações da Aplicação ---
app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0 # Desabilita o cache de arquivos estáticos durante o desenvolvimento

# Define os caminhos para os arquivos de dados (relativos à pasta do projeto)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data_files')

patt = os.path.join(DATA_DIR, 'fct_2025-07-14', '*Estoque*.csv')
caminho_feriados = os.path.join(DATA_DIR, 'feriados_nacionais.xls')
path_map_entes = os.path.join(DATA_DIR, 'MAP_ENTES.xlsx')
path_relacoes = os.path.join(DATA_DIR, 'relacoes.csv')


# --- Conteúdo do Relatório ---
mapa_descricoes = {
    'Cedentes': 'Analisa as métricas de risco e retorno agrupadas por cada Cedente (originador) dos títulos.',
    'Tipo de Contrato': 'Agrupa os dados por Tipo de Ativo (CCB, Contrato) para comparar o desempenho de cada um.',
    'Ente Consignado': 'Métricas detalhadas por cada Convênio (ente público ou privado) onde a consignação é feita.',
    'Situação': 'Compara o desempenho dos títulos com base na sua situação atual (ex: Aditado, Liquidado).',
    'Tipo de Pessoa Sacado': 'Diferencia a análise entre sacados Pessoa Física (F) e Jurídica (J).',
    'Pagamento Parcial': 'Verifica se há impacto nas métricas para títulos que aceitam pagamento parcial.',
    'Tem Muitos Contratos': 'Compara sacados com um número baixo vs. alto de contratos (CCBs) ativos.',
    'Tem Muitos Entes': 'Compara sacados que operam em poucos vs. múltiplos convênios (entes).',
    'Sacado é BMP': 'Isola e analisa especificamente as operações com o sacado BMP S.A.',
    'Nível do Ente': 'Agrupa os convênios por nível governamental (Municipal, Estadual, Federal).',
    'Previdência': 'Identifica e analisa separadamente os convênios que são de regimes de previdência.',
    'Ente Genérico': 'Agrupa convênios que não se encaixam em uma categoria específica.',
    'CAPAG': 'Métricas baseadas na Capacidade de Pagamento (CAPAG) do município ou estado, uma nota do Tesouro Nacional.',
    'Faixa Pop. Municipal': 'Agrupa os convênios municipais por faixas de população.',
    'Faixa Pop. Estadual': 'Agrupa os convênios estaduais por faixas de população.',
    'UF': 'Agrega todas as métricas por Unidade Federativa (Estado).'
}

footer_main_text = """
Esta dashboard tem como objetivo apresentar uma análise de desempenho do fundo FCT Consignado II (CNPJ 52.203.615/0001-19), realizada pelo Porto Real Investimentos na qualidade de cogestora. Os prestadores de serviço do fundo são: FICTOR ASSET (Gestor), Porto Real Investimentos (Cogestor), e VÓRTX DTVM (Administrador e Custodiante).
"""
footer_disclaimer = """
Disclaimer: Esta Dashboard foi preparada pelo Porto Real Asset Management exclusivamente para fins informativos e não constitui uma oferta de venda, solicitação de compra ou recomendação para qualquer investimento. As informações aqui contidas são baseadas em fontes consideradas confiáveis na data de sua publicação, mas não há garantia de sua precisão ou completude. Rentabilidade passada não representa garantia de rentabilidade futura.
"""


# --- Rota Principal do Flask ---
@app.route('/')
def dashboard():
    """
    Rota principal que carrega os dados, processa e renderiza o template.
    """
    # Executa a análise completa para obter as tabelas HTML e a data
    mapa_tabelas_html, data_relatorio = obter_dados_dashboard(
        patt, caminho_feriados, path_map_entes, path_relacoes
    )

    # Renderiza o template, passando todas as variáveis necessárias
    return render_template(
        'index.html',
        data_relatorio=data_relatorio.strftime('%d/%m/%Y'),
        mapa_tabelas=mapa_tabelas_html,
        mapa_descricoes=mapa_descricoes,
        footer_main_text=footer_main_text.strip(),
        footer_disclaimer=footer_disclaimer.strip()
    )


if __name__ == '__main__':
    # PORT 8001 pro meu WINDOWS
    app.run(debug=True, port=8001)