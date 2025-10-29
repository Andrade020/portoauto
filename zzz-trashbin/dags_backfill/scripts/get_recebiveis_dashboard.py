from common.vortx_handler import VortxHandler

def _salvar_rankings_csv(handler, dados_lista, source_name, nome_arquivo):
    """Helper interno para salvar os rankings Top10."""
    if not dados_lista or not isinstance(dados_lista, list) or len(dados_lista) == 0:
        return
    try:
        handler.save_csv(dados_lista, source_name, nome_arquivo)
    except Exception as e:
        print(f"AVISO: Não foi possível salvar o CSV '{nome_arquivo}'. Erro: {e}")

def main_recebiveis_dashboard(data_str: str):
    """
    Busca os dados do dashboard para a data de consulta.
    """
    print(f"--- Iniciando busca de Dashboard Recebíveis para {data_str} ---")
    handler = VortxHandler()
    
    cnpj_fundo = handler.config.get('VORTX_RECEBIVEIS', 'cnpj_com_mascara')
    source_name = "recebiveis_dashboard" # Nome da pasta
    
    url_endpoint = "/dashboard"
    params = {
        "fundoCnpj": cnpj_fundo,
        "date": data_str # Usamos a data da DAG
    }

    try:
        response = handler.make_recebiveis_request(url_endpoint, params)
        dados = response.json()
        print(">>> Sucesso! Dados do dashboard recebidos.")
        
        # --- Salvando os arquivos ---
        nome_arquivo_base = f"dashboard_{cnpj_fundo.replace('/', '-')}_{data_str.replace('-', '')}"

        # 1. Salvar o JSON completo
        handler.save_json(dados, source_name, f"{nome_arquivo_base}_completo")

        # 2. Salvar os rankings em CSV
        _salvar_rankings_csv(handler, dados.get('Top10CedenteVP'), source_name, f"{nome_arquivo_base}_top10_cedentes_vp.csv")
        _salvar_rankings_csv(handler, dados.get('Top10CedentePDD'), source_name, f"{nome_arquivo_base}_top10_cedentes_pdd.csv")
        _salvar_rankings_csv(handler, dados.get('Top10SacadoVP'), source_name, f"{nome_arquivo_base}_top10_sacados_vp.csv")
        
        print("--- Busca de Dashboard Recebíveis concluída ---")

    except Exception as e:
        print(f"ERRO: Falha ao buscar o dashboard. {e}")
        # Propaga o erro para o Airflow
        raise Exception(f"Falha ao buscar o dashboard: {e}")

# (Não adicione o `if __name__ == "__main__":`)