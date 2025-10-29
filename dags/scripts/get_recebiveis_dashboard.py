from common.vortx_handler import VortxHandler

def _salvar_rankings_csv(handler, dados_lista, source_name, tipo_ranking: str, data_ref: str):
    """
    Função auxiliar para salvar especificamente as listas de ranking (Top 10) em CSV.

    Args:
        handler: A instância do seu VortxHandler (para usar o save_csv).
        dados_lista: A lista de dicionários (o ranking em si). Ex: dados.get('Top10CedenteVP').
        source_name: O nome da pasta principal. Ex: 'recebiveis_dashboard'.
        tipo_ranking: O nome específico deste ranking para o nome do arquivo. 
                      Ex: 'dashboard_top10_cedentes_vp'.
        data_ref: A data de referência da execução da task (o data_str).
    """
    # 1. Checa se tem alguma coisa pra salvar. Se a lista veio vazia, nem tenta.
    if not dados_lista or not isinstance(dados_lista, list) or len(dados_lista) == 0:
        print(f"AVISO: Lista de dados para o ranking '{tipo_ranking}' (data: {data_ref}) está vazia. Nenhum CSV será salvo.")
        return # Cai fora da função

    # 2. Se tem dados, chama a função save_csv do Handler.
    #    É o Handler quem vai montar o nome do arquivo bonitão e salvar.
    try:
        handler.save_csv(
            data=dados_lista,          # A lista com os dados do ranking
            source_name=source_name,   # A pasta onde vai salvar (ex: recebiveis_dashboard)
            tipo=tipo_ranking,         # O tipo específico para o nome do arquivo
            data_ref=data_ref          # A data da execução (para o nome do arquivo)
            # Não passamos data_ini/data_fim aqui, pois o handler usa data_ref por padrão
        )
        # Se chegou aqui, o handler.save_csv imprimiu a mensagem de sucesso.
        
    except Exception as e:
        # Se der ruim no save_csv (permissão, disco cheio, erro no handler),
        # imprime um aviso mas não quebra o script principal.
        print(f"AVISO: Falha ao tentar salvar o CSV do ranking '{tipo_ranking}' para data {data_ref}. Erro: {e}")

        

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
        handler.save_json(
            data=dados,
            source_name=source_name, # ex: 'recebiveis_dashboard'
            tipo="dashboard_recebiveis", 
            data_ref=data_str
        )

        # 2. Chamar o helper _salvar_rankings_csv (que agora chama handler.save_csv internamente)
        _salvar_rankings_csv(handler, dados.get('Top10CedenteVP'), source_name, "dashboard_top10_cedentes_vp", data_str)
        _salvar_rankings_csv(handler, dados.get('Top10CedentePDD'), source_name, "dashboard_top10_cedentes_pdd", data_str)
        _salvar_rankings_csv(handler, dados.get('Top10SacadoVP'), source_name, "dashboard_top10_sacados_vp", data_str)
        print("--- Busca de Dashboard Recebíveis concluída ---")

    except Exception as e:
        print(f"ERRO: Falha ao buscar o dashboard. {e}")
        # Propaga o erro para o Airflow
        raise Exception(f"Falha ao buscar o dashboard: {e}")

# (Não adicione o `if __name__ == "__main__":`)