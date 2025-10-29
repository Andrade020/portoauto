import time
from common.vortx_handler import VortxHandler

def main_recebiveis_estoque(data_str: str):
    """
    Busca o relatório de estoque (paginado) para a data de referência.
    """
    print(f"--- Iniciando busca de Estoque Recebíveis para {data_str} ---")
    handler = VortxHandler()
    
    cnpj_fundo = handler.config.get('VORTX_RECEBIVEIS', 'cnpj_sem_mascara')
    source_name = "recebiveis_estoque" # Nome da pasta em data_raw
    
    url_endpoint = f"/relatorios/estoque/{cnpj_fundo}"
    
    estoque_completo = []
    skip = 0
    take = 100000  # Máximo por página
    pagina = 1

    while True:
        print(f"Buscando página {pagina} (a partir do registro {skip})...")
        
        params = {
            'skip': skip,
            'take': take,
            'DataReferencia': data_str # Usamos a data da DAG
        }
        
        try:
            # Usamos o novo método do handler
            response = handler.make_recebiveis_request(url_endpoint, params)
            dados_pagina = response.json()
            
            if not isinstance(dados_pagina, list):
                print(f"ERRO: A resposta da API não foi uma lista. Resposta: {dados_pagina}")
                break

            num_registros = len(dados_pagina)
            print(f"Recebidos {num_registros} registros.")
            
            if num_registros > 0:
                estoque_completo.extend(dados_pagina)

            if num_registros < take:
                print("Última página alcançada.")
                break # Sai do loop
                
            skip += take
            pagina += 1
            time.sleep(1) # Pausa entre páginas

        except Exception as e:
            print(f"ERRO: Falha ao buscar a página {pagina}. {e}")
            # Propaga o erro para o Airflow
            raise Exception(f"Falha na paginação do estoque: {e}")

    if not estoque_completo:
        print("Nenhum registro de estoque encontrado.")
        return

    print(f"Download completo! Total de {len(estoque_completo)} registros.")

    # --- Salvando os arquivos ---
    nome_arquivo_base = f"estoque_{cnpj_fundo}_{data_str.replace('-', '')}"
    
    # Salva usando os métodos do handler
    handler.save_json(estoque_completo, source_name, f"{nome_arquivo_base}_completo")
    handler.save_csv(estoque_completo, source_name, f"{nome_arquivo_base}_entradas")
    
    print("--- Busca de Estoque Recebíveis concluída ---")

# (Não adicione o `if __name__ == "__main__":`)