import sys
from common.vortx_handler import VortxHandler

def main(exec_date):
    print(f"--- Iniciando busca da Carteira para {exec_date} ---")
    handler = VortxHandler()
    
    source_name = handler.config.get('CARTEIRA', 'source_name')
    cnpjs = [cnpj.strip() for cnpj in handler.config.get('CARTEIRA', 'cnpjs').split(',')]

    if not handler.authenticate():
        raise Exception("Falha na autenticação Vórtx")

    try:
        params = {"cnpjFundos[]": cnpjs, "dataCarteira": exec_date}
        response = handler.make_rest_request('GET', '/carteira-liberada/buscarCarteiraJSON', params=params)
        dados_carteira = response.json()
        
        filename = f"carteira_{'_'.join(cnpjs)}_{exec_date.replace('-', '')}"
        handler.save_json(dados_carteira, source_name, filename)
        
        print("--- Busca de Carteira concluída ---")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        raise e # Propaga o erro para o Airflow
