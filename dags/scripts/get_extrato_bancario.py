import sys
from common.vortx_handler import VortxHandler

QUERY = """
query GetExtratoFundo($cnpjFundo: String, $dataInicial: Date, $dataFinal: Date) {
    getExtratoFundo(cnpjFundo: $cnpjFundo, dataInicial: $dataInicial, dataFinal: $dataFinal) {
      cnpjFundo dataInicial dataFinal saldoInicial saldoFinal
      dadosBancariosFundo { banco conta digito agencia }
      lancamentos { debito credito saldoInicial saldoFinal dataPosicao descricao detalhes tipo documento contraparte{ banco agencia conta } }
    }
}
"""
def flatten_lancamentos(lancamentos):
    # "achata" a estrutura da contraparte para o csv
    flat_list = []
    for item in lancamentos:
        item_plano = item.copy()
        contraparte = item_plano.pop('contraparte', {}) or {}
        item_plano['contraparte_banco'] = contraparte.get('banco')
        item_plano['contraparte_agencia'] = contraparte.get('agencia')
        item_plano['contraparte_conta'] = contraparte.get('conta')
        flat_list.append(item_plano)
    return flat_list

def main(exec_date):
    print(f"--- Iniciando busca do Extrato Bancário para {exec_date} ---")
    handler = VortxHandler()
    
    source_name = handler.config.get('EXTRATO', 'source_name')
    cnpj = handler.config.get('EXTRATO', 'cnpj_formatado')

    if not handler.authenticate():
        raise Exception("Falha na autenticação Vórtx")

    try:
        # por padrão, busca apenas para o dia da execução
        variables = {"cnpjFundo": cnpj, "dataInicial": exec_date, "dataFinal": exec_date}
        response_data = handler.make_graphql_request(QUERY, variables)
        
        dados_extrato_raw = response_data.get("getExtratoFundo")
        if not dados_extrato_raw:
            print("Query executou, mas não retornou dados.")
            return
        
        dados_extrato = dados_extrato_raw[0] if isinstance(dados_extrato_raw, list) else dados_extrato_raw

        filename_base = f"extrato_{cnpj.replace('.', '').replace('/', '').replace('-', '')}_{exec_date.replace('-', '')}"
        handler.save_json(dados_extrato, source_name, f"{filename_base}_completo")
        
        lancamentos = flatten_lancamentos(dados_extrato.get("lancamentos", []))
        handler.save_csv(lancamentos, source_name, f"{filename_base}_lancamentos")
        
        print("--- Busca do Extrato Bancário concluída ---")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        raise e # Propaga o erro para o Airflow