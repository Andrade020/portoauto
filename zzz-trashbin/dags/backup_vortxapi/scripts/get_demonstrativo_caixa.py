import sys
from common.vortx_handler import VortxHandler

QUERY = """
query GetDemonstrativoCaixa($cnpjFundo: String!, $data: Date!) {
    getDemonstrativoCaixa(cnpjFundo: $cnpjFundo, data: $data) {
        entradas { titulo tituloCp data historico tipo debito credito saldo isDetalheTotal }
        saldoInicial { data saldo }
        saldoFinal { data saldo debito credito }
        carteira nomeDoFundo dataInicio dataFim
    }
}
"""

def main(exec_date):
    print(f"--- Iniciando busca do Demonstrativo de Caixa para {exec_date} ---")
    handler = VortxHandler()
    
    source_name = handler.config.get('CAIXA', 'source_name')
    cnpj = handler.config.get('CAIXA', 'cnpj_formatado')

    if not handler.authenticate():
        sys.exit(1)

    try:
        variables = {"cnpjFundo": cnpj, "data": exec_date}
        response_data = handler.make_graphql_request(QUERY, variables)
        
        dados_caixa_raw = response_data.get("getDemonstrativoCaixa")
        if not dados_caixa_raw:
            print("Query executou, mas não retornou dados.")
            return

        dados_caixa = dados_caixa_raw[0] if isinstance(dados_caixa_raw, list) else dados_caixa_raw
        
        filename_base = f"caixa_{cnpj.replace('.', '').replace('/', '').replace('-', '')}_{exec_date.replace('-', '')}"
        handler.save_json(dados_caixa, source_name, f"{filename_base}_completo")
        handler.save_csv(dados_caixa.get("entradas", []), source_name, f"{filename_base}_entradas")

        print("--- Busca do Demonstrativo de Caixa concluída ---")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    execution_date = sys.argv[1] if len(sys.argv) > 1 else "2025-02-26" # data de exemplo para teste
    main(execution_date)