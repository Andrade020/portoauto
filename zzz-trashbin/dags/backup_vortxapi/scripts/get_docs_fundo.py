import sys
from common.vortx_handler import VortxHandler
import requests

def main(exec_date):
    print(f"--- Iniciando busca de Documentos do Fundo para {exec_date} ---")
    handler = VortxHandler()
    
    # pega configs da seção [DOCS]
    source_name = handler.config.get('DOCS', 'source_name')
    cnpj = handler.config.get('DOCS', 'cnpj')
    cnpj_formatado = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"

    if not handler.authenticate():
        sys.exit(1) # falha o script para o Airflow saber

    try:
        # 1. Obter a lista de documentos
        print(f"Buscando lista de docs para o CNPJ: {cnpj_formatado}")
        response = handler.make_rest_request('GET', '/fundos/documentos', params={'cnpj': cnpj_formatado})
        lista_documentos = response.json()

        if not lista_documentos:
            print("Nenhum documento encontrado.")
            return

        # 2. Baixar cada documento
        print(f"Encontrados {len(lista_documentos)} documentos. Iniciando download...")
        for doc in lista_documentos:
            url = doc.get("url")
            nome = doc.get("nomeDocumento", f"doc_sem_nome_{doc.get('id')}")
            tipo = doc.get("tipoDocumento", "Outros")

            if not url: continue
            
            try:
                # baixa o conteudo binario
                file_response = requests.get(url, timeout=120)
                file_response.raise_for_status()
                # salva usando o handler
                handler.save_binary_file(file_response.content, source_name, tipo, f"{nome}.pdf")
            except requests.RequestException as e:
                print(f"  -> Falha ao baixar '{nome}': {e}")
        
        print("--- Busca de Documentos concluída ---")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # o Airflow vai passar a data como argumento. se rodar manualmente, usa a data de hoje.
    execution_date = sys.argv[1] if len(sys.argv) > 1 else "MANUAL_RUN"
    main(execution_date)