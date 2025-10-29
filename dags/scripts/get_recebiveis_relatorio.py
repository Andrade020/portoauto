# /home/felipe/portoauto/dags/scripts/get_recebiveis_relatorio.py
import pendulum
from common.vortx_handler import VortxHandler

def _formatar_cnpj_com_mascara(cnpj_sem_mascara: str) -> str:
    """Formata um CNPJ limpo (só números) para o formato XX.XXX.XXX/XXXX-XX."""
    return f"{cnpj_sem_mascara[:2]}.{cnpj_sem_mascara[2:5]}.{cnpj_sem_mascara[5:8]}/{cnpj_sem_mascara[8:12]}-{cnpj_sem_mascara[12:]}"

#                                                    👇
# <<< AQUI ESTÁ A CORREÇÃO: a função PRECISA aceitar 'tipo_relatorio'
#                                                    👇
def main_relatorio(data_str: str, tipo_relatorio: str):
    """
    Busca um relatório de download (Base64).
    - "estoque" usa data_str como dataInicial e dataFinal.
    - "aquisicao" e "liquidacao" usam data_str para calcular o *mês anterior*.
    """
    print(f"--- Iniciando busca de Relatório '{tipo_relatorio}' para {data_str} ---")
    handler = VortxHandler()
    
    # Pega as configs
    cnpj_sem_mascara = handler.config.get('VORTX_RECEBIVEIS', 'cnpj_sem_mascara')
    extensao = handler.config.get('VORTX_RECEBIVEIS', 'extensao_relatorio')
    cnpj_com_mascara = _formatar_cnpj_com_mascara(cnpj_sem_mascara)
    
    url_endpoint = "/relatorios/download"
    params = {
        "cnpjFundo": cnpj_com_mascara,
        "tipoRelatorio": tipo_relatorio,
        "extensao": extensao
    }

    # Lógica de data
    if tipo_relatorio == "estoque":
        # Estoque é diário. API considera 'dataInicial' (e 'dataFinal' deve ser >=)
        params["dataInicial"] = data_str
        params["dataFinal"] = data_str
    else:
        # Aquisicao/Liquidacao buscam o MÊS ANTERIOR.
        dia_da_execucao = pendulum.parse(data_str)
        data_fim_mes_anterior = dia_da_execucao.subtract(days=1).end_of_month()
        data_ini_mes_anterior = data_fim_mes_anterior.start_of_month()

        params["dataInicial"] = data_ini_mes_anterior.to_date_string()
        params["dataFinal"] = data_fim_mes_anterior.to_date_string()
        print(f"Relatório mensal. Buscando período: {params['dataInicial']} a {params['dataFinal']}")

    try:
        response = handler.make_recebiveis_request(url_endpoint, params)
        response_data = response.json()
        print(">>> Resposta recebida com sucesso. Decodificando...")

        base64_arquivo = response_data.get("arquivo")
        nome_arquivo_original = response_data.get("nome")
        
        if not base64_arquivo or not nome_arquivo_original:
            raise Exception(f"API não retornou 'arquivo' ou 'nome'. Resposta: {response_data}")

        nome_arquivo_final = f"{tipo_relatorio}_{cnpj_sem_mascara}_{params['dataInicial']}_a_{params['dataFinal']}.{extensao}"
        source_name = f"recebiveis_{tipo_relatorio}"

        handler.save_base64_file(
            base64_str=base64_arquivo,
            source_name=source_name,       # ex: 'recebiveis_estoque'
            tipo=tipo_relatorio,           # ex: 'estoque'
            data_ref=data_str,             # Data da task (ou 1º dia útil)
            data_ini=params["dataInicial"],# Data inicial do período do relatório
            data_fim=params["dataFinal"]   # Data final do período do relatório
        )


        print(f"--- Relatório '{tipo_relatorio}' concluído ---")

    except Exception as e:
        print(f"ERRO: Falha ao buscar o relatório '{tipo_relatorio}'. {e}")
        raise Exception(f"Falha ao buscar o relatório '{tipo_relatorio}': {e}")