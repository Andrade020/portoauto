

def authenticate_m1(url, credentials, timeout=15):
    """
    Faz a autenticação na API e retorna o Bearer Token.
    """
    
    # Importar as bibliotecas necessŕias
    import requests

    # Dados para autenticação
    payload = {"token": credentials["TOKEN"], "login": credentials["CPF"]}

    # Fazer o post e obter a resposta do request de autenticação
    response = requests.post(url, json=payload, timeout=timeout)

    # Extrair o status code
    code = response.status_code

    # Retornar None caso a status code seja diferente de 200
    if code != 200:
        # Mensagem apresentando o código de retorno
        print(f"A resposta retornou código {code}!")
        print("Não é possível continuar.")
        
        # Retornar None
        return None

    # Extrair o bearer token
    bearer = response.json().get("token")

    # Retornar o resultado
    return bearer
    

def create_header(bearer):
    """
    Cria o dicionário de cabeçalho a ser usado nas requisições.
    """
    # Criar o dicionário
    headers = {"Authorization": f"Bearer {bearer}"}
    # Retornar o resultado
    return headers
    

def get_url(base_url, endpoint):
    """
    Função para retornar a URL para requisição. 
    Concatena a URL base com o endpoint por um método "seguro".
    Os dois parâmetros são as strings a serem combinadas, na ordem.
    """
    from urllib.parse import urljoin
    return urljoin(base_url, endpoint)


def get_url2(config_dict, key):
    """
    Função para retornar a URL para requisição. 
    Concatena a URL base com o endpoint a partir das chaves fornecidas.
    O primeiro parâmetro é um dicionário e o segundo é a string com o nome da chave.
    """
    from urllib.parse import urljoin
    return urljoin(config_dict['BASE_URL'], config_dict[key])


def create_params(ep, dict_data):
    """
    Cria o dicionário de parâmetros. Os parâmetros variam em função do endpoint.
    """
    # Importar bibliotecas necessárias
    from itertools import product

    # Converter o valor de DATAS para lista caso não seja
    if "DATAS" in dict_data:
        if not isinstance(dict_data["DATAS"], list):
            dict_data["DATAS"] = [dict_data["DATAS"]]
    
    # Verificar se o valor de ep (endpoint) é valido
    assert ep in ['CARTEIRA_JSON']

    # Criar uma lista para receber os dicionários de parâmetros
    all_params = []
    
    if ep == 'CARTEIRA_JSON':
        # Somente aceita uma data e não sei se funciona para mais de 1 fundo por vez
        # Neste caso, vamos fazer um dicionário de parâmetros para cada data
        for fundo, data in product(dict_data['LISTA_FUNDOS'], dict_data['DATAS']):
            params_aux = {
                'cnpjFundos[]': fundo,
                'dataCarteira': data
            }
            all_params.append(params_aux)

    else:
        # Retornar dicionário vazio caso não esteja nos casos definidos acima.
        all_params = []

    return all_params