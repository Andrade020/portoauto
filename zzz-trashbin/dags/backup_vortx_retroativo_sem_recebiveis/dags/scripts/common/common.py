
def get_common_path():
    """
    Esta função cria um artifício para retornar o diretório do presente arquivo.
    """
    
    # Importar biblioteca
    import os

    # Full path do arquivo atual
    abspath = os.path.abspath(__file__)
    
    # Extrair o diretório
    common_path = '/'.join(abspath.split('/')[:-1])

    return common_path


def read_config(path="", verbose=False):
    """
    Lê o arquivo config dentro da pasta common do diretório do Airflow.
    Linhas iniciadas por # são ignoradas.
    """
    # Importar as bibliotecas necessárias
    import os

    # Se o path não for fornecido, lê o config base dentro da pasta common das dags
    if not path:
        # Lê o diretório atual
        current_directory = get_common_path()
    
        # Definir o path do arquivo para leitura
        config_path = os.path.join(current_directory, "common.cfg")

    # Se for fornecido, usar o fornecido
    else:
        config_path = path

    # Imprimir o path se verbose == True
    if verbose:
        print(f"Path do arquivo config base: {config_path}")
    
    # Abre o arquivo texto para leitura
    with open(config_path, 'r') as file:
        # Lê todas as linhas do arquivo
        lines = file.readlines()
    
    # Separar chave e valor pelo sinal de "="
    items = [line.split("=") for line in lines if "=" in line and not line.strip().startswith('#')]

    # Remover espaços em branco das chaves e valores
    keys = [item[0].strip() for item in items]
    vals = [item[1].strip() for item in items]

    # Criar dicionário com os itens
    config_dict = dict(zip(keys,vals))

    # Retornar o resultado
    return config_dict

    
def get_feriados_nacionais(config, return_type='list'):

    """
    Lê diretamente o arquivo xlx na pasta Raw.

    Return type deve ser um dentre:
    "list": retorna uma lista python com a série de datas
    "series": retorna uma série do Pandas com as datas
    "dataframe": retorna todo o DataFrame
    

    [Melhoria futura]
    Podemos fazer uma DAG para converter periodicamente (ex:  1 vez por mês) o excel em arquivo texto.
    A leitura direta do arquivo texto seria mais rápida (por exemplo, não demanda uma engine do excel).
    """

    # Importar as bibliotecas necessárias
    import pandas as pd
    from glob import glob
    import os

    # Converte o return_type para minúsiculas e verifica se o é válido
    return_type = return_type.lower()
    assert return_type in ['list', 'series', 'dataframe']

    # Identificar o path do arquivo de feriados
    # OBS: A ideia do glob é verificar se há arquivos com nome identificando a data do download
    #      E utilizar aquele com a maior data.
    #      Entretanto, esse recurso não foi programado ainda (só há um arquivo de feriados)
    base_path = config['PATH_FERIADOS']
    pattern = os.path.join(base_path, 'feriados*.xls')
    file_path = sorted(glob(pattern))[-1]

    # Ler o xls com os feriados
    df_feriados = pd.read_excel(file_path)

    # Converter a coluna de datas para datetime
    df_feriados['Data'] = pd.to_datetime(df_feriados['Data'], errors='coerce')

    # Verificar se a série vai pelo menos até o ano 2080
    # Aqui 2080 foi hard coded
    first_nan_pos = df_feriados['Data'].idxmax()
    assert df_feriados['Data'].iloc[first_nan_pos].year > 2080

    # Remover as linhas onde Data é nula
    # Isso é necessários devido às notas no final do DataFrame
    df_feriados = df_feriados.dropna(subset = ['Data'])

    # Retornar o resultado de acordo com o tipo solicitado
    # Retornar uma lista python
    if return_type == 'list':
        return df_feriados['Data'].tolist()
    # Retornar um pandas series
    if return_type == 'series':
        return df_feriados['Data']
    # Retornar todo o DataFrame
    else:
        return df_feriados



def get_write_log_path(config):
    """
    Função que retorna full path do arquivo de log para escrita.
    A função *NÃO* cria o arquivo caso ele não exista.
    """
    
    # Importar bibliotecas
    from datetime import datetime
    import os
    
    # Datetime agora
    now = datetime.now()
    
    # Datetime em string
    now_str = now.strftime("%Y-%m-%d")

    # Full path do arquivo de log
    fname = f"logs_{now_str}.csv"
    save_path = os.path.join(config['PATH_LOGS'], fname)

    # Retornar o path
    return save_path    