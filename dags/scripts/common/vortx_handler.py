# /home/felipe/portoauto/dags/scripts/common/vortx_handler.py
import os
import json
import csv
import re
from configparser import ConfigParser
from pathlib import Path
import requests
from typing import List, Dict
import time
import base64 # O Script 1 usa, vamos deixar aqui
from datetime import datetime

class VortxHandler:
    """
    Classe central para interagir com a API da Vórtx.
    Cuida da autenticação, chamadas e salvamento de arquivos.
    """
    def __init__(self, config_path: str = '/home/felipe/portoauto/config.cfg'):
        self.config = self._read_config(config_path)
        self.auth_url = self.config.get('VORTX_API', 'auth_url')
        self.cpf = self.config.get('VORTX_API', 'cpf')
        self.access_tokens = [token.strip() for token in self.config.get('VORTX_API', 'access_tokens').split(',')]
        self.base_raw_path = Path(self.config.get('DEFAULT', 'data_raw_path'))
        self.session_token = None
        # --- ADICIONE ESTE BLOCO ---
        # Carrega configs da nova API de Recebíveis
        try:
            self.recebiveis_url = self.config.get('VORTX_RECEBIVEIS', 'base_url')
            self.handshake_key = self.config.get('VORTX_RECEBIVEIS', 'handshake_key')
        except Exception:
            print("AVISO: Config [VORTX_RECEBIVEIS] não encontrada. Funções de recebíveis falharão.")
            self.recebiveis_url = ""
            self.handshake_key = ""
    
    
    def _get_recebiveis_headers(self) -> dict:
        """Retorna o header de autenticação para a API de Recebíveis."""
        if not self.handshake_key:
            raise Exception("Handshake Key de Recebíveis não configurada no config.cfg")
        return {"Authorization": self.handshake_key}

    def make_recebiveis_request(self, endpoint: str, params: dict) -> requests.Response:
        """
        Faz uma chamada GET para a API VX Recebíveis (com Handshake Key).
        """
        time.sleep(3) # Throttle
        headers = self._get_recebiveis_headers()
        url = f"{self.recebiveis_url}{endpoint}"
        
        print(f"Chamando API Recebíveis: {url} com params: {params}")
        
        # --- ALTERAÇÃO AQUI ---
        # Aumenta o timeout para 10 minutos (600s) para relatórios grandes
        response = requests.get(url, headers=headers, params=params, timeout=600)
        # --- FIM DA ALTERAÇÃO ---

        response.raise_for_status()
        return response

    def _read_config(self, config_path: str) -> ConfigParser:
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Arquivo de config não encontrado: {config_path}")
        parser = ConfigParser()
        parser.read(config_path)
        return parser

    def authenticate(self) -> bool:
        """Tenta gerar um token de sessão JWT, iterando sobre os access_tokens da config."""
        print("Iniciando processo de autenticação...")
        for token in self.access_tokens:
            print(f"Tentando autenticar com token final ...{token[-6:]}")
            payload = {"token": token, "login": self.cpf}
            try:
                response = requests.post(self.auth_url, json=payload, timeout=20)
                if response.status_code == 200 and response.json().get("token"):
                    self.session_token = response.json()["token"]
                    print(">>> Sucesso! Token de sessão gerado.")
                    return True
            except requests.RequestException as e:
                print(f"  -> Erro de conexão na autenticação: {e}")
                continue # tenta o proximo token
        
        print("FALHA: Nenhum token de acesso funcionou.")
        return False

    def _get_auth_headers(self) -> dict:
        if not self.session_token:
            raise Exception("Não autenticado. Chame o método .authenticate() primeiro.")
        return {"Authorization": f"Bearer {self.session_token}"}

    def make_rest_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Faz uma chamada para um endpoint REST da API."""
        time.sleep(1)
        headers = self._get_auth_headers()
        # permite que headers customizados sobrescrevam os de auth, se necessario
        headers.update(kwargs.pop('headers', {})) 
        
        url = f"{self.config.get('VORTX_API', 'base_url')}{endpoint}"
        response = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        response.raise_for_status() # levanta erro para status http != 2xx
        return response

    def make_graphql_request(self, query: str, variables: dict) -> dict:
        time.sleep(1)
        """Faz uma chamada para o endpoint GraphQL."""
        url = self.config.get('VORTX_API', 'graphql_url')
        headers = self._get_auth_headers()
        headers["Content-Type"] = "application/json"
        
        payload = {"query": query, "variables": variables}
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=90)
        response.raise_for_status()

        response_data = response.json()
        if "errors" in response_data:
            raise Exception(f"Erro na query GraphQL: {response_data['errors']}")
        return response_data.get("data", {})

    def _get_output_dir(self, source_name: str) -> Path:
        """Cria e retorna o diretório de saída para uma fonte de dados."""
        output_dir = self.base_raw_path / source_name
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def _generate_filename(self, tipo: str, data_ref: str, data_ini: str = None, data_fim: str = None) -> (str, str):
        """
        Gera o nome do arquivo e a extensão com base nas novas regras.
        Formato: tipo_cnpj_YYYY-MM-DD-HH-MM-SS_YYYY-MM-DD[_YYYY-MM-DD].ext
        Retorna (nome_base_arquivo, extensao)
        """
        now_ts = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        
        # Define as datas inicial e final para o nome do arquivo
        d_ini = data_ini if data_ini else data_ref
        d_fim = data_fim if data_fim else data_ref
        
        # Define a extensão
        if tipo == "estoque": # Mantém estoque como zip
            ext = "zip"
        # Ajuste aqui se outros tipos tiverem extensões específicas
        elif tipo.startswith("docs_"): # Para os docs, manter PDF
            ext = "pdf"
        elif tipo in ["carteira", "caixa", "extrato", "dashboard_recebiveis"]:
             ext = "json" 
        else: # Para os relatórios de download (aquisicao, liquidacao)
             ext = self.config.get('VORTX_RECEBIVEIS', 'extensao_relatorio', fallback='csv')

        # Monta o nome base
        cnpj = self.config.get('VORTX_RECEBIVEIS', 'cnpj_sem_mascara', fallback='CNPJ_NA')
        
        # Lógica para data final opcional no nome
        if d_ini == d_fim:
            # Se data inicial e final são iguais (ou só data_ref foi passada), usa só uma data
            filename_base = f"{tipo}_{cnpj}_{now_ts}_{d_ini}"
        else:
            # Se são diferentes (relatórios mensais), usa ambas
            filename_base = f"{tipo}_{cnpj}_{now_ts}_{d_ini}_{d_fim}"
            
        return filename_base, ext

    # --- ATENÇÃO: Ajuste a chamada em save_binary_file ---
    # O método save_binary_file precisa ser ligeiramente ajustado para passar
    # o 'tipo' correto para _generate_filename e usar a extensão retornada.

    def save_binary_file(self, content: bytes, source_name: str, tipo_documento: str, data_ref: str, filename_for_log_only: str, data_ini: str = None, data_fim: str = None):
         """Salva conteúdo binário (ex: PDF) com o novo nome."""
         # Usa um 'tipo' genérico para o nome base, mas inclui o tipo_documento real
         # O tipo_documento aqui é 'Assembleia', 'Regulamento', etc.
         tipo_base_nome = f"docs_fundo_{tipo_documento.lower().replace(' ','_')}" # Ex: docs_fundo_assembleia
         
         filename_base, ext = self._generate_filename(tipo_base_nome, data_ref, data_ini, data_fim)
         # A extensão retornada por _generate_filename será 'pdf' se tipo começar com 'docs_'
         
         # Cria subpasta baseada no tipo real do documento
         safe_subfolder = re.sub(r'[\\/*?:"<>|]', "", tipo_documento)
         output_dir = self._get_output_dir(source_name) / safe_subfolder
         output_dir.mkdir(parents=True, exist_ok=True)
         
         # Usa o nome original seguro + nome base para evitar colisões
         safe_original_name = re.sub(r'[\\/*?:"<>|]', "", filename_for_log_only.replace(f'.{ext}',''))
         final_filename = f"{safe_original_name}_{filename_base}.{ext}" # Usa a extensão correta
         
         filepath = output_dir / final_filename
         try:
              with open(filepath, 'wb') as f:
                   f.write(content)
              print(f"Salvo Binário: {filepath}")
         except Exception as e:
              print(f"ERRO ao salvar arquivo binário {final_filename}. {e}")
              raise

    # ... (Resto da classe: save_json, save_csv, save_base64_file usam _generate_filename automaticamente)
    def _generate_filename(self, tipo: str, data_ref: str, data_ini: str = None, data_fim: str = None) -> (str, str):
        """
        Gera o nome do arquivo e a extensão com base nas novas regras.
        Retorna (nome_base_arquivo, extensao)
        """
        now_ts = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        
        # Define as datas inicial e final para o nome do arquivo
        d_ini = data_ini if data_ini else data_ref
        d_fim = data_fim if data_fim else data_ref
        
        # Define a extensão
        if tipo == "estoque":
            ext = "zip" # (REQ 6) Estoque agora é .zip
        elif tipo in ["carteira", "caixa", "extrato", "dashboard_recebiveis", "estoque_recebiveis"]: # estoque_recebiveis era JSON paginado
             ext = "json" # Assumindo que os dados originais são JSON
        else:
             ext = self.config.get('VORTX_RECEBIVEIS', 'extensao_relatorio', fallback='csv') # Pega do config ou usa 'csv'

        # Monta o nome base
        cnpj = self.config.get('VORTX_RECEBIVEIS', 'cnpj_sem_mascara', fallback='CNPJ_NA')
        filename_base = f"{tipo}_{cnpj}_{now_ts}_{d_ini}_{d_fim}"
        
        return filename_base, ext

    def save_json(self, data: dict, source_name: str, tipo: str, data_ref: str, data_ini: str = None, data_fim: str = None):
        """Salva um dicionário como arquivo JSON com o novo nome."""
        filename_base, ext = self._generate_filename(tipo, data_ref, data_ini, data_fim)
        filepath = self._get_output_dir(source_name) / f"{filename_base}.json" # Sempre salva o JSON original
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print(f"Salvo JSON: {filepath}")
            
            # Tenta salvar CSV também se for uma lista (útil para Estoque paginado e Dashboard)
            if isinstance(data, list) and data:
                 try:
                      csv_path = self._get_output_dir(source_name) / f"{filename_base}.csv"
                      fieldnames = data[0].keys()
                      with open(csv_path, 'w', newline='', encoding='utf-8') as f_csv:
                           writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
                           writer.writeheader()
                           writer.writerows(data)
                      print(f"Salvo CSV (derivado): {csv_path}")
                 except Exception as e_csv:
                      print(f"AVISO: Falha ao tentar salvar CSV derivado para {tipo}. Erro: {e_csv}")

        except Exception as e:
            print(f"ERRO ao salvar JSON para {tipo}. {e}")
            raise # Propaga o erro

    def save_csv(self, data: List[Dict], source_name: str, tipo: str, data_ref: str, data_ini: str = None, data_fim: str = None):
        """Salva uma lista de dicionários como arquivo CSV com o novo nome."""
        if not data:
            print(f"Aviso: Não há dados CSV para salvar para {tipo} em {data_ref}")
            return
            
        filename_base, ext = self._generate_filename(tipo, data_ref, data_ini, data_fim)
        # Usa a extensão calculada (.csv ou .zip)
        filepath = self._get_output_dir(source_name) / f"{filename_base}.{ext}"
        
        try:
            fieldnames = data[0].keys()
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            print(f"Salvo CSV: {filepath}")
        except Exception as e:
            print(f"ERRO ao salvar CSV para {tipo}. {e}")
            raise

    def save_base64_file(self, base64_str: str, source_name: str, tipo: str, data_ref: str, data_ini: str = None, data_fim: str = None):
        """Decodifica Base64 e salva com o novo nome e extensão correta."""
        filename_base, ext = self._generate_filename(tipo, data_ref, data_ini, data_fim)
        filepath = self._get_output_dir(source_name) / f"{filename_base}.{ext}" # Usa .zip para estoque, .csv para outros
        
        try:
            content = base64.b64decode(base64_str)
            with open(filepath, 'wb') as f:
                f.write(content)
            print(f"Salvo (Base64): {filepath}")
        except Exception as e:
            print(f"ERRO: Falha ao decodificar/salvar Base64 para {tipo}. {e}")
            raise Exception(f"Falha ao decodificar Base64 para {tipo}")