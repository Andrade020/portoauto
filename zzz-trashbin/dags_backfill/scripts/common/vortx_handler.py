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

    def save_base64_file(self, base64_str: str, source_name: str, filename: str):
        """Decodifica uma string Base64 e salva como arquivo binário."""
        try:
            content = base64.b64decode(base64_str)
            output_dir = self._get_output_dir(source_name)
            filepath = output_dir / filename
            with open(filepath, 'wb') as f:
                f.write(content)
            print(f"Salvo (Base64): {filepath}")
        except Exception as e:
            print(f"ERRO: Falha ao decodificar/salvar Base64. {e}")
            raise Exception(f"Falha ao decodificar Base64 para {filename}")


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

    def save_json(self, data: dict, source_name: str, filename: str):
        """Salva um dicionário como arquivo JSON."""
        output_dir = self._get_output_dir(source_name)
        filepath = output_dir / f"{filename}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"Salvo: {filepath}")

    def save_csv(self, data: List[Dict], source_name: str, filename: str, fieldnames: List[str] = None):
        """Salva uma lista de dicionários como arquivo CSV."""
        if not data:
            print(f"Aviso: Não há dados para salvar em {filename}.csv")
            return
        output_dir = self._get_output_dir(source_name)
        filepath = output_dir / f"{filename}.csv"
        
        # se os nomes dos campos nao forem passados, pega do primeiro item
        if not fieldnames:
            fieldnames = data[0].keys()

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"Salvo: {filepath}")

    def save_binary_file(self, content: bytes, source_name: str, subfolder: str, filename: str):
        """Salva conteúdo binário (ex: PDF) em uma subpasta."""
        # sanitiza nomes para evitar erros de path
        safe_subfolder = re.sub(r'[\\/*?:"<>|]', "", subfolder)
        safe_filename = re.sub(r'[\\/*?:"<>|]', "", filename)
        
        output_dir = self._get_output_dir(source_name) / safe_subfolder
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / safe_filename
        with open(filepath, 'wb') as f:
            f.write(content)
        print(f"Salvo: {filepath}")