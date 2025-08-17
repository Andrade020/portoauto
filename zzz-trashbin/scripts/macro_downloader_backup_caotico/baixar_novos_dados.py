# -*- coding: utf-8 -*-
"""
Rotina de download de dados adicionais (FIDC) — versão ajustada
----------------------------------------------------------------

Mudanças:
1) IBGE (PMC/PMS/PIM): passa a usar agregados *atuais* e descobre a variável por metadados (API v3, view=flat).
   - PMC: 8880 (base 2022) — ver "Replacement of tables..." do IBGE.
   - PMS: 5906 (base 2022).
   - PIM: 8160 (indicadores especiais).
2) IBGE (PIB Municípios): detecta dinamicamente o último ano em /Pib_Municipios/<ANO>/base/ e baixa o ZIP "base_de_dados_*_xlsx.zip".
   Se não existir, tenta /xlsx/tabelas_completas.xlsx (ou ODS).
3) INMET: tenta A904 (Sorriso/MT) e, se vier vazio, tenta A001 (Brasília) como fallback; janela ~90 dias (limite oficial).
4) CAGED (FTP) e TSE mantidos com pequenas robustezes.

Dependências: requests, pandas, py7zr, python-dateutil, beautifulsoup4
"""

import os
import re
import io
import zipfile
import urllib.parse
from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd
import py7zr

from ftplib import FTP
from bs4 import BeautifulSoup


# --- CONFIG ---
BASE_DIR = 'dados_adicionais_fidc'
print(f"Diretório base para salvar os dados: '{BASE_DIR}'")

def criar_pastas():
    os.makedirs(os.path.join(BASE_DIR, 'ibge', 'pesquisas_mensais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'ibge', 'contas_regionais'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'trabalho', 'caged'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'tse', 'eleitorado'), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, 'inmet', 'clima'), exist_ok=True)
    print("Estrutura de pastas verificada/criada.")


# ======================================================================
# 1) IBGE – API V3 (Agregados) — coleta automática de variável (view=flat)
# ======================================================================

IBGE_V3 = "https://servicodados.ibge.gov.br/api/v3/agregados"

def _str_norm(s):
    return re.sub(r'\s+', ' ', (s or '')).strip().lower()

def _match_all_terms(text, termos):
    t = _str_norm(text)
    return all(_str_norm(term) in t for term in termos)

def buscar_variavel_por_texto(agregado, termos_busca, nivel_local="N1"):
    """
    Busca uma variável do agregado cuja descrição contenha TODOS os termos indicados.
    Usa endpoint: /agregados/{agregado}/variaveis?localidades=N1[all]
    Retorna (var_id, var_desc) ou (None, None) se não encontrar.
    """
    url = f"{IBGE_V3}/{agregado}/variaveis?localidades={nivel_local}[all]"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    vars_json = r.json()
    # A resposta pode vir como lista de objetos de variável
    for v in vars_json:
        desc = v.get('nome') or v.get('variavel') or v.get('descricao') or ''
        if _match_all_terms(desc, termos_busca):
            return v.get('id'), desc
    # Se não bateu todos, tenta heurística: pegar a primeira variável com "índice"
    for v in vars_json:
        desc = v.get('nome') or v.get('variavel') or v.get('descricao') or ''
        if 'índice' in _str_norm(desc) or 'indice' in _str_norm(desc):
            return v.get('id'), desc
    return None, None

def baixar_dados_agregados_v3_auto(agregado, termos_busca, nome_arquivo, nivel_local="N1"):
    """
    1) Descobre variável pelo texto (ex.: ['índice','volume']) no agregado.
    2) Baixa /periodos/all/variaveis/{var}?localidades=N1[all]&view=flat
    3) Salva CSV.
    """
    print(f"  -> Verificando (v3): {nome_arquivo}")
    out_csv = os.path.join(BASE_DIR, 'ibge', 'pesquisas_mensais', f'{nome_arquivo}.csv')
    if os.path.exists(out_csv):
        print(f"     Arquivo '{nome_arquivo}.csv' já existe. Pulando.")
        return

    try:
        var_id, var_desc = buscar_variavel_por_texto(agregado, termos_busca, nivel_local=nivel_local)
        if not var_id:
            print(f"     Não localizei variável compatível em {agregado} para termos {termos_busca}.")
            return

        url = (f"{IBGE_V3}/{agregado}/periodos/all/variaveis/{var_id}"
               f"?localidades={nivel_local}[all]&view=flat")
        r = requests.get(url, timeout=90)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or len(data) == 0:
            print(f"     API v3 retornou vazio para '{nome_arquivo}'.")
            return

        # 'view=flat' normalmente traz colunas padronizadas (V, D1N, D2N, etc.)
        df = pd.DataFrame(data)
        # Renomeia algumas colunas comuns, se existirem:
        ren = {}
        if 'V' in df.columns: ren['V'] = 'valor'
        if 'D2N' in df.columns: ren['D2N'] = 'periodo'
        if 'D1N' in df.columns: ren['D1N'] = 'localidade'
        df = df.rename(columns=ren)
        # Mantém apenas algumas colunas úteis quando existirem
        cols_prior = [c for c in ['localidade','periodo','valor'] if c in df.columns]
        if cols_prior:
            df = df[cols_prior + [c for c in df.columns if c not in cols_prior]]

        df.to_csv(out_csv, index=False)
        print(f"     '{nome_arquivo}.csv' criado com {len(df)} linhas (agregado {agregado}, var {var_id}).")

    except Exception as e:
        print(f"     ERRO (v3) ao processar '{nome_arquivo}': {e}")

def chamar_downloads_ibge_mensal():
    print("\n[IBGE] Verificando Pesquisas Mensais (PMC, PMS, PIM)...")
    # Agregados atuais (base 2022 / pós-substituições oficiais do IBGE):
    # PMC (varejo): 8880 – procurar algo como "índice" + "volume"
    baixar_dados_agregados_v3_auto(agregado='8880',
                                   termos_busca=['índice','volume'],
                                   nome_arquivo='pmc_volume_vendas')

    # PMS (serviços): 5906 – procurar "índice" + "volume" + "serviços"
    baixar_dados_agregados_v3_auto(agregado='5906',
                                   termos_busca=['índice','volume','servi'],
                                   nome_arquivo='pms_receita_servicos')

    # PIM (produção industrial): 8160 – procurar "índice" + "produção"
    baixar_dados_agregados_v3_auto(agregado='8160',
                                   termos_busca=['índice','produ'],
                                   nome_arquivo='pim_producao_industrial')


# ======================================================================
# 2) IBGE – PIB dos Municípios (descoberta dinâmica em /Pib_Municipios/<ANO>/base/)
# ======================================================================


def _is_year_dir(name):
    # aceita "2021/" ou "2010_2021/"
    n = name.strip('/').lower()
    return bool(re.fullmatch(r'\d{4}(_\d{4})?', n))

def _dir_end_year(name):
    n = name.strip('/').lower()
    if '_' in n:
        a, b = n.split('_', 1)
        try:
            return int(b)
        except:
            return int(a)
    try:
        return int(n)
    except:
        return -1

def baixar_pib_municipios_ibge():
    print("\n[IBGE] Verificando PIB dos Municípios...")
    base = "https://ftp.ibge.gov.br/Pib_Municipios/"

    try:
        idx = requests.get(base, timeout=45)
        idx.raise_for_status()
        soup = BeautifulSoup(idx.text, "html.parser")

        # lista subpastas com anos
        subdirs = [a.get('href') for a in soup.find_all('a') if (a.get('href') or '').endswith('/')]
        anos = [d for d in subdirs if _is_year_dir(d)]
        if not anos:
            raise RuntimeError("Nenhuma pasta de ano encontrada em /Pib_Municipios/")

        # escolhe a pasta cujo 'ano final' seja o mais alto
        anos.sort(key=_dir_end_year, reverse=True)
        pasta_ano = anos[0]
        url_ano = urllib.parse.urljoin(base, pasta_ano)

        # prioriza /base/ com zip "base_de_dados_*_xlsx.zip"
        url_base = urllib.parse.urljoin(url_ano, "base/")
        try:
            idx_base = requests.get(url_base, timeout=45)
            idx_base.raise_for_status()
            soup_b = BeautifulSoup(idx_base.text, "html.parser")
            zips = [a.get('href') for a in soup_b.find_all('a') if (a.get('href') or '').lower().endswith('.zip')]
            # filtra por base_de_dados_*_xlsx.zip se existir
            zips_pref = [z for z in zips if 'base_de_dados' in z.lower() and 'xlsx' in z.lower()]
            target_zip = (sorted(zips_pref) or sorted(zips))[-1] if (zips_pref or zips) else None
            if target_zip:
                url_zip = urllib.parse.urljoin(url_base, target_zip)
                out_zip = os.path.join(BASE_DIR, 'ibge', 'contas_regionais', target_zip)
                if os.path.exists(out_zip):
                    print("     Base do PIB dos Municípios já foi baixada. Pulando.")
                    return
                print(f"     Baixando '{target_zip}' de {url_zip} ...")
                r = requests.get(url_zip, timeout=180)
                r.raise_for_status()
                with open(out_zip, 'wb') as f:
                    f.write(r.content)
                print("     Extraindo...")
                with zipfile.ZipFile(out_zip, 'r') as zf:
                    zf.extractall(os.path.join(BASE_DIR, 'ibge', 'contas_regionais'))
                print("     Extração concluída.")
                return
        except Exception:
            pass

        # fallback: tentar /xlsx/tabelas_completas.xlsx
        for sub in ['xlsx/', 'ods/']:
            url_alt = urllib.parse.urljoin(url_ano, sub)
            resp = requests.get(url_alt, timeout=45)
            if resp.ok:
                soup_alt = BeautifulSoup(resp.text, "html.parser")
                arquivos = [a.get('href') for a in soup_alt.find_all('a') if a.get('href')]
                # prioriza tabelas_completas
                candidatos = [x for x in arquivos if 'tabelas_completas' in x.lower()]
                if not candidatos:
                    # qualquer planilha como último recurso
                    candidatos = [x for x in arquivos if x.lower().endswith(('.xlsx','.ods','.zip'))]
                if candidatos:
                    nome = sorted(candidatos)[-1]
                    url_arq = urllib.parse.urljoin(url_alt, nome)
                    out_path = os.path.join(BASE_DIR, 'ibge', 'contas_regionais', nome)
                    if os.path.exists(out_path):
                        print("     Base do PIB dos Municípios já foi baixada (formato alternativo). Pulando.")
                        return
                    print(f"     Baixando '{nome}' de {url_arq} ...")
                    r = requests.get(url_arq, timeout=180)
                    r.raise_for_status()
                    with open(out_path, 'wb') as f:
                        f.write(r.content)
                    print("     Download concluído (formato alternativo).")
                    return

        raise RuntimeError("Não foi possível localizar ZIP/planilha de PIB em /<ANO>/{base|xlsx|ods}/")

    except Exception as e:
        print(f"     ERRO ao processar PIB dos Municípios: {e}")


# ======================================================================
# 3) Ministério do Trabalho – NOVO CAGED (FTP oficial + fallback HTTP)
# ======================================================================

def baixar_novo_caged():
    print("\n[Trabalho] Verificando dados do Novo CAGED...")
    data_ref = datetime.now() - relativedelta(months=2)
    ano = data_ref.strftime('%Y')
    ano_mes = data_ref.strftime('%Y%m')

    caminho_final_txt = os.path.join(BASE_DIR, 'trabalho', 'caged', f"CAGEDMOV{ano_mes}.txt")
    if os.path.exists(caminho_final_txt):
        print(f"     Dados do CAGED para {ano_mes} já existem. Pulando.")
        return

    ftp_host = "ftp.mtps.gov.br"
    ftp_dir = f"/pdet/microdados/NOVO CAGED/{ano}/{ano_mes}/"
    arq_7z = f"CAGEDMOV{ano_mes}.7z"
    local_7z_path = os.path.join(BASE_DIR, 'trabalho', 'caged', arq_7z)

    try:
        print(f"     Conectando ao FTP {ftp_host} ...")
        with FTP(ftp_host, timeout=120) as ftp:
            ftp.login()  # anônimo
            ftp.cwd(ftp_dir)
            with open(local_7z_path, 'wb') as f:
                ftp.retrbinary(f"RETR {arq_7z}", f.write)

        print("     Download completo (FTP). Descompactando .7z...")
        with py7zr.SevenZipFile(local_7z_path, 'r') as z:
            z.extractall(path=os.path.join(BASE_DIR, 'trabalho', 'caged'))
        os.remove(local_7z_path)
        print("     Arquivo descompactado e .7z removido.")

    except Exception as e:
        print(f"     Erro no FTP ({e}). Tentando fallback HTTP...")
        url = f"https://pdet.mte.gov.br/microdados/NOVO%20CAGED/{ano}/{ano_mes}/{arq_7z}"
        try:
            r = requests.get(url, timeout=300)
            r.raise_for_status()
            with open(local_7z_path, 'wb') as f:
                f.write(r.content)
            print("     Download (HTTP) ok. Descompactando .7z...")
            with py7zr.SevenZipFile(local_7z_path, 'r') as z:
                z.extractall(path=os.path.join(BASE_DIR, 'trabalho', 'caged'))
            os.remove(local_7z_path)
            print("     Arquivo descompactado e .7z removido.")
        except Exception as e2:
            print(f"     ERRO ao processar CAGED (HTTP fallback): {e2}")


# ======================================================================
# 4) TSE – Perfil do Eleitorado (HEAD + diff de timestamp)
# ======================================================================

def baixar_perfil_eleitorado_tse():
    print("\n[TSE] Verificando Perfil do Eleitorado...")
    url = "https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_ATUAL.zip"
    arquivo_zip = os.path.join(BASE_DIR, 'tse', 'eleitorado', 'perfil_eleitorado.zip')
    try:
        response_head = requests.head(url, timeout=15)
        response_head.raise_for_status()
        remote_last_modified = response_head.headers.get('Last-Modified')

        if os.path.exists(arquivo_zip) and remote_last_modified:
            local_mtime = os.path.getmtime(arquivo_zip)
            remote_dt = datetime.strptime(remote_last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            if local_mtime >= remote_dt.timestamp():
                print("     Dados do TSE já estão atualizados. Pulando.")
                return

        print("     Nova versão dos dados do TSE encontrada. Baixando...")
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        with open(arquivo_zip, 'wb') as f:
            f.write(response.content)
        print("     Download completo. Extraindo...")
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(BASE_DIR, 'tse', 'eleitorado'))
        print("     Extração concluída.")
    except Exception as e:
        print(f"     ERRO ao baixar dados do TSE: {e}")


# ======================================================================
# INMET – via "Dados Históricos" (ZIP anual) -> filtra estações alvo
# ======================================================================

def _baixar_zip_inmet_ano(ano, pasta_dest):
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
    try:
        h = requests.head(url, timeout=30)
        if not h.ok:
            print(f"     {ano}.zip indisponível ({h.status_code}).")
            return None
        print(f"     Baixando {ano}.zip de {url} ...")
        r = requests.get(url, timeout=300)
        r.raise_for_status()
        zpath = os.path.join(pasta_dest, f"inmet_{ano}.zip")
        with open(zpath, "wb") as f:
            f.write(r.content)
        return zpath
    except Exception as e:
        print(f"     Falhou baixar {ano}.zip: {e}")
        return None

def _ler_csv_inmet(fileobj):
    """
    Lê CSV do INMET tentando primeiro pular 8 linhas de metadados (padrão frequente),
    depois sem skiprows. Retorna DataFrame (ou None).
    """
    for skip in (8, 0):
        try:
            df = pd.read_csv(fileobj, sep=';', encoding='latin-1',
                             on_bad_lines='skip', dtype='object', skiprows=skip)
            if df is not None and len(df.columns) >= 5:
                return df
        except Exception:
            fileobj.seek(0) if hasattr(fileobj, 'seek') else None
            continue
    return None

def _normalizar_colunas_inmet(df):
    # Mapeia variações comuns de nomes
    ren = {
        'Data': 'DATA (YYYY-MM-DD)',
        'Hora UTC': 'HORA (UTC)',
        'HORA (UTC)': 'HORA (UTC)'
    }
    df = df.rename(columns={c: ren.get(c, c) for c in df.columns})
    # Apenas ordena se as colunas padrão existirem
    base_cols = [c for c in ['ESTACAO', 'DATA (YYYY-MM-DD)', 'HORA (UTC)'] if c in df.columns]
    if base_cols:
        outras = [c for c in df.columns if c not in base_cols]
        df = df[base_cols + outras]
    return df

def baixar_dados_climaticos_inmet():
    print("\n[INMET] Verificando dados climáticos (Dados Históricos)...")
    pasta = os.path.join(BASE_DIR, 'inmet', 'clima')
    os.makedirs(pasta, exist_ok=True)

    # Ajuste aqui as estações prioritárias (A904 = Sorriso):
    estacoes_alvo = ['A904']  # pode incluir ['A904','A917','A901', ...]
    anos_tentar = [datetime.now().year, datetime.now().year - 1]

    urls_testadas = []
    total_registros = 0
    try:
        for ano in anos_tentar:
            zpath = _baixar_zip_inmet_ano(ano, pasta)
            if not zpath:
                urls_testadas.append(f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip")
                continue

            try:
                with zipfile.ZipFile(zpath, 'r') as zf:
                    nomes = zf.namelist()
                    # para cada estação, pegue os arquivos cujo nome contenha o código
                    for cod in estacoes_alvo:
                        alvos = [n for n in nomes if cod.upper() in os.path.basename(n).upper()
                                 and n.lower().endswith(('.csv', '.txt'))]
                        if not alvos:
                            continue

                        # lê cada arquivo da estação e acumula
                        dfs = []
                        for nome in sorted(alvos):
                            try:
                                with zf.open(nome) as fh:
                                    by = io.BytesIO(fh.read())
                                    df = _ler_csv_inmet(by)
                                    if df is None or df.empty:
                                        continue
                                    df['__ARQUIVO_ORIGEM__'] = os.path.basename(nome)
                                    dfs.append(_normalizar_colunas_inmet(df))
                            except Exception:
                                continue

                        if dfs:
                            df_all = pd.concat(dfs, ignore_index=True)
                            # Deduplicação simples por (Data, Hora) se existir:
                            keys = [k for k in ['DATA (YYYY-MM-DD)', 'HORA (UTC)'] if k in df_all.columns]
                            if keys:
                                df_all = df_all.drop_duplicates(subset=keys)

                            out_csv = os.path.join(pasta, f"estacao_{cod}.csv")
                            # append/merge se já existir
                            if os.path.exists(out_csv):
                                try:
                                    df_old = pd.read_csv(out_csv, sep=';', on_bad_lines='skip', dtype='object')
                                    cols_comuns = list(set(df_old.columns) & set(df_all.columns))
                                    if cols_comuns:
                                        df_merged = pd.concat([df_old[cols_comuns], df_all[cols_comuns]], ignore_index=True)
                                        if keys and all(k in df_merged.columns for k in keys):
                                            df_merged = df_merged.drop_duplicates(subset=keys)
                                        df_merged.to_csv(out_csv, index=False, sep=';')
                                    else:
                                        df_all.to_csv(out_csv, index=False, sep=';')
                                except Exception:
                                    df_all.to_csv(out_csv, index=False, sep=';')
                            else:
                                df_all.to_csv(out_csv, index=False, sep=';')

                            nlin = len(df_all)
                            total_registros += nlin
                            print(f"     OK: '{os.path.basename(out_csv)}' atualizado ({nlin} linhas do ano {ano}).")
            finally:
                # opcional: remover o zip para economizar espaço
                try:
                    os.remove(zpath)
                except Exception:
                    pass

        if total_registros == 0:
            print("     Não encontrei arquivos para as estações e anos testados.")
            readme = os.path.join(pasta, "README.txt")
            with open(readme, "w", encoding="utf-8") as f:
                f.write("INMET - Nenhum arquivo encontrado nos Dados Históricos para as estações/anos configurados.\n")
                f.write("Tente abrir manualmente (verifica se existe e baixa no navegador):\n")
                for ano in anos_tentar:
                    u = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
                    urls_testadas.append(u)
                    f.write(f" - {u}\n")
            print(f"     Anotei as URLs testadas em: {readme}")
        else:
            print(f"     Total de linhas salvas/atualizadas (todas as estações): {total_registros}")

    except Exception as e:
        print(f"     ERRO ao processar INMET (Dados Históricos): {e}")


# ======================================================================
# 6) Fontes para ação manual (mantido)
# ======================================================================

def exibir_notas_fontes_manuais():
    print("\n" + "=" * 50)
    print("FONTES ADICIONAIS RECOMENDADAS (AÇÃO MANUAL)")
    print("=" * 50)
    print("""
[FGV IBRE] Índices de Confiança:
  - Link: https://portal.fgv.br/ibre/estatisticas
[FENABRAVE] Emplacamentos de Veículos:
  - Link: https://www.fenabrave.org.br/
[ANEEL] Tarifas de Energia:
  - Link: https://www.gov.br/aneel/pt-br/dados/tarifas
[Consumidor.gov.br] Reclamações:
  - Link: https://www.consumidor.gov.br/pages/indicadores/
""")


# =========================
# ROTINA PRINCIPAL
# =========================
if __name__ == "__main__":
    criar_pastas()
    print()
    chamar_downloads_ibge_mensal()
    baixar_pib_municipios_ibge()
    baixar_novo_caged()
    baixar_perfil_eleitorado_tse()
    baixar_dados_climaticos_inmet()
    exibir_notas_fontes_manuais()
    print("\nRotina de download de dados adicionais concluída.")
