# macro\_downloader

Carregador **simples e de alto nível** para ler, padronizar e disponibilizar **dados macroeconômicos** diretamente em `pandas.DataFrame`, a partir dos arquivos baixados pelo `downloader.py`.

> **Objetivo:** permitir que qualquer script, notebook ou análise acesse os **dados mais recentes com uma única linha**, abstraindo onde estão os arquivos, como lê‑los e como padronizá‑los.

---

## ✨ Funcionalidades

* **API simples**: funções intuitivas como `get_caged()`, `get_bcb_selic()`, etc.
* **Carregamento inteligente**: identifica automaticamente os **arquivos do período mais recente** para fontes com múltiplas datas (ex.: CVM, CAGED).
* **Padronização básica**: converte colunas de **data** e **valores numéricos** para os tipos corretos, facilitando a análise imediata.
* **Reutilizável**: pode ser importado em qualquer script ou notebook do seu ambiente.

---

## 📋 Pré‑requisitos

1. **Baixar os dados brutos** ao menos **uma vez** com o `downloader.py`, para que o diretório `data_raw/` exista e esteja populado.

   ```bash
   # Na raiz do projeto
   python src/macro_downloader/downloader.py
   ```

2. Ter o ambiente Python configurado (virtualenv/conda recomendado).

---

## ⚙️ Instalação (modo editável)

Instale o pacote na **raiz do projeto** (onde está o `pyproject.toml`). O modo editável (`-e`) é recomendado para que futuras melhorias fiquem disponíveis imediatamente:

```bash
pip install -e .
```

---

## 🚀 Uso rápido

```python
from macro_downloader import get_ibge_pnad, get_tesouro_direto

# Carrega a série completa da PNAD Contínua (taxa de desocupação)
df_pnad = get_ibge_pnad()
print("--- Últimos dados da PNAD Contínua ---")
print(df_pnad.tail())

# Carrega o histórico do Tesouro Direto (preços e taxas)
df_tesouro = get_tesouro_direto()
print("\n--- Últimos dados do Tesouro Direto ---")
print(df_tesouro.tail())
```

---

## 📦 Carregar **todos** os dados de uma vez

O script abaixo importa todas as fontes disponíveis e armazena cada `DataFrame` em um dicionário `dados`.

```python
import pandas as pd
from macro_downloader import (
    get_caged, get_cvm, get_pib_municipios, get_tse, get_bcb_selic,
    get_bcb_cdi, get_bcb_ipca, get_bcb_inadimplencia_pj, get_ibge_pnad,
    get_tesouro_direto, get_ibge_pmc, get_ibge_pms, get_ibge_pim,
    get_inmet_sorriso
)

# Opções de exibição
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 120)

# Dicionário para armazenar todos os DataFrames
dados = {}

# Todas as funções de carregamento disponíveis
funcoes_de_carga = {
    "CAGED": get_caged,
    "CVM_FIDC": get_cvm,
    "PIB_Municipios_Amostra": get_pib_municipios,
    "TSE_Eleitorado_Amostra": get_tse,
    "BCB_SELIC": get_bcb_selic,
    "BCB_CDI": get_bcb_cdi,
    "BCB_IPCA": get_bcb_ipca,
    "BCB_Inadimplencia_PJ": get_bcb_inadimplencia_pj,
    "IBGE_PNAD": get_ibge_pnad,
    "Tesouro_Direto": get_tesouro_direto,
    "IBGE_PMC": get_ibge_pmc,
    "IBGE_PMS": get_ibge_pms,
    "IBGE_PIM": get_ibge_pim,
    "INMET_Sorriso": get_inmet_sorriso,
}

print("======================================================")
print("==     Iniciando carregamento de todos os dados     ==")
print("======================================================")

for nome, funcao in funcoes_de_carga.items():
    print(f"\nCarregando: {nome}...")
    df = funcao()
    if df is not None and not df.empty:
        dados[nome] = df
        print(f"✅ Sucesso! {nome} carregado com {df.shape[0]} linhas e {df.shape[1]} colunas.")
        print("Amostra:")
        print(df.head(2))
    else:
        print(f"⚠️ Aviso: Nenhum dado encontrado ou erro ao carregar {nome}.")

print("\n\n======================================================")
print("==               CARGA FINALIZADA               ==")
print("======================================================")
print("Resumo dos DataFrames carregados no dicionário 'dados':")
for nome, df in dados.items():
    print(f"- {nome:<25} | Shape: {df.shape}")

# Agora você pode acessar cada DataFrame facilmente:
# print("\nExibindo os últimos 5 registros do Tesouro Direto:")
# print(dados["Tesouro_Direto"].tail())
```

---

## 📚 Referência da API

| Função                       | Fonte de Dados                      | Notas                                           |
| ---------------------------- | ----------------------------------- | ----------------------------------------------- |
| `get_caged()`                | Novo CAGED (Ministério do Trabalho) | Carrega o arquivo do **último mês** disponível. |
| `get_cvm()`                  | CVM – Informes Mensais FIDC         | Une os arquivos do **último mês** disponível.   |
| `get_pib_municipios()`       | IBGE – PIB dos Municípios           | Retorna **amostra** do arquivo do último ano.   |
| `get_tse()`                  | TSE – Perfil do Eleitorado          | Retorna **amostra** do maior CSV.               |
| `get_bcb_selic()`            | Banco Central (SGS)                 | Série histórica completa da **taxa SELIC**.     |
| `get_bcb_cdi()`              | Banco Central (SGS)                 | Série histórica completa da **taxa CDI**.       |
| `get_bcb_ipca()`             | Banco Central (SGS)                 | Série histórica completa do **IPCA**.           |
| `get_bcb_inadimplencia_pj()` | Banco Central (SGS)                 | Série histórica de **inadimplência PJ**.        |
| `get_ibge_pnad()`            | IBGE – PNAD Contínua                | Série histórica da **taxa de desocupação**.     |
| `get_tesouro_direto()`       | Tesouro Nacional                    | Histórico completo de **preços e taxas**.       |
| `get_ibge_pmc()`             | IBGE – Pesquisa Mensal de Comércio  | Série histórica completa da **PMC**.            |
| `get_ibge_pms()`             | IBGE – Pesquisa Mensal de Serviços  | Série histórica completa da **PMS**.            |
| `get_ibge_pim()`             | IBGE – Pesquisa Industrial Mensal   | Série histórica completa da **PIM**.            |
| `get_inmet_sorriso()`        | INMET – Estação A904 (Sorriso/MT)   | Dados climáticos **consolidados** da estação.   |

---

## 🗂️ Diretórios esperados

* `data_raw/`: onde o `downloader.py` salva os arquivos **brutos**.
* `src/macro_downloader/`: código‑fonte do pacote (módulos `get_*`).

> Dica: mantenha `data_raw/` fora do versionamento (adicione ao `.gitignore`).

---

## Observação

* **Os dados não aparecem:** confirme se você executou o `downloader.py` e se o diretório `data_raw/` contém arquivos para a fonte desejada.
* **Tipos de dados estranhos:** as funções já convertem datas e números mais comuns; se precisar de regras adicionais, aplique `.assign()`/`astype()` após o carregamento.

---



