
#%%
# =============================================================================
# Bloco 1: Importação de Bibliotecas e Configurações Iniciais
# =============================================================================
# Descrição: Importa as bibliotecas necessárias para a análise (pandas, glob, numpy, matplotlib)
# e define configurações de exibição para o pandas, garantindo que mais colunas e
# linhas sejam mostradas nas saídas.

import pandas as pd
from glob import glob
import numpy as np
import matplotlib.pyplot as plt

# Configurações de exibição do Pandas
pd.options.display.max_columns = 100
pd.options.display.max_rows = 200


#%%
# =============================================================================
# Bloco 2: Carregamento e Leitura dos Dados de Estoque
# =============================================================================
# Descrição: Localiza todos os arquivos CSV de estoque, define os tipos de dados
# para colunas específicas (data, texto, numérico) para otimizar a leitura e,
# em seguida, carrega e concatena todos os arquivos em um único DataFrame.

# Padrão para encontrar os arquivos de estoque
caminho_feriados = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\feriados_nacionais.xls'
patt = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\data_temp\fct_2025-07-14\*Estoque*.csv'  # Usando o caminho mais específico do primeiro script
list_files = glob(patt)
print("Arquivos encontrados:", list_files)

# Definição das colunas por tipo de dado
colunas_data = [
    'DataEmissao', 'DataAquisicao', 'DataVencimento', 'DataGeracao'
]

colunas_texto = [
    'Situacao', 'PES_TIPO_PESSOA', 'CedenteCnpjCpf', 'TIT_CEDENTE_ENT_CODIGO',
    'CedenteNome', 'Cnae', 'SecaoCNAEDescricao', 'NotaPdd', 'SAC_TIPO_PESSOA',
    'SacadoCnpjCpf', 'SacadoNome', 'IdTituloVortx', 'TipoAtivo', 'NumeroBoleto',
    'NumeroTitulo', 'CampoChave', 'PagamentoParcial', 'Coobricacao',
    'CampoAdicional1', 'CampoAdicional2', 'CampoAdicional3', 'CampoAdicional4',
    'CampoAdicional5', 'IdTituloVortxOriginador', 'Registradora',
    'IdContratoRegistradora', 'IdTituloRegistradora', 'CCB', 'Convênio'
]

# Dicionário de tipos para colunas de texto
dtype_texto = {col: str for col in colunas_texto}

# Leitura e concatenação dos arquivos CSV
dfs = []
for file in list_files:
    print(f"Lendo o arquivo: {file}")
    df_ = pd.read_csv(file, sep=';', encoding='latin1', dtype=dtype_texto,
                      decimal=',', parse_dates=colunas_data, dayfirst=True)
    dfs.append(df_)

df_final = pd.concat(dfs, ignore_index=True)
print("Leitura e concatenação concluídas.")


#%%
# =============================================================================
# Bloco 3: Engenharia de Atributos (Criação de Colunas Auxiliares)
# =============================================================================
# Descrição: Cria novas colunas no DataFrame para facilitar análises futuras.
# - _ValorLiquido: Valor Presente ajustado pelo PDD.
# - _ValorVencido: Identifica o valor de títulos já vencidos.
# - _MuitosContratos: Flag para sacados com um número elevado de contratos (CCB).
# - _MuitosEntes: Flag para sacados associados a múltiplos convênios.

# Criar coluna de Valor Líquido
df_final['_ValorLiquido'] = df_final['ValorPresente'] - df_final['PDDTotal']

# Criar coluna de Valor Vencido
df_final['_ValorVencido'] = (df_final['DataVencimento'] <= df_final['DataGeracao']).astype('int') * df_final['ValorPresente']

# Identificar sacados com muitos contratos (CCB)
sacado_contratos = df_final.groupby('SacadoNome')['CCB'].nunique()
k = 3
mask_contratos = sacado_contratos >= k
sacado_contratos_alto = sacado_contratos[mask_contratos].index
df_final['_MuitosContratos'] = df_final['SacadoNome'].isin(sacado_contratos_alto).astype(str)

# Identificar sacados com muitos entes (Convênios)
sacados_entes = df_final.groupby('SacadoCnpjCpf')['Convênio'].nunique()
k2 = 3
mask_entes = sacados_entes >= k2
sacados_entes_alto = sacados_entes[mask_entes].index
df_final['_MuitosEntes'] = df_final['SacadoCnpjCpf'].isin(sacados_entes_alto).astype(str)

print("Criação de colunas auxiliares concluída.")


#%%
# =============================================================================
# Bloco 4: Verificação Inicial do DataFrame
# =============================================================================
# Descrição: Realiza uma verificação rápida do DataFrame, mostrando o uso de
# memória e o valor total do estoque (Valor Presente).

memoria_mb = df_final.memory_usage(deep=True).sum() / 1024**2
print(f"Uso de memória do DataFrame: {memoria_mb:.2f} MB")

valor_total_estoque = df_final["ValorPresente"].sum()
print(f"Valor Presente Total do Estoque: R$ {valor_total_estoque:_.2f}".replace('.', ',').replace('_', '.'))


#%%
# =============================================================================
# Bloco 5: Análise Exploratória Geral com value_counts()
# =============================================================================
# Descrição: Itera sobre as colunas de texto para entender a distribuição de suas
# categorias. As perguntas no comentário guiam esta exploração.

# Comentários / dúvidas do segundo script:
# 1. [Situacao] - diferença entre sem cobrança e aditado
# 2. [SAC_TIPO_PESSOA] - tipo J = jurídico?? tem isso?
# 3. [SacadoCnpjCpf] - por que tem CNPJ?
# 4. [SacadoNome] 'BMP SOCIEDADE DE CREDITO DIRETO S.A'
# 5. [SacadoCnpjCpf'] - verificar consistência dos CPFs
# 6. [TipoAtivo] - CCB e Contrato. Por que tem contrato?
# 7. [DataGeracao] - data de referência ou data de processamento?
# 8. [SacadoCnpjCpf] - tem sacado com 1040 linhas (!)
# 9. [Convênio] - sacados com muitos convênios (3)

df_final2 = df_final[~df_final['Situacao'].isna()].copy()

print("Analisando a contagem de valores para colunas de texto (geral):")
for col in df_final2.select_dtypes(include=['object']).columns:
    print(f"--- Análise da coluna: {col} ---")
    print(df_final2[col].value_counts(dropna=False))
    print('*' * 80)


#%%
# =============================================================================
# Bloco 6: Verificação do Impacto de Linhas com 'Situacao' Nula
# =============================================================================
# Descrição: Analisa se as linhas com 'Situacao' nula têm impacto significativo
# no cálculo do Valor Presente total.

print("Verificando o impacto das linhas com 'Situacao' nula...")
v1_ = df_final['ValorPresente'].sum()
v2_ = df_final[~df_final['Situacao'].isna()]['ValorPresente'].sum()
print(f"Valores são próximos? {np.isclose(v1_, v2_)}")

# Libera memória
del df_final


#%%
# =============================================================================
# Bloco 7: Análise de Sacados com SAC_TIPO_PESSOA == 'J'
# =============================================================================
# Descrição: Isola e analisa registros onde o tipo de sacado é 'J'.

print("Analisando sacados com tipo 'J'...")
df_sacado_J = df_final2[df_final2['SAC_TIPO_PESSOA'] == 'J']
if not df_sacado_J.empty:
    print("Tamanhos de 'SacadoCnpjCpf' para tipo 'J':", df_sacado_J['SacadoCnpjCpf'].map(len).unique())
    display(df_sacado_J.sample(min(5, len(df_sacado_J))))
    #! DISPLAY, NO JUPYTER
else:
    print("Nenhum sacado do tipo 'J' encontrado.")


#%%
# =============================================================================
# Bloco 8: Análise Específica de Sacados com CNPJ
# =============================================================================
# Descrição: Filtra sacados com CNPJ e realiza uma análise detalhada sobre eles,
# incluindo uma nova verificação de value_counts() para todas as colunas
# categóricas dentro deste subconjunto.

print("Analisando sacados com CNPJ...")
df_final2_cnpj = df_final2[df_final2['SacadoCnpjCpf'].map(len) == 18].copy()

if not df_final2_cnpj.empty:
    print("\nNomes únicos de sacados com CNPJ:")
    print(df_final2_cnpj['SacadoNome'].unique())

    # >>> VERIFICAÇÃO EXTRA ADICIONADA AQUI <<<
    print("\n--- value_counts() para o subconjunto de Sacados com CNPJ ---")
    for col in df_final2_cnpj.select_dtypes(include=['object']).columns:
        print(f"--- Análise da coluna: {col} (Apenas CNPJ) ---")
        print(df_final2_cnpj[col].value_counts(dropna=False))
        print('*'*80)

    print("\nEstatísticas descritivas para dados numéricos de sacados com CNPJ:")
    display(df_final2_cnpj.describe(include=[np.number]))

    print("\nSoma dos valores numéricos:")
    display(df_final2_cnpj.select_dtypes(include=[np.number]).sum())

    # Exportar para Excel
    # df_final2_cnpj.to_excel('df_final2_cnpj.xlsx')
else:
    print("Nenhum sacado com formato de CNPJ (18 caracteres) encontrado.")


#%%
# =============================================================================
# Bloco 9: Validação de CPFs na Coluna 'SacadoCnpjCpf'
# =============================================================================
# Descrição: Aplica uma função para validar a estrutura matemática de CPFs.

def validar_cpf(cpf):
    """Função para validar um número de CPF."""
    cpf = ''.join(filter(str.isdigit, str(cpf)))
    if len(cpf) != 11 or cpf == cpf[0] * 11: return False
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    resto = (soma * 10) % 11
    if resto == 10: resto = 0
    if resto != int(cpf[9]): return False
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    resto = (soma * 10) % 11
    if resto == 10: resto = 0
    if resto != int(cpf[10]): return False
    return True

mask_cpf = df_final2['SacadoCnpjCpf'].map(len) == 14
df_final2['CPF_válido'] = False
df_final2.loc[mask_cpf, 'CPF_válido'] = df_final2.loc[mask_cpf, 'SacadoCnpjCpf'].apply(validar_cpf)
cpfs_invalidos = df_final2[(mask_cpf) & (~df_final2['CPF_válido'])]['SacadoCnpjCpf'].unique()
print(f"Encontrados {len(cpfs_invalidos)} CPFs com estrutura inválida.")


#%%
# =============================================================================
# Bloco 10: Análise do Percentual de PDD por Variável Categórica
# =============================================================================
# Descrição: Calcula o percentual de PDD agrupado por diversas variáveis
# categóricas para identificar categorias de maior risco.

cat_cols = [
    'Situacao', 'CedenteNome', 'SAC_TIPO_PESSOA', 'PagamentoParcial',
    'TipoAtivo', '_MuitosContratos', '_MuitosEntes', 'Convênio'
]

pdd_ref = (1 - df_final2['_ValorLiquido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"PDD de Referência (Total): {pdd_ref:.2f}%\n")

for col in cat_cols:
    print(f"--- Análise de PDD por '{col}' ---")
    aux_ = df_final2.groupby(col)[['_ValorLiquido', 'ValorPresente']].sum() / 1e6
    aux_['%PDD'] = (1 - aux_['_ValorLiquido'] / aux_['ValorPresente']) * 100
    if col == 'Convênio':
        aux_ = aux_.sort_values('ValorPresente', ascending=False)
    display(aux_.head(20))
    print("\n" + "="*80 + "\n")


#%%
# =============================================================================
# Bloco 11: Análise do Percentual de Títulos Vencidos por Variável Categórica
# =============================================================================
# Descrição: Calcula a proporção do valor vencido em relação ao Valor Presente
# para cada categoria, identificando os grupos com maior inadimplência.

venc_ref = (df_final2['_ValorVencido'].sum() / df_final2['ValorPresente'].sum()) * 100
print(f"Percentual de Vencidos de Referência (Total): {venc_ref:.2f}%\n")

for col in cat_cols:
    print(f"--- Análise de Vencidos por '{col}' ---")
    aux_ = df_final2.groupby(col)[['_ValorVencido', 'ValorPresente']].sum() / 1e6
    aux_['%Vencido'] = (aux_['_ValorVencido'] / aux_['ValorPresente']) * 100
    if col == 'Convênio':
        aux_ = aux_.sort_values('ValorPresente', ascending=False)
    else:
        aux_ = aux_.sort_values('%Vencido', ascending=False)
    display(aux_.head(20))
    print("\n" + "="*80 + "\n")


#%%
# =============================================================================
# Bloco 12: Verificação de Sacados Presentes em Múltiplos Convênios ('Entes')
# =============================================================================
# Descrição: Identifica sacados com pulverização entre diferentes parceiros.

print("Analisando sacados presentes em múltiplos convênios...")
sacados_multi_entes = df_final2.groupby('SacadoCnpjCpf')['Convênio'].agg(['nunique', pd.unique])
sacados_multi_entes = sacados_multi_entes.sort_values('nunique', ascending=False)
display(sacados_multi_entes.head(35))


#%%
# =============================================================================
# Bloco 13: Verificação de Consistência das Datas
# =============================================================================
# Descrição: Realiza verificações de sanidade nas colunas de data.

print("Verificando consistência das datas...")
check1 = (df_final2['DataEmissao'] > df_final2['DataAquisicao']).sum()
print(f"Registros com Data de Emissão > Data de Aquisição: {check1}")
check2 = (df_final2['DataAquisicao'] > df_final2['DataVencimento']).sum()
print(f"Registros com Data de Aquisição > Data de Vencimento: {check2}")


#%%
# =============================================================================
# Bloco 14: Verificação de Consistência dos Valores Monetários
# =============================================================================
# Descrição: Realiza verificações de sanidade nos valores financeiros.

print("\nVerificando consistência dos valores...")
check_v1 = (df_final2['ValorAquisicao'] > df_final2['ValorNominal']).sum()
print(f"Registros com Valor de Aquisição > Valor Nominal: {check_v1}")
check_v2 = (df_final2['ValorAquisicao'] > df_final2['ValorPresente']).sum()
print(f"Registros com Valor de Aquisição > Valor Presente: {check_v2}")
check_v3 = (df_final2['ValorPresente'] > df_final2['ValorNominal']).sum()
print(f"Registros com Valor Presente > Valor Nominal: {check_v3}")


#%%
#==================================================
# Bloco 15: Geração do relatório 
#==================================================
import pandas as pd
import numpy as np
import os
from scipy.optimize import brentq # Usaremos o solver robusto 'brentq'

# ATUALIZADO: Substituído '_PopulacaoFaixa' pelas duas novas colunas
cat_cols = [
    'Situacao', 'CedenteNome', 'SAC_TIPO_PESSOA', 'PagamentoParcial',
    'TipoAtivo', '_MuitosContratos', '_MuitosEntes', 'Convênio',
    '_SacadoBMP', '_NIVEL', '_PREV', '_GENERICO',
    '_CAPAG', '_FaixaPop_Mun', '_FaixaPop_Est', '_UF' # Novas dimensões separadas
]

dimensoes_analise = {
    'Cedentes': 'CedenteNome',
    'Tipo de Contrato': 'TipoAtivo',
    'Ente Consignado': 'Convênio',
    'Situação': 'Situacao',
    'Tipo de Pessoa Sacado':'SAC_TIPO_PESSOA',
    'Pagamento Parcial': 'PagamentoParcial',
    'Tem Muitos Contratos':'_MuitosContratos',
    'Tem Muitos Entes':'_MuitosEntes',
    'Sacado é BMP': '_SacadoBMP',
    'Nível do Ente': '_NIVEL',
    'Previdência': '_PREV',
    'Ente Genérico': '_GENERICO'
}

COST_DICT = {
    'ASSEMBLEIA. MATO GROSSO': [0.03, 2.14],
    'GOV. ALAGOAS': [0.035, 5.92],
}
DEFAULT_COST = COST_DICT.get('GOV. ALAGOAS', [0.035, 5.92])

output_path = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\metricas_tabelas'
output_filename = os.path.join(output_path, 'analise_metricas_consolidadas.xlsx')

os.makedirs(output_path, exist_ok=True)
print(f"Arquivos de saída serão salvos em: {output_filename}")


#***************************
#*********** DADOS
#****************************


print("\n" + "="*80)
print("INICIANDO PREPARAÇÃO E ENRIQUECIMENTO DOS DADOS")
print("="*80)

# Criar coluna p sacado BMP
try:
    mask_bmp = df_final2['SacadoCnpjCpf'] == '34.337.707/0001-00'
    df_final2['_SacadoBMP'] = mask_bmp
    print("Coluna '_SacadoBMP' criada com sucesso.")
except KeyError:
    print("[AVISO] Coluna 'SacadoCnpjCpf' não encontrada.")

# Mapear Entes
try:
    path_map_entes = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\MAP_ENTES.xlsx'
    df_map_entes = pd.read_excel(path_map_entes)
    map_nivel = dict(zip(df_map_entes['NOME'], df_map_entes['_NIVEL']))
    map_prev = dict(zip(df_map_entes['NOME'], df_map_entes['_PREV']))
    map_generic = dict(zip(df_map_entes['NOME'], df_map_entes['_GENERICO']))
    df_final2['_NIVEL'] = df_final2['Convênio'].map(map_nivel)
    df_final2['_PREV'] = df_final2['Convênio'].map(map_prev)
    df_final2['_GENERICO'] = df_final2['Convênio'].map(map_generic)
    print("Colunas de mapeamento de entes criadas com sucesso.")
except Exception as e:
    print(f"[AVISO] Falha ao processar o mapeamento de entes: {e}")
# ADICIONE ESTA LINHA AQUI:
print(f"[DIAGNÓSTICO] Valores únicos encontrados na coluna _NIVEL: {df_final2['_NIVEL'].unique()}")

#****************
#* funcao usada em montar quintis
#***************
def formatar_pop(n):
    """vou formatar para notacao de engenharia"""
    
    # ===== CORREÇÃO ADICIONADA AQUI =====
    # Se o valor de entrada for nulo ou NaN, retorna 'N/D' (Não Disponível)
    if pd.isna(n):
        return "N/D"
    # ===== FIM DA CORREÇÃO =====
    
    n = float(n)
    if n >= 1_000_000:
        return f'{n / 1_000_000:.1f}M'.replace('.0M', 'M')
    if n >= 1_000:
        return f'{n / 1_000:.0f}k'
    return str(int(n))

# ==============================================================================
# BLOCO DE ENRIQUECIMENTO UNIFICADO E CORRIGIDO
# ==============================================================================
print("\n" + "="*80)
print("INICIANDO ENRIQUECimento DE DADOS (UF, CAPAG, POPULAÇÃO E QUINTIS SEPARADOS)")
print("="*80)

try:
    # 1. LIMPEZA PARA RE-EXECUÇÃO (Idempotência)
    # ATUALIZADO: Inclui as novas colunas a serem criadas
    cols_a_criar = ['_UF', '_CAPAG', '_FaixaPop_Mun', '_FaixaPop_Est', 'Convênio_normalized', 'populacao']
    cols_para_remover = [col for col in cols_a_criar if col in df_final2.columns]
    if cols_para_remover:
        print(f"[OBS] Removendo colunas de uma execução anterior: {cols_para_remover}")
        df_final2 = df_final2.drop(columns=cols_para_remover)

    # 2. CARREGAR E PREPARAR DADOS EXTERNOS
    path_relacoes = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\src\fct_consig\relacoes.csv'
    df_relacoes = pd.read_csv(path_relacoes, sep=';')
    print(f"Arquivo de relações '{path_relacoes}' carregado com sucesso.")
    df_relacoes_unico = df_relacoes.sort_values('populacao', ascending=False).drop_duplicates(subset='Convênio', keep='first').copy()


    # 3. ENRIQUECER df_final2 com UF, CAPAG e População
    df_final2 = pd.merge(
        df_final2,
        df_relacoes_unico[['Convênio', 'UF', 'CAPAG', 'populacao']],
        on='Convênio',
        how='left'
    )
    df_final2['_UF'] = df_final2.pop('UF').fillna('Não Informado')
    df_final2['_CAPAG'] = df_final2.pop('CAPAG').fillna('Não Informado')
    print("Enriquecimento com UF, CAPAG e População concluído.")

    # 4. CRIAR FAIXAS SEPARADAS PARA MUNICÍPIOS E ESTADOS
    # Substitua a função inteira por esta versão corrigida:
    def calcular_faixas_para_nivel(df_nivel, df_rel):
        """
        Calcula os quintis de VP e os labels de faixa populacional para um subset do DataFrame.
        Recebe um DataFrame filtrado (só municípios ou só estados).
        Retorna um dicionário mapeando cada convênio ao seu respectivo label de faixa.
        """
        if df_nivel.empty:
            print(f"Nenhum dado encontrado para este nível. Pulando.")
            return {}

        vp_por_convenio = df_nivel.groupby('Convênio')['ValorPresente'].sum().sort_values()
        if vp_por_convenio.sum() == 0: return {}

        vp_cumulativo = vp_por_convenio.cumsum()
        vp_total = vp_por_convenio.sum()

        limites = [0] + [vp_total * q for q in [0.2, 0.4, 0.6, 0.8]] + [vp_total + 1]
        
        # ===== CORREÇÃO APLICADA AQUI =====
        # A lógica agora é len(limites) - 1 para gerar o número correto de labels (5).
        labels_base = [f'{chr(ord("A") + i)}' for i in range(len(limites) - 1)]
        # ===== FIM DA CORREÇÃO =====

        quintil_por_convenio = pd.cut(vp_cumulativo, bins=limites, labels=labels_base, include_lowest=True)

        df_quintil_temp = quintil_por_convenio.reset_index(name='QuintilBase')
        df_pop_e_quintil = pd.merge(df_quintil_temp, df_rel, on='Convênio', how='left').dropna(subset=['QuintilBase', 'populacao'])
        
        pop_ranges = df_pop_e_quintil.groupby('QuintilBase').agg(pop_min=('populacao', 'min'), pop_max=('populacao', 'max'))

        mapa_label_final = {}
        for quintil_base, row in pop_ranges.iterrows():
            min_fmt = formatar_pop(row['pop_min'])
            max_fmt = formatar_pop(row['pop_max'])
            label_final = f"{quintil_base}. Pop: {min_fmt}" if min_fmt == max_fmt else f"{quintil_base}. Pop: {min_fmt} a {max_fmt}"
            mapa_label_final[quintil_base] = label_final

        return quintil_por_convenio.map(mapa_label_final).to_dict()

    # Para estas, usando os nomes corretos que descobrimos:
    df_municipais = df_final2[df_final2['_NIVEL'] == 'MUNICIPIO']
    df_estaduais = df_final2[df_final2['_NIVEL'] == 'ESTADO']
    
    print("\n--- Processando Convênios Municipais ---")
    mapa_municipais = calcular_faixas_para_nivel(df_municipais, df_relacoes_unico)
    print("\n--- Processando Convênios Estaduais ---")
    mapa_estaduais = calcular_faixas_para_nivel(df_estaduais, df_relacoes_unico)

    df_final2['_FaixaPop_Mun'] = df_final2['Convênio'].map(mapa_municipais)
    df_final2['_FaixaPop_Est'] = df_final2['Convênio'].map(mapa_estaduais)

    print("\nCriação de faixas populacionais separadas concluída.")

except FileNotFoundError:
    print(f"[ERRO GRAVE] O arquivo de relações não foi encontrado em '{path_relacoes}'.")
except Exception as e:
    print(f"[ERRO] Falha inesperada durante o enriquecimento: {e}")

# 5. ATUALIZAR O DICIONÁRIO DE ANÁLISE
dimensoes_analise.update({
    'CAPAG': '_CAPAG',
    'Faixa Pop. Municipal': '_FaixaPop_Mun',
    'Faixa Pop. Estadual': '_FaixaPop_Est',
    'UF': '_UF'
})
print("\nDicionário 'dimensoes_analise' atualizado com as novas chaves separadas.")
print("="*80)


#******************
#* TIR com brentq
#*******************
def calculate_xirr(cash_flows, days):
    cash_flows = np.array(cash_flows)
    days = np.array(days)
    def npv(rate):
        if rate <= -1: return float('inf')
        with np.errstate(divide='ignore', over='ignore'):
            return np.sum(cash_flows / (1 + rate) ** (days / 21.0))
    try:
        return brentq(npv, 0, 1.0)
    except ValueError:
        try:
            return brentq(npv, -0.9999, 0)
        except (RuntimeError, ValueError):
            return np.nan

#***********************
#* CÁLCULO DAS MÉTRICAS
#***********************
print("\n" + "="*80)
print("INICIANDO CÁLCULO DAS MÉTRICAS DE RISCO E INADIMPLÊNCIA")
print("="*80)

tabelas_pdd = {}
tabelas_vencido = {}

# Risco: % PDD
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns: continue
    aux_pdd = df_final2.groupby(coluna, observed=False)[['_ValorLiquido', 'ValorPresente']].sum()
    aux_pdd['%PDD'] = (1 - aux_pdd['_ValorLiquido'] / aux_pdd['ValorPresente']) * 100
    aux_pdd = aux_pdd.rename(columns={'ValorPresente': 'ValorPresente (M)', '_ValorLiquido': 'ValorLiquido (M)'})
    aux_pdd[['ValorPresente (M)', 'ValorLiquido (M)']] /= 1e6
    tabelas_pdd[nome_analise] = aux_pdd

# Inadimplencia --- #* % Vencido
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns: continue
    aux_venc = df_final2.groupby(coluna, observed=False)[['_ValorVencido', 'ValorPresente']].sum()
    aux_venc['%Vencido'] = (aux_venc['_ValorVencido'] / aux_venc['ValorPresente']) * 100
    aux_venc = aux_venc.rename(columns={'ValorPresente': 'ValorPresente (M)', '_ValorVencido': 'ValorVencido (M)'})
    aux_venc[['ValorPresente (M)', 'ValorVencido (M)']] /= 1e6
    tabelas_vencido[nome_analise] = aux_venc

print("Métricas de PDD e Inadimplência calculadas.")

#***********************
#* TICKET MÉDIO PONDERADO
#***********************
print("\n" + "="*80)
print("INICIANDO CÁLCULO DO TICKET MÉDIO PONDERADO")
print("="*80)

tabelas_ticket = {}

for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns: continue
    df_temp = df_final2.dropna(subset=[coluna, 'ValorPresente', 'ValorNominal'])
    if df_temp.empty: continue
    grouped = df_temp.groupby(coluna, observed=False)
    numerador = grouped.apply(lambda g: (g['ValorNominal'] * g['ValorPresente']).sum(), include_groups=False)
    denominador = grouped['ValorPresente'].sum()
    ticket_ponderado = (numerador / denominador).replace([np.inf, -np.inf], 0)
    ticket_ponderado.name = "Ticket Ponderado (R$)"
    tabelas_ticket[nome_analise] = pd.DataFrame(ticket_ponderado)

print("Cálculo de Ticket Médio Ponderado concluído.")

#***********************
#* TIR
#***********************
print("\n" + "="*80)
print("INICIANDO CÁLCULO DA TAXA INTERNA DE RETORNO (TIR)")
print("="*80)

ref_date = df_final2['DataGeracao'].max()
print(f"Data de Referência para o cálculo da TIR: {ref_date.strftime('%d/%m/%Y')}")

try:
    df_feriados = pd.read_excel(caminho_feriados)
    holidays = pd.to_datetime(df_feriados['Data']).values.astype('datetime64[D]')
    print(f"Sucesso: {len(holidays)} feriados carregados.")
except Exception as e:
    print(f"[AVISO] Não foi possível carregar feriados: {e}")
    holidays = []

df_avencer = df_final2[df_final2['DataVencimento'] > ref_date].copy()
try:
    start_dates = np.datetime64(ref_date.date())
    end_dates = df_avencer['DataVencimento'].values.astype('datetime64[D]')
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.busday_count(start_dates, end_dates, holidays=holidays)
    df_avencer = df_avencer[df_avencer['_DIAS_UTEIS_'] > 0]
except Exception as e:
    print(f"[ERRO] Falha ao calcular dias úteis: {e}")
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.nan

df_avencer['CustoVariavel'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[0])
df_avencer['CustoFixo'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[1])
df_avencer['CustoTotal'] = df_avencer['CustoFixo'] + (df_avencer['CustoVariavel'] * df_avencer['ValorNominal'])
df_avencer['ReceitaLiquida'] = df_avencer['ValorNominal'] - df_avencer['CustoTotal']

all_tirs = []
segmentos_para_analise = [('Carteira Total', 'Todos')] + \
                         [(col, seg) for col in cat_cols if col in df_avencer.columns for seg in df_avencer[col].dropna().unique()]

for tipo_dimensao, segmento in segmentos_para_analise:
    df_segmento = df_avencer if tipo_dimensao == 'Carteira Total' else df_avencer[df_avencer[tipo_dimensao] == segmento]
    if df_segmento.empty or df_segmento['_DIAS_UTEIS_'].isnull().all(): continue

    vp_bruto = df_segmento['ValorPresente'].sum()
    tir_bruta, tir_pdd, tir_custos, tir_completa = np.nan, np.nan, np.nan, np.nan
    
    if vp_bruto > 0:
        pdd_rate = df_segmento['PDDTotal'].sum() / vp_bruto
        
        fluxos_brutos = df_segmento.groupby('_DIAS_UTEIS_', observed=False)['ValorNominal'].sum()
        tir_bruta = calculate_xirr([-vp_bruto] + fluxos_brutos.values.tolist(), [0] + fluxos_brutos.index.tolist())

        fluxos_pdd = (df_segmento['ValorNominal'] * (1 - pdd_rate)).groupby(df_segmento['_DIAS_UTEIS_']).sum()
        tir_pdd = calculate_xirr([-vp_bruto] + fluxos_pdd.values.tolist(), [0] + fluxos_pdd.index.tolist())

        fluxos_custos = df_segmento.groupby('_DIAS_UTEIS_', observed=False)['ReceitaLiquida'].sum()
        tir_custos = calculate_xirr([-vp_bruto] + fluxos_custos.values.tolist(), [0] + fluxos_custos.index.tolist())
        
        df_segmento_copy = df_segmento.copy()
        df_segmento_copy['FluxoCompleto'] = (df_segmento_copy['ValorNominal'] * (1 - df_segmento_copy['CustoVariavel'])) * (1 - pdd_rate) - df_segmento_copy['CustoFixo']
        fluxos_completos = df_segmento_copy.groupby('_DIAS_UTEIS_', observed=False)['FluxoCompleto'].sum()
        tir_completa = calculate_xirr([-vp_bruto] + fluxos_completos.values.tolist(), [0] + fluxos_completos.index.tolist())

    all_tirs.append({
        'DimensaoColuna': tipo_dimensao,
        'Segmento': segmento,
        'Valor Presente TIR (M)': vp_bruto / 1e6,
        'TIR Bruta a.m. (%)': tir_bruta * 100 if pd.notna(tir_bruta) else np.nan,
        'TIR Líquida (PDD) a.m. (%)': tir_pdd * 100 if pd.notna(tir_pdd) else np.nan,
        'TIR Líquida (Custos) a.m. (%)': tir_custos * 100 if pd.notna(tir_custos) else np.nan,
        'TIR Líquida (PDD & Custos) a.m. (%)': tir_completa * 100 if pd.notna(tir_completa) else np.nan,
    })

df_tir_summary = pd.DataFrame(all_tirs)
tir_cols_to_fill = [col for col in df_tir_summary.columns if 'TIR' in col]
df_tir_summary[tir_cols_to_fill] = df_tir_summary[tir_cols_to_fill].fillna(-100.0)
print("Cálculo de TIR concluído.")

#***********************
#* EXPORTAÇÃO PARA EXCEL
#***********************
print("\n" + "="*80)
print("UNIFICANDO MÉTRICAS E GERANDO ARQUIVO EXCEL")
print("="*80)

# ATUALIZADO: Incluídas as novas faixas para ordenação alfabética
dimensoes_ordem_alfabetica = ['Faixa Pop. Municipal', 'Faixa Pop. Estadual', 'CAPAG']

with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
    for nome_analise, coluna in dimensoes_analise.items():
        if coluna not in df_final2.columns or df_final2[coluna].isnull().all(): continue
        
        print(f"--> Processando e unificando dados para a categoria: '{nome_analise}'")
        
        df_pdd = tabelas_pdd.get(nome_analise)
        df_venc = tabelas_vencido.get(nome_analise)
        df_ticket = tabelas_ticket.get(nome_analise)
        df_tir = df_tir_summary[df_tir_summary['DimensaoColuna'] == coluna].set_index('Segmento')

        # Se a tabela de pdd (base) não foi gerada, pula para a próxima dimensão
        if df_pdd is None:
            print(f"    [AVISO] Sem dados para a dimensão '{nome_analise}'. Pulando.")
            continue

        df_final = df_pdd.join(df_venc.drop(columns=['ValorPresente (M)']), how='outer')
        
        if df_ticket is not None:
            df_final = df_final.join(df_ticket, how='outer')

        df_final = df_final.join(df_tir.drop(columns=['DimensaoColuna']), how='outer')
        df_final.index.name = nome_analise
        df_final.reset_index(inplace=True)
        
        df_final = df_final.drop(columns=['ValorVencido (M)', 'Valor Presente TIR (M)'], errors='ignore')

        # ===== INÍCIO DA CORREÇÃO =====
        # Cria a ordem das colunas de forma dinâmica, verificando se elas existem
        
        colunas_ordem = [nome_analise, 'ValorLiquido (M)', 'ValorPresente (M)']
        
        # Adiciona o Ticket Ponderado apenas SE ele foi calculado e existe em df_final
        if 'Ticket Ponderado (R$)' in df_final.columns:
            colunas_ordem.append('Ticket Ponderado (R$)')
        
        colunas_ordem.extend(['%PDD', '%Vencido'])
        # ===== FIM DA CORREÇÃO =====

        colunas_tir_existentes = [col for col in df_tir.columns if col in df_final.columns and 'TIR' in col]
        colunas_finais = colunas_ordem + sorted(colunas_tir_existentes)
        
        outras_colunas = [col for col in df_final.columns if col not in colunas_finais]
        
        df_final = df_final[colunas_finais + outras_colunas]
        
        if nome_analise in dimensoes_ordem_alfabetica:
            df_final = df_final.sort_values(nome_analise, ascending=True).reset_index(drop=True)
        else:
            df_final = df_final.sort_values('ValorPresente (M)', ascending=False).reset_index(drop=True)
        
        df_final.to_excel(writer, sheet_name=nome_analise, index=False)
        
print("\n" + "="*80)
print("ANÁLISE CONCLUÍDA COM SUCESSO!")
print(f"O arquivo consolidado foi salvo em: {output_filename}")
print("="*80)


#%% Exportar para HTML: 
# *****************************************************************************
# * BLOCO DE EXPORTAÇÃO (FINAL, COM AJUSTES FINAIS DE ESTILO)
# *****************************************************************************



#***********************
#* CÁLCULO DAS MÉTRICAS
#***********************
print("\n" + "="*80)
print("INICIANDO CÁLCULO DAS MÉTRICAS DE RISCO E INADIMPLÊNCIA")
print("="*80)

# --- NOVOS NOMES DAS COLUNAS ---
vp_col_name = 'Valor Presente \n(R$ MM)'
vl_col_name = 'Valor Líquido \n(R$ MM)'
# -----------------------------

tabelas_pdd = {}
tabelas_vencido = {}

# Risco: % PDD
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns: continue
    aux_pdd = df_final2.groupby(coluna, observed=False)[['_ValorLiquido', 'ValorPresente']].sum()
    aux_pdd['%PDD'] = (1 - aux_pdd['_ValorLiquido'] / aux_pdd['ValorPresente']) * 100
    # MODIFICADO AQUI: Renomeando para os novos nomes com quebra de linha
    aux_pdd = aux_pdd.rename(columns={'ValorPresente': vp_col_name, '_ValorLiquido': vl_col_name})
    aux_pdd[[vp_col_name, vl_col_name]] /= 1e6
    tabelas_pdd[nome_analise] = aux_pdd

# Inadimplencia --- #* % Vencido
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns: continue
    aux_venc = df_final2.groupby(coluna, observed=False)[['_ValorVencido', 'ValorPresente']].sum()
    aux_venc['%Vencido'] = (aux_venc['_ValorVencido'] / aux_venc['ValorPresente']) * 100
    # MODIFICADO AQUI: Renomeando para os novos nomes com quebra de linha
    aux_venc = aux_venc.rename(columns={'ValorPresente': vp_col_name, '_ValorVencido': 'ValorVencido (M)'})
    aux_venc[[vp_col_name, 'ValorVencido (M)']] /= 1e6
    tabelas_vencido[nome_analise] = aux_venc

print("Métricas de PDD e Inadimplência calculadas.")

#***********************
#* TIR
#***********************
print("\n" + "="*80)
print("INICIANDO CÁLCULO DA TAXA INTERNA DE RETORNO (TIR)")
print("="*80)

ref_date = df_final2['DataGeracao'].max()
print(f"Data de Referência para o cálculo da TIR: {ref_date.strftime('%d/%m/%Y')}")

try:
    df_feriados = pd.read_excel(caminho_feriados)
    holidays = pd.to_datetime(df_feriados['Data']).values.astype('datetime64[D]')
    print(f"Sucesso: {len(holidays)} feriados carregados.")
except Exception as e:
    print(f"[AVISO] Não foi possível carregar feriados: {e}")
    holidays = []

df_avencer = df_final2[df_final2['DataVencimento'] > ref_date].copy()
try:
    start_dates = np.datetime64(ref_date.date())
    end_dates = df_avencer['DataVencimento'].values.astype('datetime64[D]')
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.busday_count(start_dates, end_dates, holidays=holidays)
    df_avencer = df_avencer[df_avencer['_DIAS_UTEIS_'] > 0]
except Exception as e:
    print(f"[ERRO] Falha ao calcular dias úteis: {e}")
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.nan

df_avencer['CustoVariavel'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[0])
df_avencer['CustoFixo'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[1])
df_avencer['CustoTotal'] = df_avencer['CustoFixo'] + (df_avencer['CustoVariavel'] * df_avencer['ValorNominal'])
df_avencer['ReceitaLiquida'] = df_avencer['ValorNominal'] - df_avencer['CustoTotal']

all_tirs = []
segmentos_para_analise = [('Carteira Total', 'Todos')] + \
                         [(col, seg) for col in cat_cols if col in df_avencer.columns for seg in df_avencer[col].dropna().unique()]

for tipo_dimensao, segmento in segmentos_para_analise:
    df_segmento = df_avencer if tipo_dimensao == 'Carteira Total' else df_avencer[df_avencer[tipo_dimensao] == segmento]
    if df_segmento.empty or df_segmento['_DIAS_UTEIS_'].isnull().all(): continue

    vp_bruto = df_segmento['ValorPresente'].sum()
    tir_bruta, tir_pdd, tir_custos, tir_completa = np.nan, np.nan, np.nan, np.nan

    if vp_bruto > 0:
        pdd_rate = df_segmento['PDDTotal'].sum() / vp_bruto
        fluxos_brutos = df_segmento.groupby('_DIAS_UTEIS_', observed=False)['ValorNominal'].sum()
        tir_bruta = calculate_xirr([-vp_bruto] + fluxos_brutos.values.tolist(), [0] + fluxos_brutos.index.tolist())
        fluxos_pdd = (df_segmento['ValorNominal'] * (1 - pdd_rate)).groupby(df_segmento['_DIAS_UTEIS_']).sum()
        tir_pdd = calculate_xirr([-vp_bruto] + fluxos_pdd.values.tolist(), [0] + fluxos_pdd.index.tolist())
        fluxos_custos = df_segmento.groupby('_DIAS_UTEIS_', observed=False)['ReceitaLiquida'].sum()
        tir_custos = calculate_xirr([-vp_bruto] + fluxos_custos.values.tolist(), [0] + fluxos_custos.index.tolist())
        df_segmento_copy = df_segmento.copy()
        df_segmento_copy['FluxoCompleto'] = (df_segmento_copy['ValorNominal'] * (1 - df_segmento_copy['CustoVariavel'])) * (1 - pdd_rate) - df_segmento_copy['CustoFixo']
        fluxos_completos = df_segmento_copy.groupby('_DIAS_UTEIS_', observed=False)['FluxoCompleto'].sum()
        tir_completa = calculate_xirr([-vp_bruto] + fluxos_completos.values.tolist(), [0] + fluxos_completos.index.tolist())

    # MODIFICADO AQUI: Usando os novos nomes para as colunas de TIR
    all_tirs.append({
        'DimensaoColuna': tipo_dimensao,
        'Segmento': segmento,
        'Valor Presente TIR (M)': vp_bruto / 1e6,
        'TIR Bruta \n(% a.m. )': tir_bruta * 100 if pd.notna(tir_bruta) else np.nan,
        'TIR Líquida de PDD \n(% a.m. )': tir_pdd * 100 if pd.notna(tir_pdd) else np.nan,
        'TIR Líquida de custos \n(% a.m. )': tir_custos * 100 if pd.notna(tir_custos) else np.nan,
        'TIR Líquida Final \n(% a.m. )': tir_completa * 100 if pd.notna(tir_completa) else np.nan,
    })

df_tir_summary = pd.DataFrame(all_tirs)
tir_cols_to_fill = [col for col in df_tir_summary.columns if 'TIR' in col]
df_tir_summary[tir_cols_to_fill] = df_tir_summary[tir_cols_to_fill].fillna(-100.0)
print("Cálculo de TIR concluído.")





#####################################################################################3
import base64
import os

print("\n" + "="*80)
print("GERANDO RELATÓRIO HTML FINAL COM AJUSTES DE ESTILO")
print("="*80)

# --- 1. PREPARAÇÃO DOS ATIVOS (LOGO E DATA) ---
def encode_image_to_base64(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except FileNotFoundError:
        print(f"[AVISO] Arquivo de imagem não encontrado em: {image_path}. A logo não será exibida.")
        return None

logo_path = r'C:\Users\Leo\Desktop\Porto_Real\portoauto\images\logo_inv.png'
logo_base64 = encode_image_to_base64(logo_path)
report_date = ref_date.strftime('%d/%m/%Y')

# --- 2. DEFINIÇÃO DO CSS COMPLETO ---

# MODIFICADO AQUI: Ajustes finais no CSS do cabeçalho
html_css = """
<style>
    /* Configurações Gerais */
    body {
        font-family: "Gill Sans MT", Arial, sans-serif;
        background-color: #f9f9f9;
        color: #313131;
        margin: 0;
        padding: 0;
    }
    .main-content {
        padding: 25px;
    }

    /* --- CABEÇALHO --- */
    header {
        background-color: #163f3f;
        color: #FFFFFF;
        padding: 20px 40px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        border-bottom: 5px solid #76c6c5;
    }
    /* 3. LOGO MAIOR: Altura da logo aumentada novamente */
    header .logo img {
        height: 75px; /* Aumentado de 65px para 75px */
    }
    header .report-title {
        text-align: left;
        font-family: "Gill Sans MT", Arial, sans-serif;
    }
    header .report-title h1, header .report-title h2, header .report-title h3 {
        margin: 0;
        padding: 0;
        font-weight: normal;
    }
    /* 2. FONTE MENOR: Tamanho do título principal reduzido */
    header .report-title h1 { font-size: 1.6em; /* Reduzido de 1.8em para 1.6em */ }
    header .report-title h2 { font-size: 1.4em; color: #d0d0d0; }
    header .report-title h3 { font-size: 1.1em; color: #a0a0a0; }

    /* Estilos dos Botões e Tabelas (sem alterações) */
    .container-botoes { display: flex; flex-wrap: wrap; gap: 15px; margin-bottom: 25px; }
    .container-botoes > details { flex: 1 1 280px; border: 1px solid #76c6c5; border-radius: 8px; overflow: hidden; }
    .container-botoes > details[open] { flex-basis: 100%; }
    details summary { font-size: 1.1em; font-weight: bold; color: #FFFFFF; background-color: #163f3f; padding: 15px 20px; cursor: pointer; outline: none; list-style-type: none; }
    details summary:hover { background-color: #0e5d5f; }
    details[open] summary { background-color: #76c6c5; color: #313131; }
    details[open] summary:hover { filter: brightness(95%); }
    summary::-webkit-details-marker { display: none; }
    summary::before { content: '► '; margin-right: 8px; font-size: 0.8em;}
    details[open] summary::before { content: '▼ '; }
    details .content-wrapper { padding: 20px; background-color: #FFFFFF; }
    table.dataframe, th, td { border: 1px solid #bbbbbb; }
    table.dataframe { border-collapse: collapse; width: 100%; }
    th, td { text-align: left; padding: 10px; vertical-align: middle; }
    th { background-color: #163f3f; color: #FFFFFF; }
    tr:nth-child(even) { background-color: #eeeeee; }

    /* --- RODAPÉ --- */
    footer {
        background-color: #f0f0f0;
        color: #555555;
        font-size: 0.8em;
        line-height: 1.6;
        padding: 25px 40px;
        margin-top: 40px;
        border-top: 1px solid #dddddd;
    }
    footer .disclaimer {
        margin-top: 20px;
        font-style: italic;
        border-top: 1px solid #dddddd;
        padding-top: 20px;
    }
</style>
"""

# --- 3. CONSTRUÇÃO DO HTML COMPLETO ---

html_parts = []
html_parts.append("<!DOCTYPE html><html lang='pt-BR'><head>")
html_parts.append("<meta charset='UTF-8'><title>Análise de Desempenho - FCT Consignado II</title>")
html_parts.append(html_css)
html_parts.append("</head><body>")

# --- Adiciona o Cabeçalho ---
html_parts.append("<header>")
html_parts.append(f"""
<div class="report-title">
    <h1>Análise de desempenho</h1>
    <h2>FCT CONSIGNADO II</h2>
    <h3>{report_date}</h3>
</div>
""")
if logo_base64:
    html_parts.append(f'<div class="logo"><img src="data:image/png;base64,{logo_base64}" alt="Logo"></div>')
html_parts.append("</header>")

html_parts.append("<div class='main-content'>")
mapa_descricoes = {
    'Cedentes': 'Analisa as métricas de risco e retorno agrupadas por cada Cedente (originador) dos títulos.', 'Tipo de Contrato': 'Agrupa os dados por Tipo de Ativo (CCB, Contrato) para comparar o desempenho de cada um.', 'Ente Consignado': 'Métricas detalhadas por cada Convênio (ente público ou privado) onde a consignação é feita.', 'Situação': 'Compara o desempenho dos títulos com base na sua situação atual (ex: Aditado, Liquidado).', 'Tipo de Pessoa Sacado': 'Diferencia a análise entre sacados Pessoa Física (F) e Jurídica (J).', 'Pagamento Parcial': 'Verifica se há impacto nas métricas para títulos que aceitam pagamento parcial.', 'Tem Muitos Contratos': 'Compara sacados com um número baixo vs. alto de contratos (CCBs) ativos.', 'Tem Muitos Entes': 'Compara sacados que operam em poucos vs. múltiplos convênios (entes).', 'Sacado é BMP': 'Isola e analisa especificamente as operações com o sacado BMP S.A.', 'Nível do Ente': 'Agrupa os convênios por nível governamental (Municipal, Estadual, Federal).', 'Previdência': 'Identifica e analisa separadamente os convênios que são de regimes de previdência.', 'Ente Genérico': 'Agrupa convênios que não se encaixam em uma categoria específica.', 'CAPAG': 'Métricas baseadas na Capacidade de Pagamento (CAPAG) do município ou estado, uma nota do Tesouro Nacional.', 'Faixa Pop. Municipal': 'Agrupa os convênios municipais por faixas de população.', 'Faixa Pop. Estadual': 'Agrupa os convênios estaduais por faixas de população.', 'UF': 'Agrega todas as métricas por Unidade Federativa (Estado).'
}
html_parts.append("<div class='container-botoes'>")
dimensoes_ordem_alfabetica = ['Faixa Pop. Municipal', 'Faixa Pop. Estadual', 'CAPAG']
vp_col_name = 'Valor Presente \n(R$ MM)'
vl_col_name = 'Valor Líquido \n(R$ MM)'
for nome_analise, coluna in dimensoes_analise.items():
    if coluna not in df_final2.columns or df_final2[coluna].isnull().all(): continue
    print(f"--> Processando e gerando HTML para o botão: '{nome_analise}'")
    df_pdd = tabelas_pdd.get(nome_analise)
    df_venc = tabelas_vencido.get(nome_analise)
    df_ticket = tabelas_ticket.get(nome_analise)
    df_tir = df_tir_summary[df_tir_summary['DimensaoColuna'] == coluna].set_index('Segmento')
    if df_pdd is None: continue
    df_final = df_pdd.join(df_venc.drop(columns=[vp_col_name]), how='outer')
    if df_ticket is not None: df_final = df_final.join(df_ticket, how='outer')
    df_final = df_final.join(df_tir.drop(columns=['DimensaoColuna']), how='outer')
    df_final.index.name = nome_analise
    df_final.reset_index(inplace=True)
    df_final = df_final.drop(columns=['ValorVencido (M)', 'Valor Presente TIR (M)'], errors='ignore')
    colunas_ordem = [nome_analise, vl_col_name, vp_col_name]
    if 'Ticket Ponderado (R$)' in df_final.columns: colunas_ordem.append('Ticket Ponderado (R$)')
    colunas_ordem.extend(['%PDD', '%Vencido'])
    colunas_tir_existentes = sorted([col for col in df_tir.columns if col in df_final.columns and 'TIR' in col])
    colunas_finais = colunas_ordem + colunas_tir_existentes
    outras_colunas = [col for col in df_final.columns if col not in colunas_finais]
    df_final = df_final[colunas_finais + outras_colunas]
    if nome_analise in dimensoes_ordem_alfabetica:
        df_final = df_final.sort_values(nome_analise, ascending=True).reset_index(drop=True)
    else:
        df_final = df_final.sort_values(vp_col_name, ascending=False).reset_index(drop=True)
    formatters = { vl_col_name: lambda x: f'{x:,.2f}', vp_col_name: lambda x: f'{x:,.2f}', 'Ticket Ponderado (R$)': lambda x: f'R$ {x:,.2f}', '%PDD': lambda x: f'{x:,.2f}%', '%Vencido': lambda x: f'{x:,.2f}%', }
    for col in colunas_tir_existentes: formatters[col] = lambda x: f'{x:,.2f}%'
    df_final.columns = [col.replace('\n', '<br>') for col in df_final.columns]
    html_parts.append("<details>")
    descricao = mapa_descricoes.get(nome_analise, 'Descrição não disponível.')
    html_parts.append(f'<summary title="{descricao}">{nome_analise}</summary>')
    html_parts.append("<div class='content-wrapper'>")
    html_table = df_final.to_html(index=False, classes='dataframe', formatters=formatters, na_rep='-', escape=False)
    html_parts.append(html_table)
    html_parts.append("</div></details>")
html_parts.append("</div>")
html_parts.append("</div>")

# --- Adiciona o Rodapé ---
footer_main_text = """
Este documento tem como objetivo apresentar uma análise de desempenho do fundo FCT Consignado II (CNPJ 52.203.615/0001-19), realizada pelo Porto Real Investimentos na qualidade de cogestora. Os prestadores de serviço do fundo são: FICTOR ASSET (Gestor), Porto Real Investimentos (Cogestor), e VÓRTX DTVM (Administrador e Custodiante).
"""
footer_disclaimer = """
Disclaimer: Este relatório foi preparado pelo Porto Real Investimentos exclusivamente para fins informativos e não constitui uma oferta de venda, solicitação de compra ou recomendação para qualquer investimento. As informações aqui contidas são baseadas em fontes consideradas confiáveis na data de sua publicação, mas não há garantia de sua precisão ou completude. Rentabilidade passada não representa garantia de rentabilidade futura.
"""
html_parts.append("<footer>")
html_parts.append(f'<p>{footer_main_text.strip()}</p>')
html_parts.append(f'<div class="disclaimer">{footer_disclaimer.strip()}</div>')
html_parts.append("</footer>")

html_parts.append("</body></html>")

# --- 4. SALVA O ARQUIVO FINAL ---
final_html_content = "\n".join(html_parts)
html_output_filename = os.path.join(output_path, 'analise_metricas_consolidadas.html')
try:
    with open(html_output_filename, 'w', encoding='utf-8') as f:
        f.write(final_html_content)
    print("\n" + "="*80)
    print("ANÁLISE CONCLUÍDA COM SUCESSO!")
    print(f"O relatório HTML final foi salvo em: {html_output_filename}")
    print("="*80)
except Exception as e:
    print(f"\n[ERRO GRAVE] Não foi possível salvar o arquivo HTML: {e}")