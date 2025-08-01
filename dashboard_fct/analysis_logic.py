# analysis_logic.py

import pandas as pd
import numpy as np
from glob import glob
from scipy.optimize import brentq
import os

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

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

def calculate_xirr(cash_flows, days):
    """Calcula a TIR para uma série de fluxos de caixa em dias específicos."""
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

def formatar_pop(n):
    """Formata um número para notação de engenharia (k, M)."""
    if pd.isna(n):
        return "N/D"
    n = float(n)
    if n >= 1_000_000:
        return f'{n / 1_000_000:.1f}M'.replace('.0M', 'M')
    if n >= 1_000:
        return f'{n / 1_000:.0f}k'
    return str(int(n))

# =============================================================================
# FUNÇÃO PRINCIPAL DA ANÁLISE
# =============================================================================

def obter_dados_dashboard(patt, caminho_feriados, path_map_entes, path_relacoes):
    """
    Executa todo o pipeline de análise de dados e retorna os resultados prontos para o dashboard.
    """
    print("Iniciando a carga de dados...")
    # --- Bloco 2: Carregamento dos Dados ---
    list_files = glob(patt)
    colunas_data = ['DataEmissao', 'DataAquisicao', 'DataVencimento', 'DataGeracao']
    colunas_texto = ['Situacao', 'PES_TIPO_PESSOA', 'CedenteCnpjCpf', 'TIT_CEDENTE_ENT_CODIGO', 'CedenteNome', 'Cnae', 'SecaoCNAEDescricao', 'NotaPdd', 'SAC_TIPO_PESSOA', 'SacadoCnpjCpf', 'SacadoNome', 'IdTituloVortx', 'TipoAtivo', 'NumeroBoleto', 'NumeroTitulo', 'CampoChave', 'PagamentoParcial', 'Coobricacao', 'CampoAdicional1', 'CampoAdicional2', 'CampoAdicional3', 'CampoAdicional4', 'CampoAdicional5', 'IdTituloVortxOriginador', 'Registradora', 'IdContratoRegistradora', 'IdTituloRegistradora', 'CCB', 'Convênio']
    dtype_texto = {col: str for col in colunas_texto}
    dfs = [pd.read_csv(file, sep=';', encoding='latin1', dtype=dtype_texto, decimal=',', parse_dates=colunas_data, dayfirst=True) for file in list_files]
    df_raw = pd.concat(dfs, ignore_index=True)
    df_final2 = df_raw[~df_raw['Situacao'].isna()].copy()
    del df_raw # Libera memória

    print("Iniciando engenharia de atributos e enriquecimento...")
    # --- Bloco 3: Engenharia de Atributos ---
    df_final2['_ValorLiquido'] = df_final2['ValorPresente'] - df_final2['PDDTotal']
    df_final2['_ValorVencido'] = (df_final2['DataVencimento'] <= df_final2['DataGeracao']).astype('int') * df_final2['ValorPresente']
    sacado_contratos = df_final2.groupby('SacadoNome')['CCB'].nunique()
    df_final2['_MuitosContratos'] = df_final2['SacadoNome'].isin(sacado_contratos[sacado_contratos >= 3].index).astype(str)
    sacados_entes = df_final2.groupby('SacadoCnpjCpf')['Convênio'].nunique()
    df_final2['_MuitosEntes'] = df_final2['SacadoCnpjCpf'].isin(sacados_entes[sacados_entes >= 3].index).astype(str)

    # --- Bloco 9: Validação de CPFs ---
    mask_cpf = df_final2['SacadoCnpjCpf'].map(len) == 14
    df_final2.loc[mask_cpf, 'CPF_válido'] = df_final2.loc[mask_cpf, 'SacadoCnpjCpf'].apply(validar_cpf)

    # --- Enriquecimento de Dados ---
    # Coluna _SacadoBMP
    df_final2['_SacadoBMP'] = (df_final2['SacadoCnpjCpf'] == '34.337.707/0001-00')
    # Mapear Entes
    df_map_entes = pd.read_excel(path_map_entes)
    df_final2['_NIVEL'] = df_final2['Convênio'].map(dict(zip(df_map_entes['NOME'], df_map_entes['_NIVEL'])))
    df_final2['_PREV'] = df_final2['Convênio'].map(dict(zip(df_map_entes['NOME'], df_map_entes['_PREV'])))
    df_final2['_GENERICO'] = df_final2['Convênio'].map(dict(zip(df_map_entes['NOME'], df_map_entes['_GENERICO'])))
    
    # Enriquecer com UF, CAPAG, População
    df_relacoes = pd.read_csv(path_relacoes, sep=';')
    df_relacoes_unico = df_relacoes.sort_values('populacao', ascending=False).drop_duplicates(subset='Convênio', keep='first')
    df_final2 = pd.merge(df_final2, df_relacoes_unico[['Convênio', 'UF', 'CAPAG', 'populacao']], on='Convênio', how='left')
    df_final2['_UF'] = df_final2.pop('UF').fillna('Não Informado')
    df_final2['_CAPAG'] = df_final2.pop('CAPAG').fillna('Não Informado')

    # Criar Faixas Populacionais
    def calcular_faixas_para_nivel(df_nivel, df_rel):
        if df_nivel.empty: return {}
        vp_por_convenio = df_nivel.groupby('Convênio')['ValorPresente'].sum().sort_values()
        if vp_por_convenio.sum() == 0: return {}
        vp_cumulativo = vp_por_convenio.cumsum()
        vp_total = vp_por_convenio.sum()
        limites = [0] + [vp_total * q for q in [0.2, 0.4, 0.6, 0.8]] + [vp_total + 1]
        labels_base = [f'{chr(ord("A") + i)}' for i in range(len(limites) - 1)]
        quintil_por_convenio = pd.cut(vp_cumulativo, bins=limites, labels=labels_base, include_lowest=True)
        df_quintil_temp = quintil_por_convenio.reset_index(name='QuintilBase')
        df_pop_e_quintil = pd.merge(df_quintil_temp, df_rel, on='Convênio', how='left').dropna(subset=['QuintilBase', 'populacao'])
        pop_ranges = df_pop_e_quintil.groupby('QuintilBase').agg(pop_min=('populacao', 'min'), pop_max=('populacao', 'max'))
        mapa_label_final = {}
        for quintil_base, row in pop_ranges.iterrows():
            min_fmt, max_fmt = formatar_pop(row['pop_min']), formatar_pop(row['pop_max'])
            label_final = f"{quintil_base}. Pop: {min_fmt}" if min_fmt == max_fmt else f"{quintil_base}. Pop: {min_fmt} a {max_fmt}"
            mapa_label_final[quintil_base] = label_final
        return quintil_por_convenio.map(mapa_label_final).to_dict()

    mapa_municipais = calcular_faixas_para_nivel(df_final2[df_final2['_NIVEL'] == 'MUNICIPIO'], df_relacoes_unico)
    mapa_estaduais = calcular_faixas_para_nivel(df_final2[df_final2['_NIVEL'] == 'ESTADO'], df_relacoes_unico)
    df_final2['_FaixaPop_Mun'] = df_final2['Convênio'].map(mapa_municipais)
    df_final2['_FaixaPop_Est'] = df_final2['Convênio'].map(mapa_estaduais)

    print("Iniciando cálculo das métricas...")
    # --- Bloco 10 e 11: Cálculo de Métricas (PDD, Vencido, Ticket, TIR) ---
    dimensoes_analise = { 'Cedentes': 'CedenteNome', 'Tipo de Contrato': 'TipoAtivo', 'Ente Consignado': 'Convênio', 'Situação': 'Situacao', 'Tipo de Pessoa Sacado':'SAC_TIPO_PESSOA', 'Pagamento Parcial': 'PagamentoParcial', 'Tem Muitos Contratos':'_MuitosContratos', 'Tem Muitos Entes':'_MuitosEntes', 'Sacado é BMP': '_SacadoBMP', 'Nível do Ente': '_NIVEL', 'Previdência': '_PREV', 'Ente Genérico': '_GENERICO', 'CAPAG': '_CAPAG', 'Faixa Pop. Municipal': '_FaixaPop_Mun', 'Faixa Pop. Estadual': '_FaixaPop_Est', 'UF': '_UF'}
    vp_col_name = 'Valor Presente \n(R$ MM)'
    vl_col_name = 'Valor Líquido \n(R$ MM)'

    tabelas_pdd = {}
    for nome, col in dimensoes_analise.items():
        if col not in df_final2.columns: continue
        aux = df_final2.groupby(col, observed=True)[['_ValorLiquido', 'ValorPresente']].sum()
        aux['%PDD'] = (1 - aux['_ValorLiquido'] / aux['ValorPresente']) * 100
        aux = aux.rename(columns={'ValorPresente': vp_col_name, '_ValorLiquido': vl_col_name})
        aux[[vp_col_name, vl_col_name]] /= 1e6
        tabelas_pdd[nome] = aux

    tabelas_vencido = {}
    for nome, col in dimensoes_analise.items():
        if col not in df_final2.columns: continue
        aux = df_final2.groupby(col, observed=True)[['_ValorVencido', 'ValorPresente']].sum()
        aux['%Vencido'] = (aux['_ValorVencido'] / aux['ValorPresente']) * 100
        aux = aux.rename(columns={'ValorPresente': vp_col_name, '_ValorVencido': 'ValorVencido (M)'})
        aux[[vp_col_name, 'ValorVencido (M)']] /= 1e6
        tabelas_vencido[nome] = aux
        
    tabelas_ticket = {}
    for nome, col in dimensoes_analise.items():
        if col not in df_final2.columns: continue
        df_temp = df_final2.dropna(subset=[col, 'ValorPresente', 'ValorNominal'])
        if df_temp.empty: continue
        grouped = df_temp.groupby(col, observed=True)
        numerador = grouped.apply(lambda g: (g['ValorNominal'] * g['ValorPresente']).sum(), include_groups=False)
        denominador = grouped['ValorPresente'].sum()
        ticket_ponderado = (numerador / denominador).replace([np.inf, -np.inf], 0)
        ticket_ponderado.name = "Ticket Ponderado (R$)"
        tabelas_ticket[nome] = pd.DataFrame(ticket_ponderado)

    # Cálculo da TIR
    ref_date = df_final2['DataGeracao'].max()
    holidays = pd.to_datetime(pd.read_excel(caminho_feriados)['Data']).values.astype('datetime64[D]')
    df_avencer = df_final2[df_final2['DataVencimento'] > ref_date].copy()
    df_avencer.loc[:, '_DIAS_UTEIS_'] = np.busday_count(np.datetime64(ref_date.date()), df_avencer['DataVencimento'].values.astype('datetime64[D]'), holidays=holidays)
    df_avencer = df_avencer[df_avencer['_DIAS_UTEIS_'] > 0]
    
    COST_DICT = {'ASSEMBLEIA. MATO GROSSO': [0.03, 2.14], 'GOV. ALAGOAS': [0.035, 5.92]}
    DEFAULT_COST = COST_DICT.get('GOV. ALAGOAS')
    df_avencer['CustoVariavel'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[0])
    df_avencer['CustoFixo'] = df_avencer['Convênio'].map(lambda x: COST_DICT.get(x, DEFAULT_COST)[1])
    df_avencer['ReceitaLiquida'] = df_avencer['ValorNominal'] - (df_avencer['CustoFixo'] + (df_avencer['CustoVariavel'] * df_avencer['ValorNominal']))

    all_tirs = []
    cat_cols_tir = [col for col in dimensoes_analise.values() if col in df_avencer.columns]
    segmentos_para_analise = [('Carteira Total', 'Todos')] + [(col, seg) for col in cat_cols_tir for seg in df_avencer[col].dropna().unique()]
    
    for tipo_dimensao, segmento in segmentos_para_analise:
        df_segmento = df_avencer if tipo_dimensao == 'Carteira Total' else df_avencer[df_avencer[tipo_dimensao] == segmento]
        if df_segmento.empty or df_segmento['_DIAS_UTEIS_'].isnull().all(): continue
        vp_bruto = df_segmento['ValorPresente'].sum()
        if vp_bruto > 0:
            pdd_rate = df_segmento['PDDTotal'].sum() / vp_bruto
            tir_bruta = calculate_xirr([-vp_bruto] + df_segmento.groupby('_DIAS_UTEIS_')['ValorNominal'].sum().values.tolist(), [0] + df_segmento.groupby('_DIAS_UTEIS_')['ValorNominal'].sum().index.tolist())
            tir_pdd = calculate_xirr([-vp_bruto] + (df_segmento['ValorNominal'] * (1 - pdd_rate)).groupby(df_segmento['_DIAS_UTEIS_']).sum().values.tolist(), [0] + df_segmento.groupby('_DIAS_UTEIS_')['ValorNominal'].sum().index.tolist())
            tir_custos = calculate_xirr([-vp_bruto] + df_segmento.groupby('_DIAS_UTEIS_')['ReceitaLiquida'].sum().values.tolist(), [0] + df_segmento.groupby('_DIAS_UTEIS_')['ReceitaLiquida'].sum().index.tolist())
            df_segmento_copy = df_segmento.copy()
            df_segmento_copy['FluxoCompleto'] = (df_segmento_copy['ValorNominal'] * (1 - df_segmento_copy['CustoVariavel'])) * (1 - pdd_rate) - df_segmento_copy['CustoFixo']
            tir_completa = calculate_xirr([-vp_bruto] + df_segmento_copy.groupby('_DIAS_UTEIS_')['FluxoCompleto'].sum().values.tolist(), [0] + df_segmento_copy.groupby('_DIAS_UTEIS_')['FluxoCompleto'].sum().index.tolist())
            all_tirs.append({'DimensaoColuna': tipo_dimensao, 'Segmento': segmento, 'TIR Bruta \n(% a.m. )': tir_bruta * 100 if pd.notna(tir_bruta) else np.nan, 'TIR Líquida de PDD \n(% a.m. )': tir_pdd * 100 if pd.notna(tir_pdd) else np.nan, 'TIR Líquida de custos \n(% a.m. )': tir_custos * 100 if pd.notna(tir_custos) else np.nan, 'TIR Líquida Final \n(% a.m. )': tir_completa * 100 if pd.notna(tir_completa) else np.nan})

    df_tir_summary = pd.DataFrame(all_tirs).fillna(-100.0)
    
    print("Montando as tabelas finais...")
    # --- Montagem Final das Tabelas ---
    mapa_tabelas_html = {}
    dimensoes_ordem_alfabetica = ['Faixa Pop. Municipal', 'Faixa Pop. Estadual', 'CAPAG']
    for nome_analise, coluna in dimensoes_analise.items():
        if coluna not in df_final2.columns: continue
        
        df_pdd = tabelas_pdd.get(nome_analise)
        if df_pdd is None: continue

        df_final = df_pdd
        if nome_analise in tabelas_vencido: df_final = df_final.join(tabelas_vencido[nome_analise].drop(columns=[vp_col_name]), how='outer')
        if nome_analise in tabelas_ticket: df_final = df_final.join(tabelas_ticket[nome_analise], how='outer')
        
        df_tir = df_tir_summary[df_tir_summary['DimensaoColuna'] == coluna].set_index('Segmento')
        df_final = df_final.join(df_tir.drop(columns=['DimensaoColuna']), how='outer')
        df_final.index.name = nome_analise
        df_final.reset_index(inplace=True)
        df_final = df_final.drop(columns=['ValorVencido (M)'], errors='ignore')

        colunas_ordem = [nome_analise, vl_col_name, vp_col_name]
        if 'Ticket Ponderado (R$)' in df_final.columns: colunas_ordem.append('Ticket Ponderado (R$)')
        colunas_ordem.extend(['%PDD', '%Vencido'])
        colunas_tir_existentes = sorted([col for col in df_tir.columns if 'TIR' in col and col in df_final.columns])
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
        
        mapa_tabelas_html[nome_analise] = df_final.to_html(index=False, classes='dataframe', formatters=formatters, na_rep='-', escape=False)

    return mapa_tabelas_html, ref_date