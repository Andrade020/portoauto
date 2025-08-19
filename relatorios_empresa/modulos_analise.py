# -*- coding: utf-8 -*-
# fiz esse código para deixar alguns módulos que sempre usamos, 
# como calcular TIR com brenq, etc

import pandas as pd

##! FUNÇÕES TOY  -------------------------------------------
def calcular_metricas_gerais(df, dimensao):
    """
    Calcula métricas de PDD, VP e Contratos por uma dimensão específica.
    """
    if dimensao not in df.columns:
        print(f"[AVISO] Dimensão '{dimensao}' não encontrada no DataFrame.")
        return pd.DataFrame()

    print(f"Calculando métricas para a dimensão: {dimensao}...")
    
    # Colunas para o relatório
    vp_col_name = 'Valor Presente (R$ MM)'
    vl_col_name = 'Valor Líquido (R$ MM)'

    df_report = df.copy()
    df_report['_ValorLiquido'] = df_report['ValorPresente'] - df_report['PDDTotal']

    grouped = df_report.groupby(dimensao)

    df_metricas = grouped.agg(
        num_contratos=('CCB', 'nunique'),
        valor_presente_total=('ValorPresente', 'sum'),
        valor_liquido_total=('_ValorLiquido', 'sum')
    ).reset_index()

    # Cálculos percentuais e de formatação
    df_metricas['%PDD'] = (1 - df_metricas['valor_liquido_total'] / df_metricas['valor_presente_total']) * 100
    df_metricas[vp_col_name] = df_metricas['valor_presente_total'] / 1e6
    df_metricas[vl_col_name] = df_metricas['valor_liquido_total'] / 1e6

    # Ordenar e selecionar colunas
    df_metricas = df_metricas.sort_values(by='valor_presente_total', ascending=False)
    
    colunas_finais = [
        dimensao,
        'num_contratos',
        vl_col_name,
        vp_col_name,
        '%PDD'
    ]
    df_metricas = df_metricas[colunas_finais]
    df_metricas.columns = [dimensao, 'Nº de Contratos', 'Valor Líquido (R$ MM)', 'Valor Presente (R$ MM)', '% PDD']
    
    return df_metricas

def calcular_prazo_medio_ponderado(df, dimensao):
    """
    Calcula o prazo médio ponderado pelo Valor Presente.
    """
    if dimensao not in df.columns or 'Prazo' not in df.columns:
        return pd.DataFrame()
        
    print(f"Calculando prazo médio para a dimensão: {dimensao}...")
    
    df_temp = df.dropna(subset=[dimensao, 'ValorPresente', 'Prazo'])
    grouped = df_temp.groupby(dimensao)

    prazo_ponderado = grouped.apply(
        lambda g: (g['Prazo'] * g['ValorPresente']).sum() / g['ValorPresente'].sum(),
        include_groups=False
    ).reset_index(name='Prazo Médio Ponderado (meses)')
    
    return prazo_ponderado