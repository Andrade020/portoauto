import pandas as pd
import json
import os
import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re
import base64

#$#$#$#$#$#$#$#$# configs #$#$#$#$#$#$#$#$#

#? path p/ historico de carteiras json da vortx
p_cart_hist = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\data\Carteiras"
#? path p/ arquivos .csv do novo estoque
p_estq_novo = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\data\Estoques"
#? path p/ relatorios de despesas em excel
p_despesas = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\src\vortx_estoques\data\Despesas"
#? path da imagem do logo p/ o relatorio
p_logo = r"C:\caminho\para\sua\logo.png" 
#? nome do fundo que aparece no cabecalho do relatorio
nm_fundo = "FIDC FCT CONSIGNADO II"
#? nome do arquivo de saida do relatorio
out_file = "relatorio_fluxo_caixa_final.html"

#>>>>>> funcs >>>>>>

def get_carteira_recente(pth):
    """acha e carrega a carteira mais nova pela data interna do json."""
    print("procurando carteira recente...")
    j_files = glob.glob(os.path.join(pth, "*.json"))
    if not j_files: return None, None, None

    rec_file, rec_date = None, None
    for f in j_files:
        try:
            with open(f, 'r', encoding='utf-8') as file:
                dados = json.load(file)
                dt_str = dados[0]['carteiras'][0]['dataAtual']
                dt_obj = datetime.strptime(dt_str.split('T')[0], '%Y-%m-%d')
                if rec_date is None or dt_obj > rec_date: #* logica p/ achar o mais novo
                    rec_date, rec_file = dt_obj, f
        except Exception: #! ignora arquivos com erro de leitura ou sem a data
            continue
    
    if rec_file:
        print(f"usando carteira: {os.path.basename(rec_file)}")
        with open(rec_file, 'r', encoding='utf-8') as f:
            return json.load(f)[0], rec_date, os.path.basename(rec_file)
    return None, None, None

def proc_estoque_novo(pth):
    """le o ultimo lote de arquivos de estoque e consolida num df."""
    print("\ncarregando estoque novo...")
    all_files = glob.glob(os.path.join(pth, '**/*.csv'), recursive=True)
    if not all_files: return pd.DataFrame()

    rec_date = None
    regex_data = re.compile(r'(\d{2}\.\d{2}\.\d{2})') #* acha data no nome do arq (dd.mm.aa)
    
    #* primeiro, acha a data mais recente entre todos os arquivos
    for f in all_files:
        match = regex_data.search(os.path.basename(f))
        if match:
            dt_obj = datetime.strptime(match.group(1), '%d.%m.%y')
            if rec_date is None or dt_obj > rec_date: rec_date = dt_obj

    if rec_date is None: return pd.DataFrame() #* se n achou data, retorna df vazio

    rec_date_str = rec_date.strftime('%d.%m.%y')
    print(f"usando estoque de: {rec_date_str}")

    files_to_read = [f for f in all_files if rec_date_str in os.path.basename(f)]
    dfs_parts = [pd.read_csv(f, sep=';', decimal=',', encoding='utf-8', on_bad_lines='warn') for f in files_to_read]
    
    if not dfs_parts: return pd.DataFrame()

    df_consol = pd.concat(dfs_parts, ignore_index=True)
    #* garante que os nomes das colunas estao padronizados
    df_consol.rename(columns={
        'DataVencimento': 'Data Vencimento', 
        'ValorPresente': 'Valor Presente',
        'ValorNominal': 'Valor Nominal' #! importante ter essa coluna no csv
    }, inplace=True)
    df_consol['Data Vencimento'] = pd.to_datetime(df_consol['Data Vencimento'], dayfirst=True, errors='coerce')
    df_consol['Valor Nominal'] = pd.to_numeric(df_consol['Valor Nominal'], errors='coerce') #* converte p/ numero
    
    print(f"estoque novo consolidado: {len(df_consol)} linhas.")
    return df_consol

def proc_despesas(pth, dt_base_proj, n_meses_proj=12):
    """encontra o relatorio de despesas mais recente e projeta p/ o futuro."""
    print("\ncarregando despesas...")
    xls_files = glob.glob(os.path.join(pth, '**/Despesas_Consolidadas.xlsx'), recursive=True)
    if not xls_files: return pd.DataFrame(), None

    df_rec, dt_rec = None, None
    for f in xls_files:
        try:
            df_temp = pd.read_excel(f, header=6)
            df_temp['Data'] = pd.to_datetime(df_temp['Data'], errors='coerce', dayfirst=True)
            dt_atual = df_temp['Data'].max()
            if dt_rec is None or dt_atual > dt_rec:
                dt_rec, df_rec = dt_atual, df_temp
        except Exception: continue
    
    if df_rec is None: return pd.DataFrame(), None

    print(f"usando relatorio de despesas de: {dt_rec.strftime('%d/%m/%Y')}")
    df_base = df_rec.rename(columns={'Título': 'Categoria', 'Valor Mercado': 'Valor'})
    df_base['Valor'] = -pd.to_numeric(df_base['Valor'], errors='coerce').abs() #! valor fica negativo
    df_base['Data Pagamento'] = pd.to_datetime(df_base['Data Pagamento'], errors='coerce', dayfirst=True)
    df_base = df_base[['Categoria', 'Valor', 'Data Pagamento']].dropna()

    desp_finais = []
    desp_reais_futuras = df_base[df_base['Data Pagamento'] >= dt_base_proj]
    desp_finais.extend(desp_reais_futuras.to_dict('records'))

    mes_ini_proj = (dt_base_proj.replace(day=1) + relativedelta(months=1))
    for i in range(n_meses_proj):
        mes_atual = mes_ini_proj + relativedelta(months=i)
        dt_pgto_proj = mes_atual.replace(day=5) #* projeta p/ dia 5
        while dt_pgto_proj.weekday() >= 5: dt_pgto_proj += timedelta(days=1) #* se fds, joga p/ frente

        if dt_pgto_proj >= dt_base_proj:
            for _, row in df_base.iterrows():
                desp_finais.append({'Data Pagamento': dt_pgto_proj, 'Categoria': row['Categoria'], 'Valor': row['Valor']})

    df_final = pd.DataFrame(desp_finais)
    print(f"{len(df_final)} despesas futuras (reais + projetadas) criadas.")
    return df_final, dt_rec

def prep_fluxos_det(d_cart_rec, df_estq, df_desp):
    """cria uma lista com todos os eventos de fluxo de caixa (entradas/saidas)."""
    fluxos = []
    dt_base = datetime.strptime(d_cart_rec['carteiras'][0]['dataAtual'].split('T')[0], '%Y-%m-%d')

    #* busca por cada tipo de ativo no json da carteira
    disp = next((a.get('Disponibilidade') for a in d_cart_rec['ativos'] if a.get('Disponibilidade')), None)
    if disp:
        fluxos.append({'data': dt_base, 'categoria': 'Conta Corr.', 'valor': disp['ativos'][0]['saldo'], 'descricao': 'Saldo Inicial'})

    cotas = next((a.get('Cotas') for a in d_cart_rec['ativos'] if a.get('Cotas')), None)
    if cotas:
        for c in cotas['ativos']:
            fluxos.append({
                'data': dt_base, #! <-- MUDANCA CRUCIAL AQUI: de d+1 para d+0
                'categoria': 'Fundo inv.', 
                'valor': c['valorBruto'], 
                'descricao': f"Resgate {c.get('titulo', 'Fundo')}"
            })

    rf = next((a.get('RendaFixa') for a in d_cart_rec['ativos'] if a.get('RendaFixa')), None)
    if rf:
        for titulo in rf['ativos']:
            fluxos.append({'data': datetime.strptime(titulo['dataVencimento'], '%Y-%m-%d'), 'categoria': 'Títulos Renda Fixa', 'valor': titulo['mercadoAtual'], 'descricao': f"Venc. {titulo.get('codigoCustodia', 'Título RF')}"})
    
    #* adiciona recebiveis do estoque (usando VALOR NOMINAL)
    if not df_estq.empty:
        estq_valido = df_estq.dropna(subset=['Data Vencimento', 'Valor Nominal']) #! checa se tem data e valor nominal
        estq_futuro = estq_valido[estq_valido['Data Vencimento'] >= dt_base]
        
        print(f"\n{len(estq_futuro)} parcelas do estoque com venc. futuro incluidas na projecao.")
        
        for _, row in estq_futuro.iterrows():
            fluxos.append({
                'data': row['Data Vencimento'], 
                'categoria': 'Pagamentos Estoque', 
                'valor': row['Valor Nominal'], #! <-- usando valor nominal
                'descricao': f"Título Nº {row.get('NumeroTitulo', 'N/A')}"
            })

    #* adiciona despesas
    if not df_desp.empty:
        desp_futuras = df_desp[df_desp['Data Pagamento'] >= dt_base]
        for _, desp in desp_futuras.iterrows():
            fluxos.append({'data': desp['Data Pagamento'], 'categoria': 'Despesas', 'valor': desp['Valor'], 'descricao': desp['Categoria']})
            
    #* adiciona amortizacao da cota senior
    cart_sr = next((c for c in d_cart_rec['carteiras'] if 'SR2' in c['nome']), None)
    if cart_sr:
        pl_sr = cart_sr['pl']
        vlr_amort = (pl_sr / 36) * -1 #! valor negativo
        dt_amort = datetime(2026, 1, 16)
        for i in range(36):
            dt_pgto = dt_amort + relativedelta(months=i)
            while dt_pgto.weekday() >= 5: dt_pgto += pd.Timedelta(days=1) #* se fds, joga p/ frente
            fluxos.append({'data': dt_pgto, 'categoria': 'Amort./ Resgat. Cota', 'valor': vlr_amort, 'descricao': f"Parcela {i+1}/36"})
            
    return pd.DataFrame(fluxos)

def gera_relatorio_dmais(df_flx, dt_base):
    """gera a tabela resumo no formato D+N."""
    prazos = [0, 1, 5, 10, 21, 63, 365] #* D+
    cols = ['Conta Corr.', 'Fundo inv.', 'Títulos Renda Fixa', 'Pagamentos Estoque', 'Despesas', 'Amort./ Resgat. Cota']
    linhas = []

    for d in prazos:
        dt_limite = dt_base + timedelta(days=d)
        saldos = df_flx[df_flx['data'] <= dt_limite].groupby('categoria')['valor'].sum()
        linha = {'Prazo': f"D+{d}"}
        for col in cols: linha[col] = saldos.get(col, 0.0)
        linhas.append(linha)
    
    df_rel = pd.DataFrame(linhas).set_index('Prazo')
    df_rel['Disponibilidades'] = df_rel[cols[:4]].sum(axis=1) #* o que entra
    df_rel['Necessidades'] = df_rel[cols[4:]].sum(axis=1)    #* o que sai
    df_rel['Caixa Projetado'] = df_rel['Disponibilidades'] + df_rel['Necessidades']
    
    return df_rel

def img_to_base64(img_path):
    """codifica a imagem do logo para por no html."""
    try:
        with open(img_path, "rb") as f:
            return base64.b64encode(f.read()).decode('utf-8')
    except FileNotFoundError:
        return "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=" #* fallback p/ img em branco 1x1

def gera_html(df_sum, df_det, nm_fundo, dt_ref, p_logo, out_file):
    """gera o arquivo html final, com detalhamento (exceto p/ estoque)."""
    logo_b64 = img_to_base64(p_logo)
    
    #* prepara os detalhes, mas tira o estoque p/ n ficar pesado
    df_det_leve = df_det[df_det['categoria'] != 'Pagamentos Estoque'].copy()
    df_det_leve['data'] = pd.to_datetime(df_det_leve['data'], errors='coerce').dt.strftime('%d/%m/%Y')
    df_det_leve.dropna(subset=['data'], inplace=True)
    det_json = df_det_leve.to_json(orient='records', force_ascii=False)

    df_sum_fmt = df_sum.applymap('{:,.2f}'.format)
    tbl_html = '<table id="relatorioTabela" class="display compact"><thead><tr><th>Prazo</th>'
    for col in df_sum_fmt.columns: tbl_html += f'<th>{col}</th>'
    tbl_html += '</tr></thead><tbody>'
    for prazo, row in df_sum_fmt.iterrows():
        tbl_html += f'<tr><th>{prazo}</th>'
        for col, val in row.items():
            tbl_html += f'<td data-prazo="{prazo}" data-categoria="{col}">{val}</td>'
        tbl_html += '</tr>'
    tbl_html += '</tbody></table>'

    #* o template do html/css/js continua o mesmo
    html_template = f"""
    <!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><title>Relatório de Fluxo de Caixa Interativo</title>
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <style>
        body {{ font-family: "Gill Sans MT", Arial, sans-serif; background-color: #FFFFFF; color: #313131; font-size: 1.1em; padding: 20px; }}
        h1 {{ font-size: 2.2em; color: #163f3f; margin: 0; }} .header-container {{ display: flex; align-items: center; margin-bottom: 20px; }}
        .info-box {{ border: 1px solid #e0e0e0; background-color: #f5f5f5; padding: 15px; margin-bottom: 30px; }}
        table.dataTable thead th {{ background-color: #163f3f !important; color: white !important; }}
        table.dataTable.display tbody tr.odd {{ background-color: #f5f5f5; }} table.dataTable.display tbody tr.even {{ background-color: #e0e0e0; }}
        table.dataTable tbody tr:hover {{ background-color: #FFD8A7 !important; }} table.dataTable tbody th {{ font-weight: bold; background-color: #0e3030; color: white; }}
        #relatorioTabela tbody td[data-categoria="Pagamentos Estoque"] {{ cursor: default; }}
        #relatorioTabela tbody td:not([data-categoria="Pagamentos Estoque"]):not(:empty) {{ cursor: pointer; }}
        .modal {{ display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0,0,0,0.5); }}
        .modal-content {{ background-color: #fefefe; margin: 5% auto; padding: 20px; border: 1px solid #888; width: 80%; max-width: 900px; }}
        .close-button {{ color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }}
        #modal-table-container table {{ width: 100%; border-collapse: collapse; }} #modal-table-container th, #modal-table-container td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        #modal-table-container th {{ background-color: #f2f2f2; }}
    </style></head><body>
    <div class="header-container"><img src="data:image/png;base64,{logo_b64}" style="max-height:80px; margin-right:20px;"><h1>Relatório de Fluxo de Caixa Interativo</h1></div>
    <div class="info-box"><strong>Fundo:</strong> {nm_fundo}<br><strong>Data do Relatório:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}<br><strong>Data de Referência (D+0):</strong> {dt_ref.strftime('%d/%m/%Y')}</div>
    {tbl_html}
    <div id="detailModal" class="modal"><div class="modal-content"><span class="close-button">&times;</span><h2 id="modal-title">Detalhamento</h2><div id="modal-table-container" style="max-height: 400px; overflow-y: auto;"></div></div></div>
    <script src="https://code.jquery.com/jquery-3.7.0.js"></script><script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <script>var dadosDetalhados = {det_json}; var dataReferencia = new Date('{dt_ref.strftime("%Y-%m-%d")}T00:00:00');</script>
    <script>
    $(document).ready(function() {{
        $('#relatorioTabela').DataTable({{ "paging": false, "searching": false, "info": false, "ordering": false }});
        var modal = $('#detailModal');
        $('#relatorioTabela tbody').on('click', 'td', function() {{
            var cell = $(this); var prazo = cell.data('prazo'); var categoria = cell.data('categoria');
            if (!prazo || !categoria || ['Disponibilidades', 'Necessidades', 'Caixa Projetado', 'Pagamentos Estoque'].includes(categoria) || cell.text().trim() === '0.00' || cell.text().trim() === '0,00') {{ return; }}
            var dias = parseInt(prazo.replace('D+', '')); var dataLimite = new Date(dataReferencia); dataLimite.setDate(dataLimite.getDate() + dias);
            var itensFiltrados = dadosDetalhados.filter(function(item) {{
                var dataItem = new Date(item.data.split('/').reverse().join('-') + 'T00:00:00');
                return item.categoria === categoria && dataItem <= dataLimite;
            }});
            var tableHtml = '<table><thead><tr><th>Data</th><th>Descrição</th><th style="text-align:right;">Valor</th></tr></thead><tbody>';
            var total = 0;
            itensFiltrados.forEach(function(item) {{
                total += item.valor;
                tableHtml += '<tr><td>' + item.data + '</td><td>' + (item.descricao || '') + '</td><td style="text-align:right;">' + item.valor.toLocaleString('pt-BR', {{style: 'currency', currency: 'BRL'}}) + '</td></tr>';
            }});
            tableHtml += '</tbody><tfoot><tr><th colspan="2">Total</th><th style="text-align:right;">' + total.toLocaleString('pt-BR', {{style: 'currency', currency: 'BRL'}}) + '</th></tr></tfoot></table>';
            $('#modal-title').text('Detalhamento de: ' + categoria + ' (até ' + prazo + ')');
            $('#modal-table-container').html(tableHtml);
            modal.show();
        }});
        $('.close-button').on('click', function() {{ modal.hide(); }});
        $(window).on('click', function(event) {{ if (event.target == modal[0]) modal.hide(); }});
    }});
    </script></body></html>
    """

    with open(out_file, 'w', encoding='utf-8') as f:
        f.write(html_template)
    print(f"✅ relatorio html gerado: '{os.path.abspath(out_file)}'")

#$#$#$#$#$#$#$#$# execucao #$#$#$#$#$#$#$#$#
if __name__ == "__main__":
    d_cart, dt_ref, _ = get_carteira_recente(p_cart_hist)
    
    if d_cart:
        df_estq = proc_estoque_novo(p_estq_novo)                         #* carrega dados de estoque
        df_desp, _ = proc_despesas(p_despesas, dt_ref)                   #* carrega e projeta despesas
        df_fluxos = prep_fluxos_det(d_cart, df_estq, df_desp)            #*  consolida todos os fluxos
        rel_final = gera_relatorio_dmais(df_fluxos, dt_ref)             #* cria o resumo d+
        gera_html(rel_final, df_fluxos, nm_fundo, dt_ref, p_logo, out_file) #* gera o relatorio final
    else:
        print("#! nenhuma carteira recente encontrada. encerrando.")