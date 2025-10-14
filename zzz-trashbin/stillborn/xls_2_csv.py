import pandas as pd

# Caminhos dos arquivos
arquivo_xls = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\feriados_nacionais.xls"
arquivo_csv = r"C:\Users\Leo\Desktop\Porto_Real\portoauto\feriados_nacionais.csv"

# Lê o arquivo Excel
df = pd.read_excel(arquivo_xls)

# Salva como CSV (sem o índice numérico)
df.to_csv(arquivo_csv, index=False, encoding='utf-8-sig')

print(f"Arquivo convertido com sucesso para: {arquivo_csv}")
