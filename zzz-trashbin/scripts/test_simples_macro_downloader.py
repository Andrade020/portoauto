import pandas as pd
from macro_downloader import get_ibge_pim
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 120)
df_test = md.get_ibge_pim()
print(df_test)