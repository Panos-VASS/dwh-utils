#%%
from utils import *


#%%

url = 'https://www.ine.es/jaxi/files/_px/es/csv_bd/t20/p274/serie/def/p03/l0/03003.csv_bd?nocab=1'
df = download_and_parse_csv(url, column_delimiter='\t')

#%%
map_column(df, 'Provincias')

# %%
force_int_conversion(df, 'Provincias')
# %%
adjust_dtype(df)
# %%
df2 = sample_data(df, n=200)
# %%
