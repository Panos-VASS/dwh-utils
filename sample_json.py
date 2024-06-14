#%%
from utils import *


#%%

url = "https://raw.githubusercontent.com/corysimmons/colors.json/master/colors.json"
df = download_and_parse_json(url)
# %%
adjust_dtype(df)

# %%
