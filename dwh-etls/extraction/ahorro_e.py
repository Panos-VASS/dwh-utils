#%%
import sys
import os
from IPython import get_ipython

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../dwh-utils')))

import utils_extraction as utils

#%%
def get_notebook_filename():
    """
    Helper function to get the notebook filename in a Jupyter environment.
    Returns None if not in a Jupyter notebook.
    """
    try:
        return get_ipython().ev('__file__')
    except AttributeError:
        return None


#%%

notebook_filename = get_notebook_filename()

if notebook_filename:
    # Running in a Jupyter notebook
    config = utils.load_config(notebook_filename=notebook_filename)
else:
    # Running in a Python script or Airflow environment
    script_filename = os.path.abspath(__file__)
    config = utils.load_config(notebook_filename=script_filename)




# %%
extracted_data = utils.perform_extraction(config, script_filename = notebook_filename)
# %%
