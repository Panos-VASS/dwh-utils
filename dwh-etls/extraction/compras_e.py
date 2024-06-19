#%%
import sys
import os
from IPython import get_ipython

# Add the path to dwh-utils/dwh-utils to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../dwh-utils')))

# Import the utils module
import utils

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

# Proceed with your main logic using config
print(config)


# %%
