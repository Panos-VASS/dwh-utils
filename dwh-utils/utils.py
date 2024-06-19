import os
import requests
import pandas as pd
import tempfile
import datetime
import inspect
import json
import inspect


def is_notebook():
    """Check if the code is running in a Jupyter notebook."""
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True
        elif shell == 'TerminalInteractiveShell':
            return False
        else:
            return False
    except NameError:
        return False



def download_and_parse_csv(url, column_delimiter=None):
    """
    Downloads a CSV file from a given URL, parses it into a Pandas DataFrame, and deletes the CSV file.

    Parameters:
    url (str): The URL of the CSV file.
    use_tab_delimiter (bool): Whether to use a tab delimiter. Default is False.

    Returns:
    pd.DataFrame: The CSV content as a Pandas DataFrame.
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, 'temp_file.csv')
            
            # Download the CSV file
            response = requests.get(url)
            response.raise_for_status()

            with open(temp_file_path, 'wb') as file:
                file.write(response.content)

            print(f"CSV file successfully downloaded and saved to {temp_file_path}")

            if column_delimiter != None:
                df = pd.read_csv(temp_file_path, delimiter=column_delimiter)
            else:
                df = pd.read_csv(temp_file_path)

            print(f"CSV file successfully loaded into DataFrame and deleted from {temp_file_path}")

            return df

    except requests.exceptions.RequestException as e:
        print(f"Failed to download the file: {e}")
        return None

def download_and_parse_json(url):
    """
    Downloads a JSON file from a given URL and parses it into a Pandas DataFrame.

    Parameters:
    url (str): The URL of the JSON file.

    Returns:
    pd.DataFrame or None: The JSON content as a Pandas DataFrame, or None if failed to download or parse.
    """
    try:
        
        response = requests.get(url)
        response.raise_for_status()

        df = pd.DataFrame(response.json())

        print(f"JSON file successfully downloaded and parsed")

        return df

    except requests.exceptions.RequestException as e:
        print(f"Failed to download or parse the JSON file: {e}")
        return None


def map_column(df, df_column_name, mapping_column_name=None, dq=True, dq_export=False, script_name=None, full_map=False):

    """
    Maps values in a DataFrame column based on a mapping Excel sheet and handles data quality (DQ) checks.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the column to be mapped.
    df_column_name (str): The name of the column in the DataFrame to be mapped.
    mapping_column_name (str): The name of the column in the Excel sheet used for mapping.
                               Defaults to the same name as df_column_name.
    dq (bool): Whether to perform data quality checks. Default is True.
    dq_export (bool): Whether to export the unmatched values to a DataFrame and an Excel file. Default is False.
    script_name (str): The name of the script or notebook calling this function. Used for naming the output file in dq_export.
    full_map (bool): Whether to replace input values not in the mapping dictionary with NaN. Default is False.

    Returns:
    pd.Series: The mapped column as a Pandas Series.
    """


    # Set the default mapping column name if not provided
    if mapping_column_name is None:
        mapping_column_name = df_column_name

    try:
        mapping_df = pd.read_excel('static/Mapping.xlsx', sheet_name=mapping_column_name, header=None)

        mapping_dict = dict(zip(mapping_df.iloc[:, 1], mapping_df.iloc[:, 0]))

        unmatched_values = df[~df[df_column_name].isin(mapping_dict.keys())][df_column_name].unique()

        if full_map:
            mapped_column = df[df_column_name].map(mapping_dict)
        else:
            mapped_column = df[df_column_name].map(mapping_dict).fillna(df[df_column_name])

        df[df_column_name] = mapped_column



        # Data Quality (DQ) checks
        if dq:
            
            if len(unmatched_values)>0:
                print("------------Unmatched Values------------")
                for element in unmatched_values:
                    print(element)


            if dq_export:
                dq_df = pd.DataFrame({
                    'value': unmatched_values,
                    'column_name': df_column_name,
                    'date': datetime.date.today()
                })

                if is_notebook():
                    script_name = 'notebook'

                elif script_name is None:
                    script_filename = os.path.basename(inspect.stack()[1].filename)
                    script_name = os.path.splitext(script_filename)[0]

                excel_file_name = f"{script_name}_dq.xlsx"
                dq_dir = 'dq'

                if not os.path.exists(dq_dir):
                    os.makedirs(dq_dir)

                excel_file_path = os.path.join(dq_dir, excel_file_name)
                dq_df.to_excel(excel_file_path, index=False)
                print(f"DataFrame exported to {excel_file_path}")

                return mapped_column, dq_df
            
            else:
                return mapped_column, unmatched_values

        return mapped_column


    except Exception as e:
        print(f"Error occurred while mapping column: {e}")
        return None

def force_int_conversion(df, column_name):
    """
    Attempt to convert all values in the specified column to integers.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the column to be processed.
    column_name (str): The name of the column in the DataFrame to be processed.

    Returns:
    None. The function modifies the DataFrame in-place.
    """
    for index, value in df[column_name].items():
        try:
            int_value = int(value)
            df.at[index, column_name] = int_value
        except (ValueError, TypeError):
            # If conversion fails, leave the original value
            pass
    
    if pd.api.types.is_integer_dtype(df[column_name]):
        df[column_name] = df[column_name].astype(int)


def adjust_dtype(df):
    """
    Adjust the dtype of columns in the DataFrame based on their contents.

    Parameters:
    df (pd.DataFrame): The DataFrame to be processed.

    Returns:
    None. The function modifies the DataFrame in-place.
    """

    for column in df.columns:
        if pd.api.types.is_integer_dtype(df[column]):
            df[column] = df[column].astype('int64')

        elif pd.api.types.is_float_dtype(df[column]):
            if df[column].apply(lambda x: isinstance(x, float) or pd.isna(x)).all():
                df[column] = df[column].astype('float64')

def sample_data(df, n=100, frac=None, stratify_by=None, random_state=None):

    """
    Take a sample from the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to sample from.
    n (int): Number of samples to take (default is 100). Ignored if frac is provided.
    frac (float): Fraction of samples to take (e.g., 0.1 for 10% of the DataFrame). 
    stratify_by (str): Column to stratify the sample by.
    random_state (int): Seed for the random number generator for reproducibility.

    Returns:
    pd.DataFrame: DataFrame containing the sample.
    """

    if stratify_by:
        if stratify_by not in df.columns:
            raise ValueError(f"Column '{stratify_by}' does not exist in the DataFrame.")
        if frac:
            sampled_df = df.groupby(stratify_by, group_keys=False).apply(
                lambda x: x.sample(frac=frac, random_state=random_state)
            )
        else:
            sampled_df = df.groupby(stratify_by, group_keys=False).apply(
                lambda x: x.sample(n=n // len(df[stratify_by].unique()), random_state=random_state)
            )
    else:
        sampled_df = df.sample(n=n, frac=frac, random_state=random_state)
    
    return sampled_df.reset_index(drop=True)

from IPython import get_ipython


def load_config(notebook_filename):
    """
    Loads the configuration from a JSON file based on the script or notebook filename.

    The function attempts to determine the base name from the filename,
    constructs the path to the corresponding JSON configuration file located in the '../config' directory,
    and reads the configuration into a dictionary.

    Parameters:
    - notebook_filename (str): Full path of the notebook or script filename.

    Raises:
        FileNotFoundError: If the configuration file does not exist.

    Returns:
        dict: The configuration data.
    """
    try:
        # Extract the base name from the filename (example_e.py -> example)
        script_name = os.path.basename(notebook_filename)
        base_name = os.path.splitext(script_name)[0].split('_')[0]
        
        # Construct the path to the JSON configuration file
        base_path = os.path.abspath(os.path.join(os.path.dirname(notebook_filename), '..'))
        config_dir = os.path.join(base_path, 'config')
        config_file = os.path.join(config_dir, f'{base_name}.json')
        
        # Read the JSON file into a dictionary
        if os.path.exists(config_file):
            with open(config_file, 'r') as file:
                config = json.load(file)
        else:
            raise FileNotFoundError(f"Config file {config_file} not found.")
        
        return config
    
    except Exception as e:
        raise RuntimeError(f"Error loading config: {str(e)}")

# Example usage with notebook_filename provided
notebook_filename = 'c:\\Users\\panagiotis.c\\OneDrive - Grupo VASS\\Desktop\\dwh-utils\\dwh-etls\\extraction\\example_e.py'
try:
    config_data = load_config(notebook_filename)
    print(config_data)
except Exception as e:
    print(f"Error: {str(e)}")