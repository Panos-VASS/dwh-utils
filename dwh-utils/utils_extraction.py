import os
import requests
import pandas as pd
import tempfile
import datetime
import inspect
import json
from datetime import datetime
import mysql.connector


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




def download_and_parse_csv(url, column_delimiter=None, load_s3=False, output_folder='../../s3/temp_files', script_filename=None):
    """
    Downloads a CSV file from a given URL, parses it into a Pandas DataFrame, and optionally uploads it as a CSV to a specified folder.

    Parameters:
    url (str): The URL of the CSV file.
    column_delimiter (str): The delimiter for columns in the CSV file.
    load_s3 (bool): Whether to save the DataFrame as a CSV file in the specified folder.
    output_folder (str): The folder to save the CSV file if load_s3 is True.
    script_filename (str): The full path of the script file for naming the output file.

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

            if column_delimiter is not None:
                df = pd.read_csv(temp_file_path, delimiter=column_delimiter)
            else:
                df = pd.read_csv(temp_file_path)

            print(f"CSV file successfully loaded into DataFrame and deleted from {temp_file_path}")

            if load_s3:
                current_date = datetime.now().strftime('%Y%m%d')
                date_folder = os.path.join(output_folder, current_date)
                os.makedirs(date_folder, exist_ok=True)

                script_name = os.path.splitext(os.path.basename(script_filename))[0] if script_filename else 'output'
                timestamp = datetime.now().strftime('%H%M%S')
                output_file_name = f"{script_name}_{timestamp}.csv"
                output_file_path = os.path.join(date_folder, output_file_name)

                df.to_csv(output_file_path, index=False)
                print(f"File successfully uploaded to {output_file_path}")

            return df

    except requests.exceptions.RequestException as e:
        print(f"Failed to download the file: {e}")
        return None

def flatten_json(json_data):
    """
    Flatten a JSON object by recursively extracting nested dictionaries into a flat dictionary.

    Parameters:
    json_data (dict): The JSON data to be flattened.

    Returns:
    dict: The flattened dictionary.
    """
    flattened = {}

    def flatten_helper(item, prefix=''):
        if isinstance(item, dict):
            for key, value in item.items():
                new_key = f"{prefix}_{key}" if prefix else key
                flatten_helper(value, new_key)
        else:
            flattened[prefix] = item

    flatten_helper(json_data)
    return flattened

def download_and_parse_json(url, load_s3=False, output_folder='../../s3/temp_files', script_filename=None):
    """
    Downloads a JSON file from a given URL, parses it into a Pandas DataFrame, and optionally uploads it as a CSV to a specified folder.

    Parameters:
    url (str): The URL of the JSON file.
    load_s3 (bool): Whether to save the DataFrame as a CSV file in the specified folder.
    output_folder (str): The folder to save the CSV file if load_s3 is True.
    script_filename (str): The full path of the script file for naming the output file.

    Returns:
    pd.DataFrame or None: The JSON content as a Pandas DataFrame, or None if failed to download or parse.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        # Flatten each item in the JSON array
        flattened_data = [flatten_json(item) for item in data]

        df = pd.DataFrame(flattened_data)

        print(f"JSON file successfully downloaded and parsed")

        if load_s3:
            current_date = datetime.now().strftime('%Y%m%d')
            date_folder = os.path.join(output_folder, current_date)
            os.makedirs(date_folder, exist_ok=True)

            script_name = os.path.splitext(os.path.basename(script_filename))[0] if script_filename else 'output'
            timestamp = datetime.now().strftime('%H%M%S')
            output_file_name = f"{script_name}_{timestamp}.csv"
            output_file_path = os.path.join(date_folder, output_file_name)

            df.to_csv(output_file_path, index=False)
            print(f"File successfully uploaded to {output_file_path}")

        return df

    except requests.exceptions.RequestException as e:
        print(f"Failed to download or parse the JSON file: {e}")
        return None


def download_from_mysql(host, username, password, database, table, load_s3=False, output_folder='../../s3/temp_files', script_filename=None):
    """
    Downloads data from a MySQL database table and returns it as a Pandas DataFrame, optionally saving it as a CSV.

    Parameters:
    - host (str): MySQL server host address.
    - username (str): MySQL username.
    - password (str): MySQL password.
    - database (str): MySQL database name.
    - table (str): Name of the table from which to download data.
    - load_s3 (bool): Whether to save the DataFrame as a CSV file in the specified folder.
    - output_folder (str): The folder to save the CSV file if load_s3 is True.
    - script_filename (str): The full path of the script file for naming the output file.

    Returns:
    pd.DataFrame: DataFrame containing the downloaded data.
    """
    try:
        # Establish a connection to the MySQL server
        conn = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
            database=database
        )

        # Create a cursor object using the connection
        cursor = conn.cursor()

        # Query to select all data from the specified table
        query = f"SELECT * FROM {table};"

        # Execute the query
        cursor.execute(query)

        # Fetch all rows from the result set
        data = cursor.fetchall()

        # Get column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        # Create a Pandas DataFrame
        df = pd.DataFrame(data, columns=columns)

        # Close cursor and connection
        cursor.close()
        conn.close()

        if load_s3:
            current_date = datetime.now().strftime('%Y%m%d')
            date_folder = os.path.join(output_folder, current_date)
            os.makedirs(date_folder, exist_ok=True)

            script_name = os.path.splitext(os.path.basename(script_filename))[0] if script_filename else 'output'
            timestamp = datetime.now().strftime('%H%M%S')
            output_file_name = f"{script_name}_{timestamp}.csv"
            output_file_path = os.path.join(date_folder, output_file_name)

            df.to_csv(output_file_path, index=False)
            print(f"File successfully uploaded to {output_file_path}")

        return df

    except mysql.connector.Error as e:
        print(f"Error downloading data from MySQL: {e}")
        return None

def perform_extraction(config, script_filename = "output"):
    """
    Performs data extraction based on the provided configuration.

    Parameters:
    config (dict): Configuration dictionary containing extraction details for JSON, SQL, and CSV.

    Returns:
    dict: Dictionary containing the extracted DataFrames.
    """
    extracted_data = {}

    extraction_config = config.get("extraction", {})

    json_configs = extraction_config.get("json", [])
    extracted_data["json"] = []
    for json_config in json_configs:
        print("Performing JSON extraction...")
        df_json = download_and_parse_json(
            url=json_config.get("url"),
            load_s3=json_config.get("load_s3", False),
            output_folder=json_config.get("output_folder", '../../s3/temp_files'),
            script_filename=script_filename
        )
        extracted_data["json"].append(df_json)

    sql_configs = extraction_config.get("sql", [])
    extracted_data["sql"] = []
    for sql_config in sql_configs:
        print("Performing SQL extraction...")
        df_sql = download_from_mysql(
            host=sql_config.get("host"),
            username=sql_config.get("username"),
            password=sql_config.get("password"),
            database=sql_config.get("database"),
            table=sql_config.get("table"),
            load_s3=sql_config.get("load_s3", False),
            output_folder=sql_config.get("output_folder", '../../s3/temp_files'),
            script_filename=script_filename
        )
        extracted_data["sql"].append(df_sql)

    csv_configs = extraction_config.get("csv", [])
    extracted_data["csv"] = []
    for csv_config in csv_configs:
        print("Performing CSV extraction...")
        df_csv = download_and_parse_csv(
            url=csv_config.get("url"),
            column_delimiter=csv_config.get("column_delimiter"),
            load_s3=csv_config.get("load_s3", False),
            output_folder=csv_config.get("output_folder", '../../s3/temp_files'),
            script_filename=script_filename
        )
        extracted_data["csv"].append(df_csv)


    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    output_folder = os.path.abspath(extraction_config.get("output_excel_path", ""))

    try:
        temp_name = os.path.splitext(os.path.basename(script_filename))[0]
        base_filename = temp_name.split("_")[0]

    except:
        base_filename = script_filename

    output_folder = os.path.join(output_folder, base_filename)
    os.makedirs(output_folder, exist_ok=True)

    output_excel_path = os.path.join(output_folder, f"{base_filename}_{timestamp}.xlsx")

    # Write extracted data to Excel
    with pd.ExcelWriter(output_excel_path) as writer:
        for key, df_list in extracted_data.items():
            for idx, df in enumerate(df_list):
                sheet_name = f"{key}_{idx}"  # Sheet names like json_0, json_1, etc.
                df.to_excel(writer, sheet_name=sheet_name, index=False)


    return extracted_data


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
        script_name = os.path.basename(notebook_filename)
        base_name = os.path.splitext(script_name)[0].split('_')[0]
        
        base_path = os.path.abspath(os.path.join(os.path.dirname(notebook_filename), '..'))
        config_dir = os.path.join(base_path, 'config')
        config_file = os.path.join(config_dir, f'{base_name}.json')
        
        if os.path.exists(config_file):
            with open(config_file, 'r') as file:
                config = json.load(file)
        else:
            raise FileNotFoundError(f"Config file {config_file} not found.")
        
        return config
    
    except Exception as e:
        raise RuntimeError(f"Error loading config: {str(e)}")

