#%%
import os
import requests
import pandas as pd
import tempfile
import datetime


#%%
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
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Define the temporary file path
            temp_file_path = os.path.join(temp_dir, 'temp_file.csv')
            
            # Download the CSV file
            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful

            # Save the CSV to the temporary file path
            with open(temp_file_path, 'wb') as file:
                file.write(response.content)

            print(f"CSV file successfully downloaded and saved to {temp_file_path}")

            # Load the CSV into a Pandas DataFrame with appropriate delimiter
            if column_delimiter != None:
                df = pd.read_csv(temp_file_path, delimiter=column_delimiter)
            else:
                df = pd.read_csv(temp_file_path)

            # The file will be automatically deleted when the with block is exited
            print(f"CSV file successfully loaded into DataFrame and deleted from {temp_file_path}")

            return df

    except requests.exceptions.RequestException as e:
        print(f"Failed to download the file: {e}")
        return None



#%%
# Example usage
url = 'https://www.ine.es/jaxi/files/_px/es/csv_bd/t20/p274/serie/def/p03/l0/03003.csv_bd?nocab=1'
df = download_and_parse_csv(url, column_delimiter='\t')
# %%

def map_column(df, df_column_name, mapping_column_name=None, dq=True, dq_export=False):
    """
    Maps values in a DataFrame column based on a mapping Excel sheet.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the column to be mapped.
    df_column_name (str): The name of the column in the DataFrame to be mapped.
    mapping_column_name (str): The name of the column in the Excel sheet used for mapping.
                               Defaults to the same name as df_column_name.

    Returns:
    pd.Series: The mapped column as a Pandas Series.
    """
    # Set the default mapping column name if not provided
    if mapping_column_name is None:
        mapping_column_name = df_column_name

    try:
        # Read the Excel file
        mapping_df = pd.read_excel('static/Mapping.xlsx', sheet_name=mapping_column_name)

        # Create a mapping dictionary from the Excel sheet
        mapping_dict = dict(zip(mapping_df.iloc[:, 1], mapping_df.iloc[:, 0]))

        # Map the values in the DataFrame column
        mapped_column = df[df_column_name].map(mapping_dict)

        # Data Quality (DQ) checks
        if dq:
            unmatched_values = df[df_column_name][~df[df_column_name].isin(mapping_dict.keys())].unique()

            
            if len(unmatched_values)>0:
                print(unmatched_values)


            if dq_export:
                dq_df = pd.DataFrame({
                    'value': unmatched_values,
                    'column_name': df_column_name,
                    'date': datetime.date.today()
                })
                # Export the DataFrame to an Excel file
                script_name = os.path.splitext(os.path.basename(__file__))[0]
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
# %%
mapped_category = map_column(df, 'Provincias', dq_export=True)
# %%
