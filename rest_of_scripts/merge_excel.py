#%%
import pandas as pd
import os

# Directory where your CSV files are located
folder_path = 'tables needed/'

# Initialize an empty list to store DataFrame objects
dfs = []

# Iterate over each file in the directory
for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv'):
        file_path = os.path.join(folder_path, file_name)
        # Read each CSV file into a DataFrame
        df = pd.read_csv(file_path)
        # Append the DataFrame to the list
        dfs.append(df)

# Concatenate all DataFrames into one
combined_df = pd.concat(dfs, ignore_index=True)

# Deduplicate the combined DataFrame based on all columns
combined_df = combined_df.drop_duplicates()

# Output file path for the combined CSV file
output_file = 'table_schema.csv'

# Write the combined DataFrame to a CSV file
combined_df.to_csv(output_file, index=False)
print(f'Combined data saved to {output_file}')
# %%
