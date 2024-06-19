#%%
import mysql.connector
import pandas as pd

#%%
def download_from_mysql(host, username, password, database, table):
    """
    Downloads data from a MySQL database table and returns it as a Pandas DataFrame.

    Parameters:
    - host (str): MySQL server host address.
    - username (str): MySQL username.
    - password (str): MySQL password.
    - database (str): MySQL database name.
    - table (str): Name of the table from which to download data.

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

        return df

    except mysql.connector.Error as e:
        print(f"Error downloading data from MySQL: {e}")
        return None
#%%
# Example usage:
if __name__ == "__main__":
    # Example parameters
    host = "localhost"
    username = "root"
    password = "root"
    database = "example_db"
    table = "sales"

    # Call the function to download data from MySQL
    df = download_from_mysql(host, username, password, database, table)

    # Display the downloaded data
    if df is not None:
        print(f"Downloaded {len(df)} rows from table '{table}'")
        print(df.head())
    else:
        print("Failed to download data.")
# %%
