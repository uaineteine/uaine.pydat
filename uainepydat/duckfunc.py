import os
import sys
from datetime import datetime
from pandas import DataFrame
from uainepydat import dataio

# Function to get the list of attached databases
def get_attached_dbs(db_con) -> DataFrame:
    """
    Get the list of attached databases.
    
    Args:
        db_con: The database connection object.

    Returns:
        DataFrame: A DataFrame containing the database name, path, and type.
    """
    return db_con.sql("SELECT database_name as DB_NAME, path as PATH, type FROM duckdb_databases")

# Function to get the inventory of tables
def get_inventory(db_con) -> DataFrame:
    """
    Get the inventory of tables.
    
    Args:
        db_con: The database connection object.

    Returns:
        DataFrame: A DataFrame containing all tables.
    """
    return db_con.sql("SELECT * from duckdb_tables")

# Function to check if a table exists
def does_table_exist(db_con, dbname: str, tablename: str) -> bool:
    """
    Check if a table exists in the specified database.
    
    Args:
        db_con: The database connection object.
        dbname (str): The name of the database.
        tablename (str): The name of the table.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    ex_string = f"SELECT COUNT(*) AS c from duckdb_tables WHERE table_name = '{tablename}'"
    ex_string += f" AND database_name = '{dbname}'"
    count = db_con.sql(ex_string).df()["c"][0]
    if count == 1:
        return True
    # else
    return False

def getCurrentTimeForDuck(timezone_included: bool = False) -> str:
    """
    Get the current time formatted for DuckDB, optionally including the timezone.
    
    Args:
        timezone_included (bool): If True, includes the timezone in the returned string.
    
    Returns:
        str: The current time formatted as 'YYYY-MM-DD HH:MM:SS' (with optional timezone).
    """
    if timezone_included:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S %z')
    # else:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def init_table(con, frame: 'DataFrame', db: str, tablename: str) -> bool:
    """
    Initialize a table in the specified database.
    
    Args:
        con: The database connection object.
        frame (DataFrame): A DataFrame containing columns VARNAME and TYPE, which should be DuckDB-compatible.
        db (str): The name of the database.
        tablename (str): The name of the table.
    
    Returns:
        bool: True if the table was created, False if it already exists.
    """
    # Check if the table exists
    exist = does_table_exist(con, db, tablename)
    if not exist:
        print("Creating table " + db + "." + tablename)

        tbl_ref = db + "." + tablename
        exstring = "CREATE TABLE IF NOT EXISTS " + tbl_ref + "("
        # Create a comma-delimited list with variable names and types
        exstring += ', '.join([f"{row['VARNAME']} {row['TYPE']}" for _, row in frame.iterrows()])
        exstring += ")"
        # Execute the SQL command to create the table
        con.sql(exstring)
        return True
    # else
    return False

def getDuckVersion(con) -> str:
    """
    Get the connected DuckDB version.
    
    Args:
        con: The database connection object.
    
    Returns:
        str: The version of the DuckDB you have open.
    """
    df = con.sql("SELECT version() AS version").df()
    return df['version'][0]

def get_table_as_df(con, db_name: str, table_name: str) -> DataFrame:
    """
    Query a table from the specified database and return it as a pandas DataFrame.
    
    Args:
        con: Database connection object
        db_name (str): Name of the database
        table_name (str): Name of the table
        
    Returns:
        DataFrame: The table contents as a pandas DataFrame, or None if the table doesn't exist
    """
    # Check if the table exists
    if not does_table_exist(con, db_name, table_name):
        print(f"Table {db_name}.{table_name} does not exist.")
        return None
    
    # Query the table
    query = f"SELECT * FROM {db_name}.{table_name}"
    return con.sql(query).df()

def save_from_db(con, db_name: str, table_name: str, output_path: str) -> bool:
    """
    Query a table from the specified database and save it to the given output path.
    The output format is determined from the file extension of the output path.

    Args:
        con: Database connection object
        db_name (str): Name of the database
        table_name (str): Name of the table
        output_path (str): Path to save the output file (extension determines format)
        
    Returns:
        bool: True if the table existed and was saved, False otherwise
    """
    # Get the table as a DataFrame
    df = get_table_as_df(con, db_name, table_name)
    
    if df is None:
        print(f"Skipping export of {db_name}.{table_name}.")
        return False

    # Determine format from output_path extension and use dataio to write file
    dataio.write_flat_df(df, output_path)
    
    print(f"Dumped {db_name}.{table_name} to {output_path}")
    return True

def load_csv_to_db(con, tablename:str, csvpath:str):
    """
    Load a csv file directly to a table
    """
    if not os.path.exists(csvpath):
        raise FileNotFoundError(f"CSV file {csvpath} does not exist.")
    
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {tablename} AS SELECT * FROM read_csv_auto('{csvpath}')
    """)

def make_version_meta_table(con, schema_version:str, db_name:str):
        """
        Create or update a meta table including the following content:
        
        * database version
        * python version
        * duckdb version
        """
        duckdbver = getDuckVersion(con)
        pyver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        con.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        con.execute("DELETE FROM meta")
        meta_entries = [
            (f"{db_name}_version", schema_version),
            ("duckdb_version", duckdbver),
            ("python_version", pyver)
        ]
        con.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta_entries)
