from datetime import datetime
from pandas import DataFrame

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
