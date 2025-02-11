import duckdb
from datetime import datetime

def get_attached_dbs(db_con):
    return db_con.sql("SELECT database_name as DB_NAME, path as PATH, type FROM duckdb_databases")

def get_inventory(db_con):
    return db_con.sql("SELECT * from duckdb_tables")

def does_table_exist(db_con, dbname, tablename):
    ex_string = "SELECT COUNT(*) AS c from duckdb_tables WHERE table_name = '" + tablename + "'"
    ex_string = ex_string + " AND database_name = '" + dbname + "'"
    count = db_con.sql(ex_string).df()["c"][0]
    if (count == 1):
        return True
    #else
    return False

def getCurrentTimeForDuck(timezone_included=False):
    if (timezone_included):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S %z')
    #else:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def init_table(con, frame, db, tablename):
    #takes in a frame of string columns VARNAME and TYPE
    #those formats should be duckDB compatible
    exist = does_table_exist(con, db, tablename)
    if (exist == False):
        print("Creating table " + db + "." + tablename)

        tbl_ref = db + "." + tablename
        exstring = "CREATE TABLE IF NOT EXISTS " + tbl_ref + "("
        # Create a comma-delimited list with variable names and types
        exstring = exstring + ', '.join([f"{row['VARNAME']} {row['TYPE']}" for _, row in frame.iterrows()])
        exstring = exstring + ")"
        con.sql(exstring)
        return True
    #else
    return False
