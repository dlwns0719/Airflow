import sqlite3
import mp_dbconfig as dbc

def create_db(db_name):
    """Make sqlite db
    
    Author:
        smlee

    Args:
        db_name: target db name 
        
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.close()
    conn.close()

def create_table(db_name,table_name):
    """Make data table within a sqlite db
    
    Author:
        smlee

    Args:
        db_name: target db name 
        table_name: target table name
        
    """
    if table_name == "meta":
        with open(dbc.meta_file) as f:
            headers = next(f).strip("#").split(",")
        
        sql_cols = ""
        for header in headers:
            header = header.strip("\n")
            sql_cols += f"[{header}] {dbc.meta_property[header]}, "
        sql_cols = sql_cols.strip(", ")

        query = f"CREATE TABLE IF NOT EXISTS {table_name} " \
                f"({sql_cols})"
        
    if table_name == "seq":
        query = f"CREATE TABLE IF NOT EXISTS {table_name} " \
                f"([Accession] {dbc.seq_property['Accession']}, " \
                f"[Sequence] {dbc.seq_property['Sequence']})"
    
    if table_name == "taxlist":
        query = f"CREATE TABLE IF NOT EXISTS {table_name} " \
                f"([Taxid] {dbc.taxlist_property['Taxid']}, " \
                f"[Taxlist] {dbc.taxlist_property['Taxlist']})"

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def bulk_insert(db_name:str, table_name:str, rows:list):
    """Insert information to a data table within a db
    
    Author:
        smlee

    Args:
        db_name: target db name 
        table_name: target table name
        rows: list formatted or tuple formatted information within a list
        
    """
    # Check data type
    assert type(rows) == list
    assert len(rows) > 0

    no_value = len(rows[0])
    value_placeholder = ','.join(["?"]*no_value)

    query = f"INSERT OR IGNORE INTO {table_name} " \
            f"VALUES ({value_placeholder})"

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.executemany(query,rows)
    conn.commit()
    cur.close()
    conn.close()
