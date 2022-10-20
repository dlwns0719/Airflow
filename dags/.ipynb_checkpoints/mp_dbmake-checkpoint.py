import mp_taxlist as mpt
import mp_ncbi as mpm
import mp_dbmethod as dbm
import mp_dbconfig as dbc
import argparse

def ncbi_method(db_name:str, meta_file:str, seq_file:str):
    """Make meta&seq sqlite db
    
    Author:
        smlee

    Args:
        db_name: target db name
        meta_file: target All Nuc meta file
        seq_file: target All Nuc seq file
        
    """
    # Make db
    dbm.create_db(db_name)

    # Make table
    dbm.create_table(db_name, "meta")
    dbm.create_table(db_name, "seq")

    # Input meta
    mpm.prepare_meta(db_name, meta_file)

    # Input seq
    mpm.prepare_seq(db_name, seq_file)

def taxlist_method(db_name:str, table_name:str, tax_file:str):
    """Make meta&seq sqlite db
    
    Author:
        smlee

    Args:
        db_name: target db name
        table_name: target table name
        tax_file: target newtaxdump file
        
    """

    # Make db
    dbm.create_db(db_name)

    # Make table
    dbm.create_table(db_name, table_name)

    # Input taxlist
    mpt.prepare_taxlist(db_name, table_name, tax_file)

def main(mode:str):
    """Main function for dbmake process
    
    Author:
        smlee

    Args:
        mode: meta db making or taxlist db making?
    """

    if mode == "ncbi":
        ncbi_method(dbc.ncbi_db, dbc.meta_file, dbc.seq_file)

    elif mode == "taxlist":
        taxlist_method(dbc.tax_db, "taxlist", dbc.taxdump)
    
    else:
        print("Unrecognized mode: Exiting", flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="blank")
    parser.add_argument('-m', '--mode', default=None, help='function mode, update or parse or init or delete')
    args = parser.parse_args()

    main(args.mode)
