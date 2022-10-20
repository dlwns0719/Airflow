# Sqlite Based Taxlist & Seq Info Database


## Code structure
    - mp_dbconfig.py: contains sqlite database properties for each database and tables
    - mp_dbmethod.py: contains create database, create table and insert method
    - mp_ncbi.py: contains methods to build meta sqlite database
    - mp_taxlist.py: contain methods to build taxlist sqlite database
    - mp_dbmake.py: main function to initialize sqlite database construction

## Database properties

Property for meta
"""
"Accession" : "TEXT PRIMARY KEY",
"SRA_Accession" : "TEXT",
"Submitters" : "TEXT",
"Release_Date" : "NUMERIC",
"Species" : "TEXT",
"Genus" : "TEXT",
"Family" : "TEXT",
"Molecule_type" : "TEXT",
"Length" : "INTEGER",
"Sequence_Type" : "TEXT",
"Nuc_Completeness" : "TEXT",
"Genotype" : "TEXT",
"Segment" : "TEXT",
"Publications" : "TEXT",
"Geo_Location" : "TEXT",
"USA" : "TEXT",
"Host" : "TEXT",
"Isolation_Source" : "TEXT",
"Collection_Date" : "NUMERIC",
"BioSample" : "TEXT",
"GenBank_Title" : "TEXT",
"Taxon_ID" : "INTEGER",
"Lineage" : "TEXT"
"""

Property for seq
"""
"Accession" : "TEXT PRIMARY KEY",
"Sequence": "TEXT"
"""

Property for taxlist
"""
"Taxid" : "NUMERIC PRIMARY KEY",
"Taxlist" : "TEXT"
"""
## Database structure
"""
ncbi_db |-- meta_table 
        |-- seq_table

taxlist_db |-- taxlist_table
"""

## Database usage
Connect to sqlite database following sqlite3 api

'''python
    import sqlite3

    conn = sqlite3.connect("path-to-db")
    cur = conn.cursor()
    query = "SELECT * from Taxlist"
    cur.execute(query)
    result = cur.fetchall()

'''

## Main command
'''bash
    python3 mp_dbmake.py --mode "taxlist"
    python3 mp_dbmake.py --mode "ncbi"
'''