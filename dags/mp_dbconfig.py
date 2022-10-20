import datetime

# [DB output path]
db_path = "/opt/airflow/datadrive/db/"

# [DB name setup]
now = datetime.datetime.now()
ncbi_db = f"{db_path}/ncbi_{now.year}-{now.month}-{now.day}.db"
tax_db = f"{db_path}/tax_{now.year}-{now.month}-{now.day}.db"


# [Target meta information and seq information path]
meta_file = "/opt/airflow/datadrive/AllNuclMetadata_withTax.csv"
seq_file = "/opt/airflow/datadrive/Data_4.fasta"
taxdump = "/opt/airflow/datadrive/db/taxdump/taxdump.tar.gz"

# [Table information]
meta_property = {
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
}

seq_property = {
    "Accession" : "TEXT PRIMARY KEY",
    "Sequence": "TEXT"
}

taxlist_property = {
    "Taxid" : "NUMERIC PRIMARY KEY",
    "Taxlist" : "TEXT"
}
