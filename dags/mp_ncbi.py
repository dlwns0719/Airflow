import mp_dbmethod as dbm
from Bio import SeqIO

def selective_split(line:str) -> list:
    """Split comma separated text file with special case
    
    Author:
        smlee

    Args:
        line: one line in a file

    Returns:
        splitted row within a list
    
    """
    mode = True
    prev = 0
    res = list()
    
    for index in range(0,len(line)):
        if line[index] == ',' and mode == True:
            res.append(line[prev:index].strip("\n"))
            prev = index+1
        if line[index] == '"' and mode == True:
            mode = False
            continue
        if line[index] == '"' and mode == False:
            mode = True
            continue
    res.append(line[prev:].strip("\n"))

    return res

def prepare_metainfo(meta_file:str):
    """Prepare meta information from meta info file
    within a list
    
    Author:
        smlee

    Args:
        meta_file: All Nucleotide meta info file

    Returns:
        rows from meta information within a list
    
    """

    with open(meta_file) as f:
        headers = next(f).strip("#").split(",")
        
        rows = list()
        
        for line in f:
            row = selective_split(line)
            try:
                assert len(row) == 23
                rows.append(row)
            except Exception as e:
                print(f"Error - {line} : {e}")

        return rows

def prepare_meta(db_name:str, meta_file:str):
    """Insert meta info to a db
    
    Author:
        smlee

    Args:
        db_name: target db_name
        meta_file: All Nucleotide meta info file

    
    """
    rows = prepare_metainfo(meta_file)
    dbm.bulk_insert(f"{db_name}","meta",rows)

def prepare_seq(db_name:str, seq_file:str):
    """Insert seq info to a db
    
    Author:
        smlee

    Args:
        db_name: target db_name
        seq_file: All Nucleotide seq file

    
    """
    rows = list()
    checker = 0
    with open(seq_file) as f:
        for record in SeqIO.parse(f):
            rows.append( (record.id, str(record.seq)) )
            checker += len(str(record.seq))

            if checker > 1e9:
                dbm.bulk_insert(f"{db_name}.db","seq",rows)
                rows = list()
                checker = 0
    dbm.bulk_insert(f"{db_name}","seq",rows)

