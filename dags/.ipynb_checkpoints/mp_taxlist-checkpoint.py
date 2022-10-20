import tarfile
import mp_dbmethod as dbm

def extract_node(filepath:str) -> list:
    """Extract node lines from new taxdump tar gz
    
    Author:
        smlee

    Args:
        filepath: path to taxdump tar file

    Returns:
        rows from nodes.dmp within a list
    
    """
    with tarfile.open(filepath, "r:gz") as ntdump:

        # Peek
        for filename in ntdump.getnames():
            if filename.split(".")[0] == "nodes":
                fn = ntdump.extractfile(filename)
                data = fn.read().decode('utf-8')
                lines = data.split("\n")

    nodes = list()

    for line in lines:
        row = [ele.strip() if ele != "" else None for ele in line.split("|")]
        try:
            nodes.append( (row[0], row[1]) )
        except:
            continue

    return nodes

def nested_tree(relationship: list) -> dict:
    """Make nested dictionary
    
    Author:
        smlee

    Args:
        relationship: (child, parent) tuple within a list
    
    Returns:
        nested tree dictionary of taxids: {1: {2: ... }}

    """

    tree = { node: dict() for link in relationship for node in link }
    for child, parent in relationship:
        tree[parent].update( {child:tree[child]} )

    return tree

def recursive_node(tree_member: set, tree: dict):
    """Find all taxid from the tree
    
    Author:
        smlee

    Args:
        tree_member: a empty set to save this relationship
        tree: nested dictionary contain taxid info

    Returns:
    """

    for node, leaf in tree.items():
        tree_member.add(node)
        tree_member |= set(leaf.keys())

        if type(leaf) is dict:
            recursive_node(tree_member, leaf)

def prepare_lowtax(filepath:str):
    """Find all associated taxids to a taxid
    
    Author:
        smlee

    Args:
        filepath: path to newtaxdump file

    Returns:
    """
    txid_rel = extract_node(filepath)
    ntree = nested_tree(txid_rel)

    # Remove 1 as it breaks recursion
    ntree.pop("1")

    # Initiate txid list and count
    taxlist = list()

    for node in ntree.keys():

        tree_member = set()

        recursive_node(tree_member, ntree[node])
        tree_member.add(node)

        taxlist.append( (node, ','.join(map(str,list(tree_member)))) )
    
    return taxlist

def prepare_taxlist(db_name:str, table_name:str, tax_file:str):
    """Prepare low tax info and insert to db
    
    Author:
        smlee

    Args:
        db_name: target sqlite db name
        table_name: target table name
        tax_file: new taxdump file in tar.gz format

    """
    # Prepare lowtax
    tax_info = prepare_lowtax(tax_file)
    # Insert
    dbm.bulk_insert(f"{db_name}",table_name,tax_info)