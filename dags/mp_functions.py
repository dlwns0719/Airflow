def make_folder_list() -> str:
    """Make current time data folder
    
    Author:
        jhlee5

    Returns:
        data_folder_path : data folder path
        
        
    """
    import os
    from datetime import datetime
    current_time = datetime.today().strftime('%Y-%m-%d')
    data_folder_with_time = os.path.join("/opt/airflow/datadrive", current_time)
    if not os.path.exists(data_folder_with_time):
        os.mkdir(data_folder_with_time)
        for i in range(1,5):
            data_folder_with_time_num = os.path.join(data_folder_with_time, 
                                                     "Data_{}".format(i))
            data_folder_with_time_num_rf = os.path.join(data_folder_with_time_num, 
                                                        "real_frequency")
            data_folder_with_time_num_ms = os.path.join(data_folder_with_time_num, 
                                                        "monthly_split")
            os.mkdir(data_folder_with_time_num)
            os.mkdir(data_folder_with_time_num_rf)
            os.mkdir(data_folder_with_time_num_ms)
    return data_folder_with_time


def taxid_allocation_function(data_folder_path:str):
    """Taxon id allocation to meta data
        
    Author:
        jhlee5
    
    Args:
        data_folder_path : data folder path
    
    
    """
    import pandas as pd
    import os
    import gzip
    allnucl_meta_data_path = os.path.join(data_folder_path, 'AllNuclMetadata.csv')
    allnucl_meta_data_df = pd.read_csv(allnucl_meta_data_path, low_memory=False)
    allnucl_acc_set = set(allnucl_meta_data_df['#Accession'].values)
    allnucl_acc_to_taxid_dict = dict()

    with gzip.open(os.path.join(data_folder_path, 'nucl_gb.accession2taxid.gz'), 'rt') as db:
        db.readline()
        while db:
            line = db.readline()
            if not line:
                break
            line_slpit = line.strip().split('\t')
            accid = line_slpit[0]
            accid_v = line_slpit[1]
            if accid in allnucl_acc_set or accid_v in allnucl_acc_set:
                if accid in allnucl_acc_set:
                    allnucl_acc_to_taxid_dict[accid] = line_slpit[2]
                elif accid_v in allnucl_acc_set:
                    allnucl_acc_to_taxid_dict[accid_v] = line_slpit[2]

    with gzip.open(os.path.join(data_folder_path, 'nucl_wgs.accession2taxid.gz'), 'rt') as db:
        db.readline()
        while db:
            line = db.readline()
            if not line:
                break
            line_slpit = line.strip().split('\t')
            accid = line_slpit[0]
            accid_v = line_slpit[1]
            if accid in allnucl_acc_set or accid_v in allnucl_acc_set:
                if accid in allnucl_acc_set:
                    allnucl_acc_to_taxid_dict[accid] = line_slpit[2]
                elif accid_v in allnucl_acc_set:
                    allnucl_acc_to_taxid_dict[accid_v] = line_slpit[2]
    
    taxid_list = list()
    for a in allnucl_meta_data_df['#Accession'].values:
        if a in allnucl_acc_to_taxid_dict:
            taxid_list.append(allnucl_acc_to_taxid_dict[a])
        else:
            taxid_list.append(' ')
    
    allnucl_meta_data_df['Taxon_ID'] = taxid_list
    allnucl_meta_data_df.to_csv(allnucl_meta_data_path, index=False)


def make_data_function(data_folder_path:str, data_name:str):
    """Make fasta file from AllNucleotide.fa with data lable in metadata  
        
    Author:
        jhlee5
    
    Args:
        data_folder_path : data folder path
        data_name : data name 
                    ex) Data_1
    
    
    """
    import os
    from Bio import SeqIO
    import pandas as pd
    allnucl_fasta_path = os.path.join(data_folder_path, 'AllNucleotide.fa')
    allnucl_meta_data_path = os.path.join(data_folder_path, 'AllNuclMetadata.csv')
    allnucl_fasta = SeqIO.index(allnucl_fasta_path, format='fasta')
    allnucl_meta_data_df = pd.read_csv(allnucl_meta_data_path, low_memory=False)
    meta_splited = allnucl_meta_data_df[allnucl_meta_data_df[data_name]==True]
    meta_splited.to_csv(os.path.join(data_folder_path, '{}/{}_Metadata.csv'\
                                     .format(data_name,data_name)), index=False)

    with open(os.path.join(data_folder_path, "{}/{}.fasta".format(data_name, data_name)), 'w') as f:
        for acc in meta_splited['#Accession']:
            try:
                seq = allnucl_fasta[acc].seq
            except:
                continue
            f.write(">{}\n".format(acc))
            f.write("{}\n".format(seq))


def monthly_split(data_folder_path:str, data_name:str):
    """Split fasta file to monthly fasta file with collection date in metadata
        
    Author:
        jhlee5
    
    Args:
        data_folder_path : data folder path
        data_name : data name 
                    ex) Data_1
    
    
    """
    import pandas as pd
    from Bio import SeqIO
    import os
    meta = pd.read_csv(os.path.join(data_folder_path, "{}/{}_Metadata.csv"\
                                    .format(data_name,data_name)))
    meta = meta[meta['Collection_Date'].notna()]
    acc_date_dict = dict()
    for index, x in meta.iterrows():
        acc_date_dict[x["#Accession"]] = x["Collection_Date"]
    all_seq = SeqIO.parse(os.path.join(data_folder_path, "{}/{}.fasta"\
                                       .format(data_name,data_name)),"fasta")

    for one_virus in all_seq:
        acc_id = one_virus.id
        if acc_date_dict.get(acc_id):
            date = acc_date_dict[acc_id] # date
            year_month_day = date.split('-')
            sequence = str(one_virus.seq) # sequence
            # date is only year or sequence have "N" pass
            if len(year_month_day) == 1 or 'N' in sequence: 
                continue
            file_name = ''.join(year_month_day[:2])
            with open(os.path.join(data_folder_path, "{}/monthly_split/{}_{}.fasta"\
                                   .format(data_name, file_name, data_name)), 'a') as f:
                f.write(">{}\n".format(acc_id, date))
                f.write("{}\n".format(sequence))


def make_taxon_db()->str:
    """Make taxon database file 
        
    Author:
        jhlee5
    
    Returns:
        dbc.tax_db : taxon database file path
    
    
    """
    import mp_dbmake as dbm
    import mp_dbconfig as dbc
    dbm.taxlist_method(dbc.tax_db, "taxlist", dbc.taxdump)
    return dbc.tax_db


def make_dataset_label_to_meta(data_folder_path:str, db_file_name:str):
    """Make dataset label column in metadata
        
    Author:
        jhlee5
    
    Args:
        data_folder_path : data folder path
        db_file_name : taxon database file path
                   
    
    """
    import os
    import sqlite3
    import pandas as pd
    
    data_to_taxid = {
        'Data_1':'2697049',
        'Data_2':'11118',
        'Data_3':'2559587',
        'Data_4':'10239'
    }
    
    db_folder = "/opt/airflow/datadrive/db"
    db_file = os.path.join(db_folder, db_file_name)
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    
    allnucl_meta_data_path = os.path.join(data_folder_path, 'AllNuclMetadata.csv')
    allnucl_meta_data_df = pd.read_csv(allnucl_meta_data_path, low_memory=False)
    
    data_set_taxid_dict = dict()
    
    for k, v in data_to_taxid.items():
        query = "SELECT * FROM Taxlist where Taxid={}".format(v)
        cur.execute(query)
        result = cur.fetchall()
        data_set_taxid_dict[k] = set(result[0][1].split(','))
    
    data_set_label = list()
    
    for t in allnucl_meta_data_df['Taxon_ID'].values:
        data_set_label.append([ t in v for _, v in data_set_taxid_dict.items() ])
    
    data_set_label = list(map(list, zip(*data_set_label)))
    
    allnucl_meta_data_df['Data_1'] = data_set_label[0]
    allnucl_meta_data_df['Data_2'] = data_set_label[1]
    allnucl_meta_data_df['Data_3'] = data_set_label[2]
    allnucl_meta_data_df['Data_4'] = data_set_label[3]
    allnucl_meta_data_df.to_csv(allnucl_meta_data_path, index=False)


def make_gsalign_output(gsalign_location:str, ref_seq:str, 
                        monthly_split_folder:str, real_frequency_folder:str):
    """Run GSAlign 
        
    Author:
        jhlee5
    
    Args:
        gsalign_location : gsalign binary file path
        ref_seq : reference sequence file path
        monthly_split_folder : monthly splited fasta file folder path
        real_frequency_folder : output folder path
    
    
    """
    from mp_gsalign import run_gsalign
    import os
    
    thread_num = '6'
    for alt in [x for x in os.listdir(monthly_split_folder) if 'fasta' in x]:
        input_alt= os.path.join(monthly_split_folder, alt)
        output_maf_vcf = os.path.join(real_frequency_folder, alt.split('.')[0])
        run_gsalign(gsalign_location, thread_num, 
                    ref_seq, input_alt, output_maf_vcf)


def make_real_frequency_output(real_frequency_folder:str):
    """Make mutation count file(.mc file) 
        
    Author:
        jhlee5
    
    Args:
        real_frequency_folder : output folder path
                   
    
    """
    from mp_gsalign import make_real_frequency_file
    import os
    
    for vcf in [x for x in os.listdir(real_frequency_folder) if 'vcf' in x]:
        input_vcf = os.path.join(real_frequency_folder, vcf)
        output_mc = os.path.join(real_frequency_folder, "{}.mc".format(vcf.split('.')[0]))
        make_real_frequency_file(input_vcf, output_mc)


def make_real_frequency_statistics(ref_seq:str, 
                                   monthly_split_folder:str, 
                                   real_frequency_folder:str):
    """Make mutation result CSV file 
        
    Author:
        jhlee5
    
    Args:
        ref_seq : reference sequence file path
        monthly_split_folder : monthly splited fasta file folder path
        real_frequency_folder : output folder path
                   
    
    """
    from mp_gsalign import make_real_frequency_dataframe
    import os
    
    mc_file_list = [x for x in os.listdir(real_frequency_folder) if 'mc' in x]
    fasta_file_list = [x for x in os.listdir(monthly_split_folder) if 'fasta' in x]
    ref_seq_file = "{}.fasta".format(ref_seq)
    
    for mc, fas in zip(mc_file_list, fasta_file_list):
        input_mc = os.path.join(real_frequency_folder, mc)
        input_fas = os.path.join(monthly_split_folder, fas)
        result = make_real_frequency_dataframe(input_mc, input_fas, ref_seq_file)
        result_file = "{}.csv".format(input_mc.split('.')[0])
        result.to_csv(result_file, index=False)
            