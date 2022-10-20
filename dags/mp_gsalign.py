import subprocess
import os
import pandas as pd
from Bio import SeqIO


class monthly_mutation_frequency:
    def __init__(self, number_of_alts, A_freq_list, T_freq_list, 
                 C_freq_list, G_freq_list, in_freq_list, del_freq_list,
                 total_freq_list):
        self.number_of_alts = number_of_alts
        self.A_freq_list = A_freq_list
        self.T_freq_list = T_freq_list
        self.C_freq_list = C_freq_list
        self.G_freq_list = G_freq_list
        self.in_freq_list = in_freq_list
        self.del_freq_list = del_freq_list
        self.total_freq_list = total_freq_list


def run_gsalign(gsalign_location:str, thread_num:str, 
                ref_seq:str, mut_seq:str, output:str):
    """Run GSAlign using subprocess module
    
    Author:
        jhlee5

    Args:
        gsalign_location : gsalign binary file path
        thread_num : GSAlign option 
        ref_seq : reference sequence file path
        mut_seq : alt sequence fasta file path
        output : output folder path
        
        
    """
    args = ("{}/bin/GSAlign".format(gsalign_location), 
            "-t", thread_num, "-i", ref_seq, 
            "-q", mut_seq, "-o", output, "-slen", "10")
    popen = subprocess.Popen(args, stdout=subprocess.PIPE)
    popen.wait()


def make_real_frequency_file(input_vcf:str, output_mc:str):
    """Make mutation count file using awk
    
    Author:
        jhlee5

    Args:
        input_vcf : input vcf file path
        output_mc : output mc file path 
        
        
    """
    cmd = "awk '{print $1,$2,$4,$5}' " + input_vcf + \
          " | sort | uniq -c | sort -nk 3 > " + output_mc
    os.system(cmd)


def make_real_frequency_dataframe(mc_file:str, fasta_file:str, ref_seq_file:str) \
                                  -> pd.core.frame.DataFrame:
    """Make real frequency output dataframe
    
    Author:
        jhlee5

    Args:
        mc_file : input mc file path 
        fasta_file : input alt sequence fasta file
        ref_seq_file : input reference sequence fasta file
    Returns:
        Result pandas data frame
        
    """
    tmp = SeqIO.index(ref_seq_file, format='fasta')
    ref_acc = list(tmp.keys())[0]
    reference_seq = str(tmp[ref_acc].seq)
    with open(mc_file, 'r') as f:
        mutation_count = f.readlines()
    for _ in range(6):
        mutation_count.pop(0)
    ref_length = len(reference_seq)
    A_freq_list = [0 for _ in range(ref_length)]
    T_freq_list = [0 for _ in range(ref_length)]
    C_freq_list = [0 for _ in range(ref_length)]
    G_freq_list = [0 for _ in range(ref_length)]
    in_freq_list = [0 for _ in range(ref_length)]
    del_freq_list = [0 for _ in range(ref_length)]
    for single_mut in mutation_count:
        tmp = single_mut.split()
        freq = int(tmp[0])
        position = int(tmp[2])
        ref_nt = tmp[3]
        alt_nt = tmp[4]
        if len(alt_nt) > 1: # in case of insertion
            in_freq_list[position-1] += freq
        elif len(ref_nt) > 1: # in case of deletion
            del_freq_list[position-1] += freq
        elif alt_nt == 'A':
            A_freq_list[position-1] += freq
        elif alt_nt == 'T':
            T_freq_list[position-1] += freq
        elif alt_nt == 'C':
            C_freq_list[position-1] += freq
        elif alt_nt == 'G':
            G_freq_list[position-1] += freq

    total_freq_list = [sum(x) for x in zip(A_freq_list, T_freq_list, 
                                           C_freq_list, G_freq_list, 
                                           in_freq_list, del_freq_list)]
    tmp = SeqIO.parse(fasta_file, format='fasta')
    number_of_alts = len(list(tmp))
    for position, nt in enumerate(reference_seq):
        tot_ = A_freq_list[position] + T_freq_list[position] + \
               C_freq_list[position] + G_freq_list[position] + \
               in_freq_list[position] + del_freq_list[position]
        not_mut_count = number_of_alts - tot_
        if nt == 'A':
            A_freq_list[position] = not_mut_count
        elif nt == 'T':
            T_freq_list[position] = not_mut_count
        elif nt == 'C':
            C_freq_list[position] = not_mut_count
        elif nt == 'G':
            G_freq_list[position] = not_mut_count

    mmf = monthly_mutation_frequency(number_of_alts, A_freq_list, 
                                     T_freq_list, C_freq_list,
                                     G_freq_list, in_freq_list, 
                                     del_freq_list, total_freq_list)
    
    tot = mmf.total_freq_list
    a = mmf.A_freq_list
    t = mmf.T_freq_list
    c = mmf.C_freq_list
    g = mmf.G_freq_list
    insertion = mmf.in_freq_list
    deletion = mmf.del_freq_list

    return pd.DataFrame({'Position':list(range(1,ref_length+1)), 'Frequency':tot, 
                         'A':a, 'T':t, 'C':c, 'G':g, "Insertion":insertion, 
                         "Deletion":deletion, "Reference":list(reference_seq)})