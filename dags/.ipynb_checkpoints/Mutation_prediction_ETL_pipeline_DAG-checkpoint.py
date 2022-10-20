from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from mp_functions import make_folder_list, taxid_allocation_function, \
                         make_data_function, monthly_split
from mp_functions import make_taxon_db, make_dataset_label_to_meta, \
                         make_gsalign_output, make_real_frequency_output, \
                         make_real_frequency_statistics

default_args = {
    'owner': 'junhyun lee',
    'email': ['jhlee5@seegene.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2022,6,28)
}

with DAG(
        dag_id='Mutation_prediction_ETL_pipeline_DAG', 
        description='prototype',
        schedule_interval=None,
        default_args=default_args,
        tags=['test']
        ) as dag:
    
    
    make_folder = PythonOperator(
        task_id='Make_folder',
        python_callable=make_folder_list
    )
    
    
    download_all_fasta = BashOperator(
        task_id='Extract_AllNucleotide',
        bash_command='wget -O {}/AllNucleotide.fa https://ftp.ncbi.nlm.nih.gov/genomes/Viruses/AllNucleotide/AllNucleotide.fa'.format(make_folder.output),
        dag=dag)
    download_all_meta = BashOperator(
        task_id='Extract_AllNuclMeta',
        bash_command='wget -O {}/AllNuclMetadata.csv.gz https://ftp.ncbi.nlm.nih.gov/genomes/Viruses/AllNuclMetadata/AllNuclMetadata.csv.gz && gzip -d -f {}/AllNuclMetadata.csv.gz'.format(make_folder.output, make_folder.output),
        dag=dag)
    download_gb_taxid = BashOperator(
        task_id='Extract_taxid_1',
        bash_command='wget -O {}/nucl_gb.accession2taxid.gz https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz'.format(make_folder.output),
        dag=dag)
    download_wgs_taxid = BashOperator(
        task_id='Extract_taxid_2',
        bash_command='wget -O {}/nucl_wgs.accession2taxid.gz https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/accession2taxid/nucl_wgs.accession2taxid.gz'.format(make_folder.output),
        dag=dag)
    
    download_taxdump = BashOperator(
        task_id='Extract_taxdump_data',
        bash_command="wget -O $AIRFLOW_HOME/datadrive/db/taxdump/taxdump.tar.gz ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz && tar -zxvf $AIRFLOW_HOME/datadrive/db/taxdump/taxdump.tar.gz -C $AIRFLOW_HOME/datadrive/db/taxdump/",
        dag=dag)
    
       
    taxid_allocation = PythonOperator(
        task_id='Transform_taxid_allocation',
        python_callable=taxid_allocation_function,
        op_kwargs={
            'data_folder_path': make_folder.output
        }
    )
    
    
    make_taxon_db = PythonOperator(
        task_id='Make_taxon_DB',
        python_callable=make_taxon_db,
    )
    
    make_data_label = PythonOperator(
        task_id='Make_data_Label',
        python_callable=make_dataset_label_to_meta,
        op_kwargs={
            'data_folder_path': make_folder.output,
            'db_file_name' : make_taxon_db.output
        }
    )
    
    
    
    for i in range(1,5):
        make_data_task = PythonOperator(
            task_id='Make_data_args_' + str(i),
            python_callable=make_data_function,
            op_kwargs={
                'data_folder_path': make_folder.output,
                'data_name': 'Data_{}'.format(i),
            }
        )
        monthly_split_task = PythonOperator(
            task_id='Monthly_split_' + str(i),
            python_callable=monthly_split,
            op_kwargs={
                'data_folder_path': make_folder.output,
                'data_name': 'Data_{}'.format(i)
            }
        )

        
        [download_all_fasta, make_data_label] >> make_data_task >> monthly_split_task 

    make_folder >> [download_all_meta, download_gb_taxid, download_wgs_taxid, download_all_fasta]
    [download_all_meta, download_gb_taxid, download_wgs_taxid] >> taxid_allocation 
    download_taxdump >> make_taxon_db
    [taxid_allocation, make_taxon_db] >> make_data_label