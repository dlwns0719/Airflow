from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from mp_functions import make_gsalign_output, make_real_frequency_output, \
                         make_real_frequency_statistics

default_args = {
    'owner': 'junhyun lee',
    'email': ['jhlee5@seegene.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2022,6,28)
}

with DAG(
        dag_id='Mutation_prediction_ETL_GSAlign', 
        description='prototype',
        schedule_interval=None,
        default_args=default_args,
        tags=['test']
        ) as dag:
    
    make_gsalign_output_task = PythonOperator(
        task_id='Run_GSAlign',
        python_callable=make_gsalign_output,
        op_kwargs={
            'gsalign_location': '/opt/airflow/datadrive/GSAlign',
            'ref_seq': "{{ dag_run.conf['ref_seq'] }}",
            'monthly_split_folder': '{}/monthly_split'.format("{{ dag_run.conf['data_folder'] }}"),
            'real_frequency_folder': '{}/real_frequency'.format("{{ dag_run.conf['data_folder'] }}")
        }
    )
    make_real_frequency_output_task = PythonOperator(
        task_id='Make_Real_Frequency',
        python_callable=make_real_frequency_output,
        op_kwargs={
            'real_frequency_folder': '{}/real_frequency'.format("{{ dag_run.conf['data_folder'] }}")
        }
    )
    make_real_frequency_statistics_task = PythonOperator(
        task_id='Make_Real_Frequency_Statistics',
        python_callable=make_real_frequency_statistics,
        op_kwargs={
            'real_frequency_folder': '{}/real_frequency'.format("{{ dag_run.conf['data_folder'] }}"),
            'monthly_split_folder': '{}/monthly_split'.format("{{ dag_run.conf['data_folder'] }}"),
            'ref_seq': "{{ dag_run.conf['ref_seq'] }}"
        }
    )

    make_gsalign_output_task >> make_real_frequency_output_task >> make_real_frequency_statistics_task