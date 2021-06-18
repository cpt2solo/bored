"""
### Bored xcom Documentation
Test DAG to retreive information from bored API at 
[here](http://www.boredapi.com) and store it to HDFS
using XCOM
"""

from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['cpt2solo@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'bored_xcom',
    catchup=False,
    default_args=default_args,
    description='Bored xcom DAG',
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(0),
    tags=['bored'],
) as dag:

    dag.doc_md = __doc__

    t1 = BashOperator(
        task_id='get_activity_xcom',
        bash_command='curl -sw "\n" http://www.boredapi.com/api/activity ',
        dag=dag,
    )

    t2 = BashOperator(
        task_id='save_activity_xcom',
        bash_command='echo \'{{ ti.xcom_pull("get_activity_xcom") }}\' >/tmp/q.txt ',
        dag=dag,
    )

    t1 >> t2

