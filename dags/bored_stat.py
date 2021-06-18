"""
### Bored stat Documentation
Test DAG to obtain statistics from json data received from
[here](http://www.boredapi.com) and store it to HDFS
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
    'bored_stat',
    catchup=False,
    default_args=default_args,
    description='Bored stat DAG',
    schedule_interval=timedelta(hours=3),
    start_date=days_ago(0),
    tags=['bored'],
) as dag:

    dag.doc_md = __doc__

    t1 = BashOperator(
        task_id='get_json',
        bash_command='hdfs dfs -cat /user/hduser/bored/bsum-`date +"%F"`*.json > /tmp/bstat.json',
        dag=dag,
    )

    t2 = BashOperator(
        task_id='get_stat',
        bash_command='jq -r ".type" /tmp/bstat.json | sort | uniq -c | sort -nr > /tmp/bstat-`date +"%F"`.txt',
        dag=dag,
    )

    t3 = BashOperator(
        task_id='save_stat',
        bash_command='hdfs dfs -moveFromLocal /tmp/bstat*.txt /user/hduser/bored/',
        dag=dag,
    )

    t1 >> t2 >> t3

