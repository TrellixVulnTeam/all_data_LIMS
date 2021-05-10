from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'cpearson',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['chris.pearson089@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'college_scoreboard',
    default_args=default_args,
    description='DAG to pull College Scoreboard data',
    schedule_interval='@monthly',
)

t1 = BashOperator(
    task_id='college_scoreboard_refresh',
    bash_command='/home/chris/all_data/.venv/bin/python3.9 /home/chris/all_data/el_scorecard_data.py',
    dag=dag)

t1
