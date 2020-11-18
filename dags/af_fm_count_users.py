from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import psycopg2

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,1,13,0),
}

dag = DAG(
    dag_id='fm_count_users',
    default_args=args,
    schedule_interval='*/30 * * * *',
    tags=['Fortemarket']
)

def count_users():
    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    cur = conn.cursor()
    cur.execute('delete from dar_group.fm_count_users where date_ = current_date')

    cur.execute('insert into dar_group.fm_count_users \
                select brand,id_space,current_date as date_,count(distinct identifier_id) as users \
                from ( select upper(brand) as brand,id_space,state,identifier_id,created::date \
                from dar_group.dar_account where lower(state) like \'%active%\' and lower(brand) = \'forte_market\' \
                and lower(id_space) like \'%mobile%\') as a group by brand, id_space')
    conn.commit()

t1 = PythonOperator(task_id='count_users',python_callable=count_users,dag=dag,\
                    email_on_failure=True,email='dnurtailakov@one.kz')