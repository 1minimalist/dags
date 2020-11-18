import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,10,19,9,0),
    'retries': 0,
    'provide_context': True
}

dag = DAG(
    dag_id="fm_nomen_update",
    default_args=args,
    schedule_interval='*/30 * * * *',
    tags=['Fortemarket']
    )

conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()
cur1 = conn.cursor()

def get_nomen_enabled(**kwargs):
    query = ''' select * from (
            select event_dtime, merchant_id, unnest(enabled) enabled \
            from ( \
            select event_dtime, merchant_id,
            regexp_split_to_array(replace(replace(replace(enabled_products,'{',''),'}',''),'"",', ''),',') as enabled \
            from dar_group.fct_fm_nomen_update) t) t
            where t.enabled!='""' and t.enabled is not null '''
    cur1.execute('delete from dar_group.fct_fm_nomen_enabled')
    conn.commit()
    
    cur.execute(query)
    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO dar_group.fct_fm_nomen_enabled VALUES %s",
                       records)
        conn.commit()

def get_nomen_disabled(**kwargs):
    query = ''' select * from (
            select event_dtime, merchant_id, unnest(disabled) disabled \
            from ( \
            select event_dtime, merchant_id,
            regexp_split_to_array(replace(replace(replace(disabled_products,'{',''),'}',''),'"",', ''),',') as disabled \
            from dar_group.fct_fm_nomen_update) t ) t
            where t.disabled!='""' and t.disabled is not null '''
    cur1.execute('delete from dar_group.fct_fm_nomen_disabled')
    conn.commit()
    
    cur.execute(query)
    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO dar_group.fct_fm_nomen_disabled VALUES %s",
                       records)
        conn.commit()

    cur.close()
    cur1.close()
    conn.close()


t1 = PythonOperator(
    task_id="get_nomen_enabled",
    python_callable=get_nomen_enabled,
    provide_context=True,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t2 = PythonOperator(
    task_id="get_nomen_disabled",
    python_callable=get_nomen_disabled,
    provide_context=True,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t1 >> t2