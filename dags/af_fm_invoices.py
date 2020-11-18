import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,8,31),
    'retries': 0,
    'provide_context': True
}

dag = DAG(
    dag_id="fm_invoices",
    default_args=args,
    schedule_interval='0 07 * * *',
    tags=['Fortemarket']
    )

src_conn = PostgresHook(postgres_conn_id='pgConn_inv',).get_conn()
dest_conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
dest_cursor = dest_conn.cursor()

def get_card_data(**kwargs):
    query1 = 'select * from invoices_forte_market.card'

    cur1 = src_conn.cursor()
    cur1.execute(query1)
    dest_cursor.execute('delete from dar_group.invoices')
    while True:
        records = cur1.fetchall()
        if not records:
            break
        execute_values(dest_cursor,
                       "INSERT INTO dar_group.invoices VALUES %s",
                       records)
        dest_conn.commit()
    cur1.close()

def get_cash_data(**kwargs):
    query2 = 'select * from invoices_forte_market.installment'  
    cur2 = src_conn.cursor()
    cur2.execute(query2)
    dest_cursor.execute('delete from dar_group.invoices2')
    while True:
        records = cur2.fetchall()
        if not records:
            break
        execute_values(dest_cursor,
                       "INSERT INTO dar_group.invoices2 VALUES %s",
                       records)
        dest_conn.commit()
    cur2.close()

def get_forte_express_data(**kwargs):
    query3 = 'select * from invoices_forte_market.forte_express'
    cur3 = src_conn.cursor()
    cur3.execute(query3)
    dest_cursor.execute('delete from dar_group.forte_express')
    while True:
        records = cur3.fetchall()
        if not records:
            break
        execute_values(dest_cursor,
                       "INSERT INTO dar_group.forte_express VALUES %s",
                       records)
        dest_conn.commit()

    cur3.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()


t1 = PythonOperator(
    task_id="get_card_data",
    python_callable=get_card_data,
    provide_context=True,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t2 = PythonOperator(
    task_id="get_cash_data",
    python_callable=get_cash_data,
    provide_context=True,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t3 = PythonOperator(
    task_id="get_forte_express_data",
    python_callable=get_forte_express_data,
    provide_context=True,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t1 >> t2 >> t3