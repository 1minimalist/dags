# -*- coding: utf-8 -*-
import psycopg2
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,1),
    'retries': 0,
}

dag = DAG(
    dag_id="fm_count_by_product",
    default_args=args,
    schedule_interval='30 23 * * *',
    tags=['Fortemarket']
    )

def agg_count():
    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    cur = conn.cursor()

    cur.execute('delete from dar_group.agg_fm_count_act_product where date_ = current_date')
    conn.commit()
    word1 = '{darbazar}'
    word2 = 'Архив'
    s = 'insert into dar_group.agg_fm_count_act_product\
        select current_date as date_, c.title as category,\
        count(distinct f.id) as koltovara\
        from dar_group.fm_showcase f\
        join dar_group.darbazar_categories c on substring(f.default_category, 7, 36) = c.id\
        where f.is_visible = \'true\' and f.scope_ = \'{}\' and c.title != \'{}\' \
        group by c.title,f.merchant_id'.format(word1,word2)
    cur.execute(s)
    conn.commit()
    
    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="fm_count_by_product",
    python_callable=agg_count,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)