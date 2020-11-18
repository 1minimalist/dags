import json
import psycopg2

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values


default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020,11,13),
}

dag = DAG(
    dag_id='fk_report_jobs',
    description='Fortekassa reports',
    schedule_interval='45 6 * * *',
    default_args = default_args,
    tags=['Fortekassa']
    )


conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
conn1 = PostgresHook(postgres_conn_id='pgConn_merch').get_conn()
cur = conn.cursor()
cur1 = conn1.cursor()


def get_unidentified():
    cur1.execute('truncate table merchant.d_report_infs')

    cur.execute('''  
                    SELECT 
                    fk_infs.id, fk_infs.terminal_id,
                    cast(darbazar_merchants.address::json->'regionId' as varchar) as bank_region,
                    darbazar_merchants.legal_name,
                    fk_merchant_requisites.legal_id,
                    fk_merchant_requisites.director_name,
                    cast(darbazar_merchants.address::json->'regionId' as varchar) as regionId, cast(darbazar_merchants.address::json->'street' as varchar) as street, cast(darbazar_merchants.address::json->'streetNum' as varchar) as streetNum, cast(darbazar_merchants.address::json->'aptNum' as varchar) as aptNum,
                    darbazar_merchants.telephone,
                    fk_merchant_requisites.tariff_plan,
                    fk_merchant_requisites.tariff_price,
                    fk_infs.status, fk_merchant_requisites.client_type,
                    fk_merchant_requisites.pos_term_type,
                    fk_infs.created_date,
                    darbazar_merchants.biz_categories,
                    fk_infs.store_id
                    
                    
                    FROM dar_group.fk_infs
                    LEFT JOIN dar_group.darbazar_merchants ON fk_infs.merchant_id = darbazar_merchants.id
                    LEFT JOIN dar_group.fk_merchant_requisites ON fk_infs.merchant_id = fk_merchant_requisites.merchant_id
                    LEFT JOIN dar_group.fk_stock_info  ON fk_infs.merchant_id = fk_stock_info.merchant_id
                    where darbazar_merchants.merchant_type = '{FORTE_KASSA}'
                    group by 
                    fk_infs.id,
                    fk_infs.terminal_id,
                    legal_name,
                    legal_id,
                    director_name,
                    regionid,
                    street,
                    streetnum,
                    aptnum,
                    telephone,
                    tariff_plan,
                    tariff_price,
                    fk_infs.status, 
                    fk_merchant_requisites.pos_term_type,
                    fk_infs.created_date,
                    fk_merchant_requisites.client_type,
                    biz_categories,
                    fk_infs.store_id
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO merchant.d_report_infs VALUES %s",
                       records)
        conn1.commit()


    cur.close()
    cur1.close()
    conn.close()


t1 = PythonOperator(
    task_id = 'get_unidentified',
    python_callable = get_unidentified,
    dag = dag,
    email_on_failure = True,
    email = 'aakhmetov@one.kz'
    )


t1
