# -*- coding: utf-8 -*- 
import aerospike
from aerospike import predicates as p
from datetime import datetime
from datetime import timedelta
import sys
import csv
import string
import json
import struct

import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,1),
    'retries': 0,
}

dag = DAG(
    dag_id="darbazar_m_store",
    default_args=args,
    schedule_interval='0 06 * * *',
    tags=['Fortemarket']
    )

config = {
    'hosts': [('10.100.5.11', 3000),('10.100.5.12', 3000),('10.100.5.13', 3000),('10.100.5.14', 3000),('10.100.5.15', 3000)]
}

def get_aerospike_data():
    # Connect to aerospike
    try:
        client = aerospike.client(config).connect()
        print("Connected to aerospike host:", config['hosts'])
    except:
        print("Failed to connect to the cluster with", config['hosts'])
        sys.exit(1)

    now = datetime.now()
    time = {}
    time[0] = now.replace(hour=20, minute=0, second=0, microsecond=0) - timedelta(days=1)
    time[1] = now.replace(hour=8, minute=0, second=0, microsecond=0)
    time[2] = now.replace(hour=13, minute=0, second=0, microsecond=0)
    time[3] = now.replace(hour=20, minute=0, second=0, microsecond=0)
    time[4] = now.replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)

    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    cur = conn.cursor()

    cur.execute ("delete from dar_group.darbazar_m_store;")

    client = aerospike.client(config).connect()
    query = client.query('merchants', 'store')
    records = query.results()
    client.close()

    b = len(records)
    for x in range(0,b):
        if 'schedule' in records[x][2]:
            ida1 = records[x][2]['merchant_id'] if 'merchant_id' in records[x][2] else None
            ida6 = records[x][2]['brand_name'] if 'brand_name' in records[x][2] else None
            if 'address' in records[x][2]:
                ida7 = records[x][2]['address']['street_num'] if 'street_num' in records[x][2]['address'] else None
                ida8 = records[x][2]['address']['street'] if 'street' in records[x][2]['address'] else None
                ida9 = records[x][2]['address']['region_id'] if 'region_id' in records[x][2]['address'] else None
                ida10 = records[x][2]['address']['apt_num'] if 'apt_num' in records[x][2]['address'] else None
            else:
                None
            d = json.dumps(records[x][2]['schedule'])
            o= json.loads(d)
            if d != '""':
                y = json.loads(o)
                for t in range(0,7):
                    ida2 = y[t]['day'] if 'day' in y[t] else None
                    ida3 = y[t]['isOpen'] if 'isOpen' in y[t] else None
                    ida4 = y[t]['openTime'] if 'openTime' in y[t] else None
                    ida5 = y[t]['closeTime'] if 'closeTime' in y[t] else None
                    cur.execute ("INSERT INTO dar_group.darbazar_m_store(merchant_id,day,isopen,opentime,closetime,brand_name,street_num, \
                                street,region_id,apt_num) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                                (ida1,ida2,ida3,ida4,ida5,ida6,ida7,ida8,ida9,ida10))
            else:
                continue
        else:
            continue

    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="get_aerospike_data",
    python_callable=get_aerospike_data,
    dag=dag,
    email_on_failure=True,
    email = 'dnurtailakov@one.kz'
)