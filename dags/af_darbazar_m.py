# -*- coding: utf-8 -*- 
import aerospike
from aerospike import predicates as p
from datetime import datetime
from datetime import timedelta
import sys
import csv
import json
import struct

import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,1),
    'retries': 0,
}

dag = DAG(
    dag_id="darbazar_merchants",
    default_args=args,
    schedule_interval='0 06 * * *',
    tags=['Fortemarket']
    )

config = {
    'hosts': [('10.100.5.11', 3000),('10.100.5.12', 3000),('10.100.5.13', 3000),('10.100.5.14', 3000),('10.100.5.15', 3000)]
}

def read_aerospike():
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

    client = aerospike.client(config).connect()
    query = client.query('merchants', 'merchant')
    query.select('id', 'telephone', 'email','brand','old_id_code','merchant_id','state', 'merchant_type', 'code','description','legal_name','logal_token','surl','smap','avatar_token','banner_token','contacts','created_date','updated_date','biz_categories','address','kassa_types','website')
    records = query.results()
    client.close()

    b = len(records)

    cur.execute ("delete from dar_group.darbazar_merchants")
    conn.commit()

    for x in range(0,b):
        id = records[x][2]['id']
        id1 = records[x][2]['telephone'] if 'telephone' in records[x][2] else None
        id2 = records[x][2]['email'] if 'email' in records[x][2] else None
        id3 = records[x][2]['brand'] if 'brand' in records[x][2] else None
        id4 = records[x][2]['old_id_code'] if 'old_id_code' in records[x][2] else None
        id5 = records[x][2]['merchant_id'] if 'merchant_id' in records[x][2] else None
        id6 = records[x][2]['state'] if 'state' in records[x][2] else None
        ida7 = records[x][2]['merchant_type'] if 'merchant_type' in records[x][2] else None
        id7 = records[x][2]['code'] if 'code' in records[x][2] else None
        id8 = records[x][2]['description'] if 'description' in records[x][2] else None
        id9 = records[x][2]['legal_name'] if 'legal_name' in records[x][2] else None
        id10 = records[x][2]['logo_token'] if 'logo_token' in records[x][2] else None
        id11 = records[x][2]['surl'] if 'surl' in records[x][2] else None
        id12 = records[x][2]['smap'] if 'smap' in records[x][2] else None
        id13 = records[x][2]['avatar_token'] if 'avatar_token' in records[x][2] else None
        id14 = records[x][2]['banner_token'] if 'banner_token' in records[x][2] else None
        id15 = records[x][2]['contacts'] if 'contacts' in records[x][2] else None
        ids1 = records[x][2]['created_date'] if 'created_date' in records[x][2] else 0
        id16 = datetime.fromtimestamp(int(ids1)/1000).strftime('%Y-%m-%d %H:%M:%S')
        ids2 = records[x][2]['updated_date'] if 'updated_date' in records[x][2] else 0
        id17 = datetime.fromtimestamp(int(ids2)/1000).strftime('%Y-%m-%d %H:%M:%S')
        id18 = records[x][2]['biz_categories'] if 'biz_categories' in records[x][2] else None
        id19 = records[x][2]['address'] if 'address' in records[x][2] else None
        id20 = records[x][2]['kassa_types'] if 'kassa_types' in records[x][2] else None
        id21 = records[x][2]['website'] if 'website' in records[x][2] else None
        cur.execute ("INSERT INTO dar_group.darbazar_merchants(id,telephone,email,brand,old_id_code,merchant_id,state, merchant_type, \
                    code,description,legal_name,logo_token,surl,smap,avatar_token,banner_token,contacts,created_date, \
                    updated_date,biz_categories,address,kassa_types,website) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
                    (id,id1,id2,id3,id4,id5,id6,ida7,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19,id20,id21))
        conn.commit()
    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="read_aerospike",
    python_callable=read_aerospike,
    dag=dag,
    email_on_failure=True,
    email = 'dnurtailakov@one.kz'
)