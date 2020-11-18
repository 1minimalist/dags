#!/usr/bin/env python
import aerospike
from aerospike import predicates as p
from datetime import datetime
import sys
import json
import psycopg2

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,8,31),
    'retries': 0,
}

dag = DAG(
    dag_id="dim_merch_tariff",
    default_args=args,
    schedule_interval='0 06 * * *',
    tags=['Fortekassa']
    )

config = {
    'hosts': [('10.100.5.11', 3000),('10.100.5.12', 3000),('10.100.5.13', 3000),('10.100.5.14', 3000),('10.100.5.15', 3000)]
}

def get_aerospike_data():
    try:
        client = aerospike.client(config).connect()
        print(datetime.now(), "Connected to aerospike host:", config['hosts'])
    except:
        print("Failed to connect to the cluster with", config['hosts'])
        sys.exit(1)

    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    cur = conn.cursor()
    cur.execute('delete from dar_group.merch_com')
    conn.commit()
    query = client.query('merchants','contracts')
    records = query.results()
    client.close()
    
    print('Starting insert...')
    
    for x in records:
        id1 = x[2]['merchant_id'] if 'merchant_id' in x[2] else None
    
        if 'tariffs' in x[2]:
            try:
                s = json.loads(str(x[2]['tariffs']).replace("'",'"'))
                if s=={}:
                    id4 = ''
                    id5 = ''
                    id6 = ''
                    id7 = ''
                    id8 = ''
                    id9 = ''
                    id10 = ''
                    id11 = ''
                    id12 = ''
                    id13 = ''
                    id14 = ''
                for i in s:
                    id4 = i
                    id5 = s[i]['FORTE_EXPRESS_0_4'] if 'FORTE_EXPRESS_0_4' in s[i] else None
                    id6 = s[i]['FORTE_EXPRESS_0_12'] if 'FORTE_EXPRESS_0_12' in s[i] else None
                    id7 = s[i]['FORTE_EXPRESS_0_24'] if 'FORTE_EXPRESS_0_24' in s[i] else None
                    id8 = s[i]['FORTE_EXPRESS_18_6'] if 'FORTE_EXPRESS_18_6' in s[i] else None
                    id9 = s[i]['FORTE_EXPRESS_18_12'] if 'FORTE_EXPRESS_18_12' in s[i] else None
                    id10 = s[i]['FORTE_EXPRESS_18_24'] if 'FORTE_EXPRESS_18_24' in s[i] else None
                    id11 = s[i]['CARD_0_0'] if 'CARD_0_0' in s[i] else None                
                    id12 = s[i]['ACQUIRING_0_4'] if 'ACQUIRING_0_4' in s[i] else None
                    id13 = s[i]['ACQUIRING_0_6'] if 'ACQUIRING_0_6' in s[i] else None
                    id14 = s[i]['ACQUIRING_0_12'] if 'ACQUIRING_0_12' in s[i] else None

                    cur.execute ("INSERT INTO dar_group.merch_com(merchant_id,cat_id,FORTE_EXPRESS_0_4,FORTE_EXPRESS_0_12,FORTE_EXPRESS_0_24,FORTE_EXPRESS_18_6, \
                                FORTE_EXPRESS_18_12,FORTE_EXPRESS_18_24,CARD_0_0,ACQUIRING_0_4,ACQUIRING_0_6,ACQUIRING_0_12) \
                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(id1,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14))
                    conn.commit()
                    print('Written portion...')
            except:
                pass

    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="get_aerospike_data",
    python_callable=get_aerospike_data,
    dag=dag,
    email_on_failure=True,
    email = 'dnurtailakov@one.kz'
)