from cassandra.cluster import Cluster
import json
import psycopg2

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020,8,31),
}

dag = DAG(
    dag_id='digital_orders',
    description='Digital orders at Fortemarket',
    schedule_interval='0 0,12 * * *',
    default_args = default_args,
    tags=['Fortemarket']
    )

def read_cassandra():
    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    cur = conn.cursor()

    cluster = Cluster(["10.103.5.51", "10.103.5.52", "10.103.5.53"], port = 9042)
    session = cluster.connect('darbiz', wait_for_all_pools = True)
    session.default_timeout = 10000
    rows = session.execute('select * from forte_market_digital_orders')
    
    cur.execute('truncate table dar_group.digital_orders')
    
    for row in rows:
        d = json.loads(row.order_data)
        order_id = d['uid']
        cert_amount = d['price']
        order_type = d['order_type']
        is_gift_cert = d['is_gift_cert']
        cert_title = d['cert_title'].strip()
        gift_receiver = d['gift_receiver']['name'].strip()
        gift_receiver_mail = d['gift_receiver']['email'].strip()
        gift_receiver_mobile = d['gift_receiver']['mobile'].strip()
        act_code = d['activation_code'].strip()
        is_paid = d['paid']
        pay_type = d['pay_type'].strip()
        cert_name = d['pay_title'].strip()
        created_on = d['created_on']
        delivery_date = d['delivery_date']
        expiry_date = d['expiring_date'] if 'expiring_date' in d else None
        is_act = d['is_activated'] if 'is_activated' in d else None

        cur.execute ("INSERT INTO dar_group.digital_orders(order_id,order_type,created_dtime,is_gift_cert,cert_title,cert_name,cert_amount,\
                activation_code,is_paid,is_activated,pay_type,gift_receiver,gift_receiver_mail,gift_receiver_mobile,delivery_dtime,expiry_dtime) \
                VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s,%s)", (order_id,order_type,created_on,is_gift_cert,cert_title,cert_name,cert_amount,\
                        act_code,is_paid,is_act,pay_type,gift_receiver,gift_receiver_mail,gift_receiver_mobile,delivery_date,expiry_date))
        conn.commit()

    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id = 'read_cassandra',
    python_callable = read_cassandra,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )
