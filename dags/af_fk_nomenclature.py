#! /usr/bin/env python
# -*- coding: utf-8 -*-
import elasticsearch
import psycopg2
import json
import requests
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,10,20),
}

dag = DAG(
    dag_id="fk_nomenclature",
    default_args=args,
    schedule_interval='0 7,22 * * *',
    tags=['Fortemarket']
    )

def read_elastic():
    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    conn1 = PostgresHook(postgres_conn_id='pgConn_merch').get_conn()
    cur = conn.cursor()
    cur1 = conn1.cursor()

    cur.execute ("delete from dar_group.fk_nomenclature")
    conn.commit()
    cur1.execute ("delete from merchant.fk_nomenclature")
    conn1.commit()

    def read_page(page, table):
        d = page['hits']['hits']
        for ss in d:      
            source = ss['_source']
            try:
                temp_dict = {           
                    'id': ss['_id'],
                    'uid': source['uid'],
                    'merchant_id': source['merchant_id'],
                    'name': source['name'] if 'name' in  source else None,
                    #'categories_array': source['categories_array'] if 'categories_array' in  source else None,
                    'amount': source['amount'] if 'amount' in source else None,
                    'price': source['price'] if 'price' in source else None,
                    'articul': source['articul'] if 'articul' in source else None,
                    'status': source['status'] if 'status' in source else None,
                    'bar_code': source['bar_code'][0] if 'bar_code' in source else None,
                    'created_on': source['created_on'] if 'created_on' in  source else None,
                    'updated_on': source['updated_on'] if 'updated_on' in  source else None,
                    'is_visible': source['is_visible'] if 'is_visible' in  source else None,
                    'available': source['available'] if 'available' in source else None, 
                    #'product_id': source['product_id'] if 'product_id' in source else None,
                    'scope': source['scope'] if 'scope' in source else None,
                    #'sale_channels': source['sale_channels'][0] if 'sale_channels' in source else None,
                    #'name_ebt': source['name_ebt'] if 'name_ebt' in source else None,
                    #'sku_id': source['sku_id'] if 'sku_id' in source else None,
                    'group_id': source['group_id'] if 'group_id' in source else None,
                    #'vendor': source['vendor'] if 'vendor' in source else None
                }
                table.append(temp_dict)
            except:
                pass

    def read_catalog():
        es = elasticsearch.client.Elasticsearch(["http://10.64.0.156:9200"])
        pages = list()
        page_size = 1000
        page = es.search(
            index='kassa_nomen_*',
            doc_type='nomenclature',
            scroll='3m',
            body={
                "from": 0,
                "size": page_size,
                "sort": "created_on"
            }
        )
        read_page(page, pages)
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        chunks_count = round(scroll_size / page_size)

        for i in range(0, int(chunks_count)):
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            read_page(page, pages)
        for j in pages:
            id1 = j['id']
            id2 = j['uid']
            id3 = j['merchant_id']
            id4 = j['name']
            #id5 = j['categories_array']
            id6 = j['amount']
            id7 = j['price']
            id8 = j['articul']
            id9 = j['status']
            id10 = j['bar_code']
            id11 = j['created_on']
            id12 = j['updated_on']
            id13 = j['is_visible']
            id14 = j['available']
            #id15 = j['product_id']
            id16 = j['scope']
            #id17 = j['sale_channels']
            #id18 = j['name_ebt']
            #id19 = j['sku_id']
            id20 = str(j['group_id'])
            try:
                cur.execute ("INSERT INTO dar_group.fk_nomenclature(id, uid, merchant_id, name, amount, price, articul, status, bar_code, created_on, updated_on, is_visible, available, scope, group_id)VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(id1,id2,id3,id4,id6,id7,id8,id9,id10,id11,id12,id13,id14,id16,id20))
                cur1.execute ("INSERT INTO merchant.fk_nomenclature(id, uid, merchant_id, name, amount, price, articul, status, bar_code, created_on, updated_on, is_visible, available, scope, group_id)VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(id1,id2,id3,id4,id6,id7,id8,id9,id10,id11,id12,id13,id14,id16,id20))
            except:
                pass

    read_catalog()
    conn.commit()
    conn1.commit()
    cur.close()
    cur1.close()
    conn.close()
    conn1.close()

t1 = PythonOperator(
    task_id="read_elastic",
    python_callable=read_elastic,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)
