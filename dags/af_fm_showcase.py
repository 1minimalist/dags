# -*- coding: utf-8 -*- 
import elasticsearch
import psycopg2
import json
import requests
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,1),
    'retries': 0,
}

dag = DAG(
    dag_id="fm_showcase",
    default_args=args,
    schedule_interval='0 06 * * *',
    tags=['Fortemarket']
    )

conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()

def read_elastic():
    cur.execute ("delete from dar_group.fm_showcase")
    conn.commit()

    def read_page(page, table):
        d = page['hits']['hits']
        for ss in d:      
            source = ss['_source']
            try:
                temp_dict = {            
                    'id': ss['_id'],
                    'merchant_id': source['merchant_id'] if 'merchant_id' in source else None,
                    'description': source['description'] if 'description' in source else None,
                    'name': source['name'] if 'name' in  source else None,
                    'default_category': source['default_category'] if 'default_category' in  source else None,
                    'categories_array': source['categories_array'] if 'categories_array' in  source else None,
                    'created_on': source['created_on'] if 'created_on' in  source else None,
                    'updated_on': source['updated_on'] if 'updated_on' in  source else None,
                    'is_visible': source['is_visible'] if 'is_visible' in  source else None,
                    'uid': source['skus'][0]['uid'] if 'uid' in source['skus'][0] else None,
                    'price': source['skus'][0]['price'] if 'price' in source['skus'][0] else None,
                    'amount': source['skus'][0]['amount'] if 'amount' in source['skus'][0] else None,
                    'product_param': source['skus'][0]['product_param'] if 'product_param' in source['skus'][0] else None,
                    'label': source['label'] if 'label' in  source else None,
                    'scope': source['scope'] if 'scope' in  source else None,
                    'nomenclature': source['skus'][0]['nomenclature'] if 'nomenclature' in source['skus'][0] else None,
                    'sku': source['skus'][0]['min_nomen_id'] if 'min_nomen_id' in source['skus'][0] else None
                }
                table.append(temp_dict)
            except:
                print('ERROR')

    def read_catalog():
        es = elasticsearch.client.Elasticsearch(["http://10.103.5.43:9200"])
        pages = list()
        page_size = 1000
        page = es.search(
            index='showcase',
            doc_type='showcase',
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
            id2 = j['merchant_id']
            id3 = j['description']
            id4 = j['name']
            id5 = j['default_category']
            id6 = j['categories_array']
            id7 = j['created_on']
            id8 = j['updated_on']
            id9 = j['is_visible']
            id10 = j['uid']
            id11 = j['price']
            id12 = j['amount']
            id13 = j['product_param']
            id14 = j['label']
            id15 = j['scope']
            id16 = j['nomenclature']
            id17 = str(j['sku'])
            cur.execute ("INSERT INTO dar_group.fm_showcase(id, merchant_id, description, name_, default_category, categories_array, created_on, \
                        updated_on, is_visible, uid, price, amount, product_param,label_,scope_,nomenclature,sku) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
                        (id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,json.dumps(id13),id14,id15,id16,id17))
            conn.commit()

    read_catalog()
    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="read_elastic",
    python_callable=read_elastic,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)