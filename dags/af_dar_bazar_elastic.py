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

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,10,18),
    'retries': 0,
}

dag = DAG(
    dag_id="dar_bazar_elastic",
    default_args=args,
    schedule_interval='0 06 * * *',
    tags=['Fortemarket']
    )

conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()

def read_page(page, table):
    d = page['hits']['hits']
    for ss in d:      
        source = ss['_source']
        try:
            temp_dict = {           
                'id': ss['_id'],
                'merchant_id': source['merchant_id'],
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
                'sku': source['skus'][0]['sku'] if 'sku' in source['skus'][0] else None
            }
            table.append(temp_dict)
        except:
            pass

def write_products():
    cur.execute ("DELETE FROM dar_group.darbazar_products")
    conn.commit()

    es = elasticsearch.client.Elasticsearch(["http://10.103.5.43:9200"])
    pages = list()
    page_size = 1000
    page = es.search(
        index='products',
        doc_type='product',
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
        id17 = j['sku']
        cur.execute ("INSERT INTO dar_group.darbazar_products(id, merchant_id, description, name_, default_category, categories_array, created_on, updated_on, is_visible, uid, price, amount, product_param,label_,scope_,nomenclature,sku) \
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
                    (id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,json.dumps(id13),id14,id15,id16,id17))
        conn.commit()

def write_categories():
    cur.execute ("delete from dar_group.darbazar_categories")
    conn.commit()

    cur.execute("""select distinct d 
        from (
            SELECT substring(default_category, '[^/]*$') as d FROM dar_group.darbazar_products
        union all
            select a[3] from (
                select regexp_split_to_array(default_category,'/') FROM dar_group.darbazar_products) as dt(a)
        union all
        select a[4] from (
            select regexp_split_to_array(default_category,'/') FROM dar_group.darbazar_products) as dt(a)
        union all
        select a[5] from (
            select regexp_split_to_array(default_category,'/') FROM dar_group.darbazar_products) as dt(a)) as a
            where d is not null and length(d) > 11""")
    dd = cur.fetchall()
    s2= list()

    for x in dd:
        x0 = str(x)
        x1 = x0.replace("('","")
        x2 = x1.replace("',)","")
        s2.append(x2)

    d= list()     
    for y in s2:
        url = "http://10.103.5.43:9200/catalogs/category/"+y
        headers = {
            'cache-control': "no-cache",
            'postman-token': "b68d3087-e288-74e6-c333-b32a2db55a49"
            }
        response = requests.request("GET", url, headers=headers)
        try:
            tw = response.json()
            id1 = tw['_id']
            id2 = tw['_source']['title'] if 'title' in  tw['_source'] else None
            id3 = tw['_source']['seoTitle'] if 'seoTitle' in  tw['_source'] else None
            id4 = tw['_source']['seoKeywords'] if 'seoKeywords' in  tw['_source'] else None
            id5 = tw['_source']['dscr'] if 'dscr' in  tw['_source'] else None
            cur.execute ("INSERT INTO dar_group.darbazar_categories(id, title, seo_keywords, description, seo_title) \
                        VALUES (%s, %s, %s, %s, %s)", \
                        (id1,id2,id4,id5,id3))
            conn.commit()
        except:
            pass

def write_params():
    cur.execute ("delete from dar_group.darbazar_params")
    conn.commit()

    url = "http://10.103.5.43:9200/params/param/_search"

    querystring = {"q":"*","size":"2000","scroll":"4m"}

    headers = {
        'cache-control': "no-cache",
        'postman-token': "9638eaca-e12a-7c67-1126-215639bff41b"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)
    res = response.json()

    for c in range(1,len(res['hits']['hits'])):
        try:
            for n in range(0,len(res['hits']['hits'][c]['_source']['params_values'])):
                bbc = res['hits']['hits'][c]['_id']
                id2 = res['hits']['hits'][c]['_source']['category_uid'] #if ['category_uid'] in res['hits']['hits'][c]['_source'] else None
                id3 = res['hits']['hits'][c]['_source']['title'] #if ['title'] in res['hits']['hits'][c]['_source'] else None
                id4 = res['hits']['hits'][c]['_source']['params_values'][n]['uid_values'] #if ['uid_values'] in res['hits']['hits'][c]['_source']['params_values'][n] else None
                id5 = res['hits']['hits'][c]['_source']['params_values'][n]['values']  #if ['values'] in res['hits']['hits'][c]['_source']['params_values'][n] else None
                id6 = res['hits']['hits'][c]['_source']['params_values'][n]['sort_values']  #if ['sort_values'] in res['hits']['hits'][c]['_source']['params_values'][n] else None
                cur.execute ("INSERT INTO dar_group.darbazar_params(id, category_uid, title, uid_values, values_, sort_values) \
                            VALUES (%s, %s, %s, %s, %s, %s)", \
                            (bbc,id2,id3,id4,id5,id6))
                conn.commit()
        except:
            pass

    total = res['hits']['total']
    loop = int(total/2000)
    ddd = res['_scroll_id']

    for x in range(0, loop):
        time.sleep(5)
        url = 'http://10.103.5.43:9200/_search/scroll?scroll=4m&scroll_id='+ddd
        headers = {'cache-control': "no-cache",'postman-token': "9638eaca-e12a-7c67-1126-215639bff41b"}
        response = requests.request("GET", url, headers=headers)

        for c in range(len(res['hits']['hits'])):
            try:
                for n in range(0,len(res['hits']['hits'][c]['_source']['params_values'])):
                    id1 = res['hits']['hits'][c]['_id']# if ['_id'] in res['hits']['hits'][c] else None
                    id2 = res['hits']['hits'][c]['_source']['category_uid']# if ['category_uid'] in res['hits']['hits'][c]['_source'] else None
                    id3 = res['hits']['hits'][c]['_source']['title']# if ['title'] in res['hits']['hits'][c]['_source'] else None
                    id4 = res['hits']['hits'][c]['_source']['params_values'][n]['uid_values']# if ['uid_values'] in res['hits']['hits'][c]['_source']['params_values'][n] else None
                    id5 = res['hits']['hits'][c]['_source']['params_values'][n]['values']#  if ['values'] in res['hits']['hits'][c]['_source']['params_values'][n] else None
                    id6 = res['hits']['hits'][c]['_source']['params_values'][n]['sort_values']#  if ['sort_values'] in res['hits']['hits'][c]['_source']['params_values'][n] else None

                    cur.execute ("INSERT INTO dar_group.darbazar_params(id, category_uid, title, uid_values, values_, sort_values) \
                                VALUES (%s, %s, %s, %s, %s, %s)", \
                                (id1,id2,id3,id4,id5,id6))
                    conn.commit()
            except:
                pass
    cur.close()
    conn.close()


t1 = PythonOperator(
    task_id="write_products",
    python_callable=write_products,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t2 = PythonOperator(
    task_id="write_categories",
    python_callable=write_categories,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t3 = PythonOperator(
    task_id="write_params",
    python_callable=write_params,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t1 >> t2 >> t3