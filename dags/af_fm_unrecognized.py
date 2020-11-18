# -*- coding: utf-8 -*- 
from datetime import datetime
from openpyxl import Workbook

from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import EmailOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,2),
    'retries': 0,
}

dag = DAG(
    dag_id="fm_unrecognized",
    default_args=args,
    schedule_interval='15 09 * * *',
    tags=['Fortemarket']
    )

conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()

def form_report():
    query = '''
            select brand,merchant_id,uid,bar_code,articul,name,created_on,updated_on,price
            from(
            select distinct merchant_id, uid,bar_code,articul,name,created_on,updated_on,price
                from dar_group.fm_nomenclature
                where sale_channels = 'fortemarket' and status = 'created' and available is true
            ) as a
            left join(
            select id,brand
            from dar_group.darbazar_merchants
            ) as b
            ON a.merchant_id = b.id
        '''
    cur.execute(query)
    records = cur.fetchall()
    wb = Workbook()
    ws = wb.active
    ws.append(['brand','merchant_id','uid','bar_code','articul','name','created_on','updated_on','price'])
    for row in records:
        ws.append(row)
    wb.save('/tmp/fm_unrecognized.xlsx')

    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="form_report",
    python_callable=form_report,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

def build_email(**context):
    with open('/tmp/fm_unrecognized.xlsx', mode='r') as file:
        email_op = EmailOperator(
            task_id='send_email',
            to=['ARakhimzhanova@dar.kz', 'YesimkhanZhalel@one.kz', 'NKravtsov@dar.kz', 'MNokhrin@dar.kz', 'TTsoy@dar.kz', 'aakhmetov@one.kz'],
            subject="fm_unrecognized",
            html_content='Hello, <br/>',
            files=[file.name],
        )
        email_op.execute(context)


t2 = PythonOperator(
    task_id="send_email",
    python_callable=build_email,
    provide_context=True,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

def form_report():
    return 'FM_unrecognized report generated and sent at {0}'.format(datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))

t3 = SlackWebhookOperator(
        task_id='send_slack_notification',
        http_conn_id='slack_connection',
        message=form_report(),
        # files = '/tmp/BPM_report.xlsx',
        channel='#reports',
        dag=dag
    )

t1 >> t2 >> t3
