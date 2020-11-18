# -*- coding: utf-8 -*- 
from datetime import datetime
from openpyxl import Workbook

from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,2),
    'retries': 0,
}

dag = DAG(
    dag_id="cc_report",
    default_args=args,
    schedule_interval='0 10-19 * * *',
    tags=['Call center']
    )

conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()

def form_cc_report():
    query = '''
            select distinct "ФИО клиента", "Номер телефона", "Номер заказа", "Способ оплаты", "Дата заказа", "Использованный промокод", "Сумма скидки по промокоду", Город, Продавец, Товар, "Цена товара (без промокода)"
            from(
            select distinct on ("Номер телефона") orders.*,
            sender_title as Продавец,
            product_name as Товар,
            total as "Цена товара (без промокода)"
            from(
            select
            user_name as "ФИО клиента",
            case when uid = '2237252768848-94558' then '77773172223'
            when uid = '2237269331484-35294' then '77028186848'
            when uid = '2237270879014-75678' then '77028186848'
            when uid = '2237273560684-96222' then '77028186848'
            when uid = '2718682643955-72368' then '77013240784'
            when substr(replace(replace(replace(user_mobile, '+', ''), ' ', ''), '-', ''), 1, 1) = '8' then '7'||substr(replace(replace(replace(user_mobile, '+', ''), ' ', ''), '-', ''), 2, 100)
            when length(replace(replace(replace(user_mobile, '+', ''), ' ', ''), '-', '')) = 10 then '7'||replace(replace(replace(user_mobile, '+', ''), ' ', ''), '-', '')
            when length(replace(replace(replace(user_mobile, '+', ''), ' ', ''), '-', '')) > 11 then right(replace(replace(replace(user_mobile, '+', ''), ' ', ''), '-', ''), 11)
            else replace(replace(replace(user_mobile, '+', ''), ' ', ''), '-', '')
            end as "Номер телефона",
            uid as "Номер заказа",
            pay_title as "Способ оплаты",
            to_char(updated_on, 'dd-mm-yyyy hh24:mi') as "Дата заказа",
            updated_on,
            created_on,
            promocode_name as "Использованный промокод",
            common_discount_size as "Сумма скидки по промокоду",
            user_city as Город
            from dar_group.bazar_orders1
            where status like 'new') as orders
            join(
            select distinct uid,
            product_id,
            sku_id,
            product_merchant_id,
            (sku_price * sku_amount) as total,
            sender_title,
            product_name
            from dar_group.bazar_package
            where sender_title not like '%Dar Family%') as package
            on orders."Номер заказа" = package.uid
            order by "Номер телефона", "Дата заказа" desc ) as cancelled_orders
            where created_on  >=   current_timestamp  - interval '16 hour'
            and created_on  <   current_timestamp  - interval '1 hour'
            AND lower("ФИО клиента") NOT IN ('test','тест','ascsdbfdn')
            and "Номер заказа" not in (select tt.uid from dar_group.bazar_orders1 tt where tt.uid = cancelled_orders."Номер заказа" and tt.status <> 'new' 
            and tt.updated_on > cancelled_orders.updated_on )
    '''
    
    cur.execute(query)
    records = cur.fetchall()
    wb = Workbook()
    ws = wb.active
    ws.append(['ФИО клиента','Номер телефона','Номер заказа','Способ оплаты','Дата заказа','Использованный промокод','Сумма скидки по промокоду',
                'Город','Продавец','Товар','Цена товара (без промокода)'])
    for row in records:
        ws.append(row)
    wb.save('/tmp/cc_report.xlsx')

    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="form_cc_report",
    python_callable=form_cc_report,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

def build_email(**context):
    with open('/tmp/cc_report.xlsx', mode='r') as file:
        email_op = EmailOperator(
            task_id='send_email',
            to=['aakhmetov@one.kz', 'YesimkhanZhalel@one.kz', 'daronline.oktell@gmail.com'],
            subject="CC report",
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
    return 'Call center report generated and sent at {0}'.format(datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))

t3 = SlackWebhookOperator(
        task_id='send_slack_notification',
        http_conn_id='slack_connection',
        message=form_report(),
        # files = '/tmp/BPM_report.xlsx',
        channel='#reports',
		dag=dag
    )

t1 >> t2 >> t3