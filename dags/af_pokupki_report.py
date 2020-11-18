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
    dag_id="pokupki",
    default_args=args,
    schedule_interval='0 08 * * *',
    tags=['Fortemarket']
    )

conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()

def form_pokupki_report():
    query = '''
        select
        uid,stat,pay_title,created_on,updated_on,user_name,user_surname,user_mobile,user_city,user_address,user_house_number,
        user_flat_number,sender_title,product_name,total_price,delivery_price,ttt,comment2,cat1,cat2,cat3,cat4,iin,
        status1,status2,promocode_name,promocode,is_promocode_applied,common_discount_size
        from ( 
        select *
        from (
        select *
        from (
        select *
        ,case 
        when stat in('new/false') then 'oformleno'
        when stat in('pending_approve/false') then 'postupilo'
        when stat in('pending_pickup/false','on_delivery/false') then 'priniyat'
        when stat in('taken/true','delivered/true') then 'zaverweno'
        when stat in('returned/true') then 'vozvrat'
        when stat in('cancelled/true', 'cancelled/false') then 'otmeneno' end as ttt 
        from(
        select b.*,sender_title,total_price,product_name, product_merchant_id, sku_id, product_id
        from(
        select distinct uid, (status || '/' || paid:: text) as stat, pay_title,created_on,updated_on,user_city,user_name,user_mobile,user_address,user_flat_number,user_house_number,user_surname,delivery_price, 
        promocode_name, promocode, is_promocode_applied, common_discount_size
        from(
        select ROW_NUMBER() OVER(partition by uid ORDER BY uid, updated_on, paid asc) as no,*
                from dar_group.bazar_orders1
                where to_char(updated_on::date,'yyyy-mm') = to_char(current_date-1,'yyyy-mm') and scope = 'fortemarket'
                ) as a
        )as b
            join 
            (select distinct uid, sender_title, (sku_price * sku_amount) as total_price,product_name, product_merchant_id, sku_id, product_id
            from dar_group.bazar_package
            ) as z
            on b.uid = z.uid
        where
        sender_title not in('"Dar Family" все для семьи','"Dar Family" ??? ??? ?????')
        order by pay_title,stat
        ) as a
        UNION ALL
        select *
        ,case 
        when stat in('new/false') then 'oformleno' 
        when stat in('pending_approve/true','new/true') then 'postupilo'
        when stat in('pending_pickup/true','on_delivery/true') then 'priniyat' 
        when stat in('taken/true','delivered/true') then 'zaverweno'
        when stat in('returned/true') then 'vozvrat'
        when stat in('cancelled/true', 'cancelled/false') then 'otmeneno' end as ttt
        from(
        select b.*,sender_title,total_price,product_name, product_merchant_id, sku_id, product_id
        from(
        select distinct uid, (status || '/' || paid:: text) as stat, pay_title,created_on,updated_on,user_city,user_name,user_mobile,user_address,user_flat_number,user_house_number,user_surname,delivery_price, 
        promocode_name, promocode, is_promocode_applied, common_discount_size
        from(
        select ROW_NUMBER() OVER(partition by uid ORDER BY uid, updated_on, paid asc) as no,*
                from dar_group.bazar_orders1
                where to_char(updated_on::date,'yyyy-mm') = to_char(current_date-1,'yyyy-mm') and scope = 'fortemarket'
                ) as a
        )as b
            join 
            (select distinct uid, sender_title, (sku_price * sku_amount) as total_price,product_name, product_merchant_id, sku_id, product_id
            from dar_group.bazar_package
            ) as z
            on b.uid = z.uid
        where pay_title = 'В рассрочку' and sender_title not in('"Dar Family" все для семьи','"Dar Family" ??? ??? ?????')
        order by pay_title,stat
        ) as a
        UNION ALL
        select *
        ,case when stat in('new/false') then 'oformleno'
        when stat in('pending_approve/true','new/true') then 'postupilo'
        when stat in('pending_pickup/true','on_delivery/true') then 'priniyat'
        when stat in('taken/true','delivered/true') then 'zaverweno'
        when stat in('returned/true') then 'vozvrat'
        when stat in('cancelled/true', 'cancelled/false') then 'otmeneno' end as ttt
        from(
        select b.*,sender_title,total_price,product_name, product_merchant_id, sku_id, product_id
        from(
        select distinct uid, (status || '/' || paid:: text) as stat, pay_title,created_on,updated_on,user_city,user_name,user_mobile,user_address,user_flat_number,user_house_number,user_surname,delivery_price, 
        promocode_name, promocode, is_promocode_applied, common_discount_size
        from(
        select ROW_NUMBER() OVER(partition by uid ORDER BY uid, updated_on, paid asc) as no,*
                from dar_group.bazar_orders1
                where to_char(updated_on::date,'yyyy-mm') = to_char(current_date-1,'yyyy-mm') and scope = 'fortemarket'
                ) as a
        )as b
            join 
            (select distinct uid, sender_title, (sku_price * sku_amount) as total_price,product_name, product_merchant_id, sku_id, product_id
            from dar_group.bazar_package
            ) as z
            on b.uid = z.uid
        where pay_title = 'Банковская карта' and sender_title not in('"Dar Family" все для семьи','"Dar Family" ??? ??? ?????')
        order by pay_title,stat
        ) as a
        UNION ALL
        select *
        ,case when stat in('pending_approve/false','new/false') then 'oformleno'
        when stat in('pending_approve/false','new/false') then 'postupilo'
        when stat in('pending_pickup/false','on_delivery/false') then 'priniyat'
        when stat in('taken/true','delivered/true') then 'zaverweno'
        when stat in('returned/true') then 'vozvrat'
        when stat in('cancelled/true', 'cancelled/false') then 'otmeneno' end as ttt
        from(
        select b.*,sender_title,total_price,product_name, product_merchant_id, sku_id, product_id
        from(
        select distinct uid, (status || '/' || paid:: text) as stat, pay_title,created_on,updated_on,user_city,user_name,user_mobile,user_address,user_flat_number,user_house_number,user_surname,delivery_price, 
        promocode_name, promocode, is_promocode_applied, common_discount_size
        from(
        select ROW_NUMBER() OVER(partition by uid ORDER BY uid, updated_on, paid asc) as no,*
                from dar_group.bazar_orders1
                where to_char(updated_on::date,'yyyy-mm') = to_char(current_date-1,'yyyy-mm') and scope = 'fortemarket'
                ) as a
        )as b
            join 
            (select distinct uid, sender_title, (sku_price * sku_amount) as total_price,product_name, product_merchant_id, sku_id, product_id
            from dar_group.bazar_package
            ) as z
            on b.uid = z.uid
        where pay_title = 'Оплата наличными' and sender_title not in('"Dar Family" все для семьи','"Dar Family" ??? ??? ?????')
        order by pay_title,stat
        ) as a
        ) as a
        left join 
        (select uid as uid2, comment2
        from 
        (select distinct on (uid) *, (temp::json)->'body'->>'canceled_comments' as comment2
        from dar_group.bazar_orders1
        where scope in('fortemarket') and user_mobile is not null 
        order by uid,updated_on desc) as a
        ) as b
        on a.uid = b.uid2
        ) as a
        left join 
        (select 
        brand, c.title as cat1, c1.title as cat2, c2.title as cat3, c3.title as cat4, name, price,sku_id as s2, product_id as p2, m2
        from(
        select  sku_id,product_id,merchant_id, m2, a[1] as a,a[2] as b,a[3] as c,a[4] as d, name, price
        from(
        select sku_id,product_id,merchant_id, merchant_id as m2, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as a, name, price
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket'
        ) as a
        ) as a
        left join dar_group.darbazar_categories as c on c.id = a.a
        left join dar_group.darbazar_categories as c1 on c1.id = a.b
        left join dar_group.darbazar_categories as c2 on c2.id = a.c
        left join dar_group.darbazar_categories as c3 on c3.id = a.d
        left join dar_group.darbazar_merchants as d on a.merchant_id = d.id
        where c.title is not null
        ) as b
        on a.product_merchant_id = b.m2 and a.sku_id = b.s2 and a.product_id = b.p2
        ) as a
        left join 
        (SELECT iin, status1, status2, order_id
        FROM dar_group.fm_express_loan
        )
        as b
        on a.uid = b.order_id
        '''
    cur.execute(query)
    records = cur.fetchall()
    wb = Workbook()
    ws = wb.active
    ws.append(['UID','Статус оплаты','Тип оплаты','Дата создания','Дата обновления','Имя','Фамилия','Телефон','Город','Адрес','Номер дома',
                'Номер квартиры','Партнер','Наименование продукта','Сумма','Доставка','Статус расшифровки','Комментарий отмены',
                'Категория 1','Категория 2','Категория 3','Категория 4','ИИН','Статус1','Статус2','Название промокода','Промокод','Активирован','Скидка'])
    for row in records:
        ws.append(row)
    wb.save('/tmp/pokupki.xlsx')

    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="form_pokupki_report",
    python_callable=form_pokupki_report,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

def build_email(**context):
    with open('/tmp/pokupki.xlsx', mode='r') as file:
        email_op = EmailOperator(
            task_id='send_email',
            to=['dnurtailakov@one.kz','MNurseitov@dar.kz', 'warzhoma@gmail.com', 'ASeisenbekov@dar.kz', 'AKanetov@dar.kz', 'ARakhimzhanova@dar.kz', 'ZhRamazanova@dar.kz', 'NIbdiminova@dar.kz', 'DKulinich@dar.kz', 'mnurmukanova@dar.kz', 'tyeshtayev@dar.kz', 'YesimkhanZhalel@one.kz', 'Kmakhsutov@one.kz', 'Irakhimov@one.kz', 'aakhmetov@one.kz', 'ZhSmagulova@one.kz', 'dkaliyeva@one.kz', 'KKhamitova@one.kz'],
            subject="Fortemarket покупки",
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
    return 'Pokupki report generated and sent at {0}'.format(datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))

t3 = SlackWebhookOperator(
        task_id='send_slack_notification',
        http_conn_id='slack_connection',
        message=form_report(),
        # files = '/tmp/BPM_report.xlsx',
        channel='#reports',
		dag=dag
    )

t1 >> t2 >> t3