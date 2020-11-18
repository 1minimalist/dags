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
    'start_date': datetime(2020,9,15),
    'retries': 0,
}

dag = DAG(
    dag_id="buh_vipiska",
    default_args=args,
    schedule_interval='0 08 * * *',
    tags=['Fortemarket']
    )

conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()

def form_buh_vipiska():
    query = """
            SELECT dtime,
            order_id,
            commersant,
            filial,
            terminal_id,
            auth_code,
            card_num,
            tran_amount,
            0 comm_bank,
            total_amount,
            currency,
            ret_account,
            emit_card,
            invoice_id,
            orders,
            ''::text invoice_type,
        item_id,
        item_name,
        item_price,
        item_qnt,
        item_qnt item_count,
        legal_name,
        iban merchant_iban,
        bik merchant_bik,
        merchant_bin,
        operation_id,
        bank_status,
        status,
        promocode,
        common_discount_size,
        pay_types,
        pay_title,
        comm_comp
        FROM
            (
        select
        distinct on (uid)
                a.created_on dtime,
        --                CASE
        --                        WHEN a.created_on :: TIME >= '08:00:00' THEN
        --                    a.created_on + '1 day' :: INTERVAL ELSE a.created_on
        --                END AS dtime,
        null::text as order_id,
        NULL :: TEXT AS commersant,
        NULL :: TEXT AS filial,
        NULL :: TEXT AS terminal_id,
        NULL :: TEXT AS auth_code,
        NULL :: TEXT AS card_num,
        item_price AS tran_amount,
        0 AS bank_commission,
        item_price AS total_amount,
        NULL :: TEXT AS currency,
        NULL :: TEXT AS ret_account,
        NULL :: TEXT AS emit_card,
        invoice_id,
        order_id AS orders,
        item_id,
        item_name,
        item_price+coalesce(common_discount_size::numeric,0) item_price,
        item_qnt,
        legal_name,
        iban,
        bik,
        legal_id merchant_bin,
        bank_trans_id operation_id,
        bank_status,
        b.status,
        merch_id,
        uid,
        updated_on,
        promocode,
        common_discount_size,
        pay_types,
        pay_title,
        CASE
        WHEN pay_title = 'Кредит на 24 месяца' THEN
        FORTE_EXPRESS_18_24 
        WHEN pay_title = 'Кредит на 12 месяцев' THEN
        FORTE_EXPRESS_18_12 
        WHEN pay_title = 'Кредит на 6 месяцев' THEN
        FORTE_EXPRESS_18_6 
        WHEN pay_title = 'Рассрочка на 24 месяца' THEN
        FORTE_EXPRESS_0_24 
        WHEN pay_title = 'Рассрочка на 12 месяцев' THEN
        FORTE_EXPRESS_0_12 
        WHEN pay_title = 'Рассрочка на 4 месяца' THEN
        FORTE_EXPRESS_0_4 
        WHEN pay_title = 'Кредитная карта на 12 месяцев' THEN
        ACQUIRING_0_12 
        WHEN pay_title = 'Кредитная карта на 6 месяцев' THEN
        ACQUIRING_0_6 
        WHEN pay_title = 'Кредитная карта на 4 месяца' THEN
        ACQUIRING_0_4 
        WHEN pay_title = 'Банковская карта' THEN
            CARD_0_0
        END AS comm_comp
        FROM
            dar_group.fm_invoices a
            left join dar_group.darbazar_merchants mer on a.merch_id = mer.id
            left join dar_group.fk_merchant_requisites req on mer.id = req.merchant_id
            left join dar_group.bazar_orders1 b ON a.order_id = b.uid
        LEFT JOIN ( SELECT ID, REPLACE ( split_part( categories_array, ',', 1 ), '{', '' ) AS cat_id FROM dar_group.fm_nomenclature ) AS n ON n.ID = A.item_id
        LEFT JOIN dar_group.merch_com AS M ON M.merchant_id = A.merch_id AND M.cat_id = COALESCE ( n.cat_id, 'root' )
        where bank_trans_code='FORTE_EXPRESS'
        and legal_name != 'ТОО "Narita"'
        AND a.created_on::date = current_date-1
        ORDER BY
            uid,
            updated_on desc
            ) t
            union
        SELECT
            dtime,
            order_id,
            commersant,
            filial,
            terminal_id,
            auth_code,
            card_num,
            tran_amount,
            comm_bank,
            total_amount,
            currency,
            ret_account,
            emit_card,
            invoice_id,
            orders,
            null::text as invoice_type,
            item_id,
            item_name,
            item_price,
            item_qnt,
            item_qnt as items_count,
            legal_name,
            iban merchant_iban,
            bik merchant_bik,
            merchant_bin,
            operation_id,
            bank_status,
            status,
            promocode,
            common_discount_size,
            pay_types,
            pay_title,
            comm_comp
        FROM
            (
                SELECT DISTINCT
                    *,
                CASE
                        WHEN upload :: TIME >= '08:00:00' THEN
            upload + '1 day' :: INTERVAL ELSE upload
            END AS upload2
        FROM
            dar_group.fm_bank a
        LEFT JOIN (
        select
                DISTINCT on ( uid )
                    invoice_id,
                    a.order_id orders,
                    null::text as invoice_type,
                    item_id,
                    item_name,
                    item_price+coalesce(common_discount_size::numeric,0) item_price,
                    item_qnt,
                    item_qnt items_count,
                    brand,
                    legal_name,
                    iban,
                    bik,
                    req.legal_id merchant_bin,
                    bank_trans_id operation_id,
                    b.bank_status,
                    b.status,
                    a.merch_id,
                    b.promocode,
                    b.common_discount_size,
                    b.pay_types,
                    b.pay_title,
                    CASE
                        WHEN pay_title = 'Кредит на 24 месяца' THEN
                FORTE_EXPRESS_18_24 
                WHEN pay_title = 'Кредит на 12 месяцев' THEN
                FORTE_EXPRESS_18_12 
                WHEN pay_title = 'Кредит на 6 месяцев' THEN
                FORTE_EXPRESS_18_6 
                WHEN pay_title = 'Рассрочка на 24 месяца' THEN
                FORTE_EXPRESS_0_24 
                WHEN pay_title = 'Рассрочка на 12 месяцев' THEN
                FORTE_EXPRESS_0_12 
                WHEN pay_title = 'Рассрочка на 4 месяца' THEN
                FORTE_EXPRESS_0_4 
                WHEN pay_title = 'Кредитная карта на 12 месяцев' THEN
                ACQUIRING_0_12 
                WHEN pay_title = 'Кредитная карта на 6 месяцев' THEN
                ACQUIRING_0_6 
                WHEN pay_title = 'Кредитная карта на 4 месяца' THEN
                ACQUIRING_0_4 
                WHEN pay_title = 'Банковская карта' THEN
                CARD_0_0
            END AS comm_comp,
            m.*
        FROM
            dar_group.fm_invoices a
        left join dar_group.darbazar_merchants mer on a.merch_id = mer.id
        left join dar_group.fk_merchant_requisites req on mer.id = req.merchant_id
        LEFT JOIN dar_group.bazar_orders1 b ON a.order_id = b.uid
        LEFT JOIN ( SELECT ID, REPLACE ( split_part( categories_array, ',', 1 ), '{', '' ) AS cat_id
            FROM dar_group.fm_nomenclature ) AS n ON n.ID = a.item_id
        LEFT JOIN dar_group.merch_com AS M
            ON M.merchant_id = A.merch_id
            ORDER BY
                uid,
                updated_on desc
        ) AS b ON b.operation_id||'9' = a.order_id
        WHERE
            upload IS NOT null
            ORDER BY
            upload desc ) t
            where upload2::date = current_date
    """
    
    cur.execute(query)
    records = cur.fetchall()
    wb = Workbook()
    ws = wb.active
    ws.append(['dtime','order_id','commersant','filial','terminal_id','auth_code','card_num','tran_amount','comm_bank','total_amount',
                'currency','ret_account','emit_card','invoice_id','orders','invoice_type','item_id','item_name','item_price',
                'item_quantity','items_count','legal_name','merchant_iban','merchant_bik','merchant_bin','operation_id','bank_status',
                'status','promocode','common_discount_size','pay_types','pay_title','comm_comp'])
    for row in records:
        ws.append(row)
    wb.save('/tmp/vipiski.xlsx')

    cur.close()
    conn.close()

t1 = PythonOperator(
    task_id="form_buh_vipiska",
    python_callable=form_buh_vipiska,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

def build_email(**context):
    with open('/tmp/vipiski.xlsx', mode='r') as file:
        email_op = EmailOperator(
            task_id='send_email',
            to=['dnurtailakov@one.kz','mnurmukanova@dar.kz','MNurseitov@dar.kz','ZhSmagulova@one.kz','dkaliyeva@one.kz','KKhamitova@one.kz','aakhmetov@one.kz'],
            subject="Fortemarket выписка",
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
    return 'Vipiska report generated and sent at {0}'.format(datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))

t3 = SlackWebhookOperator(
        task_id='send_slack_notification',
        http_conn_id='slack_connection',
        message=form_report(),
        # files = '/tmp/BPM_report.xlsx',
        channel='#reports',
		dag=dag
    )

t1 >> t2 >> t3