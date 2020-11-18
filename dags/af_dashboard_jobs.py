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
    'start_date':datetime(2020,11,11),
}

dag = DAG(
    dag_id='dashboard_jobs',
    description='Fortemarket dashboards',
    schedule_interval='45 8,10,12,14,16,18,20,22 * * *',
    default_args = default_args,
    tags=['Fortemarket']
    )


conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()
cur1 = conn.cursor()


def get_unidentified():
    cur1.execute('truncate table marts.fm_unidentified_products')

    cur.execute('''
        select distinct a.*, c.title as cat1 
        from(
        select brand, merchant_id, uid, bar_code, articul, name, created_on, updated_on, price, a[1] as a 
        from(
        select distinct merchant_id, uid, bar_code, articul, name, created_on, updated_on, price, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as a
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket' and status = 'created' and available is true) as a
        join (
        select distinct id, brand
        from dar_group.darbazar_merchants
        where state = 'ACTIVE') as b on a.merchant_id = b.id) as a
        left join dar_group.darbazar_categories as c on c.id = a.a
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_unidentified_products VALUES %s",
                       records)
        conn.commit()


def get_orders():
    cur1.execute('truncate table marts.fm_orders')

    cur.execute('''
        select distinct a.*, identifier_id as reg_fm, 
        case when identifier_id is not null then uid end as uid_reg_fm,
        case when identifier_id is not null then total end as total_reg_fm,
        case when city_id like '%KZ-AKM-111010000%' then 'Кокшетау'
        when city_id like '%KZ-AKT-151010000%' then 'Актобе'
        when city_id like '%KZ-ALA%' then 'Алматы'
        when city_id like '%KZ-ALM-191010000%' then 'Талдыкорган'
        when city_id like '%KZ-ALM-195220100%' then 'Каскелен'
        when city_id like '%KZ-AST%' then 'Нур-Султан'
        when city_id like '%KZ-ATY-231010000%' then 'Атырау'
        when city_id like '%KZ-KAR-351010000%' then 'Караганда'
        when city_id like '%KZ-KUS-391010000%' then 'Костанай'
        when city_id like '%KZ-KZY-431010000%' then 'Кызылорда'
        when city_id like '%KZ-MAN-471010000%' then 'Актау'
        when city_id like '%KZ-PAV-551010000%' then 'Павлодар'
        when city_id like '%KZ-PAV-552210000%' then 'Экибастуз'
        when city_id like '%KZ-SEV-591010000%' then 'Петропавловск'
        when city_id like '%KZ-SHY%' then 'Шымкент'
        when city_id like '%KZ-TUR-611010000%' then 'Туркестан'
        when city_id like '%KZ-VOS-631010000%' then 'Усть-Каменогорск'
        when city_id like '%KZ-VOS-632810000%' then 'Семей'
        when city_id like '%KZ-ZAP-271010000%' then 'Уральск'
        when city_id like '%KZ-ZHA-311010000%' then 'Тараз' end as city_ru
        from(
        select distinct uid, status, status_ru, status_ru_num::text, delivery_type, cancelled_reason1 as cancelled_reason, merchant_approve, contract_signed, created_on2, created_on, updated_on2, updated_on, pay_title1 as pay_title, pay_title2, total, mobile, user_email, promocode_name, promocode, is_promocode_applied, common_discount_size, city_id, delivery_types, weight, product_merchant_id, sender_title, max(name_ebt) as name_ebt, max(category_1) as category_1, common_old_price
        from(
        select distinct a.*, name_ebt, category_name as category_1,
        case when status_ru = 'отменено' and cancelled_reason = 'active' then 'активная заявка'
        when status_ru = 'отменено' and bank_status = 'cancel' then 'отказ: скоринг'
        when status_ru = 'отменено' and cancelled_reason = 'CLIENT.DECLINE' then 'отменено клиентом'
        when status_ru = 'отменено' and cancelled_reason = 'error' then 'общие ошибки'
        when status_ru = 'отменено' and cancelled_reason = 'Таймаут на решении клиента' then 'отменено по timeout'
        when status_ru = 'отменено' and bank_status = 'rejected' then 'отказ: вн. проверки банка'
        when status like '%cancelledbybank%' then 'отменено банком'
        when status like '%cancelledbyclient%' then 'отменено клиентом'
        when status like '%cancelledbymerchant%' then 'отменено партнером'
        when status like '%cancelledbytimeout%' then 'отменено по timeout'
        when status like '%cancel%' then 'отменено' end as cancelled_reason1
        from (
        select distinct a.*, product_id, product_merchant_id, sender_title, product,
        case when pay_types_code = 'ACQUIRING_0_4' then 'Кредитная карта на 4 месяца'
        when pay_types_code = 'ACQUIRING_0_6' then 'Кредитная карта на 6 месяцев'
        when pay_types_code = 'ACQUIRING_0_12' then 'Кредитная карта на 12 месяцев'
        when pay_title in ('Кредитная карта в рассрочку на 4 месяца', 'Кредитная карта на 4 месяца') then 'Кредитная карта на 4 месяца'
        when pay_title in ('Кредитная карта в рассрочку на 6 месяцев', 'Кредитная карта на 6 месяцев') then 'Кредитная карта на 6 месяцев'
        when pay_title in ('Кредитная карта в рассрочку на 12 месяцев', 'Кредитная карта на 12 месяцев') then 'Кредитная карта на 12 месяцев'
        when pay_title in ('В рассрочку', 'Кредитная карта в рассрочку на undefined месяцев', 'Кредитная карта в рассрочку на 24 месяцев', 'В рассрочку по кред. карте') then 'Кредитная карта'
        when pay_types_code = 'FORTE_EXPRESS_0_4' then 'Рассрочка на 4 месяца'
        when pay_types_code = 'FORTE_EXPRESS_0_12' then 'Рассрочка на 12 месяцев'
        when pay_types_code = 'FORTE_EXPRESS_0_24' then 'Рассрочка на 24 месяца'
        when pay_types_code = 'FORTE_EXPRESS_18_6' then 'Кредит на 6 месяцев'
        when pay_types_code = 'FORTE_EXPRESS_18_12' then 'Кредит на 12 месяцев'
        when pay_types_code = 'FORTE_EXPRESS_18_24' then 'Кредит на 24 месяца'
        when product = 'FM 4-1' then 'Рассрочка на 4 месяца'
        when product = 'FM 6-01' then 'Кредит на 6 месяцев'
        when product = 'FM 12-01' then 'Кредит на 12 месяцев'
        when product = 'FM 24-01' then 'Кредит на 24 месяца'
        when product = 'FM 12-1-1' then 'Рассрочка на 12 месяцев'
        when product = 'FM 24-1-1' then 'Рассрочка на 24 месяца'
        when pay_title = 'Кредит на 12 месяцев' then 'Кредит на 12 месяцев'
        when pay_title in ('Экспресс кредит от Форте на 24 месяцев', 'Экспресс кредит от Форте на 24 месяцa', 'Кредит на 24 месяца') then 'Кредит на 24 месяца' 
        when pay_title in ('Экспресс кредит от Форте на 6 месяцев', 'В рассрочку на 6 месяцев', 'Кредит на 6 месяцев') then 'Кредит на 6 месяцев' 
        when pay_title in ('В рассрочку на 12 месяцев', 'Рассрочка на 12 месяцев') then 'Рассрочка на 12 месяцев'
        when pay_title in ('В рассрочку на 24 месяцев', 'Рассрочка на 24 месяца') then 'Рассрочка на 24 месяца'
        when pay_title in ('В рассрочку на 4 месяца', 'В рассрочку на 4 месяцев', 'Рассрочка на 4 месяца') then 'Рассрочка на 4 месяца'
        when pay_types like '%FORTE_EXPRESS%' then 'Экспресс кредит от Форте'
        else pay_title end as pay_title2
        from(
        select distinct on (uid, pay_title1, status_ru) *
        from(
        select distinct uid, status, 
        case when (status = 'new' and paid is false) or (status = 'pending_approve' and paid is false and pay_types = 'COD') then 'создано'
        when status in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status in ('taken', 'delivered') then 'завершено'
        when status = 'returned' then 'возврат'
        when status like '%cancel%' then 'отменено' end as status_ru,
        case when (status = 'new' and paid is false) or (status = 'pending_approve' and paid is false and pay_types = 'COD') then 1
        when status in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 2
        when status in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 3
        when status in ('taken', 'delivered') then 4
        when status = 'returned' then 5
        when status like '%cancel%' then 6 end as status_ru_num,
        case when status = 'delivered' and delivery_types = 'delivery' then 'доставка'
        when status = 'delivered' and delivery_types = 'kazpost' then 'казпочта'
        when status = 'taken' then 'самовывоз' end as delivery_type,
        case when status in('returned', 'delivered', 'taken', 'approved_not_filled', 'awaiting_loan_approve', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'pending_pickup', 'on_delivery', 'awaiting_courier_pickup') then 1 end as merchant_approve,
        case when status in ('on_delivery', 'pending_pickup', 'delivered', 'taken', 'returned') then 1 end as contract_signed,
        case when pay_types = 'ACQUIRING' then 'Кредитная карта'
        when pay_types like '%FORTE_EXPRESS%' then 'Экспресс кредит от Форте'
        else pay_title end as pay_title1,
        case when common_old_price is null then common_price else common_old_price end as common_old_price,
        pay_title, to_char(created_on, 'yyyy-mm-dd hh24:mi:ss')::timestamp as created_on2, created_on::date, to_char(updated_on, 'yyyy-mm-dd hh24:mi:ss')::timestamp as updated_on2, updated_on::date, pay_types, common_price as total, replace(replace(replace(user_mobile, '+', ''), '-', ''), ' ', '') as mobile, user_email, promocode_name, promocode, is_promocode_applied, common_discount_size::decimal, cancelled_reason, replace(city_id, '"', '') as city_id, bank_status, delivery_types, weight, pay_types_code
        from dar_group.bazar_orders1
        where scope = 'fortemarket' and pay_types is not null and pay_types != 'CREDIT' and pay_title not in ('?????? ?????????', '? ?????????', 'Оплата в рассрочку', 'Оплата онлайн')) as a
        order by uid, pay_title1, status_ru, updated_on2 desc) as a
        join(
        select distinct uid, product_id, product_merchant_id, sender_title, (sku_price * sku_amount) as total
        from dar_group.bazar_package
        where product_merchant_id != '9VYpqYXvl77LkHvenw') as b on a.uid = b.uid and a.common_old_price = b.total
        left join(
        select distinct order_id, product_sub_code as product
        from dar_group.fm_saveloanreq
        where product_sub_code is not null
        union
        select distinct order_id, productcode as product
        from dar_group.fm_sendloanreq
        where productcode is not null) as c on a.uid = c.order_id ) as a
        left join(
        select distinct product_id, merchant_id, category[1] as category_1, name_ebt, title as category_name
        from(
        select distinct product_id, merchant_id, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as category, name_ebt
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket' and product_id is not null) as a
        left join dar_group.darbazar_categories as b on a.category[1] = b.id
        where title is not null) as b on a.product_id = b.product_id and a.product_merchant_id = b.merchant_id) as a
        group by uid, status, status_ru, status_ru_num, delivery_type, cancelled_reason1, merchant_approve, contract_signed, created_on2, created_on, updated_on2, updated_on, pay_types, pay_title, pay_title1, pay_title2, total, mobile, user_email, promocode_name, promocode, is_promocode_applied, common_discount_size, city_id, delivery_types, weight, pay_types_code, product_merchant_id, sender_title, product, common_old_price) as a
        left join(
        select profile_id, brand, id_space, state, identifier_id, min(created) as created, max(email) as email
        from(
        select profile_id, upper(brand) as brand, 'MOBILE'::text as id_space, 'ACTIVE'::text as state, identifier_id, email, created::date
        from dar_group.dar_account
        where lower(state) like '%active%' and lower(brand) = 'forte_market' and lower(id_space) like '%mobile%') as a
        group by profile_id, brand, id_space, state, identifier_id ) as b on a.mobile = b.identifier_id
        order by uid, updated_on2
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_orders VALUES %s",
                       records)
        conn.commit()


def get_customer_profile():
    cur1.execute('truncate table marts.fm_customer_profile')

    cur.execute('''
        select distinct a.*, common_price::int
        from(
        select a.*, b.status, b.updated_on
        from(
        select a.*, b.income,
        case when b.income <= 200000 then 'до 200 000'
        when b.income <= 400000 then 'до 400 000'
        when b.income <= 600000 then 'до 600 000'
        when b.income > 600000 then 'более 600 000' end as income_segment,
        case when a.price <= 50000 then 'до 50 000'
        when a.price <= 200000 then 'до 200 000'
        when a.price <= 500000 then 'до 500 000'
        when a.price <= 600000 then 'до 600 000' end as price_segment,
        case when a.age <= 25 then 'до 25'
        when a.age <= 35 then 'до 35'
        when a.age <= 45 then 'до 45'
        when a.age <= 55 then 'до 55'
        when a.age > 55 then 'старше 55' end as age_segment,
        case when loan_type = 'credit' then 'Кредит'
        when loan_type = 'installment' then 'Рассрочка' end as loan_type_ru
        from(
        select distinct order_id, 
        case when temp like '%"sex": 1%' then 'male'
        when temp like '%"sex": 2%' then 'female' end as gender,
        (current_date - make_date(substr(temp, position('birthdate_str' in temp) + 23, 4)::int, substr(temp, position('birthdate_str' in temp) + 20, 2)::int, substr(temp, position('birthdate_str' in temp) + 17, 2)::int)) / 365 as age,
        substr(temp, position('"price":' in temp) + 9, position(', "description":' in temp) - (position('"price":' in temp) + 9))::decimal as price,
        mobile_phone,
        product_sub_code,
        period_::int,
        loan_type,
        sum_
        from dar_group.fm_saveloanreq) as a
        join(
        select distinct order_id, income::numeric
        from(
        select order_id, 
        replace(cast(temp::json->'body'->'bank_req'->'application'->11->'value' as varchar),'"', '') as income
        from dar_group.fm_sendloanreq
        where temp like '%"key": "Income"%') as a
        where income != '920614351542') as b
        on a.order_id = b.order_id) as a
        left join(
        select distinct uid, (status || '/' || paid:: text) as status, updated_on::date
        from dar_group.bazar_orders1
        where scope = 'fortemarket' and status in ('delivered', 'taken')) as b
        on a.order_id = b.uid
        where updated_on is not null
        order by order_id, updated_on) as a
        left join(
        select distinct uid, common_price
        from dar_group.bazar_orders1) as b on a.order_id = b.uid
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_customer_profile VALUES %s",
                       records)
        conn.commit()


def get_sellers_count():
    cur1.execute('truncate table marts.fm_sellers_count')

    cur.execute('''
        select distinct sku_id, count(distinct a.merchant_id) as sellers
        from (
        select sku_id,
        merchant_id
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket' and is_visible is true and available is true) as a
        join(
        select *
        from dar_group.darbazar_merchants
        where state = 'ACTIVE')as b
        on a.merchant_id = b.id
        where sku_id is not null
        group by sku_id
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_sellers_count VALUES %s",
                       records)
        conn.commit()


def get_products_count():
    cur1.execute('truncate table marts.fm_products_count')

    cur.execute('''
        select initcap(trim(brand)), current_date, state,
        sum(case when base = 'Кол-во активных товаров номенклатур' then sum else 0 end) as active,
        sum(case when base = 'Кол-во товаров в номенклатуре' then sum else 0 end) as available,
        sum(case when base = 'Кол-во товаров в ЕБТ' then sum else 0 end) as ebt,
        sum(case when base = 'Кол-во всех товаров в номенклатуре' then sum else 0 end) as nomenclature
        from(
        select base, sum(koltovara), brand, current_date, state
        from(
        select 1 as ord, 'Кол-во активных товаров номенклатур'::text as base, count(distinct uid) as koltovara, a.merchant_id as merchant, brand, current_date, state
        from dar_group.fm_nomenclature as a
        left join dar_group.darbazar_merchants as b
        on a.merchant_id = b.id
        where sale_channels = 'fortemarket' and is_visible is true and available is true and state ='ACTIVE'
        group by merchant,brand, state
        union
        select 2 as ord, 'Кол-во товаров в номенклатуре'::text as base, count(distinct uid) as koltovara, a.merchant_id as merchant, brand, current_date, state
        from dar_group.fm_nomenclature as a
        left join dar_group.darbazar_merchants as b
        on a.merchant_id = b.id
        where sale_channels = 'fortemarket' and available is true
        group by merchant, brand, state
        union
        select 3 as ord,'Кол-во товаров в ЕБТ'::text as base, count(distinct a.id) as koltovara, a.merchant_id as merchant, brand, current_date, state
        from dar_group.darbazar_products as a
        left join dar_group.darbazar_merchants as b
        on a.merchant_id = b.id
        where scope_ = '{darbazar}' and created_on >= '2018-10-01'
        group by merchant, brand, state
        union
        select 4 as ord, 'Кол-во всех товаров в номенклатуре'::text as base, count(distinct uid) as koltovara, a.merchant_id as merchant, brand, current_date, state
        from dar_group.fm_nomenclature as a
        left join dar_group.darbazar_merchants as b
        on a.merchant_id = b.id
        where sale_channels = 'fortemarket'
        group by merchant, brand, state
        order by ord) as a
        group by base, brand, state) as a
        where brand is not null
        group by brand, state
        order by brand
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_products_count VALUES %s",
                       records)
        conn.commit()


def get_enabled_disabled():
    cur1.execute('truncate table marts.fm_enabled_disabled')

    cur.execute('''
        select 'опубликовано'::text as base, a.*, b.name_ebt as product, initcap(trim(c.brand))
        from(
        select distinct dtime::date, merchant_id, enabled as product_id
        from(
        select distinct event_dtime at time zone 'utc' at time zone 'asia/almaty' as dtime, merchant_id, enabled
        from dar_group.fct_fm_nomen_enabled) as a) as a
        left join dar_group.fm_nomenclature as b on a.merchant_id = b.merchant_id and a.product_id = b.product_id
        left join dar_group.darbazar_merchants as c on a.merchant_id = c.id
        union
        select 'снято'::text as base, a.*, b.name_ebt as product, initcap(trim(c.brand))
        from(
        select distinct dtime::date, merchant_id, disabled as product_id
        from(
        select distinct event_dtime at time zone 'utc' at time zone 'asia/almaty' as dtime, merchant_id, disabled
        from dar_group.fct_fm_nomen_disabled) as a) as a
        left join dar_group.fm_nomenclature as b on a.merchant_id = b.merchant_id and a.product_id = b.product_id
        left join dar_group.darbazar_merchants as c on a.merchant_id = c.id
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_enabled_disabled VALUES %s",
                       records)
        conn.commit()


def get_credit_orders():
    cur1.execute('truncate table marts.fm_credit_orders')

    cur.execute('''
        select distinct uid, pay_title, sender_title, product_merchant_id, total, 
        case when status_temp like '%cancel%' then cancelled_reason end as cancelled_reason,
        updated_on, status_1, status_2, status_3, status_4, status_5, status_6, status_7, exclude_approve, status_1_ru, 
        case when status_3_ru = 'договор подписан' then 'анкета заполнена' else status_2_ru end as status_2_ru, 
        case when status_3_ru = 'завершен' then 'договор подписан' else status_3_ru end as status_3_ru, 
        case when status_3_ru = 'завершен' then 'завершен' else status_4_ru end as status_4_ru,
        case when status_3_ru = 'завершен' then status_4_ru else status_5_ru end as status_5_ru
        from(
        select distinct uid, pay_title, sender_title, product_merchant_id, total, cancelled_reason, updated_on, status_1, status_2, status_3, status_4, status_5, status_6, status_7,
        case when status_2 in ('cancelled', 'cancelledbytimeout', 'cancelledbyclient') then 1 else exclude_approve end as exclude_approve,
        (status_1 || ',' || coalesce(status_2,'') || ',' || coalesce(status_3,'') || ',' || coalesce(status_4,'') || ',' || coalesce(status_5,'') || ',' || coalesce(status_6,'') || ',' || coalesce(status_7,''):: text) as status_temp,
        case when status_1 = 'new' then 'создано'
        when status_1 in ('pending_approve', 'approved_not_filled') then 'анкета не заполнена'
        when status_1 in ('awaiting_loan_approve', 'filled_not_approved') then 'анкета заполнена'
        when status_1 in ('on_delivery', 'pending_pickup', 'sync_with_delivery', 'awaiting_parcel_ready', 'courier_delivery', 'awaiting_courier_pickup') then 'договор подписан'
        when status_1 in ('delivered', 'taken') then 'завершен'
        when status_1 = 'returned' then 'возврат'
        when status_1 like '%cancel%' then 'отменен' end as status_1_ru,
        case when status_2 = 'new' then 'создано'
        when status_2 in ('pending_approve', 'approved_not_filled') then 'анкета не заполнена'
        when status_2 in ('awaiting_loan_approve', 'filled_not_approved') then 'анкета заполнена'
        when status_2 in ('on_delivery', 'pending_pickup', 'sync_with_delivery', 'awaiting_parcel_ready', 'courier_delivery', 'awaiting_courier_pickup') then 'договор подписан'
        when status_2 in ('delivered', 'taken') then 'завершен'
        when status_2 = 'returned' then 'возврат'
        when status_2 like '%cancel%' then 'отменен' end as status_2_ru,
        case when status_3 = 'new' then 'создано'
        when status_3 in ('pending_approve', 'approved_not_filled') then 'анкета не заполнена'
        when status_3 in ('awaiting_loan_approve', 'filled_not_approved') then 'анкета заполнена'
        when status_3 in ('on_delivery', 'pending_pickup', 'sync_with_delivery', 'awaiting_parcel_ready', 'courier_delivery', 'awaiting_courier_pickup') then 'договор подписан'
        when status_3 in ('delivered', 'taken') then 'завершен'
        when status_3 = 'returned' then 'возврат'
        when status_3 like '%cancel%' then 'отменен' end as status_3_ru,
        case when status_4 = 'new' then 'создано'
        when status_4 in ('pending_approve', 'approved_not_filled') then 'анкета не заполнена'
        when status_4 in ('awaiting_loan_approve', 'filled_not_approved') then 'анкета заполнена'
        when status_4 in ('on_delivery', 'pending_pickup', 'sync_with_delivery', 'awaiting_parcel_ready', 'courier_delivery', 'awaiting_courier_pickup') then 'договор подписан'
        when status_4 in ('delivered', 'taken') then 'завершен'
        when status_4 = 'returned' then 'возврат'
        when status_4 like '%cancel%' then 'отменен' end as status_4_ru,
        case when status_5 = 'new' then 'создано'
        when status_5 in ('pending_approve', 'approved_not_filled') then 'анкета не заполнена'
        when status_5 in ('awaiting_loan_approve', 'filled_not_approved') then 'анкета заполнена'
        when status_5 in ('on_delivery', 'pending_pickup', 'sync_with_delivery', 'awaiting_parcel_ready', 'courier_delivery', 'awaiting_courier_pickup') then 'договор подписан'
        when status_5 in ('delivered', 'taken') then 'завершен'
        when status_5 = 'returned' then 'возврат'
        when status_5 like '%cancel%' then 'отменен' end as status_5_ru
        from(
        select distinct uid, pay_title, sender_title, product_merchant_id, total, cancelled_reason, updated_on, status_1,
        case when status_2 like '%cancel%' and status_3 is not null then status_3 else status_2 end as status_2,
        case when status_2 like '%cancel%' and status_3 is not null then status_4
        when status_3 like '%cancel%' and status_4 is not null then status_4 else status_3 end as status_3,
        case when status_2 like '%cancel%' and status_3 is not null then status_5
        when status_3 like '%cancel%' and status_4 is not null then status_5
        when status_4 like '%cancel%' and status_5 is not null then status_5 else status_4 end as status_4,
        case when status_2 like '%cancel%' and status_3 is not null then status_6
        when status_3 like '%cancel%' and status_4 is not null then status_6
        when status_4 like '%cancel%' and status_5 is not null then status_6
        when status_5 like '%cancel%' and status_6 is not null then status_6 else status_5 end as status_5,
        status_6, status_7,
        case when status_2 is null then 1 end as exclude_approve
        from(
        select distinct uid, pay_title, sender_title, product_merchant_id, total, 
        max(cancelled_reason) as cancelled_reason,
        max(status_1) as status_1,
        max(status_2) as status_2,
        max(status_3) as status_3,
        max(status_4) as status_4,
        max(status_5) as status_5,
        max(status_6) as status_6,
        max(status_7) as status_7,
        min(updated_on) as updated_on
        from(
        select distinct *,
        case when num = 1 then status end as status_1,
        case when num = 2 then status end as status_2,
        case when num = 3 then status end as status_3,
        case when num = 4 then status end as status_4,
        case when num = 5 then status end as status_5,
        case when num = 6 then status end as status_6,
        case when num = 7 then status end as status_7
        from(
        select distinct uid, status, status_ru, cancelled_reason1 as cancelled_reason, updated_on2, updated_on, pay_title1 as pay_title, product_merchant_id, sender_title, total,
        row_number() over(partition by uid, pay_title1 order by uid, updated_on2) num
        from(
        select distinct a.*, product_id, product_merchant_id, sender_title,
        case when status_ru = 'отменено' and cancelled_reason = 'active' then 'активная заявка'
        when status_ru = 'отменено' and bank_status = 'cancel' then 'отказ: скоринг'
        when status_ru = 'отменено' and cancelled_reason = 'CLIENT.DECLINE' then 'отменено клиентом'
        when status_ru = 'отменено' and cancelled_reason = 'error' then 'общие ошибки'
        when status_ru = 'отменено' and cancelled_reason = 'Таймаут на решении клиента' then 'отменено по timeout'
        when status_ru = 'отменено' and bank_status = 'rejected' then 'отказ: вн. проверки банка'
        when status like '%cancelledbybank%' then 'отменено банком'
        when status like '%cancelledbyclient%' then 'отменено клиентом'
        when status like '%cancelledbymerchant%' then 'отменено партнером'
        when status like '%cancelledbytimeout%' then 'отменено по timeout'
        when status like '%cancel%' then 'отменено' end as cancelled_reason1
        from(
        select distinct on (uid, pay_title1, status_ru) *
        from(
        select distinct uid, status, 
        case when (status = 'new' and paid is false) or (status = 'pending_approve' and paid is false and pay_types = 'COD') then 'создано'
        when status in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status in ('taken', 'delivered') then 'завершено'
        when status = 'returned' then 'возврат'
        when status like '%cancel%' then 'отменено' end as status_ru,
        case when pay_types = 'ACQUIRING' then 'Кредитная карта'
        when pay_types like '%FORTE_EXPRESS%' then 'Экспресс кредит от Форте'
        else pay_title end as pay_title1,
        case when common_old_price is null then common_price else common_old_price end as common_old_price,
        pay_title, to_char(updated_on, 'yyyy-mm-dd hh24:mi:ss')::timestamp as updated_on2, updated_on::date, pay_types, common_price as total, replace(replace(replace(user_mobile, '+', ''), '-', ''), ' ', '') as mobile, cancelled_reason, bank_status
        from dar_group.bazar_orders1
        where scope = 'fortemarket' and pay_types like '%FORTE_EXPRESS%') as a
        order by uid, pay_title1, status_ru, updated_on2 desc) as a
        join(
        select distinct uid, product_id, product_merchant_id, sender_title, (sku_price * sku_amount) as total
        from dar_group.bazar_package
        where product_merchant_id != '9VYpqYXvl77LkHvenw') as b on a.uid = b.uid and a.common_old_price = b.total) as a) as a) as a
        group by uid, pay_title, pay_title, sender_title, product_merchant_id, total) as a
        where status_1 = 'new' ) as a ) as a
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_credit_orders VALUES %s",
                       records)
        conn.commit()


def get_sellers():
    cur1.execute('truncate table marts.fm_sellers')

    cur.execute('''
        select distinct id, brand
        from dar_group.darbazar_merchants
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_sellers VALUES %s",
                       records)
        conn.commit()


def get_users():
    cur1.execute('truncate table marts.fm_users')

    cur.execute('''
        select profile_id, brand, id_space, state, identifier_id, min(created) as created, max(email) as email
        from (
        select profile_id, upper(brand) as brand, 'MOBILE'::text as id_space, 'ACTIVE'::text as state, identifier_id, email, created::date
        from dar_group.dar_account
        where lower(state) like '%active%' and lower(brand) = 'forte_market' and lower(id_space) like '%mobile%') as a
        group by profile_id, brand, id_space, state, identifier_id
        order by created
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_users VALUES %s",
                       records)
        conn.commit()


def get_productlist_prices():
    cur1.execute('truncate table marts.fm_productlist_prices')

    cur.execute('''
        select distinct a.*, b.minprice, b.maxprice, (minprice = maxprice) as sameprice
        from(
        select distinct sku_id, price, c.title as cat1, merchant_id, brand, name_ebt
        from(
        select distinct sku_id, merchant_id, price, a[1] as a, name_ebt, brand
        from(
        select distinct merchant_id, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as a, price, sku_id, name_ebt
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket' and is_visible is true and available is true) as a
        join (select distinct id, brand from dar_group.darbazar_merchants where state = 'ACTIVE' and id != '9VYpqYXvl77LkHvenw') as b on a.merchant_id = b.id) as a
        left join dar_group.darbazar_categories as c on c.id = a.a) as a
        left join(
        select distinct sku_id, min(price) as minprice, max(price) as maxprice
        from(
        select distinct price, sku_id
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket' and is_visible is true and available is true and merchant_id != '9VYpqYXvl77LkHvenw') as a
        group by sku_id) as b
        on a.sku_id = b.sku_id
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_productlist_prices VALUES %s",
                       records)
        conn.commit()


def get_product_prices():

    cur.execute('''
        select distinct a.*, 
        case when sameprice is false and sellers_maxprice = 1 then b.brand
        when sameprice is true then 'Цены одинаковые'
        else 'Несколько партнеров, см. реестр' end as brand_maxprice
        from(
        select distinct a.*, (maxprice - minprice) as diffprice, (minprice = maxprice) as sameprice, count(distinct b.merchant_id) as sellers_maxprice
        from(
        select distinct sku_id, cat1, a, name_ebt, min(price) as minprice, max(price) as maxprice, count(distinct merchant_id) as sellers
        from(
        select distinct sku_id, price, c.title as cat1, a, merchant_id, name_ebt
        from(
        select distinct sku_id, merchant_id, price, a[1] as a, name_ebt
        from(
        select distinct merchant_id, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as a, price, sku_id, name_ebt
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket' and is_visible is true and available is true) as a
        join (select distinct id from dar_group.darbazar_merchants where state = 'ACTIVE' and id != '9VYpqYXvl77LkHvenw') as b on a.merchant_id = b.id) as a
        left join dar_group.darbazar_categories as c on c.id = a.a) as a
        group by sku_id, cat1, a, name_ebt) as a
        left join(
        select distinct sku_id, merchant_id, price, name_ebt
        from(
        select distinct merchant_id, price, sku_id, name_ebt
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket' and is_visible is true and available is true and merchant_id != '9VYpqYXvl77LkHvenw') as a) as b
        on a.sku_id = b.sku_id and a.name_ebt = b.name_ebt and a.maxprice = b.price
        group by a.sku_id, a.cat1, a.a, a.name_ebt, a.maxprice, a.minprice, a.sellers) as a
        left join(
        select distinct sku_id, price, name_ebt, brand
        from(
        select distinct merchant_id, price, sku_id, name_ebt
        from dar_group.fm_nomenclature
        where sale_channels = 'fortemarket' and is_visible is true and available is true) as a
        join (select distinct id, brand from dar_group.darbazar_merchants where state = 'ACTIVE' and id != '9VYpqYXvl77LkHvenw') as b on a.merchant_id = b.id) as b
        on a.sku_id = b.sku_id and a.name_ebt = b.name_ebt and a.maxprice = b.price
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_product_prices VALUES %s",
                       records)
        conn.commit()


def get_passed_orders():
    cur1.execute('truncate table marts.fm_passed_orders')

    cur.execute('''
        select distinct uid, pay_title, sender_title, product_merchant_id, total, cancelled_reason, status_1_ru,    status_2_ru,    status_3_ru,    status_4_ru, postupilo_date, otmeneno_date, zaversheno_total, status_2_2 as status_1,    status_3_2 as status_2,    status_4_2 as status_3,    postupilo_vs_prinyato,    prinyato_vs_zaversheno
        from(
        select distinct *,
        (prinyato_date1 - postupilo_date) as postupilo_vs_prinyato,
        (zaversheno_date1 - prinyato_date1) as prinyato_vs_zaversheno
        from(
        select distinct *,
        case when zaversheno_date is not null then total end as zaversheno_total,
        case when prinyato_date is null and otmeneno_date is null then current_date else prinyato_date end as prinyato_date1,
        case when zaversheno_date is null and otmeneno_date is null then current_date else zaversheno_date end as zaversheno_date1,
        case when status_3 like '%cancel%' and status_3_ru != 'принято' then cancelled_reason
        when status_3 = 'courier_delivery' or (status_3 in ('awaiting_parcel_ready', 'sync_with_delivery', 'awaiting_courier_pickup') and status_4 = 'delivered') then 'казпочта: на доставке курьером'
        when status_3 = 'awaiting_parcel_ready' then 'казпочта: ожидает готовности посылки'
        when status_3 in ('awaiting_courier_pickup', 'sync_with_delivery') then 'казпочта: ожидает курьера'
        when status_2 = 'pending_pickup' or status_3 in ('pending_pickup', 'taken') then 'на выдаче'
        when status_2 = 'on_delivery' or status_3 in ('on_delivery', 'delivered') then 'на доставке'
        when status_2 = 'awaiting_loan_approve' then 'на подписании'
        when status_2 = 'approved_not_filled' then 'ожидаем анкету'
        when status_2 = 'filled_not_approved' then 'на партнере'
        when status_2 = 'pending_approve' and pay_title = 'Экспресс кредит от Форте' then 'одобрено'
        when status_2 = 'pending_approve' and pay_title != 'Экспресс кредит от Форте' then 'на партнере' end as status_2_2,
        case when status_3 like '%cancel%' and status_3_ru = 'принято' then cancelled_reason
        when status_4 like '%cancel%' then cancelled_reason
        when status_4 = 'taken' or status_3 = 'taken' then 'выдан'
        when status_4 = 'delivered' or status_3 = 'delivered' then 'доставлен' end as status_3_2,
        case when status_4 = 'returned' or status_5 = 'returned' then 'возвращен' end as status_4_2
        from (
        select distinct uid, pay_title, sender_title, product_merchant_id, total, 
        case when status_ru like '%отменено%' then cancelled_reason
        when status_ru like '%отказ%' then cancelled_reason
        when status_ru like '%общие ошибки%' then cancelled_reason
        when status_ru like '%активная заявка%' then cancelled_reason end as cancelled_reason,
        status_1, status_2, status_3, status_4, status_5, status_6, status_7, status_1_ru, 'оформлено'::text as status_2_ru, 
        case when status_2_ru = 'принято' then status_2_ru
        when status_3_ru = 'завершено' then 'принято' else status_3_ru end as status_3_ru,
        case when status_2_ru = 'принято' then status_3_ru
        when status_3_ru = 'завершено' then status_3_ru else status_4_ru end as status_4_ru,
        case when status_2_ru = 'принято' then status_4_ru
        when status_3_ru = 'завершено' then status_4_ru else status_5_ru end as status_5_ru,
        oformleno_date, postupilo_date, 
        prinyato_date, zaversheno_date, vozvrat_date, 
        case when status_ru like '%отменено%' then otmeneno_date
        when status_ru like '%отказ%' then otmeneno_date
        when status_ru like '%общие ошибки%' then otmeneno_date
        when status_ru like '%активная заявка%' then otmeneno_date end as otmeneno_date
        from(
        select distinct uid, pay_title, sender_title, product_merchant_id, total, cancelled_reason,
        'new'::text as status_1,
        case when status_1 = 'pending_approve' then status_1
        when status_1 = 'new' and status_2 like '%cancel%' and status_3 is not null then status_3
        else status_2 end as status_2,
        case when status_1 = 'pending_approve' and status_2 like '%cancel%' and status_3 is not null then status_3
        when status_1 = 'pending_approve' then status_2
        when status_1 = 'new' and status_2 like '%cancel%' and status_3 is not null then status_4
        when status_1 = 'new' and status_3 like '%cancel%' and status_4 is not null then status_4
        else status_3 end as status_3,
        case when status_1 = 'pending_approve' and status_2 like '%cancel%' and status_3 is not null then status_4
        when status_1 = 'pending_approve' and status_3 like '%cancel%' and status_4 is not null then status_4
        when status_1 = 'pending_approve' then status_3
        when status_1 = 'new' and status_2 like '%cancel%' and status_3 is not null then status_5
        when status_1 = 'new' and status_3 like '%cancel%' and status_4 is not null then status_5
        when status_1 = 'new' and status_4 like '%cancel%' and status_5 is not null then status_5
        else status_4 end as status_4,
        case when status_1 = 'pending_approve' and status_2 like '%cancel%' and status_3 is not null then status_5
        when status_1 = 'pending_approve' and status_3 like '%cancel%' and status_4 is not null then status_5
        when status_1 = 'pending_approve' and status_4 like '%cancel%' and status_5 is not null then status_5
        when status_1 = 'pending_approve' then status_4
        when status_1 = 'new' and status_2 like '%cancel%' and status_3 is not null then status_6
        when status_1 = 'new' and status_3 like '%cancel%' and status_4 is not null then status_6
        when status_1 = 'new' and status_4 like '%cancel%' and status_5 is not null then status_6
        when status_1 = 'new' and status_5 like '%cancel%' and status_6 is not null then status_6
        else status_5 end as status_5,
        status_6, status_7, status_1_ru, status_2_ru, status_3_ru, status_4_ru, status_5_ru,
        (status_1_ru || ',' || coalesce(status_2_ru,'') || ',' || coalesce(status_3_ru,'') || ',' || coalesce(status_4_ru,'') || ',' || coalesce(status_5_ru,''):: text) as status_ru,
        oformleno_date, 
        case when postupilo_date is null and prinyato_date is not null then prinyato_date 
        when status_1 = 'pending_approve' and postupilo_date is null then oformleno_date
        when prinyato_date is null and zaversheno_date is not null then zaversheno_date
        else postupilo_date end as postupilo_date, 
        case when prinyato_date is null and zaversheno_date is not null then zaversheno_date else prinyato_date end as prinyato_date,
        zaversheno_date, vozvrat_date, otmeneno_date
        from(
        select distinct *,
        case when status_1 = 'new' and status_2 like '%cancel%' and status_3 is null then cancelled_reason
        when status_1 = 'new' and status_2 like '%cancel%' and status_3 is not null then 
        (case when status_3 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_3 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_3 in ('taken', 'delivered') then 'завершено'
        when status_3 = 'returned' then 'возврат' end)
        when status_1 = 'pending_approve' then 'оформлено'
        else
        (case when status_2 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_2 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_2 in ('taken', 'delivered') then 'завершено'
        when status_2 = 'returned' then 'возврат' end) end as status_2_ru,
        case when status_1 = 'pending_approve' and status_2 like '%cancel%' and status_3 is null then cancelled_reason
        when status_1 = 'pending_approve' and status_2 like '%cancel%' and status_3 is not null then 
        (case when status_3 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_3 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_3 in ('taken', 'delivered') then 'завершено'
        when status_3 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_3 like '%cancel%' and status_4 is null then cancelled_reason
        when status_1 = 'new' and status_3 like '%cancel%' and status_4 is not null then 
        (case when status_4 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_4 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_4 in ('taken', 'delivered') then 'завершено'
        when status_4 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_2 like '%cancel%' and status_3 is not null then 
        (case when status_4 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_4 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_4 in ('taken', 'delivered') then 'завершено'
        when status_4 = 'returned' then 'возврат' end)
        when status_1 = 'pending_approve' then 
        (case when status_2 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_2 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_2 in ('taken', 'delivered') then 'завершено'
        when status_2 = 'returned' then 'возврат' end)
        else
        (case when status_3 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_3 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_3 in ('taken', 'delivered') then 'завершено'
        when status_3 = 'returned' then 'возврат' end) end as status_3_ru,
        case when status_1 = 'pending_approve' and status_3 like '%cancel%' and status_4 is null then cancelled_reason
        when status_1 = 'pending_approve' and status_3 like '%cancel%' and status_4 is not null then 
        (case when status_4 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_4 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_4 in ('taken', 'delivered') then 'завершено'
        when status_4 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_4 like '%cancel%' and status_5 is null then cancelled_reason
        when status_1 = 'new' and status_4 like '%cancel%' and status_5 is not null then 
        (case when status_5 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_5 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_5 in ('taken', 'delivered') then 'завершено'
        when status_5 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_2 like '%cancel%' and status_3 is not null then 
        (case when status_5 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_5 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_5 in ('taken', 'delivered') then 'завершено'
        when status_5 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_3 like '%cancel%' and status_4 is not null then 
        (case when status_5 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_5 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_5 in ('taken', 'delivered') then 'завершено'
        when status_5 = 'returned' then 'возврат' end)
        when status_1 = 'pending_approve' then 
        (case when status_3 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_3 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_3 in ('taken', 'delivered') then 'завершено'
        when status_3 = 'returned' then 'возврат' end)
        else
        (case when status_4 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_4 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_4 in ('taken', 'delivered') then 'завершено'
        when status_4 = 'returned' then 'возврат' end) end as status_4_ru,
        case when status_1 = 'pending_approve' and status_4 like '%cancel%' and status_5 is null then cancelled_reason
        when status_1 = 'pending_approve' and status_4 like '%cancel%' and status_5 is not null then 
        (case when status_5 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_5 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_5 in ('taken', 'delivered') then 'завершено'
        when status_5 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_5 like '%cancel%' and status_6 is null then cancelled_reason
        when status_1 = 'new' and status_5 like '%cancel%' and status_6 is not null then 
        (case when status_6 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_6 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_6 in ('taken', 'delivered') then 'завершено'
        when status_6 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_2 like '%cancel%' and status_3 is not null then 
        (case when status_6 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_6 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_6 in ('taken', 'delivered') then 'завершено'
        when status_6 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_3 like '%cancel%' and status_4 is not null then 
        (case when status_6 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_6 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_6 in ('taken', 'delivered') then 'завершено'
        when status_6 = 'returned' then 'возврат' end)
        when status_1 = 'new' and status_4 like '%cancel%' and status_5 is not null then 
        (case when status_6 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_6 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_6 in ('taken', 'delivered') then 'завершено'
        when status_6 = 'returned' then 'возврат' end)
        when status_1 = 'pending_approve' then 
        (case when status_4 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_4 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_4 in ('taken', 'delivered') then 'завершено'
        when status_4 = 'returned' then 'возврат' end)
        else
        (case when status_5 in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status_5 in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status_5 in ('taken', 'delivered') then 'завершено'
        when status_5 = 'returned' then 'возврат' end) end as status_5_ru
        from(
        select distinct uid, pay_title, sender_title, product_merchant_id, total,
        max(oformleno_date) as oformleno_date, max(postupilo_date) as postupilo_date, max(prinyato_date) as prinyato_date, max(zaversheno_date) as zaversheno_date, max(vozvrat_date) as vozvrat_date, max(otmeneno_date) as otmeneno_date,
        max(cancelled_reason) as cancelled_reason,
        max(status_1) as status_1, max(status_2) as status_2, max(status_3) as status_3, max(status_4) as status_4, max(status_5) as status_5, max(status_6) as status_6, max(status_7) as status_7,
        'создано' as status_1_ru
        from(
        select distinct *,
        case when num = 1 then status end as status_1,
        case when num = 2 then status end as status_2,
        case when num = 3 then status end as status_3,
        case when num = 4 then status end as status_4,
        case when num = 5 then status end as status_5,
        case when num = 6 then status end as status_6,
        case when num = 7 then status end as status_7,
        case when status_ru = 'создано' then updated_on end as oformleno_date,
        case when status_ru = 'оформлено' then updated_on end as postupilo_date,
        case when status_ru = 'принято' then updated_on end as prinyato_date,
        case when status_ru = 'завершено' then updated_on end as zaversheno_date,
        case when status_ru = 'возврат' then updated_on end as vozvrat_date,
        case when status_ru = 'отменено' then updated_on end as otmeneno_date
        from(
        select distinct uid, status, status_ru, cancelled_reason1 as cancelled_reason, updated_on2, updated_on, pay_title1 as pay_title, product_merchant_id, sender_title, total,
        row_number() over(partition by uid, pay_title1 order by uid, updated_on2) num
        from(
        select distinct a.*,
        case when status_ru = 'отменено' and cancelled_reason = 'active' then 'активная заявка'
        when status_ru = 'отменено' and bank_status = 'cancel' then 'отказ: скоринг'
        when status_ru = 'отменено' and cancelled_reason = 'CLIENT.DECLINE' then 'отменено клиентом'
        when status_ru = 'отменено' and cancelled_reason = 'error' then 'общие ошибки'
        when status_ru = 'отменено' and cancelled_reason = 'Таймаут на решении клиента' then 'отменено по timeout'
        when status_ru = 'отменено' and bank_status = 'rejected' then 'отказ: вн. проверки банка'
        when status like '%cancelledbybank%' then 'отменено банком'
        when status like '%cancelledbyclient%' then 'отменено клиентом'
        when status like '%cancelledbymerchant%' then 'отменено партнером'
        when status like '%cancelledbytimeout%' then 'отменено по timeout'
        when status like '%cancel%' then 'отменено' end as cancelled_reason1
        from(
        select distinct a.*, product_merchant_id, sender_title
        from(
        select distinct on (uid, pay_title1, status_ru) *
        from(
        select distinct uid, status, 
        case when (status = 'new' and paid is false) or (status = 'pending_approve' and paid is false and pay_types = 'COD') then 'создано'
        when status in ('new', 'pending_approve', 'awaiting_loan_approve', 'filled_not_approved', 'approved_not_filled') then 'оформлено'
        when status in ('pending_pickup', 'on_delivery', 'awaiting_parcel_ready', 'sync_with_delivery', 'courier_delivery', 'awaiting_courier_pickup'
        ) then 'принято'
        when status in ('taken', 'delivered') then 'завершено'
        when status = 'returned' then 'возврат'
        when status like '%cancel%' then 'отменено' end as status_ru,
        case when pay_types = 'ACQUIRING' then 'Кредитная карта'
        when pay_types like '%FORTE_EXPRESS%' then 'Экспресс кредит от Форте'
        else pay_title end as pay_title1,
        case when common_old_price is null then common_price else common_old_price end as common_old_price,
        to_char(updated_on, 'yyyy-mm-dd hh24:mi:ss')::timestamp as updated_on2, updated_on::date, pay_types, common_price as total, cancelled_reason, bank_status
        from dar_group.bazar_orders1
        where scope = 'fortemarket' and pay_types is not null and pay_types != 'CREDIT' and pay_title not in ('?????? ?????????', '? ?????????', 'Оплата в рассрочку', 'Оплата онлайн')) as a
        order by uid, pay_title1, status_ru, updated_on2 desc) as a
        join(
        select distinct uid, product_id, product_merchant_id, sender_title, (sku_price * sku_amount) as total
        from dar_group.bazar_package
        where product_merchant_id != '9VYpqYXvl77LkHvenw') as b on a.uid = b.uid and a.common_old_price = b.total) as a) as a) as a) as a
        group by uid, pay_title, sender_title, product_merchant_id, total) as a
        where status_1 in ('new', 'pending_approve')) as a) as a
        where postupilo_date is not null) as a) as a) as a
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_passed_orders VALUES %s",
                       records)
        conn.commit()


def get_categories():
    cur1.execute('truncate table marts.fm_categories')

    cur.execute('''
        select distinct cat1 as category,
        case when cat1 = 'Смартфоны, ТВ и электроника' then 'Смартфоны, ТВ и эл.'
        when cat1 = 'Аксессуары для телефонов' then 'Ак. для телефонов'
        when cat1 = ' Товары для дома и сада' then 'Товары для дома'
        when cat1 = 'Ноутбуки и компьютеры' then 'Ноутбуки и комп.'
        when cat1 = 'Одежда и аксессуары' then 'Одежда и ак.'
        else cat1 end as category_short
        from dar_group.fm_cat_n
        order by cat1
    ''')

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(cur1,
                       "INSERT INTO marts.fm_categories VALUES %s",
                       records)
        conn.commit()

    cur.close()
    cur1.close()
    conn.close()


t1 = PythonOperator(
    task_id = 'get_unidentified',
    python_callable = get_unidentified,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t2 = PythonOperator(
    task_id = 'get_orders',
    python_callable = get_orders,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t3 = PythonOperator(
    task_id = 'get_customer_profile',
    python_callable = get_customer_profile,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t4 = PythonOperator(
    task_id = 'get_sellers_count',
    python_callable = get_sellers_count,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t5 = PythonOperator(
    task_id = 'get_products_count',
    python_callable = get_products_count,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t6 = PythonOperator(
    task_id = 'get_enabled_disabled',
    python_callable = get_enabled_disabled,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t7 = PythonOperator(
    task_id = 'get_credit_orders',
    python_callable = get_credit_orders,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t8 = PythonOperator(
    task_id = 'get_sellers',
    python_callable = get_sellers,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t9 = PythonOperator(
    task_id = 'get_users',
    python_callable = get_users,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t10 = PythonOperator(
    task_id = 'get_productlist_prices',
    python_callable = get_productlist_prices,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t11 = PythonOperator(
    task_id = 'get_product_prices',
    python_callable = get_product_prices,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t12 = PythonOperator(
    task_id = 'get_passed_orders',
    python_callable = get_passed_orders,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t13 = PythonOperator(
    task_id = 'get_categories',
    python_callable = get_categories,
    dag = dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
    )


t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12 >> t13
