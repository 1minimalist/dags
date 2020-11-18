from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import psycopg2

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,10,5),
}

dag = DAG(
    dag_id='fm_count_products_v2',
    default_args=args,
    schedule_interval='30 23 * * *',
    tags=['Fortemarket']
)

def count_products():
    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    cur = conn.cursor()
    cur.execute('delete from dar_group.fm_count_products_v2 where date = current_date')
    conn.commit()

    cur.execute("""insert into dar_group.fm_count_products_v2
                    select *
                    from(
                    select distinct date, trim(cat1) as cat1, trim(cat2) as cat2, trim(cat3) as cat3, trim(cat4) as cat4, trim(cat5) as cat5,
                    sum(case when base = 'Кол-во товаров в ЕБТ' then sku_id else 0 end) as sku_ebt,
                    sum(case when base = 'Кол-во всех товаров в номенклатуре' then sku_id else 0 end) as sku_nomenclature,
                    sum(case when base = 'Кол-во товаров в номенклатуре' then sku_id else 0 end) as sku_available,
                    sum(case when base = 'Кол-во активных товаров номенклатур' then sku_id else 0 end) as sku_active,
                    sum(sku_id) as sku_all, merchant_id, product_name, updated_on, brand, state
                    from(
                    select distinct date, ord, base, sku_id, cat1, cat2,
                    case when cat2 = cat3 then cat4
                    when cat3 is null and cat4 is not null then cat4
                    else cat3 end as cat3,
                    case when cat2 = cat3 then cat5
                    when cat3 is null and cat4 is not null then cat5
                    when cat4 is null and cat5 is not null then cat5
                    else cat4 end as cat4,
                    case when cat2 = cat3 then null
                    when cat3 is null and cat4 is not null then null
                    when cat4 is null and cat5 is not null then null
                    else cat5 end as cat5, merchant_id, product_name, updated_on, brand, state
                    from(
                    select a.*, 
                    case when cat1.title is null and cat2.title is not null then cat2.title
                    when cat1.title = ' Товары для дома и сада' and cat3.title = 'Смесители' then 'Сантехника'
                    when cat2.title = 'Бытовая техника' then 'Бытовая техника'
                    when cat2.title = 'Музыкальные инструменты' then 'Досуг'
                    when cat2.title = 'Аксессуары для компьютеров' then 'Ноутбуки и компьютеры'
                    when cat4.title = 'Видеорегистраторы' then 'Автотовары'
                    when cat3.title = 'Мангалы и грили' then 'Активный отдых и спорт '
                    when cat3.title = 'Мониторы' then 'Ноутбуки и компьютеры'
                    else cat1.title end as cat1,
                    case when cat1.title is null and cat2.title is not null then cat3.title
                    when cat1.title = ' Товары для дома и сада' and cat3.title = 'Смесители' then 'Смесители'
                    when cat2.title = 'Бытовая техника' and cat3.title = 'Весы кухонные' then 'Мелкая бытовая техника для кухни'
                    when cat2.title = 'Бытовая техника' and cat3.title = 'Весы напольные' then 'Мелкая бытовая техника для дома'
                    when cat2.title = 'Бытовая техника' and cat3.title = 'Холодильники' then 'Крупная бытовая техника'
                    when cat2.title = 'Бытовая техника' and cat3.title = 'Техника по уходу за собой' then 'Техника по уходу за собой'
                    when cat2.title = 'Бытовая техника' then null
                    when cat3.title = 'Пневматический инструмент' then 'Пневматический инструмент'
                    when cat4.title = 'Видеорегистраторы' then 'Автоэлектроника'
                    when cat3.title = 'Мангалы и грили' then 'Кемпинг и туризм '
                    when cat3.title = 'Блоки бесперебойного питания' then 'Периферия'
                    when cat3.title = 'Компьютерные мыши' then 'Периферия'
                    when cat3.title = 'Мониторы' then 'Периферия'
                    when cat3.title = 'Ноутбуки и нетбуки' then 'Компьютеры'
                    when cat3.title = 'Палки для ходьбы' then 'Летний спорт'
                    else cat2.title end as cat2,
                    case when cat1.title is null and cat2.title is not null then cat4.title
                    when cat1.title = ' Товары для дома и сада' and cat3.title = 'Смесители' then null
                    when cat2.title = 'Бытовая техника' and cat3.title = 'Техника по уходу за собой' then null
                    when cat3.title = 'Пневматический инструмент' then null
                    when cat2.title = cat3.title then cat4.title
                    when cat4.title = 'Видеорегистраторы' then 'Видеорегистраторы'
                    else cat3.title end as cat3,
                    case when cat1.title is null and cat2.title is not null then cat5.title
                    when cat2.title = cat3.title then cat5.title
                    when cat4.title = 'Видеорегистраторы' then null
                    when cat3.title = cat4.title then cat5.title
                    else cat4.title end as cat4,
                    case when cat1.title is null and cat2.title is not null then null
                    when cat2.title = cat3.title then null
                    when cat3.title = cat4.title then null
                    else cat5.title end as cat5
                    from(
                    select distinct date, ord, base, category[1] as category_1, category[2] as category_2, category[3] as category_3, category[4] as category_4, category[5] as category_5, sku_id, a.merchant_id, product_name, updated_on, brand, state
                    from(
                    select 1 as ord,'Кол-во товаров в ЕБТ'::text as base, count(distinct id) as sku_id, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as category, current_date as date, merchant_id, name_ as product_name, updated_on
                    from dar_group.darbazar_products
                    where scope_ = '{darbazar}' and created_on >= '2018-10-01'
                    group by categories_array, merchant_id, name_, updated_on
                    union
                    select 2 as ord, 'Кол-во всех товаров в номенклатуре'::text as base, count(distinct product_id) as sku_id, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as category, current_date as date, merchant_id, name_ebt as product_name, updated_on
                    from dar_group.fm_nomenclature
                    where sale_channels = 'fortemarket'
                    group by categories_array, merchant_id, name_ebt, updated_on
                    union
                    select 3 as ord, 'Кол-во товаров в номенклатуре'::text as base, count(distinct product_id) as sku_id, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as category, current_date as date, merchant_id, name_ebt as product_name, updated_on
                    from dar_group.fm_nomenclature
                    where sale_channels = 'fortemarket' and available is true
                    group by categories_array, merchant_id, name_ebt, updated_on
                    union
                    select 4 as ord, 'Кол-во активных товаров номенклатур'::text as base, count(distinct product_id) as sku_id, regexp_split_to_array(replace(replace(categories_array,'{',''),'}',''),',') as category, current_date as date, merchant_id, name_ebt as product_name, updated_on
                    from dar_group.fm_nomenclature
                    where sale_channels = 'fortemarket' and available is true and is_visible is true
                    group by categories_array, merchant_id, name_ebt, updated_on) as a
                    left join dar_group.darbazar_merchants as b on a.merchant_id = b.id) as a
                    left join dar_group.darbazar_categories as cat1 on a.category_1 = cat1.id
                    left join dar_group.darbazar_categories as cat2 on a.category_2 = cat2.id
                    left join dar_group.darbazar_categories as cat3 on a.category_3 = cat3.id
                    left join dar_group.darbazar_categories as cat4 on a.category_4 = cat4.id
                    left join dar_group.darbazar_categories as cat5 on a.category_5 = cat5.id)as a) as a
                    where cat1 is not null
                    group by date, cat1, cat2, cat3, cat4, cat5, merchant_id, product_name, updated_on, brand, state) as a
                    where sku_all > 0 and cat1 != 'Архив'
            """)
    conn.commit()

t1 = PythonOperator(task_id='count_products',python_callable=count_products,dag=dag,\
                    email_on_failure=True,email='dnurtailakov@one.kz')