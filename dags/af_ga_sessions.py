from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
import psycopg2
from datetime import datetime
from datetime import timedelta

from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,10,13,22,0),
    'retries': 0,
    'provide_context': True
}

dag = DAG(
    dag_id="ga_sessions",
    default_args=args,
    schedule_interval='0 * * * *',
    tags=['Google Analytics']
    )

conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
cur = conn.cursor()

scope = 'https://www.googleapis.com/auth/analytics.readonly'
api_name='analytics'
api_version='v3'

def get_session(**kwargs):
    # service_email = 'fortemarket@bigdata-227510.iam.gserviceaccount.com'
    service_email = 'dwh-139@dardwh.iam.gserviceaccount.com'
    key_file = '/home/forte/airflow/key.p12'

    credentials = ServiceAccountCredentials.from_p12_keyfile(service_email, key_file, scopes=scope)
    http = credentials.authorize(Http())
    service = build(api_name, api_version, http=http)

    # accounts = service.management().accounts().list().execute()
    # profile_id = get_first_profile_id(service)
    profile_id = '180700913'
    records = service.data().ga().get(\
            ids='ga:' + profile_id,\
            start_date='today',\
            end_date='today',\
            dimensions='ga:date',\
            metrics='ga:sessions').execute()
    
    task_instance = kwargs['ti']
    task_instance.xcom_push(key="ga_data", value=records)

t1 = PythonOperator(
    task_id="get_google_analytics_data",
    python_callable=get_session,
    provide_context=True,
    dag=dag
)

def write_results(**kwargs):
    ti = kwargs['ti']
    results = ti.xcom_pull(task_ids='get_google_analytics_data',key='ga_data')
    try:
        for i in results['rows']:
            f1 = str(i[0][:4])+'-'+str(i[0][4:6])+'-'+str(i[0][6:8])
            f2 = i[1]
            cur.execute ("delete from dar_group.ga_fm_session where dates=current_date")
            conn.commit()
            cur.execute ("INSERT INTO dar_group.ga_fm_session(dates,sessions) VALUES (%s, %s)",(f1,f2))
            conn.commit()
    except:
        print('No results found')
    cur.close()
    conn.close()

t2 = PythonOperator(
    task_id="write_to_postgres",
    python_callable=write_results,
    provide_context=True,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t1 >> t2
