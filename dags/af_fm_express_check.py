from cassandra.cluster import Cluster as cl
import json
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,22),
    'retries': 0,
    'provide_context': True
}

dag = DAG(
    dag_id="fm_express_loan_check",
    default_args=args,
    schedule_interval='00 0,12 * * *',
    tags=['Forte Express']
    )

def read_data(**kwargs):
    cluster = cl(['10.103.5.51', '10.103.5.52', '10.103.5.53'])
    session = cluster.connect('darbiz')

    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    cur = conn.cursor()

    rows = session.execute("SELECT * from darbiz.forte_express_loan_requests where created_on>='2020-09-20' allow filtering")
    cur.execute ("select distinct owner profile_id, uid order_id, pay_title from dar_group.bazar_orders1 where created_on>=now()-interval '24' hour")
    res = cur.fetchall()
    for user_row in rows:
        d = json.loads(user_row.loan_request)
        id0 = user_row.profile_id
        id1 = user_row.order_id
        id2 = user_row.created_on
        pp = d['person_info']['financing_info']['period'] if 'period' in d['person_info']['financing_info'] else None
        lh = datetime.now() - timedelta(hours = 12)
        if id2>=lh:
            for a,b,c in res:
                ll=c.split()
                if id1==b:
                    if pp!=int(ll[2]):
                        email = EmailOperator(\
                            task_id='send_email',\
                            to=['dnurtailakov@one.kz','azhaparov@one.kz'],\
                            subject='Ошибка в Fortemarket',\
                            html_content='Error in order_id: {} created at: {}, profile_id: {}, months in request: {}, months in orders: {}\n' \
                                .format(a, id2, b, pp, ll[2])\
                            )
                        email.execute(context=kwargs)

                        t3 = SlackWebhookOperator(
                            task_id='send_slack_notification',
                            http_conn_id='slack_connection',
                            message='Error in order_id: {} created at: {}, profile_id: {}, months in request: {}, months in orders: {}\n' \
                                .format(a, id2, b, pp, ll[2]),\
                            # files = '/tmp/BPM_report.xlsx',
                            channel='#reports',\
                            dag=dag
                        )
                        t3.execute(context=kwargs)
                else:
                    continue
        else:
            continue
        # lt = d['person_info']['financing_info']['loan_type'] if 'loan_type' in d['person_info']['financing_info'] else None

    cur.close()
    conn.close()

t2 = PythonOperator(
    task_id="load_data_to_compare",
    python_callable=read_data,
    dag=dag,
    provide_context=True,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t2