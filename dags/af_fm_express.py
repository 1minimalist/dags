from cassandra.cluster import Cluster as cl
import json
import psycopg2

from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,10,18),
    'retries': 0,
}

dag = DAG(
    dag_id="fm_express_loan",
    default_args=args,
    schedule_interval='0 07 * * *',
    tags=['Forte Express']
    )

def read_data():
    cluster = cl(['10.103.5.51', '10.103.5.52', '10.103.5.53'])
    session = cluster.connect('darbiz')

    conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    cur = conn.cursor()
    cur.execute ("truncate table dar_group.fm_express_loan;")

    rows = session.execute("SELECT * from darbiz.forte_express_loan_requests")

    for user_row in rows:
        d = json.loads(user_row.loan_request)

        id0 = d['profile_id'] if 'profile_id' in d else None
        id1 = user_row.order_id
        id2 = user_row.created_on
        id3 = d['person_info']['surname'] if 'surname' in d['person_info'] else None
        id4 = d['person_info']['name'] if 'name' in d['person_info'] else None
        id5 = d['person_info']['patronymic'] if 'patronymic' in d['person_info'] else None
        id6 = d['person_info']['birthdate'] if 'birthdate' in d['person_info'] else None
        id7 = d['person_info']['sex'] if 'sex' in d['person_info'] else None
        id8 = d['person_info']['iin'] if 'iin' in d['person_info'] else None
        id9 = d['person_info']['mobile_phone'] if 'mobile_phone' in d['person_info'] else None
        id10 = user_row.status
        id11 = d['status'] if 'status' in d else None

        # wage = d['person_info']['wage'] if 'wage' in d['person_info'] else None
        # chp = d['person_info']['childr                        
        # pr = d['person_info']['financing_info']['product'] if 'product' in d['person_info']['financing_info'] else None
        # psc = d['person_info']['financing_info']['product_sub_code'] if 'product_sub_code' in d['person_info']['financing_info'] else None
        # ps = d['person_info']['financing_info']['sum'] if 'sum' in d['person_info']['financing_info'] else None
        # pp = d['person_info']['financing_info']['period'] if 'period' in d['person_info']['financing_info'] else None
        # lt = d['person_info']['financing_info']['loan_type'] if 'loan_type' in d['person_info']['financing_info'] else None
        # fp = d['person_info']['financing_info']['fin_purpose'] if 'fin_purpose' in d['person_info']['financing_info'] else None
        # pd = d['person_info']['product_info']['description'] if 'description' in d['person_info']['product_info'] else None
        # ppr = d['person_info']['product_info']['price'] if 'price' in d['person_info']['product_info'] else None
        # pc = d['person_info']['product_info']['category'] if 'category' in d['person_info']['product_info'] else None

        # ms = d['person_info']['marital_status'] if 'marital_status' in d['person_info'] else None
        # dty = d['person_info']['document_id'] if 'document_id' in d['person_info'] else None
        # ib = d['person_info']['issued_by'] if 'issued_by' in d['person_info'] else None
        # idt = d['person_info']['issued_date'] if 'issued_date' in d['person_info'] else None
        # vdt = d['person_info']['validity_date'] if 'validity_date' in d['person_info'] else None
        # pob = d['person_info']['place_of_birth_id'] if 'place_of_birth_id' in d['person_info'] else None
        # pobn = d['person_info']['place_of_birth_name'] if 'place_of_birth_name' in d['person_info'] else None
        # rpi = d['person_info']['reg_postal_index'] if 'reg_postal_index' in d['person_info'] else None
        # rl = d['person_info']['reg_locality'] if 'reg_locality' in d['person_info'] else None
        # rmd = d['person_info']['reg_microdistrict'] if 'reg_microdistrict' in d['person_info'] else None
        # rs = d['person_info']['reg_street'] if 'reg_street' in d['person_info'] else None
        # rhn = d['person_info']['reg_house_number'] if 'reg_house_number' in d['person_info'] else None
        # rlw = d['person_info']['reg_live_with'] if 'reg_live_with' in d['person_info'] else None

        try:
            cur.execute ("INSERT INTO dar_group.fm_express_loan(profile_id,order_id,created_on,surname,name,patronymic,birthdate,sex,iin,mobile_phone,status1,status2) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
            (id0,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11))

            conn.commit()
            # cur.execute ("INSERT INTO dar_group.fm_express_loan(profile_id,order_id,created_dtime,last_name,name,mid_name,birth_date,\
            #         gender,iin,mobile,status) \
            #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
            #         (id0,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10#, \
                        # pr,psc,pd,pc,ppr,pp,lt,ps,fp,\
                            # req_ip,psi,wage,chp,\
                            #     ms,dty,ib,idt,vdt,pob,pobn,rpi,rl,rmd,rs,rhn,rlw
                                # ))
                    # product_code,product_sub_code,product_name,product_cat,product_price,loan_period,loan_type, \
                    # loan_sum,fin_purpose, \
                    #  request_ip,profile_size,wage,has_children, \
                    #  marital_status,document_id,issued_by,issued_date,valid_date,birth_place_id,birth_place, \
                    #  post_index,locality,microdistrict,street,house_num,live_with
                    # ) \
                    # %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    #, %s, %s, %s, %s, \
                        # %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",

        except Exception as e:
            print(e)

    cur.close()
    conn.close()

t2 = PythonOperator(
    task_id="read_cassandra",
    python_callable=read_data,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)