# -*- coding: utf-8 -*- 
import csv
import psycopg2
from datetime import datetime
from openpyxl import Workbook

from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,9,29),
    'retries': 0,
}

dag = DAG(
    dag_id="bpm_process_flow",
    default_args=args,
    schedule_interval='00 22 * * *',
    tags=['BPM']
    )

def load_bpm_report():
    query = '''with comp_state as 
                (select th1.all_step as completed_state, th1.proc_def_id_ from dar_group.dim_bpm_business_task_history th1
                RIGHT JOIN (select --th.all_step as completed_state, 
                    max(th.date_end_t) as date_end_t, th.proc_def_id_
                from dar_group.dim_bpm_business_task_history th where th.completed = 'true'
                and th.proc_def_id_ is not null and th.all_step is not null and th.date_end_t is not null and application_num is not null
                GROUP BY  th.proc_def_id_
                ) th2 on th1.proc_def_id_ = th2.proc_def_id_ and th1.date_end_t = th2.date_end_t)
                SELECT
                    tah.application_num,
                    tah.start_dtime,
                    tah.process_name,
                    tah.proc_inst_id_,
                    th.task_name,
                    th.due_dtime,
                    th.create_dtime,
                    tah.end_dtime,
                    emp_curr.employeenumber as curr_employeenumber,
                    th.user_state AS th_curr_fullname,
                    emp_curr.fullname as curr_fullname,
                    emp_curr.position_code as curr_position_code,
                    emp_curr.position_name as curr_position_name,
                    emp_curr.specialization_code as curr_specialization_code,
                    emp_curr.specialization_name as curr_specialization_name,
                    emp_curr.department_code as curr_department_code,
                    emp_curr.department_name as curr_department_name,
                    emp_init.employeenumber AS init_employeenumber,
                    tah.init_username,
                    emp_init.fullname AS init_fullname,
                    emp_init.position_code AS init_position_code,
                    emp_init.position_name AS init_position_name,
                    emp_init.specialization_code AS init_specialization_code,
                    emp_init.specialization_name AS init_specialization_name,
                    emp_init.department_code as init_department_code,
                    emp_init.department_name as init_department_name,
                    emp_init.branch_code as init_branch_code,
                    emp_init.branch_name as init_branch_name,
                    case when tah.state = 'true'
                            then 'COMPLETED'
                            when tah.state = 'false'
                            then 'ACTIVE'
                            else tah.state
                            end    as state,
                    
                    case when tah.state = 'true'
                            then th_ct.completed_state 
                            
                    end as completed_state,
                    
                    inc.incident_msg_,
                    inc.error_dtime
                
                from dar_group.dim_bpm_business_task_agg_hist tah
                  full JOIN ( select th.all_step as task_name, th.proc_def_id_, th.due_dtime, th.create_dtime, th.user_state, th.initiator, th.proc_start_date from dar_group.dim_bpm_business_task_history th where --th.date_end_t is null and th.proc_def_id_ is not null 
                    th.completed = 'false' and th.application_num is not null
                    ) AS th 
                    ON tah.proc_inst_id_ = th.proc_def_id_ 
                
                LEFT JOIN
                (    SELECT
                                emp.username,
                                emp.branch_name,
                                emp.fullname,
                                emp.position_name,
                                emp.specialization_name,
                                emp.department_name,
                                emp.position_code,
                                emp.employeenumber,
                                emp.department_code,
                                emp.specialization_code 
                            FROM
                                dar_group.dim_bpm_employee_profile emp 
                            ) AS emp_curr ON th.user_state = emp_curr.username
                 LEFT JOIN
                (    SELECT
                                emp.username,
                                emp.branch_name,
                                emp.fullname,
                                emp.position_name,
                                emp.specialization_name,
                                emp.department_name,
                                emp.position_code,
                                emp.employeenumber,
                                emp.department_code,
                                emp.specialization_code,
                                emp.branch_code
                            FROM
                                dar_group.dim_bpm_employee_profile emp 
                            ) AS emp_init ON tah.init_username = emp_init.username
                LEFT JOIN (
                            SELECT i.incident_msg_, e.root_proc_inst_id_, i.incident_timestamp_ as error_dtime
                        FROM dar_group.bpm_act_ru_incident i, dar_group.bpm_act_ru_execution e where i.proc_inst_id_ = e.id_ --AND e.root_proc_inst_id_ = tah.proc_inst_id_
                            ) AS inc ON tah.proc_inst_id_ = inc.root_proc_inst_id_
                            
                    LEFT JOIN comp_state as th_ct on tah.proc_inst_id_ = th_ct.proc_def_id_ 
                    
                GROUP BY
                tah.application_num,
                    tah.start_dtime,
                    tah.process_name,
                    tah.proc_inst_id_,
                    th.task_name,
                    th.due_dtime,
                    th.create_dtime,
                    th_curr_fullname,                                      
                    curr_employeenumber,
                    curr_fullname,
                    curr_position_code,
                    curr_position_name,
                    curr_specialization_code,
                    curr_specialization_name,
                    curr_department_code,
                    curr_department_name,
                    tah.init_username,
                    init_employeenumber,
                    init_fullname,
                    init_position_code,
                    init_position_name,
                    init_specialization_code,
                    init_specialization_name,
                    init_department_code,
                    init_department_name,
                    init_branch_code,
                    init_branch_name,
                    state,
                    tah.end_dtime,
                    completed_state,
                    inc.incident_msg_,
                    inc.error_dtime,
                    tah.state
                    
                order by start_dtime desc'''
    src_conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
    dest_conn = PostgresHook(postgres_conn_id='pgConn_bpm').get_conn()
    dest_cursor = dest_conn.cursor()

    cur = src_conn.cursor()
    dest_cursor.execute('truncate table bpm.fct_process_flow_new')
    cur.execute(query)

    while True:
        records = cur.fetchall()
        if not records:
            break
        execute_values(dest_cursor,
                       "INSERT INTO bpm.fct_process_flow_new VALUES %s",
                       records)
        dest_conn.commit()

    dest_cursor.execute('select * from bpm.fct_process_flow_new')
    rec1 = dest_cursor.fetchall()

    wb = Workbook()
    ws = wb.active
    ws.append(['№ заявки','Дата старта процесса','Наименование процесса','Уникальный идентификатор заявки','Название шага, на котором находится заявка', \
                'Срок исполнения заявки на шаге, на котором находится заявка','Дата и время поступления заявки на шаг, на котором находится заявка','Дата завершения', \
                'ID сотрудника исполнителя','Логин пользователя, на шаге которого находится заявка','ФИО пользователя, на шаге которого находится заявка', \
                'ID должности сотрудника, на шаге которого находится заявка','Звание пользователя, на шаге которого находится заявка', \
                'ID специализации сотрудника, на шаге которого находится заявка','Направление пользователя, на шаге которого находится заявка', \
                'ID подразделения сотрудника, на шаге которого находится заявка','Департамент пользователя, на шаге которого находится заявка','ID сотрудника инициатора', \
                'Логин автора заявки','ФИО автора заявки','ID должности сотрудника инициатора','Звание автора заявки','ID специализации сотрудника инициатора','Направление автора заявки', \
                'ID подразделения сотрудника инициатора','Департамент автора заявки','ID ГБ/Филиал','ГБ/Филиал','Статус','Последний шаг заявки','Текст  ошибки (Message)','Дата и время ошибки'])

    
    for row in rec1:
        ws.append(row)
    wb.save('/tmp/BPM_report_new.xlsx')

    cur.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()

t1 = PythonOperator(
    task_id="load_bpm_report",
    python_callable=load_bpm_report,
    dag=dag,
    email_on_failure = True,
    email = 'aakhmetov@one.kz'
)

def build_email(**context):
    with open('/tmp/BPM_report_new.xlsx', mode='r') as file:
        email_op = EmailOperator(
            task_id='send_email',
            to=['bpma@fortebank.com','bpm@one.kz','aakhmetov@one.kz', 'azhaparov@one.kz'],
            subject="Daily BPM report",
            html_content='Hello, <br/>',
            files=[file.name],
        )
        email_op.execute(context)

t2 = PythonOperator(
    task_id="send_email",
    python_callable=build_email,
    provide_context=True,
    dag=dag
)

def form_report():
    return 'BPM report generated and sent at {0}'.format(datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S'))

t3 = SlackWebhookOperator(
        task_id='send_slack_notification',
        http_conn_id='slack_connection',
        message=form_report(),
        # files = '/tmp/BPM_report_new.xlsx',
        channel='#reports',
        dag=dag
    )

t1 >> t2 >> t3
