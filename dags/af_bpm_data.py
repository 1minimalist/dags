# -*- coding: utf-8 -*- 
import elasticsearch
import psycopg2
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2020,10,1),
    'retries': 0,
}

dag = DAG(
    dag_id="bpm_data",
    default_args=args,
    schedule_interval='30 20 * * *',
    tags=['BPM']
    )

#src_conn = PostgresHook(postgres_conn_id='pgConn_camunda').get_conn()
dest_conn = PostgresHook(postgres_conn_id='pgConn_pg').get_conn()
bpm_conn = PostgresHook(postgres_conn_id='pgConn_bpm').get_conn()
dest_cursor = dest_conn.cursor()
#cursor1 = src_conn.cursor()
bpm_cur = bpm_conn.cursor()


def get_bpm_business_task_history():
    def read_page(page, table):
        d = page['hits']['hits']
        for ss in d:
            source = ss['_source']
            # try:
            temp_dict = {
                '_id': ss['_id'],
                'taskName': source['taskName'] if 'taskName' in source else None,
                'dueDate': source['dueDate'] if 'dueDate' in source else None,
                'taskStartDate': source['taskStartDate'] if 'taskStartDate' in source else None,
                'taskEndDate': source['taskEndDate'] if 'taskEndDate' in source else None,
                'assignee': source['assignee'] if 'assignee' in source else None,
                'rootProcessInstanceId': source['rootProcessInstanceId'] if 'rootProcessInstanceId' in source else None,
                'applicationNumber': source['applicationNumber'] if 'applicationNumber' in source else None,
                'processDefinitionName': source['processDefinitionName'] if 'processDefinitionName' in source else None,
                'activityInstanceId': source['activityInstanceId'] if 'activityInstanceId' in source else None,
                'initiator': source['initiator'] if 'initiator' in source else None,
                'processStartDate': source['processStartDate'] if 'processStartDate' in source else None,
                'completed': source['completed'] if 'completed' in source else None
            }
            table.append(temp_dict)

    def read_catalog():
        # get elasticsearch
        es = elasticsearch.client.Elasticsearch(["http://10.0.72.76:9200"])
        # crate list with page scrolling result
        pages = list()
        # search parameters
        page_size = 1000
        page = es.search(
            index='business_task_history',
            doc_type='business_task',
            scroll='3m',
            body={
                "from": 0,
                "size": page_size,
                "sort": "_id"
            }
        )
        # print(page)
        # get result of scrolling of the first 1000
        read_page(page, pages)

        # get scroll)id
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        chunks_count = round(scroll_size / page_size)
        for i in range(0, int(chunks_count)):
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            read_page(page, pages)
        print(len(pages))
        for j in pages:
            id1 = j['_id']
            id2 = j['taskName']
            id3 = j['dueDate']
            id4 = j['taskStartDate']
            id5 = j['taskEndDate']
            id6 = j['assignee']
            id7 = j['rootProcessInstanceId']
            id8 = j['applicationNumber']
            id9 = j['processDefinitionName']
            id10 = j['activityInstanceId']
            id11 = j['initiator']
            id12 = j['processStartDate']
            id13 = j['completed']

            dest_cursor.execute(
                "INSERT INTO dar_group.dim_bpm_business_task_history(step_id, all_step, due_dtime, create_dtime, date_end_t, user_state, proc_def_id_, application_num, process_name, activity_inst_id, initiator, proc_start_date, completed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13))
            dest_conn.commit()
            bpm_cur.execute(
                "INSERT INTO bpm.dim_bpm_business_task_history(step_id, all_step, due_dtime, create_dtime, date_end_t, user_state, proc_def_id_, application_num, process_name, activity_inst_id, initiator, proc_start_date, completed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13))
            bpm_conn.commit()

    dest_cursor.execute("truncate table dar_group.dim_bpm_business_task_history")
    bpm_cur.execute("truncate table bpm.dim_bpm_business_task_history")
    read_catalog()

def get_bpm_business_task_agg_hist():
    def read_page(page, table):
        d = page['hits']['hits']
        for ss in d:
            source = ss['_source']
            # try:
            temp_dict = {
                '_id': ss['_id'],
                'applicationNumber': source['applicationNumber'] if 'applicationNumber' in source else None,
                'startTime': source['startTime'] if 'startTime' in source else None,
                'processDefinitionName': source['processDefinitionName'] if 'processDefinitionName' in source else None,
                'startUserId': source['startUserId'] if 'startUserId' in source else None,
                'completed': source['completed'] if 'completed' in source else None,
                'endTime': source['endTime'] if 'endTime' in source else None,
                'processInstanceId': source['processInstanceId'] if 'processInstanceId' in source else None,
                'processDefinitionId': source['processDefinitionId'] if 'processDefinitionId' in source else None,
                'assignmentList': source['assignmentList'] if 'assignmentList' in source else None,
                'deleteReason': source['deleteReason'] if 'deleteReason' in source else None
            }
            table.append(temp_dict)

    def read_catalog():
        # get elasticsearch
        es = elasticsearch.client.Elasticsearch(["http://10.0.72.76:9200"])
        # crate list with page scrolling result
        pages = list()
        # search parameters
        page_size = 1000
        page = es.search(
            index='business_task_aggregate_history',
            doc_type='business_task_aggregate',
            scroll='3m',
            body={
                "from": 0,
                "size": page_size,
                "sort": "_id"
            }
        )
        # print(page)
        # get result of scrolling of the first 1000
        read_page(page, pages)

        # get scroll)id
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        chunks_count = round(scroll_size / page_size)
        for i in range(0, int(chunks_count)):
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            read_page(page, pages)
        print(len(pages))
        for j in pages:
            id1 = j['_id']
            id2 = j['applicationNumber']
            id3 = j['startTime']
            id4 = j['processDefinitionName']
            id5 = j['startUserId']
            id6 = j['completed']
            id7 = j['endTime']
            id8 = j['processInstanceId']
            id9 = j['processDefinitionId']
            id10 = j['assignmentList']
            id11 = j['deleteReason']

            # print(id1,id2,id3,id4,id5,id6,id7,id8,id9,id10)

            dest_cursor.execute(
                "INSERT INTO dar_group.dim_bpm_business_task_agg_hist(id, application_num, start_dtime, process_name, init_username, state, end_dtime, proc_inst_id_, proc_def_id_, assignment_list, delete_reason_ ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11))
            dest_conn.commit()
            bpm_cur.execute(
                "INSERT INTO bpm.dim_bpm_business_task_agg_hist(id, application_num, start_dtime, process_name, init_username, state, end_dtime, proc_inst_id_, proc_def_id_, assignment_list, delete_reason_ ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11))
            bpm_conn.commit()

    dest_cursor.execute("truncate table dar_group.dim_bpm_business_task_agg_hist")
    bpm_cur.execute("truncate table bpm.dim_bpm_business_task_agg_hist")
    read_catalog()
    
def get_bpm_emp_profile():
    def read_page(page, table):
        d = page['hits']['hits']
        for ss in d:
            source = ss['_source']
            temp_dict = {           
                'id': ss['_id'],
                'username': source['username'] if 'username' in source else None,
                'fullName': source['fullName'] if 'fullName' in source else None,
                'birthday': source['birthday'] if 'birthday' in source else None,
                'employeeNumber': source['employeeNumber'] if 'employeeNumber' in source else None,
                'employeeHeadNumber': source['employeeHeadNumber'] if 'employeeHeadNumber' in source else None,
                'employeeContractNumber': source['employeeContractNumber'] if 'employeeContractNumber' in source else None,
                'employeeContractDate': source['employeeContractDate'] if 'employeeContractDate' in source else None,
                'hireDate': source['hireDate'] if 'hireDate' in source else None,
                'iin': source['iin'] if 'iin' in source else None,
                'mobilePhoneNumber': source['mobilePhoneNumber'] if 'mobilePhoneNumber' in source else None,
                'position_code': source['position']['code'] if 'code' in source['position'] else None,
                'position_name': source['position']['name'] if 'name' in source['position'] else None,
                'specialization_code': source['specialization']['code'] if 'code' in source['specialization'] else None,
                'specialization_name': source['specialization']['name'] if 'name' in source['specialization'] else None,
                'specialization_parentCode': source['specialization']['parentCode'] if 'parentCode' in source['specialization'] else None,
                'department_code': source['department']['code'] if 'code' in source['department'] else None,
                'department_name': source['department']['name'] if 'name' in source['department'] else None,
                'department_parentCode': source['department']['parentCode'] if 'parentCode' in source['department'] else None,
                'department_curatorEmployeeNumber': source['department']['curatorEmployeeNumber'] if 'curatorEmployeeNumber' in source['department'] else None,
                'department_managerEmployeeNumber': source['department']['managerEmployeeNumber'] if 'managerEmployeeNumber' in source['department'] else None,
                'branch_name': source['branch']['name'] if 'name' in source['branch'] else None,
                'branch_code': source['branch']['code'] if 'name' in source['branch'] else None
            }
            table.append(temp_dict)        

    def read_catalog():
        es = elasticsearch.client.Elasticsearch(["http://10.0.72.76:9200"])
        pages = list()
        page_size = 1000
        page = es.search(
            index='employee_profile_v3',
            doc_type='employee_profile_v3',
            scroll='3m',
            body={
                "from": 0,
                "size": page_size,
                "sort": "hireDate"
            }
        )
        read_page(page, pages)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        chunks_count = round(scroll_size / page_size)
        for i in range(0, int(chunks_count)):
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            read_page(page, pages)
        for j in pages:
            id1 = j['id']
            id2 = j['username']
            id3 = j['fullName']
            id4 = j['birthday']
            id5 = j['employeeNumber']
            id6 = j['employeeHeadNumber']
            id7 = j['employeeContractNumber']
            id8 = j['employeeContractDate']
            id9 = j['hireDate']
            id10 = j['iin']
            id11 = j['mobilePhoneNumber']
            id12 = j['position_code']
            id13 = j['position_name']
            id14 = j['specialization_code']
            id15 = j['specialization_name']
            id16 = j['specialization_parentCode']
            id17 = j['department_code']
            id18 = j['department_name']
            id19 = j['department_parentCode']
            id20 = j['department_curatorEmployeeNumber']
            id21 = j['department_managerEmployeeNumber']
            id22 = j['branch_name']
            id23 = j['branch_code']
            dest_cursor.execute("INSERT INTO dar_group.dim_bpm_employee_profile(id,username,fullname,birthday,employeenumber,employeeheadnumber,employeecontractnumber, \
                employeecontractdate, hiredate, iin, mobilephonenumber, position_code, position_name, specialization_code, specialization_name, specialization_parentcode, \
                department_code, department_name, department_parentcode, department_curatoremployeenumber, department_manageremployeenumber, branch_name, branch_code) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
                (id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19,id20,id21,id22,id23))
            dest_conn.commit()
            bpm_cur.execute("INSERT INTO bpm.dim_bpm_employee_profile(id,username,fullname,birthday,employeenumber,employeeheadnumber,employeecontractnumber, \
                employeecontractdate, hiredate, iin, mobilephonenumber, position_code, position_name, specialization_code, specialization_name, specialization_parentcode, \
                department_code, department_name, department_parentcode, department_curatoremployeenumber, department_manageremployeenumber, branch_name, branch_code) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", \
                (id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19,id20,id21,id22,id23))
            bpm_conn.commit()

    dest_cursor.execute("truncate table dar_group.dim_bpm_employee_profile")
    bpm_cur.execute("truncate table bpm.dim_bpm_employee_profile")
    read_catalog()

def get_bpm_department():
    def read_page(page, table):
        d = page['hits']['hits']
        for ss in d:
            source = ss['_source']
            temp_dict = {           
                'id': ss['_id'],
                'code': source['code'] if 'code' in source else None,
                'name': source['name'] if 'name' in source else None,
                'nameKz': source['nameKz'] if 'nameKz' in source else None,
                'parentCode': source['parentCode'] if 'parentCode' in source else None,
                'description': source['description'] if 'description' in source else None,
                'state': source['state'] if 'state' in source else None,
                'isArchive': source['isArchive'] if 'isArchive' in source else None,
                'isHq': source['isHq'] if 'isHq' in source else None,
                'curatorEmployeeNumber': source['curatorEmployeeNumber'] if 'curatorEmployeeNumber' in source else None,
                'managerEmployeeNumber': source['managerEmployeeNumber'] if 'managerEmployeeNumber' in source else None
            }
            table.append(temp_dict)

    def read_catalog():
        es = elasticsearch.client.Elasticsearch(["http://10.0.72.76:9200"])
        pages = list()
        page_size = 1000
        page = es.search(
            index='department_v2',
            doc_type='department_v2',
            scroll='3m',
            body={
                "from": 0,
                "size": page_size,
                "sort": "_id"
            }
        )
        read_page(page, pages)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        chunks_count = round(scroll_size / page_size)
        for i in range(0, int(chunks_count)):
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            read_page(page, pages)
        for j in pages:
            id1 = j['id']
            id2 = j['code']
            id3 = j['name']
            id4 = j['nameKz']
            id5 = j['parentCode']
            id6 = j['description']
            id7 = j['state']
            id8 = j['isArchive']
            id9 = j['isHq']
            id10 = j['curatorEmployeeNumber']
            id11 = j['managerEmployeeNumber']

            dest_cursor.execute ("INSERT INTO dar_group.dim_bpm_department(id, code, name, name_kz, parent_code, description, state, is_archive, is_hq, curator_employee_number, manager_employee_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11))
            dest_conn.commit()
            bpm_cur.execute("INSERT INTO bpm.dim_bpm_department(id, code, name, name_kz, parent_code, description, state, is_archive, is_hq, curator_employee_number, manager_employee_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11))
            bpm_conn.commit()

    dest_cursor.execute("truncate table dar_group.dim_bpm_department;")
    bpm_cur.execute("truncate table bpm.dim_bpm_department;")
    read_catalog()

def get_bpm_position():
    def read_page(page, table):
        d = page['hits']['hits']
        for ss in d:
            source = ss['_source']
            temp_dict = {           
                'id': ss['_id'],
                'code': source['code'] if 'code' in source else None,
                'name': source['name'] if 'name' in source else None,
                'nameKz': source['nameKz'] if 'nameKz' in source else None,
                'isArchive': source['isArchive'] if 'isArchive' in source else None,
                'level': source['level'] if 'level' in source else None,
            }
            table.append(temp_dict)

    def read_catalog():
        es = elasticsearch.client.Elasticsearch(["http://10.0.72.76:9200"])
        pages = list()
        page_size = 1000
        page = es.search(
            index='position_v2',
            doc_type='position_v2',
            scroll='3m',
            body={
                "from": 0,
                "size": page_size,
                "sort": "_id"
            }
        )
        read_page(page, pages)
 
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        chunks_count = round(scroll_size / page_size)
        for i in range(0, int(chunks_count)):
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            read_page(page, pages)
        for j in pages:
            id1 = j['id']
            id2 = j['code']
            id3 = j['name']
            id4 = j['nameKz']
            id5 = j['isArchive']
            id6 = j['level']
            dest_cursor.execute ("INSERT INTO dar_group.dim_bpm_position(id, code, name, name_kz, is_archive, level) VALUES (%s, %s, %s, %s, %s, %s)",(id1,id2,id3,id4,id5,id6))
            dest_conn.commit()
            bpm_cur.execute("INSERT INTO bpm.dim_bpm_position(id, code, name, name_kz, is_archive, level) VALUES (%s, %s, %s, %s, %s, %s)",(id1, id2, id3, id4, id5, id6))
            bpm_conn.commit()

    dest_cursor.execute("truncate table dar_group.dim_bpm_position;")
    bpm_cur.execute("truncate table bpm.dim_bpm_position;")
    read_catalog()

def get_bpm_specialization():
    def read_page(page, table):
        d = page['hits']['hits']
        for ss in d:
            source = ss['_source']
            temp_dict = {           
                'id': ss['_id'],
                'code': source['code'] if 'code' in source else None,
                'name': source['name'] if 'name' in source else None,
                'nameKz': source['nameKz'] if 'nameKz' in source else None,
                'parentCode': source['parentCode'] if 'parentCode' in source else None,
                'curatorCode': source['curatorCode'] if 'curatorCode' in source else None,
                'isActive': source['isActive'] if 'isActive' in source else None,
                'isOnlyForOrganigram': source['isOnlyForOrganigram'] if 'isOnlyForOrganigram' in source else None,
            }
            table.append(temp_dict)

    def read_catalog():
        es = elasticsearch.client.Elasticsearch(["http://10.0.72.76:9200"])
        pages = list()
        page_size = 1000
        page = es.search(
            index='specialization_v2',
            doc_type='specialization_v2',
            scroll='3m',
            body={
                "from": 0,
                "size": page_size,
                "sort": "_id"
            }
        )
        read_page(page, pages)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        chunks_count = round(scroll_size / page_size)
        for i in range(0, int(chunks_count)):
            page = es.scroll(scroll_id=sid, scroll='3m')
            sid = page['_scroll_id']
            read_page(page, pages)
        for j in pages:
            id1 = j['id']
            id2 = j['code']
            id3 = j['name']
            id4 = j['nameKz']
            id5 = j['parentCode']
            id6 = j['curatorCode']
            id7 = j['isActive']
            id8 = j['isOnlyForOrganigram']
            dest_cursor.execute ("INSERT INTO dar_group.dim_bpm_specialization(id, code, name, name_kz, parent_code, curator_code, is_active, is_only_for_organigram) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(id1,id2,id3,id4,id5,id6,id7,id8))
            dest_conn.commit()
            bpm_cur.execute("INSERT INTO bpm.dim_bpm_specialization(id, code, name, name_kz, parent_code, curator_code, is_active, is_only_for_organigram) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(id1, id2, id3, id4, id5, id6, id7, id8))
            bpm_conn.commit()
    
    dest_cursor.execute("truncate table dar_group.dim_bpm_specialization")
    bpm_cur.execute("truncate table bpm.dim_bpm_specialization")
    read_catalog()

def get_bpm_process_steps():
    query8 = '''WITH comp_state AS (
                    SELECT
                        th1.all_step AS completed_state,
                        th1.proc_def_id_ 
                    FROM
                        dar_group.dim_bpm_business_task_history th1
                        RIGHT JOIN (
                            SELECT--th.all_step as completed_state,
                            MAX ( th.date_end_t ) AS date_end_t,
                            th.proc_def_id_ 
                        FROM
                            dar_group.dim_bpm_business_task_history th 
                        WHERE
                            th.completed = 'true' 
                            AND th.proc_def_id_ IS NOT NULL 
                            AND th.all_step IS NOT NULL 
                            AND th.date_end_t IS NOT NULL 
                            AND application_num IS NOT NULL 
                        GROUP BY
                            th.proc_def_id_ 
                        ) th2 ON th1.proc_def_id_ = th2.proc_def_id_ 
                        AND th1.date_end_t = th2.date_end_t 
                    ) SELECT
                    tah.application_num,
                    tah.start_dtime,
                    tah.process_name,
                    tah.proc_inst_id_,
                    tah.proc_def_id_,
                    th.task_name,
                    th.due_dtime,
                    th.create_dtime,
                    tah.end_dtime,
                    emp_curr.employeenumber AS curr_employeenumber,
                    th.user_state AS th_curr_fullname,
                    emp_curr.fullname AS curr_fullname,
                    emp_curr.position_code AS curr_position_code,
                    emp_curr.position_name AS curr_position_name,
                    emp_curr.specialization_code AS curr_specialization_code,
                    emp_curr.specialization_name AS curr_specialization_name,
                    emp_curr.department_code AS curr_department_code,
                    emp_curr.department_name AS curr_department_name,
                    emp_init.employeenumber AS init_employeenumber,
                    tah.init_username,
                    emp_init.fullname AS init_fullname,
                    emp_init.position_code AS init_position_code,
                    emp_init.position_name AS init_position_name,
                    emp_init.specialization_code AS init_specialization_code,
                    emp_init.specialization_name AS init_specialization_name,
                    emp_init.department_code AS init_department_code,
                    emp_init.department_name AS init_department_name,
                    emp_init.branch_code AS init_branch_code,
                    emp_init.branch_name AS init_branch_name,
                CASE
                        
                        WHEN tah.STATE = 'true' THEN
                        'COMPLETED' 
                        WHEN tah.STATE = 'false' THEN
                        'ACTIVE' ELSE tah.STATE 
                    END AS STATE,
                    th_alls.step_id,
                    th_alls.activity_inst_id,
                    th_alls.all_step,
                    th_alls.date_start_t,
                    
                    --th_alls.date_end_t,
                    COALESCE((TIMESTAMP 'epoch' + (th_alls.date_end_t_s::numeric / 1000) * INTERVAL '1 SECONDS'), th_alls.date_end_t_d::TIMESTAMP) as date_end_t,
                    th_alls.user_state,
                CASE
                        
                        WHEN tah.STATE = 'true' THEN
                        th_ct.completed_state 
                    END AS completed_state,
                    inc.incident_msg_,
                    inc.error_dtime 
                FROM
                    dar_group.dim_bpm_business_task_agg_hist AS tah
                    FULL JOIN (
                    SELECT
                        th.all_step AS task_name,
                        th.proc_def_id_,
                        th.due_dtime,
                        th.create_dtime,
                        th.user_state,
                        th.initiator,
                        th.proc_start_date 
                    FROM
                        dar_group.dim_bpm_business_task_history th 
                        WHERE--th.date_end_t is null and th.proc_def_id_ is not null
                        th.completed = 'false' 
                        AND th.application_num IS NOT NULL 
                    ) AS th ON tah.proc_inst_id_ = th.proc_def_id_
                    LEFT JOIN (
                    SELECT
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
                    LEFT JOIN (
                    SELECT
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
                    SELECT
                        i.incident_msg_,
                        e.root_proc_inst_id_,
                        i.incident_timestamp_ AS error_dtime 
                    FROM
                        dar_group.bpm_act_ru_incident i,
                        dar_group.bpm_act_ru_execution e 
                    WHERE
                        i.proc_inst_id_ = e.id_ --AND e.root_proc_inst_id_ = tah.proc_inst_id_
                        
                    ) AS inc ON tah.proc_inst_id_ = inc.root_proc_inst_id_
                    LEFT JOIN (
                    SELECT
                        th.all_step AS all_step,
                        th.proc_def_id_,
                        th.step_id,
                        th.create_dtime AS date_start_t,
                        th.user_state,
                        th.date_end_t,
                        th.activity_inst_id,
                        case when th.date_end_t like '%-%' then th.date_end_t end as date_end_t_d,
                case when th.date_end_t not like '%-%' then th.date_end_t end as date_end_t_s
                    FROM
                        dar_group.dim_bpm_business_task_history th 
                    WHERE
                        th.proc_def_id_ IS NOT NULL 
                    ) AS th_alls ON tah.proc_inst_id_ = th_alls.proc_def_id_
                    LEFT JOIN comp_state AS th_ct ON tah.proc_inst_id_ = th_ct.proc_def_id_ 
                    --where tah.end_dtime is not null
                GROUP BY
                    tah.application_num,
                    tah.start_dtime,
                    tah.process_name,
                    tah.proc_inst_id_,
                    tah.proc_def_id_,
                    th_alls.activity_inst_id,
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
                --init_,
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
                    STATE,
                    tah.end_dtime,
                    th_alls.step_id,
                    completed_state,
                    th_alls.all_step,
                    th_alls.date_start_t,
                    th_alls.date_end_t,
                    th_alls.user_state,
                    inc.incident_msg_,
                    inc.error_dtime,
                    tah.STATE,
                    th_alls.date_end_t_s,
                    th_alls.date_end_t_d
                ORDER BY
                    start_dtime DESC'''

    bpm_cur.execute('delete from bpm.fct_process_all_steps_new')
    bpm_conn.commit()
    dest_cursor.execute(query8)
    while True:
        records = dest_cursor.fetchall()
        if not records:
            break
        execute_values(bpm_cur,
                       "INSERT INTO bpm.fct_process_all_steps_new VALUES %s",
                       records)
        bpm_conn.commit()

   # cursor1.close()
    dest_cursor.close()
    bpm_cur.close()
   # src_conn.close()
    dest_conn.close()
    bpm_conn.close()


t1 = PythonOperator(
    task_id="get_bpm_business_task_history",
    python_callable=get_bpm_business_task_history,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t2 = PythonOperator(
    task_id="get_bpm_business_task_agg_hist",
    python_callable=get_bpm_business_task_agg_hist,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t3 = PythonOperator(
    task_id="get_bpm_emp_profile",
    python_callable=get_bpm_emp_profile,
    dag=dag,
    email_on_failure = True,
    email = 'dnurtailakov@one.kz'
)

t3_2 = PythonOperator(
    task_id="get_bpm_department",
    python_callable=get_bpm_department,
    dag=dag,
    # email_on_failure = True,
    # email = 'dnurtailakov@one.kz'
)

t4 = PythonOperator(
    task_id="get_bpm_position",
    python_callable=get_bpm_position,
    dag=dag,
    # email_on_failure = True,
    # email = 'dnurtailakov@one.kz'
)

t5 = PythonOperator(
    task_id="get_bpm_specialization",
    python_callable=get_bpm_specialization,
    dag=dag,
    # email_on_failure = True,
    # email = 'dnurtailakov@one.kz'
)

t11 = PythonOperator(
    task_id="get_bpm_process_steps",
    python_callable=get_bpm_process_steps,
    dag=dag,
    # email_on_failure = True,
    # email = 'dnurtailakov@one.kz'
)

t1 >> t2 >> t11
t3 >> t3_2 >> t4 >> t5 >> t11