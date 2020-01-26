import datetime
import os
import shutil
import time
import pandas
import requests
import json
import pymongo
import cx_Oracle
import pymssql
import asyncio
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler


def run_flu_once():
    date_str = datetime.datetime.now().strftime('%Y%m%d')
    basic_path = 'C:/Users/Elysion/Desktop/流感上报/' + date_str + '/'
    upload_listener_path = 'C:/Users/Elysion/Desktop/流感上报/upload/'
    # 判断文件夹是否已经存在
    if os.path.exists(basic_path):
        shutil.rmtree(basic_path)
        os.mkdir(basic_path)
    else:
        os.mkdir(basic_path)
    print(basic_path)

    if not os.path.exists(upload_listener_path):
        os.mkdir(upload_listener_path)
    else:
        pass
    print(upload_listener_path)

    # ---------------------data base config----------------------
    # cdr_lis_db = pymysql.connect(host="10.0.0.192", user="root", password="incitedata", database="test")

    lis_db = pymssql.connect(host='192.168.50.1', user='CDRUser', password='CDRUser', database='ndlmis15',
                             charset='utf8')
    his_db = pymssql.connect(host='10.0.0.155', user='sa', password='elysion4u', database='chisdb_nczxyy',
                             charset='GBK')
    iih_db = cx_Oracle.connect('iih', 'avjrXzkd2m42jOBb', '10.0.66.6:1521/IIH')

    mr_db = cx_Oracle.connect('system', 'manager', '10.0.0.57/hosp')
    # mr_db = cx_Oracle.connect('system', 'manager', '10.0.0.57/hosp', mode=cx_Oracle.SYSDBA)

    # -----------------------------local functions---------------

    function_step_config = [
        {
            'func': 'lis',
            'db': lis_db,
            'sql': FLU_sql.lis_sql
        },
        {
            'func': 'flu_mz',
            'db': iih_db,
            'sql': FLU_sql.iih_mz_flu_sql
        },
        {
            'func': 'pdr_mz',
            'db': iih_db,
            'sql': FLU_sql.iih_mz_pdr_sql
        },
        {
            'func': 'flu_zy',
            'db': his_db,
            'sql': 'exec FLU_flu_zy'
        },
        {
            'func': 'pdr_zy',
            'db': his_db,
            'sql': 'exec FLU_pdr'
        },
        {
            'func': 'hda',
            'db': his_db,
            'sql': 'exec FLU_hda'
        },
        {
            'func': 'hqms',
            'db': mr_db,
            'sql': FLU_sql.hqms_sql
        },
        {
            'func': 'hdr',
            'db': his_db,
            'sql': 'exec FLU_hdr'
        }
    ]

    # ----------------lis--------------------------------------

    # 获取时间段内全部LIS结果
    lis_result = pandas.read_sql(FLU_sql.lis_sql, lis_db)
    print(lis_result)
    print('lis_all_result')
    # 获取LIS符合条件的患者列表
    lis_positive_result = lis_result[lis_result['P8004'] == '1']
    print(lis_positive_result)
    print('lis_positive_result')
    # 获取HIS符合条件的患者列表
    # ===========================================================
    his_patient_sql = "exec FLU_patient"
    his_patient_result = pandas.read_sql(his_patient_sql, his_db)
    print(his_patient_result)
    print('zy_patient_list')
    # ===========================================================
    # 获取iih符合条件的患者列表
    # ===========================================================
    iih_patient_result = pandas.read_sql(FLU_sql.iih_mz_patient_list, iih_db)
    print(iih_patient_result)
    print('mz_patient_list')
    # ===========================================================
    # 合并HIS和LIS和iih符合条件的患者列表
    union_patient_result = pandas.concat([lis_positive_result, his_patient_result, iih_patient_result], axis=0,
                                         sort=True)
    # ----------------lis-----------------------------------
    return_msg = ''
    dead_people_inpatient_no = ''
    for step in function_step_config:
        try:
            if step['func'] == 'lis':
                # 筛选LIS全部结果，获取HIS相关病人的LIS报告
                result0 = lis_result[lis_result.P7502.isin(
                    union_patient_result['inpatient_no'])]
                result1 = lis_result[lis_result.P7502.isin(
                    union_patient_result['patient_id'])]
                result = pandas.concat([result0, result1], axis=0)
            else:
                result = pandas.read_sql(step['sql'], step['db'])

            if result.shape[0] == 0:
                print(step['func'] + '无数据')
                return_msg += step['func'] + '---无数据\n'
            else:
                file_name = step['func'] + '_' + date_str
                result.to_csv(file_name + '.csv')
                shutil.move(os.getcwd() + '/' + file_name + '.csv', basic_path)
                print(step['func'] + '完成')
                return_msg += step['func'] + '---完成\n'

                if step['func'] == 'hdr':
                    dead_people_inpatient_no = '今日上报的死亡患者住院号为:\n'
                    for row in result.itertuples():
                        dead_people_inpatient_no += '{},'.format(row[1])
                    print(dead_people_inpatient_no)
                dead_people_inpatient_no += '如有新增请上传相关病例资料'

                '''
                with zipfile.ZipFile(file_name + '.zip', 'w') as zip:
                    zip.write(file_name + '.csv')
                shutil.move(os.getcwd() + '/' + file_name + '.zip', basic_path)
                '''

        except BaseException:
            print(step['func'] + '失败')
            return_msg += step['func'] + '---失败\n'

    # -----------------------短信提示-------------------------
    for phone_number in ['15583570988', '18227329509', '15181790852']:
        #    预防保健科方圆      ,'18227329509'
        r = requests.post(url='http://10.0.200.1/PlatformService/platform/api', json={
            'api_id': 'sendMsg',
            'phone': phone_number,
            'content': '\n'
                       + datetime.datetime.now().strftime('%Y-%m-%d')
                       + ' 流感上报情况：\n'
                       + return_msg
                       + dead_people_inpatient_no
        })
        print(r.text)
    return 'finish'


async def async_task(db, item):
    break_flag = True
    counter = 0
    while break_flag:
        service_id = item['service_id']
        subscription_url = "http://10.0.12.51:8780/sdk/S116/" + service_id
        response = requests.get(url=subscription_url, headers={
            'hospital_id': '45218368-8',  # 本院机构代码
            'apply_unit_id': '0',  # 默认0
            'exec_unit_id': '0',
            'service_id': service_id,  # 服务id
            'visit_type': '01',  # 01 门诊，03 住院，0401 体检，0201 急诊
            'order_exec_id': '0',
            'send_sys_id': item['send_sys_id'],
            'extend_sub_id': '0',
            'Authorization': 'Basic Zmx1OmZsdTEyMw=='
        }, data={})
        raw = json.loads(response.text)
        print(raw)

        if raw['status'] != 0:
            result = raw['data']['body']
            counter += 1
            if service_id == 'BS301':
                db[service_id].delete_many({'visitOrdNo': result['visitOrdNo']})
                for diagnosis in result['diagnosis']:
                    if diagnosis['diseaseCode'][0:1] == 'J':
                        db['flu_patients'].insert_one({'patientId': result['patientLid'],
                                                       'visitType': result['visitType'],
                                                       'visitNo': result['visitNo'],
                                                       'visitOdrNo': result['visitOrdNo'],
                                                       'comeFrom': "BS301"})
            elif service_id == 'BS302':
                if result['triggerEvent'] == 'new':
                    db[service_id].delete_many({'visitOrdNo': result['visitOrdNo']})
                    for prescription in result['prescriptions']:
                        for drugItem in prescription:
                            if drugItem['drugCode'] in ['123456', '654321']:
                                db['flu_patients'].insert_one({'patientId': result['patientLid'],
                                                               'visitType': result['visitTypeCode'],
                                                               'visitNo': result['visitNo'],
                                                               'visitOdrNo': result['visitOrdNo'],
                                                               'comeFrom': "BS302"})
                elif result['triggerEvent'] == 'renew':
                    pass
            elif service_id == 'BS311':
                pass
            elif service_id == 'BS319':
                '''
                db['flu_patients'].insert_one({'patientId': result['data']['body']['patientLid'],
                                               'visitType': result['data']['body']['visitType'],
                                               'visitNo': result['data']['body']['medicalNo'],
                                               'visitOdrNo': result['data']['body']['visitOrdNo'],
                                               'comeFrom': "BS319"})
                '''
                pass
            else:
                pass
            db[service_id].insert_one(result)
        else:
            if counter > 0:
                print('\r收到' + str(counter) + '条' + service_id + ' --- ' + str(datetime.datetime.now()))
            counter = 0
            break_flag = False
            break


def move_to_upload_folder():
    for file_name in os.listdir('C:/Users/Elysion/Desktop/流感上报/' + datetime.datetime.now().strftime('%Y%m%d') + '/'):
        if file_name.endswith('.csv'):
            # print(file_name)
            shutil.copy(
                'C:/Users/Elysion/Desktop/流感上报/' + datetime.datetime.now().strftime('%Y%m%d') +
                '/' + file_name,
                'C:/Users/Elysion/Desktop/流感上报/upload/' + file_name)


def scheduler_upload_task():
    job_defaults = {'max_instances': 2}
    scheduler = BlockingScheduler()
    scheduler.add_job(run_once, 'interval', seconds=15, misfire_grace_time=15)
    # scheduler.add_job(timer_runner, 'interval', seconds=15, misfire_grace_time=15)
    scheduler.start()

    bg_scheduler = BackgroundScheduler()
    bg_scheduler.add_job(timer_runner, 'interval', seconds=15, misfire_grace_time=15)
    bg_scheduler.start()


def run_once():
    mongo = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
    db = mongo['flu']
    for item in [
        {'service_id': 'BS301', 'send_sys_id': 'S001'},  # BS301 诊断，S001 iih
        {'service_id': 'BS302', 'send_sys_id': 'S001'},  # BS302 处方
        {'service_id': 'BS311', 'send_sys_id': 'S001'},  # BS311 住院用药医嘱
        {'service_id': 'BS319', 'send_sys_id': 'S008'}  # BS319 普通检验，S008 LIS
    ]:
        break_flag = True
        counter = 0
        while break_flag:
            service_id = item['service_id']
            subscription_url = "http://10.0.12.51:8780/sdk/S116/" + service_id
            response = requests.get(url=subscription_url, headers={
                'hospital_id': '45218368-8',  # 本院机构代码
                'apply_unit_id': '0',  # 默认0
                'exec_unit_id': '0',
                'service_id': service_id,  # 服务id
                'visit_type': '01',  # 01 门诊，03 住院，0401 体检，0201 急诊
                'order_exec_id': '0',
                'send_sys_id': item['send_sys_id'],
                'extend_sub_id': '0',
                'Authorization': 'Basic Zmx1OmZsdTEyMw=='
            }, data={})
            raw = json.loads(response.text)
            if raw['status'] != 0:
                result = raw['data']['body']
                counter += 1
                if service_id == 'BS301':
                    db[service_id].delete_many({'visitOrdNo': result['visitOrdNo']})
                    for diagnosis in result['diagnosis']:
                        if diagnosis['diseaseCode'][0:1] == 'J':
                            db['flu_patients'].insert_one({'patientId': result['patientLid'],
                                                           'visitType': result['visitType'],
                                                           'visitNo': result['visitNo'],
                                                           'visitOdrNo': result['visitOrdNo'],
                                                           'comeFrom': "BS301"})
                elif service_id == 'BS302':
                    if result['triggerEvent'] == 'new':
                        db[service_id].delete_many({'visitOrdNo': result['visitOrdNo']})
                        for prescription in result['prescriptions']:
                            for drugItem in prescription:
                                if drugItem['drugCode'] in ['123456', '654321']:
                                    db['flu_patients'].insert_one({'patientId': result['patientLid'],
                                                                   'visitType': result['visitTypeCode'],
                                                                   'visitNo': result['visitNo'],
                                                                   'visitOdrNo': result['visitOrdNo'],
                                                                   'comeFrom': "BS302"})
                    elif result['triggerEvent'] == 'renew':
                        pass
                elif service_id == 'BS311':
                    pass
                elif service_id == 'BS319':
                    '''
                    db['flu_patients'].insert_one({'patientId': result['data']['body']['patientLid'],
                                                   'visitType': result['data']['body']['visitType'],
                                                   'visitNo': result['data']['body']['medicalNo'],
                                                   'visitOdrNo': result['data']['body']['visitOrdNo'],
                                                   'comeFrom': "BS319"})
                    '''
                    pass
                else:
                    pass
                db[service_id].insert_one(result)
            else:
                if counter > 0:
                    print('\r收到' + str(counter) + '条' + service_id + ' --- ' + str(datetime.datetime.now()))
                counter = 0
                break_flag = False
                break
    mongo.close()


def timer_runner():
    for i in range(14, -1, -1):
        print('\r下次轮询间隔{}秒'.format(i), end='')
        time.sleep(1)


if __name__ == '__main__':
    print('v1.0 begin')
    run_once()
    scheduler_upload_task()
