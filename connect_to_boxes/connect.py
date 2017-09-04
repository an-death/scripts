# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from datetime import datetime, timedelta
import paramiko



store = {
    'box':{
            'login': 'login',
            'password': 'password',
            'host': 'host',
            'port': 'port'},
    'serv': {
        'server' :{
            'login': 'login',
            'password': 'password',
            'host': 'host',
            'port': 'port'
            }
        },
    'db': {
        'db': {
            'name': 'db_name',
            'port': 'port',
            'host': 'host',
            'user': 'login',
            'password': 'password'
            }

        }
    }
def get_engine(basename, auth):
    """

    :return: engine
    """
    db = auth.get('db')
    db = db.get(basename)

    user = db.get('user')
    password = db.get('password')
    host = db.get('host')
    bname = db.get('name')
    port = db.get('port')
    engine_str = 'mysql+pymysql://{u}:{p}@{h}{port}/{b_name}?charset=utf8&use_unicode=0'.format(
        u=user,
        p=password,
        h=host,
        port='' if port is None else ':' + str(port),
        b_name=bname
    )
    engine = create_engine(engine_str, convert_unicode=True, echo=False)
    return engine

def get_boxes(server, condition='all'):
    """
    :param server: shortcat of server name
    :param condition: cases: all, active, inactive
    :return: list of boxes
    """
    boxes = []
    sql_request_all=\
        '''select ws.product_key, ww.name, ww.modified_date from WITS_WELL ww inner join WITS_SOURCE ws 
        on (ww.source_id=ws.id) where ws.product_key is not NULL'''
    engine = get_engine(server, store)
    result = engine.execute(sql_request_all)
    result = result.fetchall()
    now = datetime.now()
    for row in result:
        key = row[0].decode('utf-8')
        if 'GBOX' not in key or key == 'GBOX-01':
            continue
        elif now - row[2] > timedelta(hours=10):
            continue
        else:
            box = key.split('-')[1]
            boxes.append(box)

    return boxes


def add_option_to_reporting_conf(boxes, auth):
    cond = 'customer_company_code=Ggr'
    check_cond = 'grep -q "{}" reporting/config.conf && echo True'.format(cond)

    for box in boxes:
        print('{}'.format(box))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(hostname=auth['host'] + str(box),
                           username=auth['login'],
                           password=auth['password'],
                           port=auth['port'])
        except Exception:
            'Error reading SSH protocol banner'
            continue
        print('Connected to {}'.format(box))
        stdin, stdout, stderr = client.exec_command(check_cond)
        if stderr.read():
            print('Looks like reporting not installed')
            client.close()
            continue
        elif stdout.read():
            print('Картинка уже настроена на боксе {}'.format(box))
            client.close()
            continue
        else:
            print('Добавляем опцию {} в reporting/config.conf gbox {}'.format(cond, box))
            stdin, stdout, stderr = client.exec_command(
                'echo -e "{}" >> reporting/config.conf; {} '.format(cond, check_cond)
            )
            if stdout.read() and not stderr.read() :
                print('Опцию добавили, рестартим репортинг!')
                stdin, stdout, stderr = client.exec_command(
                    "kill `ps ax | grep report | grep java | awk ' { print $1 }'`"
                )
                if not stderr.read():
                    print('Перезагрузка репортинга бокса {} выполнена'.format(box))
            else:
                print('Не добавлена, проверяй!')
        client.close()


def main():

    boxes = get_boxes('ggr', 'active')
    auth = store.get('box')
    add_option_to_reporting_conf(boxes,auth)


if __name__ == '__main__':
    main()

