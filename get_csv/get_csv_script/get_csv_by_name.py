#!/usr/bin/python3
# -*- coding: utf-8 -*-
# python ver 3.5
# pip install paramiko



from __future__ import unicode_literals
from sqlalchemy import create_engine
import codecs
import os
import paramiko

store = {
    'serv': {
        'name': 'name',
        'port': 'port',
        'host': 'host',
        'user': 'login',
        'password': 'password'
    }
}


def engine(basename):
    """

    :return: engine
    """
    db = store.get(basename)

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


########################## NOT IN USED ANYMORE################################################
def get_max(engine, *, wb_id=None):
    if wb_id is None:
        raise NameError('ERROR wellbore_id is NULL')

    sql_r = 'select max(id) from data_for_upload_{}'.format(wb_id)
    max_id = engine.execute(sql_r).fetchone()

    return max_id[0]


def create_file(filename):
    first_string = 'date,depth,mnemonic,value  \n\n'
    with codecs.open(filename, 'w', encoding='utf-8') as fd:
        fd.write(first_string)


def write_csv(well_name, data):
    file_name = str(well_name) + '.csv'
    if not os.access(file_name, os.F_OK):
        create_file(file_name)

    with codecs.open(file_name, 'a', encoding='utf-8') as fd:
        fd.write(data)


def get_data(engine, *, wellbore_id=1, min_id=1, max_id=1):
    """Not in  used anymore"""
    data_str = ''
    row_sql = '''
    select date, depth, mnemonic, value from data_for_upload_{wb_id} where id between {id_min} and {id_max} order by date;
    '''.format(wb_id=wellbore_id, id_min=min_id, id_max=max_id)

    row_data = engine.execute(row_sql)
    for data in row_data:
        mnemonic = str(data['mnemonic']).split('\'')[1]
        string = '{},  {},  {},  {}'.format(data['date'], data['depth'], mnemonic, data['value'])
        data_str = data_str + string + '\n'

    return data_str


def get_data_cycle(engine, wb_id):
    """Not in used
    """
    ssk = engine
    create_table(ssk, wb_id)
    max_id = get_max(ssk, wb_id=wb_id)
    print('Всего строк: {}'.format(max_id))
    last_max_id = 0
    for last_id in range(1, max_id + 5000, 5000):
        if last_max_id == 0:
            last_max_id = 1
        print('Делаем селект по id_min = {} and id_max = {}'.format(last_max_id, last_id))
        data = get_data(ssk, wellbore_id=wb_id, min_id=last_max_id, max_id=last_id)
        print('Селект с id {} Выполнен, обновляем id'.format(last_id))
        last_max_id = last_id
        print('Записываем в фаил')
        # write_csv(well_name, data)

############################################################################################
############################################################################################


def create_table(engine, wellbore_id):
    table_name = 'data_for_upload_{}'.format(wellbore_id)
    create_sql = '''
        drop table if exists {tb_name};
        CREATE TABLE IF NOT EXISTS {tb_name}(
        id bigint(20) NOT NULL,
        date datetime NOT NULL,
        depth double NOT NULL,
        mnemonic varchar(25) CHARACTER SET utf8 NOT NULL,
        value double NOT NULL,
        KEY ( date),
        UNIQUE KEY log_data (mnemonic,date)
        ) ;
        insert into {tb_name} select id, date, depth, mnemonic, value from WITS_RECORD1_IDX_{wb_id} wi inner join WITS_RECORD1_DATA_{wb_id} wd on (wi.id=wd.idx_id) where mnemonic not in ('ACTC', 'DMEA');
        insert into {tb_name} select id, date, depth, mnemonic, value from WITS_RECORD11_IDX_{wb_id} wi inner join WITS_RECORD11_DATA_{wb_id} wd on (wi.id=wd.idx_id) where mnemonic not in ('ACTC', 'DMEA');
        insert into {tb_name} select id, date, depth, mnemonic, value from WITS_RECORD12_IDX_{wb_id} wi inner join WITS_RECORD12_DATA_{wb_id} wd on (wi.id=wd.idx_id) where mnemonic not in ('ACTC', 'DMEA');

        '''.format(tb_name=table_name, wb_id=wellbore_id)
    print('Создаём таблицу: data_for_upload_{}'.format(wellbore_id))
    engine.execute(create_sql)
    print('Table {} was created'.format(table_name))
    return table_name


def create_csv_by_root(engine, *, table_name='1', well_name=1):
    path = '/tmp/{}.csv'.format(well_name)
    print('Создаём фаил {}'.format(path))
    row_sql = '''
    select date, depth, mnemonic, value from {tb_name} order by date INTO OUTFILE '{path}';
    '''.format(tb_name=table_name, path=path)
    engine.execute(row_sql)
    print('Фаил {} создна и готов к выгрузке'.format(path))
    return path


def drop_table(engine, table_name):
    print('Уничтожаем таблицу {}'.format(table_name))
    drop_sql = 'drop table if exists {tb_name} ;'.format(tb_name=table_name)
    engine.execute(drop_sql)
    print('Таблица уничтожена')


def get_zip_file(path, localpath):
    """Connect to server and zip filte for upload"""

    auth = {
        'login': 'login',
        'password': 'password',
        'host': 'host',
        'port': 'port'
    }

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=auth['host'],
                   username=auth['login'],
                   password=auth['password'],
                   port=auth['port'])
    stdin, stdout, stderr = client.exec_command(
        'echo "{p}" | sudo -S chown {lg}:{lg} {path}'.format(p=auth['password'], lg=auth['login'], path=path)
    )
    if not stderr.read():
        print('ERROR {} \nCannot change privileges! \nSkipping {}'.format(stderr.read(), path))
        client.close()
        exit()

    stdin, stdout, stderr = client.exec_command('gzip -9 {}'.format(path))
    zipped = path + '.gz'
    file_name = zipped.split('/')[-1]
    # Ждём пока не выполнется архиватция и обычный фаил не пропадёт
    stdin, stdout, stderr = client.exec_command('while [ -f {} ] ; do sleep 1 ; done'.format(path))
    print(stdout.read(), 'File archived complete {}'.format(zipped))
    # Делаем ls чтобы получить фаил и его размер
    stdin, stdout, stderr = client.exec_command('ls -lh {} {}'.format(path, zipped))
    print(stdout.read())

    remotepath = '{}'.format(zipped)
    local_path = localpath + file_name
    print('Скачиваем фаил {} в дирректорию {} на локальной машине.'.format(zipped, local_path))
    transport = paramiko.Transport(auth['host'], auth['port'])
    transport.connect(username=auth['login'], password=auth['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)

    sftp.get(remotepath, local_path)

    sftp.close()
    transport.close()


def main():
    ssk = engine('SSK')
    local_path = '/home/as/share/ssk/'
    list_of_wells = [(4004, 184)]
    done = [(7117, 174), (7096, 175), (1707, 173), (1708, 176), (7116, 181), ]
    for well in list_of_wells:
        if well in done:
            continue
        wb_id = well[1]
        well_name = well[0]
        print('Информация принята. Скважина {}, WELLBORE_ID={}'.format(well_name, wb_id))
        # Создаём таблицу 1 + 11 + 12 рекордов по стволу
        table_name = create_table(ssk, wb_id)
        path = create_csv_by_root(ssk, table_name=table_name, well_name=well_name)
        drop_table(ssk, table_name)
        done.append(well)
        # with codecs.open('/home/as/share/ssk/wells.data', 'w', encoding='utf-8') as fd:
        #     fd.write(done)
        get_zip_file(path, local_path)

        # data = get_data(ssk, wellbore_id=wb_id, min_id=1, max_id=2)
        # print(data)


if __name__ == '__main__':
    main()
