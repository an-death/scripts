#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import print_function

import subprocess
import datetime
import codecs

BOX = False
DEFAULT_LOGIN = 'ts'
DEFAULT_PASS = 'of1Aengo'
DEFAULT_STREAM_LOGIN = 'tetrasoft'
DEFAULT_STREAM_PATH = '/home/{}/stream/storage/'.format(DEFAULT_STREAM_LOGIN)
DEFAULT_ADDR = '10.246.75.1'  # nova
# DEFAULT_PORT = '9082'

DEFAULT_PATH_BOX = '/media/hdd/connect/video/'

START = '2017-08-04 03:30'  # время буровой
STOP = '2017-08-04 06:00'  # время буровой
SPLITTER = 5  # min
TO_MSK = 2
TO_UTC = 3
DEFAULT_TFORMAT = '%Y-%m-%d %H:%M'
DEFAULT_FILE_NAME_FORMAT = '%Y%m%d_%H%M%S'
CAM_ID = 1


def counter(func):
    def wrapper(*args):
        res = func(*args)
        wrapper.count += 1
        return res

    wrapper.count = 0
    return wrapper


@counter
def sync(pasw, host, file, to, log='ts'):
    command = 'rsync -azvP --rsh="sshpass -p {pas} ssh -l {ts}" {host}:{file} {to}'. \
        format(
        pas=pasw,
        ts=log,
        host=host,
        to=to,
        file=file
    )
    print(command)
    exit_code = subprocess.Popen(command, shell=True).wait()
    if not exit_code:
        return True

def mkdir(full_path):
    dir = full_path.rsplit('_')[0]
    subprocess.Popen('mkdir -p {} '.format(dir), shell=True)

def get_list_of_files_from_box(date):
    command = '''ls -l /home/ts/connect/video/{}/*'''.format(date)
    list_of_files = []
    res = gbox_exec(command)
    for r in res:
        r = r.decode('utf-8')
        if '_{}.flv'.format(CAM_ID) not in r:
            continue
        name = r.split()[-1]
        list_of_files.append(name)
    return list_of_files


def gbox_exec(command):
    con = '''sshpass -p {} ssh -l ts {} -t "{}"'''.format(DEFAULT_PASS, DEFAULT_ADDR, command)
    res = subprocess.Popen(con, shell=True, stdout=subprocess.PIPE).stdout.read().splitlines()
    return res


def get_product_key(box=False):
    if box:
        with codecs.open('/home/ts/connect/connect.conf', 'r', encoding='utf-8') as fd:
            for line in fd.readlines():
                if line.startswith('product'):
                    return line.split('=')[-1].strip('\n')
    else:
        command = '{}'.format('grep product connect/connect.conf')
        return gbox_exec(command)[0].decode('utf-8').split('=')[-1]


def get_time_limits(start, stop, delta, to_msk):
    start = start - to_msk
    stop = stop - to_msk
    list_of_times = []
    while start <= stop:
        list_of_times.append(start)
        start = start + delta
    return list_of_times


def convert_file_name(file_name):
    return datetime.datetime.strptime(file_name, DEFAULT_FILE_NAME_FORMAT)


def get_files_by_limit(list_of_times, list_of_files):
    files_data = {}
    product_key = get_product_key()
    for i, file in enumerate(list_of_files):
        file_name = file.split('/')[-1]
        file_cam_id, file_dt = file_name.split('_')[-1][0], file_name.rsplit('_', 1)[0]
        if int(file_cam_id) != CAM_ID:
            continue
        file_to_time = convert_file_name(file_dt)
        if list_of_times[-1] >= file_to_time >= list_of_times[0]:
            files_data[i] = {
                'path': get_path_filename(file_name, file_to_time),
                'to_path': get_path_filename(file_name, file_to_time, product_key)
            }
    return files_data


def get_path_filename(name, time, product_key=False):
    utc_name = time - datetime.timedelta(hours=TO_UTC)
    date = utc_name.date() if product_key else time.date()
    hour = utc_name.hour if product_key else  time.hour
    hour = hour if len(str(hour)) == 2 else '0' + str(hour)
    utc_name = '_'.join([utc_name.strftime(DEFAULT_FILE_NAME_FORMAT), name.rsplit('_')[-1]])
    file_name = name
    if not product_key:
        return '{}/{}/{}/{}'.format(DEFAULT_PATH_BOX, date, hour, file_name)
    else:
        return '{}/{}/video/{}/{}/{}'.format(DEFAULT_STREAM_PATH, product_key, date, hour, utc_name)


def main():
    dt_start = datetime.datetime.strptime(START, DEFAULT_TFORMAT)
    dt_stop = datetime.datetime.strptime(STOP, DEFAULT_TFORMAT)
    delta = datetime.timedelta(minutes=SPLITTER)
    to_msk = datetime.timedelta(hours=TO_MSK)
    limit_by_delta = get_time_limits(dt_start, dt_stop, delta, to_msk)
    date_start, date_end = limit_by_delta[0].date(), limit_by_delta[-1].date()
    # if BOX:
    #     all_files = path.Path(DEFAULT_PATH_BOX).walk('*.flv')

    if date_start == date_end:
        list_of_files = get_list_of_files_from_box(date_start)
    else:
        print('Ограничте промежуток одним числом! C {} ПО {}'.format(date_start, date_end))
    g_path = get_files_by_limit(limit_by_delta, list_of_files)
    # print(g_path)
    for data in g_path:
        box = g_path[data].get('path')
        stream = g_path[data].get('to_path')
        print('Копируем фаил {} в {}'.format(box, stream))
        mkdir(stream)
        while not sync(DEFAULT_PASS, DEFAULT_ADDR, box, stream, DEFAULT_LOGIN):
            print('ФАИЛ Не {} ЗАГРУЖЕН!'.format(g_path[data].get('path').rsplit('/')[-1]))
        else:
            continue

if __name__ == '__main__':
    main()
