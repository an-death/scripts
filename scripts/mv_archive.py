#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import print_function

import sys
import path
import datetime


def docs(script_name, help=False):
    if help:
        print('{n}{script_name} {n}'.format(script_name=script_name, n='\n') )
        print(main.__doc__)
        print('USAGE: puthon {} {}'.format(script_name, ' '.join(['{some_dir}', '{dest_dir}'])))
    else:
        print('USAGE: puthon {} {}'.format(script_name, ' '.join(['{some_dir}', '{dest_dir}'])))
    exit(0)

def check_input_args(*args):
    """
    :param args: Список аргуменов переданных скрипту
    :return: Class Path()
    """
    script_name = args[0]
    for arg in args:
        if arg in ['-h', '--help']:
            docs(script_name, True)

    store = path.Path(args[1])
    dest = path.Path(args[2])
    if not store.isdir():
        exit('Каталог c архивом по пути: "{}" не найден! Выходим'.format(store))
    return store, dest


def check_dest_path(dest):

    if dest.islink():
        dest = dest.readlinkabs()


    if not dest.isdir():
        exit('Указанный пусть назначения не существует! Проверьте путь {}!'.format(dest))
    elif not dest.parent.ismount():
        exit('Проверьте mount hdd /media/hdd!')

def get_datetime(file_name):

    dt = datetime.datetime.strftime(file_name[:16], '%Y%m%d_%H%M%S_')
    return dt.date(), dt.hour


def main():
    """
Скрипт для переноса восстановленного архива из обего хранилища по попкам.
ринимает:
{Первой переменной} : Путь до восстановленного архива
{Второй переменной} : Путь куда перенести архив создав новое древо подобно коннекту {date}/{hour}/{file}
"""

    store, dest = check_input_args(*sys.argv)
    check_dest_path(dest)
    for file in store.walk('*.flv'):
        date, hour = get_datetime(file)
        date = date.isoformat()
        if not (dest + date).exists():
            (dest + date).mkdir()
        if not (dest + date + hour).exists():
            (dest + date + hour).mkdir()
        file.copy(dest + date + hour)


if __name__ == '__main__':
    main()

