#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import codecs
import json
import os
import subprocess
import path
from datetime import datetime

files = []
decoded_files = './decoded_archive.json'
log_file = './archive_decoder.log'
count = 0


def get_decoded_files():
    if os.access(decoded_files, os.F_OK):
        with codecs.open(decoded_files, 'r', encoding='utf-8') as fd:
            store = json.load(fd)
        return store
    else:
        return []


def log(log_string):
    if path.Path(log_file).access(os.F_OK):
        with codecs.open(log_file, 'a', encoding='utf-8') as fd:
            stri = ' '.join([datetime.now().strftime('%Y-%m-%d %H:%M:%S'), log_string, '\n'])
            fd.write(stri)
    else:
        path.Path(log_file).touch()
        log(log_string)


def write_history(list_of_files):
    if path.Path(decoded_files).access(os.F_OK):
        with codecs.open(decoded_files, 'w', encoding='utf-8') as fd:
            json.dump(list_of_files, fd, indent=1)
    else:
        path.Path(decoded_files).touch()
        write_history(list_of_files)


def counter(func):
    def wrapper(*args):
        res = func(*args)
        wrapper.count += 1
        return res

    wrapper.count = 0
    return wrapper


@counter
def decode(file, file_name, history):
    # file = file.encode('utf-8')

    log('DECODE : FILE {} START'.format(file_name))
    tmpfile = file + '.tmp'
    path.Path(file).move(tmpfile)
    coder = ['mencoder']
    keys = '-idx {input_file} -ovc copy -oac copy -o {output_file}'.format(input_file=tmpfile, output_file=file)
    keys = coder + keys.split()
    process = subprocess.Popen(keys).wait()
    # for out in process.stdout.readlines():
    #     print out
    if not process:  # returncode == 0
        log('DONE : file {} '.format(file_name))
        path.Path(tmpfile).remove()
        log('REMOVED : file {}'.format(tmpfile))
        history.append(file_name)
    else:
        log('ERROR : Decode is stop! For file {} '.format(file))
        path.Path(tmpfile).move(file)
        exit(1)


def decode_cycle(history):
    connects_num = [digit.replace('connect', '').split('/')[-1] for digit in path.Path('/home/ts/').listdir('connect*')]
    for connect in connects_num:
        video_arc = path.Path('/home/ts/connect{}/video'.format(connect)).readlink()
        files = path.Path(video_arc).walk('*.flv')
        for file in files:
            file_name = file.split('/')[-1]
            if file_name in history:
                continue
            print('FILE {} START'.format(file))
            try:
                # todo Отлавливать файлы 0 размера и выность их отдельно
                decode(file, file_name, history)
            except KeyboardInterrupt:
                print()
                path.Path(file + '.tmp').move(file)
                print ('STOP: KeyboardInterrupt accepted! STOP on file : {}!'.format(file))


def decode_cycle_local(history, local_path):
    files = path.Path(local_path).walk('*.flv')
    for file in files:
        file_name = file.split('/')[-1]
        if file_name in history:
            continue
        print('FILE {} START'.format(file))
        try:
            decode(file, file_name, history)
        except KeyboardInterrupt:
            print()
            path.Path(file + '.tmp').move(file)
            print ('STOP: KeyboardInterrupt accepted! STOP on file : {}!'.format(file))


def main():
    TEST = False
    local_path = '/home/as/share/test_for_decode_video'
    history = get_decoded_files()
    if TEST:
        decode_cycle_local(history, local_path)
    else:
        decode_cycle(history)
    last_string = 'DECODE DONE. FILES DECODED : {}'.format(decode.count)
    print(last_string)
    log(last_string)
    write_history(history)


if __name__ == '__main__':
    main()
