#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""delete video"""
from __future__ import print_function, unicode_literals
import os
import re
import argparse
import logging
import shutil
from path import Path
from datetime import datetime
from collections import namedtuple, defaultdict

logger = logging.getLogger('delete_video')
logger.setLevel(logging.DEBUG)
log = logging.FileHandler(filename='/media/hdd/connect/log/delete_video.log')
log.setLevel(logging.DEBUG)
log.setFormatter(logging.Formatter('%(asctime)s %(levelname)s  [%(name)s] %(message)s'))
logger.addHandler(log)

# CONFIGURE HERE

# Monitor
MON = '/media/hdd*'  # 2,3,4,5 as default
LIMIT = 500 # Limit in MiB
CASES = ['video', 'connect/video', 'connect2/video']
# END OF CONFIG


DiskUsage = namedtuple('DiskUsage', 'total used free')


def _gb(bytes):
    return round(float(bytes) / 1024 ** 3, 2)


def disk_usage(path):
    """Return disk usage statistics about the given path.

    Will return the namedtuple with attributes: 'total', 'used' and 'free',
    which are the amount of total, used and free space, in bytes.
    """
    if os.path.exists(path):
        st = os.statvfs(path)
        free = _gb(st.f_bavail * st.f_frsize)
        total = _gb(st.f_blocks * st.f_frsize)
        used = _gb((st.f_blocks - st.f_bfree) * st.f_frsize)
        return DiskUsage(total, used, free)


class Disk:
    def __init__(self, path, dirs):
        self.mount_point = Path(path)
        self.dirs = dirs

    def __str__(self):
        return 'Disk : {} | {}'.format(self.mount_point, self.disk)

    @property
    def disk(self):
        return disk_usage(self.mount_point)

    @property
    def default_path(self):
        if '_default_path' not in self.__dict__:
            self._default_path = (self.mount_point.joinpath(case)
                                  for case in CASES if self.mount_point.joinpath(case).exists())
        return self._default_path

    @property
    def oldest_dirs(self):
        self._video_storage = defaultdict(list)
        for path in self.default_path:
            for date in iter(path.listdir()):
                self._video_storage[self.dir_name_to_date(date)].append(date)
        return self._video_storage[sorted(self._video_storage.keys())[0]]

    def check(self, limit):
        logger.info('Compare free space for {} : {}  < {}. Need to clean up? > {}'.
                    format(self.mount_point, self.disk.free, limit, self.disk.free < limit))
        return self.disk.free < limit

    def __clean__(self):
        logger.info('Check complete. Decision :> NEED TO CLEAN UP')
        logger.info('Start cleaning disk :> {}'.format(self))
        while True:
            if not self.check(_gb(LIMIT)):
                break
            for dir in self.oldest_dirs:
                logger.info('Removing = {}'.format(dir.__str__()))
                try:
                    shutil.rmtree(dir, ignore_errors=False)
                except Exception as e:
                    logger.error('Directory not deleted! Reason : {} '.format(e))
                logger.info('Dir {} removed'.format(dir.__str__()))

    @staticmethod
    def dir_name_to_date(str):
        str = re.findall(r'\d{4}.\d{2}.\d{2}', str)
        if str:
            return datetime.strptime(str[-1], r'%Y-%m-%d')


def parsargs():
    description = '''Скрипт для очистки жестких дисков от старого видео архива'''
    pars = argparse.ArgumentParser(description=description, add_help=True)
    pars.add_argument('-l', '--limit', help='Лимит для активации скрипта. Указыватеся в метрах. -m 500', type=int,
                      default=LIMIT)
    pars.add_argument('-d', '--dir', nargs='+', help='Какие папки проверять. По умолчанию:{}'.format(CASES),
                      default=CASES)
    # pars.add_argument('-m', '--mount',  action='append' , help='Примаунченые диски которые надо проверять. По
    # умочанию: [{}]'.format(MON), default=MON)
    args = pars.parse_args()
    return args


if __name__ == '__main__':
    import subprocess

    args = parsargs()
    LIMIT = args.limit or LIMIT
    LIMIT = LIMIT * 1024 ** 2  # to bytes
    dirs = args.dir
    fs = subprocess.Popen('mount | grep -Po "{}"'.format(MON), shell=True, stdout=subprocess.PIPE)
    fs = [f for f in fs.communicate()[0].split('\n') if f]
    if not fs:
        logger.warning('No mounted hhd found with {}.'.format(MON))
        exit(0)
    logger.info('Founded disks: [ {} ]'.format(', '.join(fs)))
    for f in fs:
        disk = Disk(f, dirs)
        if disk.check(_gb(LIMIT)):
            disk.__clean__()
        else:
            logger.info('Mount point : "{}" is OK! Free space : {} | limit : {} gb'.
                        format(disk.mount_point, disk.disk.free, _gb(LIMIT)))
