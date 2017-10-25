# -*- coding:utf-8  -*-

from collections import defaultdict

from classes import User, Dt

from base_models.wits_models import (Wits_user_log as log)
from projects import project

PERIOD = {'from': '2017-08-31', 'to': '2017-10-01'}

bke = project.Project('bke')
bke.configurate()
session = bke.sql_sessionmaker()
USERS = defaultdict(User)


def close_all_active_session(date: Dt, users_dict):
    pass


limit = {'start': Dt(PERIOD['from']), 'stop': Dt(PERIOD['to'])}
log_table = session.query(log.user_id, log.date, log.data, log.event_id)
log_table = log_table.filter(log.date.between(limit['start'].to_request(), limit['stop'].to_request()))
log_table = log_table.filter(log.data.notlike('%=wchange=%')).filter(log.data.notlike('%=vload=%'))
# print('\n'.join([str(row) for row in log_table.all()]))
