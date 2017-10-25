# -*- coding:utf-8  -*-

from collections import defaultdict

from classes import User, Dt

from base_models.wits_models import (Wits_user_log as log,
                                     Wits_user_event as event)
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

event_table = session.query(event.id, event.name_en).all()
event_dict = {k: v for v, k in event_table}

for user_id, date, data, event_id in log_table.all():

    if user_id != -1 and user_id not in USERS:
        USERS[user_id] = User(user_id, session)

    if event_id == event_dict['Stop'] and user_id == -1:
        close_all_active_session(Dt(date), USERS)
    elif event_id in (event_dict[i] for i in ['Logout', 'Session timeout', 'Stop']):
        pass
    elif event_id == event_dict['Session killed']:
        pass
    elif event_id == event_dict['Login']:
        pass
