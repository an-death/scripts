# -*- coding:utf-8  -*-

from collections import defaultdict

from classes import User, Dt

from base_models.wits_models import (Wits_user_log as log,
                                     Wits_user_event as event)
from projects import project

# todo Сделать вводом из формы или cli
FROM = '2017-08-31'
TO = '2017-10-01'
PROJECT = 'bke'
######################################################################
USERS = defaultdict(User)


def get_connect_to_db(project_name: str):
    bke = project.Project(project_name)
    bke.configurate()
    return bke.sql_sessionmaker()


def get_events_table(session):
    event_table = session.query(event.id, event.name_en).all()
    return {k: v for v, k in event_table}


def get_wits_user_log_data(session, limit):
    log_table = session.query(log.user_id, log.date, log.data, log.event_id)
    log_table = log_table.filter(log.date.between(limit['start'].to_request(), limit['stop'].to_request()))
    log_table = log_table.filter(log.data.notlike('%=wchange=%')).filter(log.data.notlike('%=vload=%'))
    # print('\n'.join([str(row) for row in log_table.all()]))
    return log_table


def close_all_active_session(date: Dt, users_dict):
    for user in users_dict.values():
        if user.is_logged():
            user.close_active_session(date)


def main(u: USERS, p: PROJECT):
    limit = {'start': Dt(FROM), 'stop': Dt(TO)}

    session = get_connect_to_db(p)
    event_dict = get_events_table(session)
    log_table = get_wits_user_log_data(session, limit)

    log_table = log_table.all()
    for user_id, date, data, event_id in log_table:
        date = Dt(date)

        if user_id not in u and user_id > 0:
            u[user_id] = User(user_id, session)

        user = u[user_id]

        if event_id == event_dict['Stop'] and user_id == -1:
            close_all_active_session(Dt(date), u)
        elif event_id in (event_dict[i] for i in ['Logout', 'Session timeout']):
            if not user.is_logged(): continue
            # todo Написать интерфейс для сессии
            user.session[data].close(date)
            user.session_stop()
        elif event_id == event_dict['Session killed']:
            if not user.is_logged(): continue
            active_session, new_session = data.split('=')
            user.session[active_session].close(date)
            user.session_stop()
        elif event_id == event_dict['Login']:
            if user.is_logged():
                user.close_active_session()
                user.session_stop()

            user.session[data].start(date)
            user.session_start()
        elif event_id == event_dict['User action']:
            # active_session, args = data.split('!')
            pass


if __name__ == '__main__':
    main(USERS, PROJECT)
