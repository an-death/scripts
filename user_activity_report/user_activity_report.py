# -*- coding:utf-8  -*-

from collections import defaultdict

import pandas as pd
from classes import User, Dt, sort_by_month
from table_writer import create_xlsx
from users_report import get_table

from base_models.wits_models import (Wits_user_log as log,
                                     Wits_user_event as event)
from projects.project import get_connect_to_db

# todo Сделать вводом из формы или cli
FROM = '2017-08-31'
TO = '2017-10-01'
PROJECT = 'bke'
######################################################################
USERS = defaultdict(User)


def apply_nested(func, lst, isatom=lambda item: not isinstance(item, list)):
    for i, item in enumerate(lst):
        if isatom(item):
            lst[i] = func(item)
        else:
            apply_nested(func, item, isatom)
    return lst


def get_events_table(session):
    """
    Дёргаем из базы таблицу WITS_USER_EVENT и приобразуем её в словарь
    :param session: 
    :return: Dict('Stop': 7)
    """
    event_table = session.query(event.id, event.name_en).all()
    return {name: _id for _id, name in event_table}


def get_wits_user_log_data(session, limit):
    log_table = session.query(log.user_id, log.date, log.data, log.event_id)
    log_table = log_table.filter(log.date.between(limit['start'].to_request(), limit['stop'].to_request()))
    log_table = log_table.filter(log.event_id.notin_([5, 6, 9]))
    # Для отловли коллизий
    # log_table = log_table.filter(log.user_id.in_([-1,311]))
    return log_table.all()


def close_all_active_session(date: Dt, users_dict):
    for user in users_dict.values():
        if user.is_logged:
            user.close_active_session(date)


def calculate_all_users_activity(users):
    for u in users.values():
        u.calculate_total()


def users_activity_as_dict(users: dict):
    users_activity_dict = {'video': {}, 'total': {}}
    for i, u in enumerate(users.values()):
        users_activity_dict['video'][i], users_activity_dict['total'][i] = u.total_video_time, u.total_monitoring_time
    return users_activity_dict['video'], users_activity_dict['total']


def prepare_table(users, data_dict):
    users_info = {}
    # формируем словарик с информацией по пользователям в формате i:{info}
    for i, u in enumerate(users.values()):
        fio = u.fio or u.param.name
        group = u.param.group.name if u.param.group_id else ' '
        users_info[i] = {
            'Group': group,
            'Fio': fio,
            'Position': u.param.position
        }
    # Преобразуем словарик с пользователями в pandas DataFrame
    user_info_table = pd.DataFrame.from_dict(users_info, orient='index')
    # Добавляем в дата-фрэйм пустой второй столбец с "Экспедицией"
    user_info_table.insert(1, 'Expedition', None)
    # В случае необходимости заполнения поля ^ Сюда вставляем данные!
    # Делаем мультииндексной шапку таблицы пользователей, т.е. теперь шапка занимает 2е строки.
    # Нужно для конката с данными
    user_info_table.columns = pd.MultiIndex.from_arrays(
        [user_info_table.columns, user_info_table.columns])

    # Заголовок для столбцов с данными (верхняя шапка мультииндекса)
    default_head = 'Колличество часов'
    # Подготовка кортежей для шапки таблиц с данными по месяцам и таблицы с суммой
    columns_time = tuple([(default_head, k) for k in sorted(data_dict[0].keys(), key=sort_by_month)])
    columns_total = ((default_head, 'Total'),)
    # Подготовка листа с данными для заполенеия таблиц
    datas = [[data.get(k[-1], Dt(0)) for k in columns_time] for data in data_dict.values()]
    datas_total = [[Dt(sum(map(int, data.values())))] for data in data_dict.values()]

    # Таблица с данными по месяцам
    time = pd.DataFrame. \
        from_records(data=(apply_nested(Dt.to_human, datas)),
                     index=data_dict.keys(),
                     columns=pd.MultiIndex.from_tuples(columns_time)
                     )
    # Таблица с общим временем
    total = pd.DataFrame. \
        from_records(data=apply_nested(Dt.to_human, datas_total),
                     index=data_dict.keys(),
                     columns=pd.MultiIndex.from_tuples(columns_total)
                     )
    # Пустой столбец "Примечание"
    empty = pd.DataFrame. \
        from_records(data=[[None]] * len(data_dict.keys()),  # Данные вставлять сюда
                     index=data_dict.keys(),
                     columns=pd.MultiIndex.from_tuples(tuples=(('Примечание', 'Примечание'),))
                     )
    # Создаём общий ДФ из таблиц
    merged = pd.concat([user_info_table, time, total, empty], axis=1, copy=False)
    return merged


def main(u: USERS, p: PROJECT):
    limit = {'start': Dt(FROM), 'stop': Dt(TO)}

    # Создаём сессию подключения к базе, передавая шорткат проекта
    dbconnection = get_connect_to_db(p)
    # Дёргаем из базы таблицу WITS_USER_EVENT и приобразуем её в словарь
    event_dict = get_events_table(dbconnection)
    # Возвращаем список всез записей из WITS_USER_LOG в пределах limits
    log_table = get_wits_user_log_data(dbconnection, limit)

    # ===============================================================================================================
    # START CYCLE
    for user_id, date, data, event_id in log_table:
        date = Dt(date)

        # Если юзера нету в словаре и это не сервер (u_id = -1)
        # Создаём юзера и добавляем в словарь
        if user_id not in u and user_id > 0:
            u[user_id] = User(user_id, dbconnection)
        # В случае рестарта стрима - обрываем все активные сессии всех пользователей
        if event_id == event_dict['Stop'] and user_id == -1:
            close_all_active_session(date, u)
            continue

        user = u[user_id]
        stop_cond = (event_dict[i] for i in ['Logout', 'Session timeout'])

        if user.is_logged:
            if event_id in stop_cond:
                user.session_stop(data, date)
            elif event_id == event_dict['Session killed']:
                active_session, new_session = data.split('=')
                user.session_stop(active_session, date)
            elif event_id == event_dict['Login']:
                # Stop active session because user already is logged!
                user.close_active_session(date)
                # Start new session
                user.session_start(data, date)
            elif event_id == event_dict['User action']:
                active_session, args = data.split('!')
                user.session_store(active_session, date, args)
        else:  # user.is_logout!
            if event_id in stop_cond:
                # user tried logout before login
                # save collision in list
                user.collision_sessions = data
                continue
            if event_id == event_dict['Login'] and data not in user.collision_sessions:
                user.session_start(data, date)
    # ===================================================================================================================
    # CYCLE END

    # Закрываем все не закрытые сессии последней полученной датой.
    close_all_active_session(date, u)
    calculate_all_users_activity(u)

    user_table = get_table(dbconnection)
    video_dict, total_dict = users_activity_as_dict(u)
    video_table = prepare_table(u, video_dict)
    total_table = prepare_table(u, total_dict)

    create_xlsx('reports/Список пользователей GTI-online.xlsx', user_table, video_table, total_table)


if __name__ == '__main__':
    main(USERS, PROJECT)
