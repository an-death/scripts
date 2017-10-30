# -*- coding:utf-8  -*-

from collections import defaultdict

import pandas as pd
from classes import User, Dt, get_connect_to_db
from users_report import get_table, aliases
from xlsxwriter.utility import xl_range

from base_models.wits_models import (Wits_user_log as log,
                                     Wits_user_event as event)

# todo Сделать вводом из формы или cli
FROM = '2017-08-31'
TO = '2017-10-01'
PROJECT = 'bke'
SHEET_NAMES = ['Сентябрь']
######################################################################
USERS = defaultdict(User)


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
    # log_table = log_table.filter(log.user_id.in_([-1,63]))
    return log_table.all()


def close_all_active_session(date: Dt, users_dict):
    for user in users_dict.values():
        if user.is_logged:
            user.close_active_session(date)


def users_activity_as_dict(users: dict):
    # Подсчитываем время
    users_activity_dict = {}
    for u in users.values():
        u.calculate_total()
        k = u.fio or u.param.name
        users_activity_dict[k] = {'video': u.total_video_time.to_human(),
                                  'total': u.total_monitoring_time.to_human()}
    return users_activity_dict


def write_sheet(sheet_name, writer, table):
    table.to_excel(writer, sheet_name=sheet_name, index_label='№п/п', startrow=1, header=False)
    book = writer.book
    # Add a header format.
    header_format = book.add_format({
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'top',
        'fg_color': '#D7E4BC',
        'border': 2})
    formats = {
        'clean': book.add_format({'border': None}),
        'fio': book.add_format({'text_wrap': True, 'border': 1}),
        'data': book.add_format({
            'text_wrap': True,
            'align': 'center',
            'valign': 'center',
            'border': 1})
    }

    sheet = writer.sheets[sheet_name]
    first_row = 1 + len(table.index)
    clean_range = xl_range(first_row, 0, first_row * 10, len(table.columns))
    # Записываем шапку
    col = ['№п/п', 'ФИО', 'Видео', 'Всего'] if sheet_name != 'Пользователи' else aliases.values()
    for col_num, value in enumerate(col):
        sheet.write(0, col_num, value, header_format)
    # Форматируем данные
    if sheet_name != 'Пользователи':
        sheet.set_column('B:B', 30, formats['fio'])
        sheet.set_column('C:C', 20, formats['data'])
        sheet.set_column('D:D', 20, formats['data'])
    else:
        sheet.set_column('B:B', 30, formats['fio'])
        sheet.set_column('C:C', 18, formats['fio'])
        sheet.set_column('D:D', 38, formats['fio'])
        sheet.set_column('E:E', 38, formats['fio'])
        sheet.set_column('F:F', 20, formats['fio'])
        sheet.set_column('G:G', 15, formats['fio'])
        sheet.set_column('H:H', 28, formats['fio'])
    # clean all below data
    sheet.conditional_format('B1', {'type': 'blanks',
                                    'multi_range': clean_range,
                                    'format': formats['clean']})


def create_xlsx(file_name, user_table, activity_table):
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        write_sheet('Пользователи', writer, user_table)
        # sheet_name = 'Сентябрь'  # todo сделать автоматическое опеределение месяца и цикл
        write_sheet(SHEET_NAMES[-1], writer, activity_table)


def main(u: USERS, p: PROJECT):
    # todo Дробить промежутки по месяцам
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
    # close_all_active_session(date, u)
    table = pd.DataFrame(users_activity_as_dict(u))
    table = table.unstack().unstack().reset_index()
    table = table.reindex(columns=['index', 'video', 'total'])
    user_table = get_table(dbconnection)
    create_xlsx('reports/Список пользователей GTI-online.xlsx', user_table, table)


if __name__ == '__main__':
    main(USERS, PROJECT)
