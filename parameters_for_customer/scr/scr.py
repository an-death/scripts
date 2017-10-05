import pandas as pd
# import os
# import json
from collections import OrderedDict
# from xlsxwriter import worksheet
from xlsxwriter.utility import xl_range, xl_col_to_name

from classes import Project, Well

RECORDS_COMPREHENSION = {
    'record_1': 'Основные временные',
    'record_11': 'Состояние емкостей',
    'record_12': 'Данные хромотографа'
}


def get_actc(connect):
    sql_query_actc = 'select id, name_ru from WITS_ACTIVITY_TYPE'
    with connect as cn:
        actc_table = pd.read_sql_query(sql_query_actc, cn)
    actc_table.id = actc_table.id.apply(float)
    return actc_table


def get_param_table(connect, record_id, source_type_id):
    sql_query_param = 'select ' \
                      'wsp.mnemonic as mnem, ' \
                      'COALESCE(wu.name_ru, wu.name_en, wu2.name_ru,wu2.name_en) as unit, ' \
                      'wp.name_ru as name ' \
                      'from  ' \
                      'WITS_SOURCE_PARAM wsp LEFT JOIN WITS_PARAM wp ON (wsp.mnemonic=wp.mnemonic) ' \
                      'LEFT OUTER JOIN WITS_UNIT wu ON (wsp.unit_id=wu.id) ' \
                      'LEFT OUTER JOIN WITS_UNIT wu2 ON (wp.unit_id=wu2.id) ' \
                      'where source_type_id in ({})  and record_id in ({})  ' \
                      'order by record_id, param_num;'.format(source_type_id, record_id)
    with connect as cn:
        param_table = pd.read_sql_query(sql_query_param, cn)
    # Заменяем спецсимволы из базы
    for i in (('3', '&#179;'), ('2', '&#178;')):
        param_table.unit = param_table.unit.str.replace(i[1], i[0])
    return param_table


def get_data_tables(connect, record_id, wellbore_id):
    sql_query_idx = 'select * from WITS_RECORD{}_IDX_{}'.format(record_id, wellbore_id)
    sql_query_data = 'select idx_id as id, mnemonic, value from WITS_RECORD{}_DATA_{}'.format(record_id, wellbore_id)
    with connect as cn:
        idx_table = pd.read_sql_query(sql_query_idx, cn, index_col='id', parse_dates=['date'])
        data_table = pd.read_sql_query(sql_query_data, cn)
    # Разворачиваем таблицу, делая колонками мнемоники
    data_table = data_table.pivot(index='id', columns='mnemonic', values='value')
    # Мержим с индексой таблицей. Добовляем к каждому значению дату и глубину
    merget_table = idx_table.merge(data_table, left_index=True, right_index=True)

    return merget_table


def get_big_table(connect, actc_table, record_id, wellbore_id):
    data_dict = {}
    table = get_data_tables(connect, record_id, wellbore_id)
    table.ACTC = table.ACTC.replace(actc_table.set_index('id').to_dict().get('name_ru'))

    for column in table:
        if column in ['depth', 'date', 'ACTC', 'ACTC2', 'DBTM', 'DRTM', 'DMEA']:
            continue
        # Создаём словарь из таблиц с min/max данными по каждому мнемонику, за исключением вышеописанных
        data_dict[column] = pd.merge(
            table.loc[table.groupby("ACTC")[column].idxmin()][['ACTC', 'date', column]].rename(
                columns={column: 'min', 'date': 'date_min'}),
            table.loc[table.groupby("ACTC")[column].idxmax()][['ACTC', 'date', column]].rename(
                columns={column: 'max', 'date': 'date_max'}),
            on='ACTC',
            suffixes=('_min', '_max')).set_index('ACTC')

    big_table = pd.concat(list(data_dict.values()), keys=data_dict.keys(), axis=1)
    big_table = big_table.unstack().unstack().unstack()
    return big_table


def return_work_table(connect, param_table, actc_table, record_id, wellbore_id):
    table = get_big_table(connect, actc_table, record_id, wellbore_id)
    # ____-----debug-------------------------------------------------
    # def repiter(table, param_table):
    #     ids_list = []
    #     for mnem in table.index:
    #         param = param_table[param_table['mnem'] == mnem]
    #         try:
    #             ids = param.index
    #             ids_list.append(int(ids.get_values()[0]))
    #         except Exception:
    #             print('{}'.format(mnem))
    #             exit(1)
    #     return ids_list
    # table.index = repiter(table, param_table)
    # _--------------------------------------------------------------
    #  Преобазуем мнемоники в id и сортируем по id
    table.index = [int(ids.get_values()[0]) for ids in
                   [param_table[param_table['mnem'] == mnem].index for mnem in table.index]]
    table.sort_index(inplace=True)
    #  Отформатировали таблицу по параметрам
    #  Преобразовываем параметры в называния и юниты
    table.index = [' '.join([param_table['name'][ids], param_table['unit'][ids]]) for ids in table.index]
    table.columns.rename(names='Код технологического этапа', level=0, inplace=True)
    table.index.name = 'Параметры'

    return table


def write_first_sheet(writer, common_tables_dict):

    sheet_name = 'Параметры'
    full_param_table = pd.concat([common_tables_dict[key] for key in common_tables_dict],
                                 copy=False, axis=1,
                                 join_axes=[common_tables_dict['record_1'].index])

    full_param_table.to_excel(writer, sheet_name, index=False, header=['Мнем', 'Юнит', 'Параметр'] * 3
                              )
    sheet = writer.sheets[sheet_name]
    for i in range(2, len(full_param_table.columns), 3):
        sheet.set_column(':'.join([xl_col_to_name(i)] * 2), 28)


def write_data_tables(writer, data_tables, formats):

    for table_key in data_tables:
        sheet_name = RECORDS_COMPREHENSION[table_key]
        table = data_tables[table_key]
        # table.reset_index(inplace=True)
        table.to_excel(writer, sheet_name=sheet_name)

        last_row = len(table.index) + 2
        last_col = len(table.columns)
        sheet = writer.sheets[sheet_name]
        # for row in range(3, last_row, 2):
        #     sheet.set_column(':'.join([xl_range(row, 0 , row, last_col)]), cell_format=blue)
        sheet.set_column('A:A', 28)  # На индексные ячейки формат не применяется =(
        for col in range(1, last_col, 2):
            dates = ':'.join([xl_range(3, col, last_row, col)])
            datas = ':'.join([xl_range(3, col + 1, last_row, col + 1)])
            sheet.set_column(dates, 18, None, {'align': 'left'})
            if datas:
                # sheet.conditional_format(datas, {'type': '3_color_scale'})
                sheet.conditional_format(datas, {'type': 'cell',
                                                 'criteria': '>',
                                                 'value': '5000',
                                                 'format': formats['red']})
                sheet.conditional_format(datas, {'type': 'cell',
                                                 'criteria': '==',
                                                 'value': '0',
                                                 'format': formats['grey']})


def excel_writer(well_name, common_tables, data_tables):
    # Записываем в фаил и форматируем как надо, пока фаил открыт.
    file_name = well_name.replace(' ', '_') + '.xlsx'
    with pd.ExcelWriter(file_name, engine='xlsxwriter', datetime_format='DD/MM/YY hh:mm:ss') as writer:
        book = writer.book
        #  Formats
        formats = {
            'red':book.add_format({'bg_color': '#FFC7CE',
                                 'font_color': '#9C0006'}),
            'grey':book.add_format({'bg_color': '#f0f0f0'}),
            'blue':book.add_format({'bg_color': '#eceff4'}),
            'date_format':book.add_format({'align': 'left'})
        }

        write_first_sheet(writer, common_tables)
        write_data_tables(writer, data_tables, formats)


def main():
    # todo Это будет инпут от пользователя.
    well_name = 'Уренгойское к. U31'
    project = 'st'
    list_of_records = [1, 11, 12]
    # ---------------------------------------------------------------
    list_of_records.sort()
    # --------------------------------------------------------------
    # Создаём движок для подключения к базе соответствующего проекта
    server = Project(project)
    server.fill()  # Загружаем конфиги
    server.sql_engine()  # Создаём SqlAlchemy движок
    well = Well(well_name, server)
    # --------------------------------------------------------------
    # Выгружаем и формируем дополнительные таблицы
    tables = {
        'actc_table': get_actc(server.engine.connect()),
        'common_tables':
            OrderedDict(('record_' + str(k),
                         get_param_table(
                             connect=server.engine.connect(),
                             record_id=k,
                             source_type_id=well.source_type_id
                         )) for k in list_of_records)
    }
    # Выгружаем и формируем табилцы из таблиц с данными.
    tables['data_tables'] = OrderedDict(('record_' + str(k),
                                         return_work_table(
                                             connect=server.engine.connect(),
                                             actc_table=tables['actc_table'],
                                             param_table=tables['common_tables'].get('record_' + str(k)),
                                             record_id=k,
                                             wellbore_id=well.wb_id
                                         )) for k in list_of_records)
    # ---------------------------------------------------------------
    # Отправляем таблицы на запись
    excel_writer(well_name, tables['common_tables'], tables['data_tables'])


if __name__ == '__main__':
    main()
