import pandas as pd
import os
import json
from classes import Project, Well
from xlsxwriter.utility import xl_col_to_name


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


def get_data_tables(connect, wellbore_id):
    sql_query_idx = 'select * from WITS_RECORD1_IDX_{}'.format(wellbore_id)
    sql_query_data = 'select idx_id as id, mnemonic, value from WITS_RECORD1_DATA_{}'.format(wellbore_id)
    with connect as cn:
        idx_table = pd.read_sql_query(sql_query_idx, cn, index_col='id', parse_dates=['date'])
        data_table = pd.read_sql_query(sql_query_data, cn)
    # Разворачиваем таблицу, делая колонками мнемоники
    data_table = data_table.pivot(index='id', columns='mnemonic', values='value')
    # Мержим с индексой таблицей. Добовляем к каждому значению дату и глубину
    merget_table = idx_table.merge(data_table, left_index=True, right_index=True)

    return merget_table


def get_big_table(connect, actc_table, wellbore_id):
    data_dict = {}
    table = get_data_tables(connect, wellbore_id)
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
    table = get_big_table(connect, actc_table, wellbore_id)
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
    table.index = [int(id.get_values()[0]) for id in
                       [param_table[param_table['mnem'] == mnem].index for mnem in table.index]]
    table.sort_index(inplace=True)
    #  Отформатировали таблицу по параметрам
    #  Преобразовываем параметры в называния и юниты
    table.index = [' '.join([param_table['name'][ids], param_table['unit'][ids]]) for ids in table.index]
    table.columns.rename(names='Код технологического этапа', level=0, inplace=True)
    table.index.name = 'Параметры'

    return table

def excel_writer(table):

    # Записываем в фаил и форматируем как надо, пока фаил открыт.
    with pd.ExcelWriter('test_1.xlsx', engine='xlsxwriter', datetime_format='DD/MM/YY hh:mm:ss') as writer:
        sheet_name = '1 Рекорд'
        # table.reset_index(inplace=True)
        table.to_excel(writer, sheet_name=sheet_name)
        book = writer.book
        sheet = writer.sheets[sheet_name]
        # i_format = book.add_format({'align': 'left', 'bold': True})
        # sheet.conditional_format('B4:I26', {'type': '3_color_scale'})
        # sheet.set_column('A:A', 3)
        sheet.set_column('A4:A{}'.format(26), 28)  # На индексные ячейки формат не применяется =(
        for col in range(1, 100, 2):#['c', 'e', 'g', 'i', 'k', 'm', 'o', 'q']:

            sheet.set_column(':'.join([xl_col_to_name(col)] * 2), 18)


def main():
    # todo Это будет инпут от пользователя.
    name = 'NewWELL'
    project = 'st'
    list_of_records = [1] #, 2, 3]
    # --------------------------------------------------------------
    # Создаём движок для подключения к базе соответствующего проекта
    server = Project(project)
    server.fill()  # Загружаем конфиги
    server.sql_engine()  # Создаём SqlAlchemy движок
    well = Well(name, server)
    # --------------------------------------------------------------
    #
    tables = {
        'actc_table': get_actc(server.engine.connect()),
        'common_tables': {'record_' + str(k): get_param_table(server.engine.connect(), k, well.source_type_id) for k in list_of_records},
    }

    tables['data_tables'] = {
        'record_' + str(k): return_work_table(
            connect=server.engine.connect(),
            actc_table=tables['actc_table'],
            param_table=tables['common_tables'].get('record_' + str(k)),
            record_id=k,
            wellbore_id=well.wb_id
        ) for k in list_of_records
    }
    excel_writer(tables['data_tables']['record_1'])

if __name__ == '__main__':
    main()
