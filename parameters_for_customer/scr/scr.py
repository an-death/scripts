import pandas as pd
import os
import json
from projects import Project
import xlsxwriter

wb_id = '1'
project = 'st'

# Создаём движек для подключения к базе соответствующего проекта
server = Project(project)
server.fill()  # Загружаем конфиги
server.sql_engine()





sql_query_idx = 'select * from WITS_RECORD1_IDX_{}'.format(wb_id)
sql_query_data = 'select idx_id as id, mnemonic, value from WITS_RECORD1_DATA_{}'.format(wb_id)
sql_query_actc = 'select id, name_ru from WITS_ACTIVITY_TYPE'
# todo Допилить определение и получение сурс-тайпа и рекорда.
sql_query_param = 'select ' \
                  'wsp.mnemonic as mnem, ' \
                  'COALESCE(wu.name_ru, wu.name_en, wu2.name_ru,wu2.name_en) as unit, ' \
                  'wp.name_ru as name ' \
                  'from  ' \
                  'WITS_SOURCE_PARAM wsp LEFT JOIN WITS_PARAM wp ON (wsp.mnemonic=wp.mnemonic) ' \
                  'LEFT OUTER JOIN WITS_UNIT wu ON (wsp.unit_id=wu.id) ' \
                  'LEFT OUTER JOIN WITS_UNIT wu2 ON (wp.unit_id=wu2.id) ' \
                  'where source_type_id in ({})  and record_id in ({})  order by record_id, param_num;'.format(3,1)  # !!!

#  Создаём подключение к серверу и извлекаем все таблицы
with server.engine.connect() as cn:
    actc_table = pd.read_sql_query(sql_query_actc, cn)
    param_table = pd.read_sql_query(sql_query_param, cn)
    idx_table = pd.read_sql_query(sql_query_idx, cn, index_col=('id'), parse_dates=['date'])
    data_table = pd.read_sql_query(sql_query_data, cn)

#  Разворачиваем таблицу, делая колонками мнемоники
data_table = data_table.pivot(index='id', columns='mnemonic', values='value')
#  Мержим с индексой таблицей. Добовляем к каждому значению дату и глубину
table = idx_table.merge(data_table, left_index=True, right_index=True)
# Подчищаем более не нужные таблицы
del idx_table
del data_table
#

actc_table.id = actc_table.id.apply(float)
param_table.unit = param_table.unit.str.replace('&#179;', '3')
param_table.unit = param_table.unit.str.replace('&#178;', '2')
table.ACTC = table.ACTC.replace(actc_table.set_index('id').to_dict().get('name_ru'))

data_dict = {}
for col in table:
    if col in ['depth', 'date', 'ACTC', 'ACTC2', 'DBTM', 'DRTM', 'DMEA']:
        continue
#  Создаём словарь из таблиц с min/max данными по каждому мнемонику, за исключением вышеописанных
    data_dict[col] = pd.merge(
        table.loc[table.groupby("ACTC")[col].idxmin()][['ACTC', 'date', col]].rename(
            columns={col: 'min', 'date': 'date_min'}),
        table.loc[table.groupby("ACTC")[col].idxmax()][['ACTC', 'date', col]].rename(
            columns={col: 'max', 'date': 'date_max'}),
        on='ACTC',
        suffixes=('_min', '_max')).set_index('ACTC')
del table

big_table = pd.concat(list(data_dict.values()), keys=data_dict.keys(), axis=1)
big_table = big_table.unstack().unstack().unstack()
#  Преобазуем мнемоники в id и сортируем по id
big_table.index = [int(id.get_values()[0]) for id in
                  [param_table[param_table['mnem'] == mnem].index for mnem in big_table.index]]
big_table.sort_index(inplace=True)
#  Отформатировали таблицу по параметрам
#  Преобразовываем параметры в называния и юниты
big_table.index = [ ' '.join([param_table['name'][ids], param_table['unit'][ids]]) for ids in big_table.index]
big_table.columns.rename(names='Код технологического этапа', level=0, inplace=True)
big_table.index.name = 'Параметры'

 # Записываем в фаил и форматируем как надо, пока фаил открыт.
with pd.ExcelWriter('test.xlsx', engine='xlsxwriter', datetime_format='DD/MM/YY hh:mm:ss') as writer:
    sheet_name = '1 Рекорд'
    # big_table.to_excel(writer, sheet_name=sheet_name)
    big_table.to_excel(writer, sheet_name=sheet_name+'2')
    book = writer.book
    sheet = writer.sheets[sheet_name+'2']
    # sheet.format('A4:A35', {'align': 'left'})
    sheet.set_column('A:A', 28)
    for col in ['b', 'd', 'f', 'h', 'j', 'l', 'n', 'p']:
        sheet.set_column(':'.join([col.upper()]*2), 18)
