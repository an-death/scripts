import pandas as pd
import os
import json
from projects import Project


wb_id = '508'
sql_query_idx = 'select * from WITS_RECORD1_IDX_{}'.format(wb_id)
sql_query_data = 'select idx_id as id, mnemonic, value from WITS_RECORD1_DATA_{}'.format(wb_id)
sql_query_actc = 'select id, name_ru from WITS_ACTIVITY_TYPE'
sql_query_param = 'select ' \
                  'wsp.mnemonic as mnem, ' \
                  'COALESCE(wu.name_ru, wu.name_en, wu2.name_ru,wu2.name_en) as unit, ' \
                  'wp.name_ru as name ' \
                  'from  ' \
                  'WITS_SOURCE_PARAM wsp LEFT JOIN WITS_PARAM wp ON (wsp.mnemonic=wp.mnemonic) ' \
                  'LEFT OUTER JOIN WITS_UNIT wu ON (wsp.unit_id=wu.id) ' \
                  'LEFT OUTER JOIN WITS_UNIT wu2 ON (wp.unit_id=wu2.id) ' \
                  'where source_type_id in ({})  and record_id in ({})  order by record_id, param_num;'.format(7,1)
server = Project('bke')
server.fill()
server.sql_engine()

with server.engine.connect() as cn:
    actc_table = pd.read_sql_query(sql_query_actc, cn)
    param_table = pd.read_sql_query(sql_query_param, cn)
    idx_table = pd.read_sql_query(sql_query_idx, cn, index_col=('id'), parse_dates=['date'])
    data_table = pd.read_sql_query(sql_query_data, cn)

data_table = data_table.pivot(index='id', columns='mnemonic', values='value')
merget = idx_table.merge(data_table, left_index=True, right_index=True)
del idx_table
del data_table

# x = merget.ACTC.apply(lambda x: actc_table[actc_table['id'] == int(x)]['name_ru'])
actc_table.id = actc_table.id.apply(float)
param_table.unit = param_table.unit.str.replace('&#179;', '3')
param_table.unit = param_table.unit.str.replace('&#178;', '2')
merget.ACTC = merget.ACTC.replace(actc_table.set_index('id').to_dict().get('name_ru'))

########################################################################################################################
# Столбовое представление параметров.
# table = merget.drop(['ACTC2','date','depth','DMEA'], axis='columns').\
#     groupby('ACTC', axis='index', sort=False, as_index=False).\
#     agg([min,max]).unstack(0).unstack(-1).unstack()
table = merget

data_dict = {}
for col in table:
    # print(col)
    if col in ['depth', 'date', 'ACTC', 'ACTC2', 'DBTM', 'DRTM', 'DMEA']:
        continue
    data_dict[col] = pd.merge(
        table.loc[table.groupby("ACTC")[col].idxmin()][['ACTC', col]].rename(
            columns={col: 'min'}),
        table.loc[table.groupby("ACTC")[col].idxmax()][['ACTC', col, 'date']].rename(
            columns={col: 'max', 'date': 'date_max'}),
        on='ACTC',
        suffixes=('_min', '_max')).set_index('ACTC')

big_table = pd.concat(list(data_dict.values()), keys=data_dict.keys(), axis=1)
big_table = big_table.unstack().unstack().unstack()
#  Преобазуем мнемоники в id и сортируем по id
big_table.index =[int(id.get_values()[0]) for id in
                  [param_table[param_table['mnem'] == mnem].index for mnem in big_table.index]]
big_table.sort_index(inplace=True)
#  Отформатировали таблицу по параметрам
#  Преобразовываем параметры в называния и юниты
big_table.index = [ ' '.join([param_table['name'][ids], param_table['unit'][ids]]) for ids in big_table.index]
big_table.columns.rename(names='Код технологического этапа', level=0, inplace=True)
big_table.index.name = 'Параметры'
#######################################################################################################################3
# Строчное представление параметров.
# merget = merget.drop(['ACTC2', 'date', 'DMEA'], axis='columns')
# merget = merget.set_index('ACTC')
# for i, mnem in enumerate(param_table.mnem.values):
#     param_name = param_table[param_table['mnem'] == mnem]['name'].values[0]
#     unit = param_table[param_table['mnem'] == mnem]['unit'].values[0]
#     if mnem == 'ACTC':
#         merget.index.name = param_name
#         continue
#     merget = merget.rename(columns={'{}'.format(mnem): '{}, {}'.format(param_name, unit)})
#
# # todo Представить все ACTC как описания
# # todo Представить все мнемоники как Описание параметра + юнит
# merget_g = merget.groupby(level=0, as_index=False, sort=False, group_keys=False).agg([min, max])
# merget_g.to_csv('nova_well.csv')
################################################

big_table.to_csv('5020.csv')
