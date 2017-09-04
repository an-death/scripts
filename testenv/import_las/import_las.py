#!/usr/bin/python3
# -*- coding: utf-8 -*-
#  read requirements.txt for manage env
#
########################################################################################################################
# IMPORT
import re
import path as p
import pandas as pd
import requests
import subprocess

from datetime import datetime
from sqlalchemy import create_engine

########################################################################################################################

########################################################################################################################
# CONFIG todo move all var to json or config file or reformat script for input
########################################################################################################################
# ALL INFO in README.md
FROM_FILE = '/home/as/Documents/wells_for_nova/374/374-10_5870-7819var.las'  # File for upload data. Change it!
# WELL CONFIGS
WELL_NAME = '374'  # ЗАМЕНИТЬ!
WELL_ID = '321'  # Замерить!
WELLBORE_ID = '358'  # Заменить!
SOURCE_TYPE = '7' # Леуза

#######################################################
# Download config
######################################################
IGNORE_RECORDS = []  # Left empty if first start
SKIP_IDS = 0  # Left 0 or empty if first start
SHIFT = 3000  # С каким шагом по индексам отсылать данные. При слишком больших запросах сервер может послать на.

#######################################################
IMPORT_RECORD = 'depth'  # else 'time'
LIST_OF_RECORDS = {
    'time': [1, 11, 12],
    'depth': [2, 8, 13]
}
patterns = ['LAS', 'las']
# Map-file will be created in parent dir your file
MAP = 'map_{}.csv'.format(IMPORT_RECORD)
# Session marker for monitoring
SESSION = ''
HOST = ''
# piece of first string for validate encoding
first_str_of_file = 'LAS файл, созданный программой "GeoData"'
ENCODINGS = ['ibm866', 'utf-8', 'WINDOWS-1251', 'KOI8-R']  # Encodings. U can add more if u need
NA_VALUE = '-32768.00' # todo Получвть из ласа
# info for connect to mysql DB
SQL_DEBUG = False  # debug for sqlalchemy
BASENAME = 'nova'
METHOD = 'INSERT'  # INSERT/REPLACE метод каким вставлять данные
store = {
    'st': {
        'name': 'name',
        'port': 'port',
        'host': '127.1',
        'user': 'user',
        'password': 'password'
    },
    'nova': {
        'name': 'name',
        'port': 'port',
        'host': '127.1',
        'user': 'login',
        'password': 'password'
    }
}

########################################################################################################################
# var for store las-file
file = {}
########################################################################################################################


def main():
    #################################################################
    #  Запращиваем путь до файла
    p = 'path_to_file'
    e = 'encoding'
    row_path = input('Введите путь до файла: ') if not FROM_FILE else FROM_FILE
    file[p] = get_file(row_path)
    if not file[p]: qq(0)
    # Определяем кодировку файла перебирая список
    file[e] = get_file_encoding(file[p], ENCODINGS, first_str_of_file)
    if not file[e]: qq(0)
    #################################################################
    #  Проверяем есть ли фаил мапинга в дирректории с ласом
    #  Если нет, создаём
    las_indexes = parse_las(file[p], file[e])
    if not las_indexes: qq(0)
    # Создаём подключение к базе
    engine = getengine(BASENAME)
    # Создаём инфо таблицу из ласа
    info = get_info_table_from_las(file[p], las_indexes, file[e])
    map_file = check_map(file[p], MAP)
    if not map_file:  # Если нет файла маппинга
        print('Создаём {}'.format(MAP))
        rec = get_param_table_db(engine)  # Берём LIST_OF_RECORDS и Выгружаем таблицу из базы
        map_default = check_of_existing_default_map(file[p])  # map_default.csv
        if map_default is not None:
            print('Найден фаил с дефолтныйм маппингом, используем его для формирования маппинга!')
            table, map_file_destination = map_values(info, rec, map_default, file[p])
            table.to_csv(map_file_destination, sep='|')  # Записываем созданный нв основе дефолта мапинг в фаил.
        else:
            table, map_file_destination = map_simple_merge(info, rec, file[p])  # Мержим, создаём путь.
            table.to_csv(map_file_destination, set='|')  # Записываем в фаил
        # print("I'm create map file, please check mapping before start me again!")
        print('Я создал MAP-file проверьте, отформатируйте его и запустите меня снова!')
        subprocess.Popen('xdg-open {}'.format(map_file_destination).split())
        qq(0)
    else:
        print('Фаил маппинга найден!\n{}\n'.format(map_file))
        mapping = read_mapping(map_file)  # pandas
        if not check_mapping(mapping, info): qq(0, 'Произошла ошибка при чтении мапинга!')
    print('Выгружаем данные из LAS-файла...')
    las_data = get_las_data(file[p], mapping, las_indexes, encoding=file[e])
    idx_table = pd.concat([las_data['DTM'], las_data['DMEA']], axis=1)
    data_table = las_data.drop('DTM', axis=1)
    print('Проверяем WELL...'
          '\nВы указали следующие данные: '
          '\nWELL_ID={} '
          '\nWELLBORE_ID={}'
          '\nВыполняем...'.format(WELL_ID, WELLBORE_ID))
    well_id = get_well_by_wellbore_id(engine, WELLBORE_ID)
    if not well_id[0] == int(WELL_ID): qq(1, message='WELL по WELLBORE_ID={} не найден! [Y/n]:'.format(WELLBORE_ID))
    well_name = get_well_name_by_well_id(well_id[0])
    if well_name[0] != WELL_NAME:
        cond = input('FILE_NAME={}\nWELL_NAME = {}. \nВы уверены? [Y/n]:'.format(file[p], well_name))
        if not true_condition(cond): qq(0)
    else:
        print('Имя скважины в конфиге соответствует имени скажины в базе. \n{} == {}'.format(well_name, WELL_NAME))
    # отправляем чтобы создать все точки в record_idx_{} b таблицы WITS_RECORD
    # send_data(las_data, mapping)  # В базу импутится но с временем now()
    # print('Проверяем таблицы.')
    check_record_tables(LIST_OF_RECORDS.get(IMPORT_RECORD), wellbore_id=WELLBORE_ID)
    insert_data({'idx': idx_table, 'data': data_table, 'map': mapping}, LIST_OF_RECORDS.get(IMPORT_RECORD),
                WELLBORE_ID)
    print('{}\nГОТОВО!\n'.format(file[p]))


########################################################################################################################
# common func's
def true_condition(answer):
    """
    Just confirm user input
    :param answer:
    :return:
    """
    if answer and answer.lower() in ('y', 'д', 'yes', 'да'):
        return True
    print('Your choose is "{}"'.format(answer))


def get_method():
    print('Выбирите метод для вставки данных в базу:\n'
          '1)"INSERT" - Для добавления данных(новая скважина/новый фаил)\n'
          '2)"REPLACE" - Для замены данных. Использовать для единого файла.(если вы не правильно указали скважину, пеняйте на себя!)\n'
          '*)Default = {}'.format(METHOD))
    ch = input('Введите [1/2/*] или q для выхода:')
    if ch in ['1', 'INSERT']:
        return 'INSERT'
    elif ch in ['2', 'REPLACE']:
        return 'REPLACE'
    elif ch in ['q', 'й']:
        qq(0)
    else:
        return METHOD

def counter(func):
    def wrapper(*args):
        res = func(*args)
        wrapper.count += 1
        return res

    wrapper.count = 0
    return wrapper


def qq(code=0, message=None):
    """
    Exit with code. Default is 0
    """
    print('{}\nВыходим'.format(message))
    exit(code)


########################################################################################################################
# WORK WITH LAS-FILE
########################################################################################################################

def get_file(row_path):
    """
    Запрашиваем имя и путь файла у пользователя
    Если введено верно возвращаем путь до файла.
    :return: path_to_file
    """
    if p.Path(row_path).isfile() and row_path.split('.')[-1] in patterns:
        print('Ваш фаил " {} "'.format(row_path))
        return row_path
    elif p.Path(row_path).isdir():
        files = []
        for pattern in patterns:
            for file in p.Path(row_path).walk('*.{}'.format(pattern)):
                files.append(file)
        print('Вы указали папку.\nА я нашел файлы:\n', '\n'.join(files))
        row_path = input('Выберете фаил: ')
        return row_path
    else:
        print('Вы указали: {}'.format(row_path), '\nМне не удалось найти там LAS-файлов')


def get_file_encoding(file, list_of_encodings, first_str_of_file):
    """
    Im not actually sure did that work or not!
    Because it's cycle with try all encoding in list
    But it's the simples way to get encoding
    Перебором пробуем все кодировки из списка. Если не возникает ошибки при декодировке
    Пишем результат для наглядности и запрашиваем у пользователя ответ.
    Если соответствует первой строке файла, корировка автоматически считается подобранной.
    :param first_str_of_file:   first string of file in right encoding
    :param file:                path to file
    :param list_of_encodings:
    :return:                    encoding
    """
    encodings = (enc for enc in list_of_encodings)  # create iterator
    for encoding in encodings:
        try:
            with open(file, encoding=encoding) as f:
                line = f.readline()
                while line.startswith('\n'):
                    line = f.readline()
                if first_str_of_file not in line:
                    print('Encoding found! I`m decoded 1st line like this:\n{}'.format(line))
                    answer = input('Did it right?[Y/n]: ')
                    if true_condition(answer):
                        return encoding
                    else:
                        print('Continue...\n')
                else:
                    print('Encoding found! I`m decoded 1st line like this:\n{}'.format(line))
                    return encoding
        except UnicodeDecodeError:
            continue
    print("[ERROR] File encoding doesnt determined! I'm didn't found right encoding from list  {} ".format(
        list_of_encodings))


def parse_las(las, encoding, list_of_fields='all'):
    """
    Reading las-file for found indexes for info_table, and data_table
    :param list_of_fields: list of fields in las-file for parse. USE DEFAULT
    :param las: path to file
    :param encoding:
    :return: dict with keys: info_table, data_table
                       values: start and end indexes
    """

    def get_idx(regx):
        lines = 1
        with open(las, encoding=encoding) as fd:
            for line in fd.readlines():
                if line.startswith(regx):
                    idx = lines - 2 if regx == '~Other Information' else lines + 1
                    return idx
                lines += 1

    if list_of_fields == 'all':
        commons = {
            'info_start': '~Curve',
            'info_stop': '~Other Information',
            'data_start': '~ASCII Log Data'
        }
        # todo make else?
        indexes = {
            'info_table': (get_idx(commons['info_start']),
                           get_idx(commons['info_stop'])),
            'data_table': (get_idx(commons['data_start']),)
        }

        return indexes


########################################################################################################################
# Work with MAP-file
########################################################################################################################
def check_map(path_to_file, map_file_name=MAP):
    """
    :param path_to_file: # todo make acceptable for smb. http, ftp link's
    :param map_file_name: Default map.csv
    :return: Bool( True / False )
    """
    dir = p.Path(path_to_file).parent
    if p.Path(dir + '/' + map_file_name).isfile():
        return p.Path(dir + '/' + map_file_name)


def check_of_existing_default_map(path_to_file):
    """
    Проверяем на наличие дефолтного маппинга в родительской дирректории.
    Если файла нет, возвращаем None
    Если фаил есть возвращаем pandas табличку
    :param path_to_file:
    :return:    True - pandas.DataFrame
                False - None
    """
    default_map_name = 'map_default.csv'
    parent_dir = p.Path(path_to_file).parent
    path_to_map = parent_dir + '/' + default_map_name
    if p.Path(path_to_map).isfile():
        return read_mapping(path_to_map)


def map_simple_merge(db, info, to_file):
    merget = pd.concat([db, info], axis=1)
    return merget, p.Path(to_file).parent + '/' + MAP


def get_info_table_from_las(path_to_file, idx, encoding):
    info_start = idx['info_table'][0]
    info_end = idx['info_table'][1]
    info_table = get_info_table(path_to_file, info_start, info_end, encoding=encoding)
    info_table.insert(1, 'unit', [col.replace('.', ' ').split()[-1] for col in info_table['X0']])
    return info_table


def get_param_table_db(engine):
    records_num = re.compile(r'\d{1,2}')
    # input('Введите имя сервера из config.json : ')
    # todo 2)Проверка ввода имени базы на нахождение данных в json
    print('Создаю подключение к "{}"'.format(BASENAME))
    records = LIST_OF_RECORDS.get('time') if IMPORT_RECORD == 'time' else LIST_OF_RECORDS.get(
        'depth')  # input('Введите через пробел номера рекордов :  ')
    # Парсим из всего ввода по маске 2а числа и запихиваем в сэт, получаем только уникальные.
    records = set(records_num.findall(str(records)))
    # Достаём sql-запрос из sql-движок из dbworker передавая имя базы и список рекордов
    row_sql, engine = request_params_from_base(BASENAME, records)
    db_records_data = pd.read_sql_query(row_sql, engine)
    return db_records_data


def map_values(info, db_param, default_map, to_file):
    """
    Сопоставляем параметры из ного ласа с параметрами из дефолтного мапинга.
    И подставляем к ним параметры из базы
    :param to_file:
    :param info:        pandas info table from LAS
    :param db_param:    pandas param table from DB
    :param default_map: pandas default mapping table from parent dir
    :return: write map table to csv
    # """
    # if len(info.index) > len(db_param.index):
    #     t1 = info
    #     t2 = db_param
    #     loc = len(t1.columns)
    # else:
    #     t1 = db_param
    #     t2 = info
    #     loc = 0
    for col in db_param.columns:
        val = []
        if col in 'name_en':
            continue
        for opt in info.X0:
            val.append(default_map[default_map.X0 == opt][col].values)
        # val = [default_map[default_map.X0 == opt][col].values[0] for opt in default_map.X0]
        info.insert(len(info.columns), col,
                    [v[0] if v else '' for v in val])
    return info, p.Path(to_file).parent + '/' + MAP


def check_mapping(mapping, info_table):
    """
    Сопоставляем данные из сохранённого мапинга с таблицей из LAS
    :param mapping:     pandas form map_{}.csv
    :param info_table:  pandas from LAS
    :return: None
    """
    # print('You are mapped values like:')
    if len(info_table.index) != len(mapping.index):
        print('{}'.format(pd.concat([info_table.X0, mapping.X0], axis=1)))
        qq(0, 'Колличество индексов не соответсвтует. Удалите старый маппинг и запустите меня снова!')
    print('Вы размапили данные из ласа следующим образом:\n')
    for i, x in enumerate(info_table['X0']):
        if mapping['mnemonic'][i]:
            print('[{}] From : {}   -> {} ({})'.format(i, x, mapping['name_ru'][i], mapping['unit_base'][i]))
    # print('\nCheck this out!\nThis is right? Do you want to CONTINUE?')
    print('Проверяйте!\nВерно ли размаплены параметры?')
    cond = input('[Y/n]')
    if not true_condition(cond): qq(0)
    print('Проверяем соответсвтие единиц измерений...')
    to_convert = [(i, x) for i, x in enumerate(mapping['unit_base']) if
                  mapping['unit'][i] != mapping['unit_base'][i] and mapping['unit_base'][i]]
    if to_convert:
        for i, unit in to_convert:
            print(
                'NEED to convert param "{}" from " {} " to -> " {} "'.format(mapping['name_ru'][i], mapping['unit'][i],
                                                                             unit))
        cond = input('Необходимо перевести значения в соответствующие единицы измерения? [Y/n]:')
        if true_condition(cond):
            print('Функция не реализована!')
            qq(0)
        else:
            print('Продолжаем!')
            return True
    else:
        # print('All values have the same units!')
        print('Не соответствия не найдены. Все параметры имеют одинковые единицы измерения.\nПродолжаем...')
        return True


########################################################################################################################
# Work with pandas
########################################################################################################################

def get_info_table(las, start_idx, end_idx, sep=':', prefix='X', encoding='utf-8'):
    info_table = pd.read_csv(
        las,
        sep=sep,
        prefix=prefix,
        skiprows=start_idx,
        nrows=end_idx - start_idx,
        header=None,
        encoding=encoding,
        engine='python',
        converters={0: lambda x: x.strip(), 1: lambda x: x.strip()}
    )
    return info_table


def get_las_data(las, mapping, idx, encoding='utf-8'):
    list_of_mnem = (x for i, x in enumerate(mapping['X0']) if mapping['mnemonic'][i])
    start_idx = idx.get('data_table')[0]
    data_table = pd.read_csv(
        las,
        names=[x for x in mapping['X0']],
        usecols=[idx.index[0] for idx in [mapping[mapping['X0'] == row] for row in list_of_mnem]],
        delim_whitespace=True,
        header=None,
        na_values=NA_VALUE,
        keep_default_na=False,
        date_parser=lambda x: datetime.fromtimestamp(float(x)),
        parse_dates=['TIME.'],
        skiprows=start_idx,
        # nrows=100,
        encoding=encoding
    )

    data_table.columns = [x for i, x in enumerate(mapping['mnemonic']) if mapping['mnemonic'][i]]
    return data_table


def read_mapping(map_file):
    """
    Выгружаем маппинг из файла и сопоставляем с инфо таблицей из ласа.
    :param map_file:    path to map file
    :return:
    """

    x = lambda s: s.strip()
    mapping = pd.read_csv(
        map_file,
        keep_default_na=False,
        converters={'X0': x, 'unit': x, 'X1': x, 'mnemonic': x, 'init_base': x},
        index_col=0,
        # sep='|'
    )

    return mapping


########################################################################################################################
# Work with monitoring
########################################################################################################################

def parse_data(data_table, mapping):
    """
    Create xml for data in POST request for stream
    :param data_table:
    :param mapping:
    :return:
    """
    curve_list = []
    curve_str = '<curve log="{l}" mnemonic="{m}" />'
    for mnem in data_table.columns:
        log_id = mapping[mapping['mnemonic'] == mnem]['record_id'].get_values()[0]
        curve_list.append(curve_str.format(l=log_id, m=mnem))
    data_list = []
    data_str = '<r v="{}"/>'
    for i, id in enumerate(data_table.values):
        if i > 10:
            break
        id = [str(ids) for ids in id]
        formated_data = data_str.format(';'.join(id))
        data_list.append(formated_data)
    xml = '''<request>
    <curves>{curve}</curves>
    <rows>{data}</rows>
    </request>'''.format(curve='\n'.join(curve_list), data='\n'.join(data_list))
    return xml


def send_data(data_table, mapping):
    """
    Request to server.
    Doesn't work with 'time'.
    Using only for 'depth'.
    :param data_table:  pandas.DataFrame with data
    :param mapping:     pandas.DataFrame from mapping
    :return: POST /sendlasdata
    """
    s = requests.Session()
    # url = 'http://localhost:8182/setlasdata'
    start_depth = data_table['DMEA'][0]
    stop_depth = data_table['DMEA'].tail(1).values[0]
    xml_data = parse_data(data_table, mapping)
    data = {
        'action': '1',
        'well_id': WELL_ID,
        'wellbore_id': WELLBORE_ID,
        's': SESSION,
        'start_depth': start_depth,
        'stop_depth': stop_depth,
        'data': xml_data
    }
    headers = {
        'Host': '91.203.37.20:9182',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 '
                      'Safari/537.36',
        'X-Requested-With': 'ShockwaveFlash/26.0.0.137',
        'Origin': 'http://91.203.37.20:9182'
    }
    send_las_data = s.post(HOST, data=data, headers=headers)


########################################################################################################################
# WORK with MYSQL DB
########################################################################################################################


def getengine(basename):
    """

    :return: engine
    """
    db = store.get(basename)

    user = db.get('user')
    password = db.get('password')
    host = db.get('host')
    bname = db.get('name')
    port = db.get('port')
    engine_str = 'mysql+pymysql://{u}:{p}@{h}{port}/{b_name}?charset=utf8&use_unicode=1'.format(
        u=user,
        p=password,
        h=host,
        port='' if port is None or '' else ':' + str(port),
        b_name=bname
    )
    engine = create_engine(engine_str, convert_unicode=True, echo=SQL_DEBUG)
    return engine

@counter
def engine_execute(sql_request):
    engine = getengine(BASENAME)
    if type(sql_request) is list:
        sql_request = ';'.join(sql_request)
    try:
        with engine.connect() as cn:
            res = cn.execute(sql_request)
        return res
    except Exception as exc:
        if str(1146) in str(exc.orig):
            print(exc.orig)
            print('Таблица отсутствует, создаём')
            # todo проверить работу с WITS_RECORD_ таблицами
            create_unexisting_table(str(exc.orig))
        else:
            print(exc.args)
        if engine_execute.count == 3:
            qq(1, 'Не удалось подключиться к базе данных!')
        engine_execute(sql_request)
        # count = 0
        # while count < 3:
        #     try:
        #         engine.execute(sql_request)
        #         break
        #     except Exception:
        #         count += 1
        # else:
        #     qq(0, 'Соединение с ДБ потеряно, пытаемся снова!')


def request_params_from_base(basename, wits_records):
    """

    :param basename: Имя базы в json
    :param wits_records: список рекордов введённых пользователем
    :return: tuple (sql, engine)
    """

    base_engine = getengine(basename=basename)
    records = ','.join(str(w) for w in wits_records)
    row_sql = 'SELECT ' \
              ' wsp.mnemonic, COALESCE(wu.name_ru,wu.name_en,wu2.name_ru,wu2.name_en) AS unit_base, wsp.record_id, wp.name_ru, wp.name_en' \
              ' FROM' \
              ' WITS_SOURCE_PARAM wsp LEFT  JOIN WITS_PARAM wp ON (wsp.mnemonic=wp.mnemonic)' \
              ' LEFT OUTER JOIN WITS_UNIT wu ON (wsp.unit_id=wu.id)' \
              ' LEFT OUTER JOIN WITS_UNIT wu2 ON (wp.unit_id=wu2.id)' \
              ' WHERE' \
              ' source_type_id={} AND record_id IN ({})' \
              ' ORDER BY record_id, param_num;'.format(SOURCE_TYPE, records)
    return row_sql, base_engine


def get_well_name_by_well_id(well_id):
    sql_req = 'select name from WITS_WELL where id = {}'.format(well_id)
    return engine_execute(sql_req).fetchone()


def get_well_by_wellbore_id(engine, wb_id):
    sql_req = 'select well_id from WITS_WELLBORE where id={}'.format(wb_id)
    return engine_execute(sql_req).fetchone()


def get_last_idx_in_base(record, wb_id):
    sql_req = 'select max(id) from WITS_RECORD{}_IDX_{}'.format(record, wb_id)
    return engine_execute(sql_req).fetchone()


def check_record_tables(rec, **kwargs):
    """
    Create records table
    :param rec:
    :param engine:
    :param kwargs: well_name, well_id, wellbore_id
    :return:
    """
    tables_formated = []
    tables = ['WITS_RECORD{r}_IDX_{w}', 'WITS_RECORD{r}_DATA_{w}']
    # if 'well' in kwargs:
    #     well = kwargs.get('well')
    #     get_wb_id = 'select * from WITS_WELL where name="{}"'.format(well)
    #     wellbore_id = engine.execute(get_wb_id).fetchone()['wellbore_id']
    # elif 'well_id' in kwargs:
    #     well_id = kwargs.get('well_id')
    #     get_wb_id = 'select * from WITS_WELL where id="{}"'.format(well_id)
    #     wellbore_id = engine.execute(get_wb_id).fetchone()['wellbore_id']
    if 'wellbore_id' in kwargs:
        wellbore_id = kwargs.get('wellbore_id')
    else:
        qq(0, 'Выставите WELLBORE_ID в конфиге')
        # print('Укажите один из пунктов ниже:'
        #       'well = Имя скважины'
        #       'well_id = id скважины'
        #       'wellbore_id = id стьвола ')
        #
    for table in tables:
        for record in rec:
            table_formated = table.format(r=record, w=wellbore_id)
            tables_formated.append(table_formated)
    for table in tables_formated:
        # base_table = table.rsplit('_', 1)[0]
        sql = 'DESC {};'.format(table)
        engine_execute(sql)
    return tables_formated


# def check_table(table):
#     """
#     Проверяем наличие record_idx_{wb_id}
#     Если нет, то создаём её
#     :param table: Table name (Str)
#     :return: None
#     """

def create_unexisting_table(exc):
    """
    Парсим вывод ошибки и создаём таблицу с именем из ошибки.
    :param exc: STR (error(gigit), comment(Str))
    :return: None
    """
    exc_comm = exc.split()[2].strip('\'')
    table_name = exc_comm.split('.')[1]
    like = '_'.join(table_name.split('_')[:-1])  # Работает только для партишонинговых таблиц!
    row_sql = 'create table if not exists {} like {}'.format(table_name, like)
    print(row_sql)
    engine_execute(row_sql)


def insert_data(tables, records, wellbore_id):
    idx = tables.get('idx')
    data = tables.get('data')
    mapping = tables.get('map')
    hashed_records = IGNORE_RECORDS  # пройденные рекорды
    hashed_id = SKIP_IDS  # Для проейденных id
    last_idx = data.index.max()
    step = SHIFT if SHIFT < last_idx else last_idx
    sql_data = []
    method = get_method()
    start_idx_in_base = 1
    print('Загружаем данные в базу...')
    for record in records:
        if record in hashed_records:
            print('Рекорд {} в списе игнориремых {}. Пропускаем!'.format(record, hashed_records))
            continue
        if method in 'INSERT':
            print('Выбран метод {} определяем последний индекс в таблицах.'.format(method))
            start_idx_in_base = get_last_idx_in_base(record, wellbore_id)[0]
            start_idx_in_base = 1 if start_idx_in_base is None else start_idx_in_base + 1  # В питоне индексы считаются с 0, в sql c 1
            print(
                'Последний индекс отпределён как {}. Заливаем данные из файла учитывая индекс'.format(
                    start_idx_in_base -1 ))

        mnemonics = mapping[mapping['record_id'] == str(record)]['mnemonic']
        compr_val = hashed_id + step

        for i, id in enumerate(idx.values):
            if i < hashed_id:
                continue
            elif i < compr_val:
                sql_idx = ('({}, "{}", {})'.format(i + start_idx_in_base, id[0], id[1]))
                sql_idx_insert = '{method} into WITS_RECORD{r}_IDX_{w} (id,date,depth) values {v}'.format(
                    r=record, w=wellbore_id, v=sql_idx, method=method)
                sql_data.append(sql_idx_insert)
                for mnem in mnemonics:
                    if mnem == 'DTM':
                        continue
                    value = data[mnem][i]
                    sql_data_insert = '{method} into WITS_RECORD{r}_DATA_{w} (idx_id, mnemonic, value, value_str) values ({i},"{m}",{d}, "NULL")'. \
                        format(i=i + start_idx_in_base, r=record, w=wellbore_id, m=mnem, d=value, method=method)
                    sql_data.append(sql_data_insert)
                continue
            # Отправляем в базу
            try:
                engine_execute(sql_data)
            except KeyboardInterrupt:  # Скипаем индесы по step
                print('Скипаем')
            #
            sql_data = []
            print_progress_bar(hashed_id, last_idx, step=step, record=record, prefix='Progress:', suffix='Complete',
                               length=50)
            hashed_id = i
            print_progress_bar(hashed_id, last_idx, step=step, record=record, prefix='Progress:', suffix='Complete',
                               length=50)
            compr_val = hashed_id + step if hashed_id + step < last_idx else last_idx
        update_idx = update_record_idx_table(wellbore_id, record, mnemonics)
        # Отправляем в базу
        print('Обновляем record_idx_{} для рекорда {}'.format(WELLBORE_ID, record))
        engine_execute(update_idx)
        #
        hashed_records.append(record)
        hashed_id = 0

        print('Рекорд № {} Загружен, всего индексов в файле {}. Загружено: {}'.format(record, last_idx, compr_val))


def update_record_idx_table(wb_id, record, mnemonics):
    limits_req = 'select ' \
                 ' min(depth) as depth_min, max(depth) as depth_max, min(date) as date_min, max(date) as date_max' \
                 ' from WITS_RECORD{}_IDX_{}'.format(record, wb_id)

    limits = engine_execute(limits_req).fetchone()
    min_depth = limits.depth_min
    max_depth = limits.depth_max
    date_min = limits.date_min
    date_max = limits.date_max
    table = 'record_idx_{}'.format(wb_id)
    # check_table(table)  # Проверяем, если нет, создаём
    request = []
    for mnem in mnemonics:
        row_sql = 'REPLACE into {t} values ({log_id},"{mnem}","{date_min}","{date_max}",{depth_min},{depth_max})'. \
            format(
                t=table,
                # method=METHOD,
                log_id=record,
                mnem=mnem,
                date_min=date_min,
                date_max=date_max,
                depth_min=min_depth,
                depth_max=max_depth
        )
        request.append(row_sql)
    return request


########################################################################################################################
# Progress
########################################################################################################################
def print_progress_bar(iteration, total, step, record, prefix='', suffix='', decimals=1, length=100, fill='█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        record      - Required  : current record (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(
        '\rRecord № {} Step: {} Current id : {} | {} |{}| {}% {}'.format(record, step, iteration, prefix, bar, percent,
                                                                         suffix),
        end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()


########################################################################################################################



if __name__ == '__main__':
    main()
