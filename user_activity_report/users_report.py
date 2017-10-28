import sys
from collections import OrderedDict

import pandas as pd
from classes import get_connect_to_db
from sqlalchemy.sql import func

from base_models.wits_models import Wits_user as users, Wits_user_group as group

aliases = OrderedDict({
    0: '№п/п',
    1: 'ФИО полностью',
    2: 'Наименование предприятия/филиала/экспедиции',
    3: 'Отдел/служба',
    4: 'Должность',
    5: 'Email',
    6: 'Логин в системе (совпадает с доменным именем)',
    7: 'Примечание'
})
BKE_comr = {
    'GPN_Development': '',
    'RTM_KF': '',
    'RTM_MSK': '',
    'RTM_NVBN': '',
    'RTM_PKH': '',
    'RTM_USK': '',
    'RTM_VIP': '',
    'RTM_ZSF': '',
    'RTM_ZSF+KF': ''
}


def get_table(con):
    query = con.query(func.CONCAT_WS(' ', users.last_name, users.first_name, users.patr_name).label('1'),
                      group.name.label('2'),
                      users.organization.label('3'),
                      users.position.label('4'),
                      users.email.label('5'),
                      users.name.label('6'),
                      users.tel.label('7'),
                      ).outerjoin(group, users.group_id == group.id)
    query = query.filter(users.removed == 0)
    query = query.order_by(group.name, users.id)
    # res = query.all()
    # table = pd.DataFrame({i: v._asdict() for i, v in enumerate(res, 1)})
    query_as_string = query.statement.compile(compile_kwargs={"literal_binds": True},
                                              dialect=query.session.bind.dialect)
    table = pd.read_sql_query(query_as_string, con.connection())
    # table = table.unstack().unstack()
    table.columns = list(aliases.values())[1:]
    return table


def main(p: str):
    dbcon = get_connect_to_db(p)
    table = get_table(dbcon)
    sheet_name = 'Пользователи'
    with pd.ExcelWriter('Список пользователей GTI-online.xlsx', engine='xlsxwriter') as writer:
        table.to_excel(writer, sheet_name=sheet_name, index_label=aliases[0], startrow=1, header=False)
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
            'fio': book.add_format({'text_wrap': True,
                                    'border': 1}),
        }

        sheet = writer.sheets[sheet_name]
        for col_num, value in enumerate(aliases.values()):
            sheet.write(0, col_num, value, header_format)
        sheet.set_column('B:B', 30, formats['fio'])
        sheet.set_column('C:C', 18, formats['fio'])
        sheet.set_column('D:D', 38, formats['fio'])
        sheet.set_column('E:E', 38, formats['fio'])
        sheet.set_column('F:F', 20, formats['fio'])
        sheet.set_column('G:G', 15, formats['fio'])
        sheet.set_column('H:H', 28, formats['fio'])


if __name__ == '__main__':
    if not sys.argv:
        exit('Введите шорткат проекта!')
    PROJECT = sys.argv[0]
    # main('bke')
    main(PROJECT)
