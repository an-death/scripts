from itertools import chain

import pandas as pd
from users_report import aliases
from xlsxwriter.utility import xl_range

DEFAULT_HEADER_VIDEO = 'Использование видеонаблюдения GTI-Online'
DEFAULT_HEADER_TOTAL = 'Использование мониторинга GTI-Online'
DEFAULT_HEADER_USERS = 'Пользователи GTI-Online'


def write_sheet(sheet_name, writer, table):
    startrow = 3

    table.to_excel(writer, sheet_name=sheet_name, index_label='№п/п', startrow=startrow, header=False)
    book = writer.book
    # Add a header format.

    formats = {
        'clean': book.add_format({'border': None}),
        'fio': book.add_format({'text_wrap': True, 'border': 1}),
        'data': book.add_format({
            'text_wrap': True,
            'align': 'center',
            'valign': 'center',
            'border': 1}),
        'head': book.add_format({
            'bold': True,
            'font_size': 15,
            'align': 'center',
            'valign': 'center',
            # 'border': 2,
            # 'fg_color': '#D7E4BC',
        }),
        'header_format': book.add_format({
            'bold': True,
            'text_wrap': True,
            'align': 'center',
            'valign': 'top',
            'fg_color': '#D7E4BC',
            # 'fg_color':'#e1e1ff',
            'border': 2})
    }

    formats['head'].set_align('center')
    formats['head'].set_align('vcenter')

    sheet = writer.sheets[sheet_name]
    last_row = startrow + len(table.index)
    clean_range = xl_range(last_row, 0, last_row * 10, len(table.columns))
    # Записываем шапку
    if sheet_name == 'Пользователи':
        col = aliases.values()
        sheet.set_row(0, 30)

        sheet.merge_range(0, 0, 0, len(col) - 1, 'Список пользователей УМБ', formats['head'])

        sheet.set_row(2, 60)
        formats['header_format'].set_text_wrap()
        for col_num, value in enumerate(col):
            sheet.write(2, col_num, value, formats['header_format'])

        sheet.set_column('B:B', 30, formats['fio'])
        sheet.set_column('C:C', 16, formats['fio'])
        sheet.set_column('D:D', 38, formats['fio'])
        sheet.set_column('E:E', 38, formats['fio'])
        sheet.set_column('F:F', 20, formats['fio'])
        sheet.set_column('G:G', 15, formats['fio'])
        sheet.set_column('H:H', 28, formats['fio'])
    # Форматируем данные
    else:  # sheet_name != 'Пользователи':
        # todo Придумать как записывать мультирендж в шапку
        head = 'Использование видеонаблюдения в системе УМБ' if sheet_name != 'Общее' else 'Использование системы УМБ'
        col = table.columns
        sheet.set_row(0, 30)
        sheet.merge_range(0, 0, 0, len(col), head, formats['head'])
        merge_range = []
        for col_num, value in enumerate(chain([('№п/п',) * 2], col)):

            if value[0] == value[1]:
                sheet.merge_range(startrow - 1, col_num, startrow, col_num, value[0], formats['header_format'])
            else:
                merge_range.append((col_num, value[0]))
                sheet.write(startrow, col_num, value[1], formats['header_format'])

            sheet.set_column(col_num, col_num, len(value[-1]) + 5, formats['fio'])

        sheet.merge_range(startrow - 1, merge_range[0][0], startrow - 1, merge_range[-1][0],
                          merge_range[-1][-1], formats['header_format'])

        sheet.set_column('A:A', 5, formats['fio'])
        sheet.set_column('D:D', 30, formats['fio'])
        sheet.set_column('J:J', 20, formats['data'])

    # clean all below data
    sheet.conditional_format('B1', {'type': 'blanks',
                                    'multi_range': clean_range,
                                    'format': formats['clean']})


def create_xlsx(file_name, users_table, video_table, activity_table):
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        write_sheet('Пользователи', writer, users_table)
        write_sheet('Видео', writer, video_table)
        write_sheet('Общее', writer, activity_table)


def main():
    pass


if __name__ == '__main__':
    main()
