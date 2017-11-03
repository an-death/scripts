import pandas as pd

from users_report import aliases
from xlsxwriter.utility import xl_range

DEFAULT_HEADER_VIDEO = 'Использование видеонаблюдения GTI-Online'
DEFAULT_HEADER_TOTAL = 'Использование мониторинга GTI-Online'
DEFAULT_HEADER_USERS = 'Пользователи GTI-Online'


def write_sheet(sheet_name, writer, table):
    table.to_excel(writer, sheet_name=sheet_name, index_label='№п/п', startrow=1)  # , header=False)
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
    col = ['№п/п', 'ФИО', 'Филиал', 'Позиция', 'Видео', 'Всего'] if sheet_name != 'Пользователи' else aliases.values()

    # todo Записывать заголовок
    # todo Решить проблему с записью мультииндексной шапки.

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


def create_xlsx(file_name, users_table, video_table, activity_table):
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        write_sheet('Пользователи', writer, users_table)
        write_sheet('Видео', writer, video_table)
        write_sheet('Общее', writer, activity_table)


def main():
    pass


if __name__ == '__main__':
    main()
