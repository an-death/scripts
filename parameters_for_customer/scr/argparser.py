import argparse


description = '''Скрипт для подготовки xslx таблиц по рекордам по скважинам для заказчика.'''
list_of_prj = [
    'h4 ','neftisa',
    'бнгф ','bngf',
    'h6', 'igs',
    'h7', 'eriell',
    'nbk', 'нбк','dulisma',
    'h8', 'gk', 'gk_new', 'gkn',
    'геолог', 'geolog',
    'burgaz', 'ubu', 'бургаз_новый', 'ubr_new' ,'un',
    'ggr', 'ггр',
    'bke', 'бке',
    'bke_test', 'бке_тест'
    'сск', 'ssk',
    'сггф','sggf',
    'новатек', 'novatek',
    'reservn', 'rn', 'reserv_novatek',
    'nsh',
    'yapg'
]
serv_help = '''Шорткат для обозначения сервера. Cases: " {} "'''.format('\n'.join(list_of_prj))

def parsargs():
    pars = argparse.ArgumentParser(description=description, add_help=True)

    pars.add_argument('server', help=serv_help, type=str)
    pars.add_argument('well_name', type=str, help='Имя скважины из поля {name} WITS_WELL')
    pars.add_argument('-r', type=list,
                      help='Список рекордов для формирования таблиц. Default: [1, 11, 12]',
                      default=[1, 11, 12]
                      )

    args = pars.parse_args()
    # print(args)
    assert args.server in list_of_prj, '"{}" нет в списе :\n{}'.format(args.server,'\n'.join(list_of_prj))
    return args.server, args.well_name, args.r


def main():
    parsargs()


if __name__ == '__main__':
    main()