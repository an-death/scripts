#!/usr/bin/python3
# -*- coding: utf-8 -*-
import re

import paramiko
import path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

path_to_config_file = '/home/as/Документы/scr/.bash_connection_info.sh'
CONFIGS = path.Path(path_to_config_file)


class MetaProject(object):
    """Meta info for Projects
    """

    def __init__(self, shortcut):
        """
        При создании объекта передаём один из вариантов его названия.
        :param shortcut:
        """
        self.shortcut = shortcut

    def __str__(self):
        prj = [
            ' Project {} '.format(self.name).center(70, '-'),
            'Main INFO:',
            'host: {},  host2: {}, port: {}'.format(self.host, self.host2, self.port),
            'VPN: {}, DNS: {}'.format(self.vpn, self.dns),
            'Monitoring INFO:',
            'URL: {}, URL2: {}'.format(self.url, self.url2),
            #  monitoring login, pass
            'Send_to_Address: {}, Send_to_Port: {}'.format(self.send_to_address, self.send_to_port),
            'SSH INFO:',
            'Ssh_user: {}, Ssh_pass: {}'.format(self.login, self.password),
            'Connection: "{}"'.format('sshpass -p {} ssh -o StrictHostKeyChecking=no {}@{} -p{}'.
                                      format(self.password,
                                             self.login,
                                             self.host,
                                             self.port)),
            'Path_to_stream: {}'.format(self.stream_path),
            'Path_to_reporting: {}'.format(self.report_path),
            'SQL INFO:',
            'Login: {}, Pass: {}, Base_name: {}, Port: {}, Gate: {}'.format(
                self.db_login,
                self.db_password,
                self.db_name,
                self.db_port,
                self.db_host
            ),
            'SQL_Connection: "{}"'.format(
                'mysql --default-character-set=utf8 --safe-updates -h {} -P {} -u{} -p{} {} -A'.
                    format(
                    self.db_host,
                    self.db_port,
                    self.db_login,
                    self.db_password,
                    self.db_name
                ))
        ]
        return '\n'.join(prj)

    def __repr__(self):
        return '<{}({})'.format(self.name, ','.join([self.host, self.port, self.login, self.password]))

    def configurate(self):
        """
        Заполняем объект полями доступными в .bash_connection_info.sh
        :return:
        """
        start = None
        attribs = []
        dict_of_attr = {}
        with open(CONFIGS, 'r', encoding='utf-8') as fd:
            for line in fd.readlines():
                if self.shortcut in line and line.endswith(')\n'):
                    start = True
                    self.shortcuts = line.rstrip(')\n').strip().split('|')
                    continue
                if start and line.endswith(';;\n'):
                    break
                if start:
                    list_of_attrs = line.strip().replace('\n', '').replace('"', '').split(';')
                    list_of_attrs.remove('')
                    attribs.append(list_of_attrs)
        attribs = sum(attribs, [])
        for attrib in attribs:
            k, v = attrib.split('=')
            dict_of_attr[k.strip()] = v.strip()
        # _default_
        _def_str = 'unspecified'.upper()
        _def_ssh = '22'
        _def_mon = '4341'
        _def_db_name = 'WMLS'
        _def_db_log = 'gtionline'
        _def_db_pass = 'tetraroot'
        _def_db_port = '3306'
        _def_db_host = '192.168.0.100'
        #
        self.name = dict_of_attr.get('projectName', _def_str)
        self.host = dict_of_attr.get('serverIP', _def_str)
        self.host2 = dict_of_attr.get('serverIP2', _def_str)
        self.port = dict_of_attr.get('sshPortForServer', _def_ssh)
        self.vpn = dict_of_attr.get('vpnServerIp', _def_str)
        self.dns = dict_of_attr.get('serverNameInDNS', _def_str)
        self.url = dict_of_attr.get('monitoringAdress', _def_str)
        self.url2 = dict_of_attr.get('monitoringAdress2', _def_str)
        self.send_to_address = dict_of_attr.get('sendToAddress', _def_str)
        self.send_to_port = dict_of_attr.get('sendToPort', _def_mon)
        self.login = dict_of_attr.get('loginForServer', _def_str)
        self.password = dict_of_attr.get('passwordForServer', _def_str)
        self.stream_path = dict_of_attr.get('pathToStream', _def_str)
        self.report_path = dict_of_attr.get('pathToReporting', _def_str)
        self.db_login = dict_of_attr.get('loginForSQLBase', _def_db_log)
        self.db_password = dict_of_attr.get('passwordSQLBase', _def_db_pass)
        self.db_port = dict_of_attr.get('portSQL', _def_db_port)
        self.db_name = dict_of_attr.get('baseNameSQL', _def_db_name)
        self.db_host = dict_of_attr.get('sqlGateIp', _def_db_host)
        self.encryptPK = dict_of_attr.get('encryptPK', _def_str)
        self.encryptLK = dict_of_attr.get('encryptLK', _def_str)

    def _sql_engine(self, loging=False):
        """
        Создаём движок для подключения к базе на основе движка sqlalchemy
        :return: engine
        """

        user = self.db_login
        password = self.db_password
        host = self.db_host
        bname = self.db_name
        port = self.db_port
        engine_str = 'mysql+pymysql://{u}:{p}@{h}{port}/{b_name}?charset=utf8&use_unicode=1'.format(
            u=user,
            p=password,
            h=host,
            port='' if port is None or '' else ':' + str(port),
            b_name=bname
        )
        self.engine = create_engine(engine_str, convert_unicode=True, echo=loging)
        return self.engine

    def _ssh_connect(self):
        """
        Инициируем соединение с сервером, записываем в атрибут ssh
        :return: self.ssh
        """
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(hostname=self.host,
                            port=int(re.findall(r'\d+',self.port)[0]),
                            username=self.login,
                            password=self.password)
        except paramiko.AuthenticationException:
            print("Auth failed")
            self.ssh.close()
            exit(0)

    def get_shortcuts(self, echo=True):
        """
        Выводим список возможных сокращений для проекта.
        :param echo: По умолчанию True - принтим список. False - Возвращаем list
        :return:
        """
        if echo:
            print('Project: {} Shortcuts: {}'.format(self.name,', '.join(self.shortcuts)))
        else:
            return self.shortcuts

    def sql_sessionmaker(self):
        self.session = sessionmaker()
        self.session.configure(bind=self._sql_engine())
        return self.session()

class Project(MetaProject):
    def info(self):
        print(MetaProject.__str__(self))

    def sql_execute(self, sql_command):
        """
        Передаём запрос, и получаем ответ.
        :param sql_command:
        :return: list
        """
        with self.engine.connect() as cn:
            res = cn.execute(sql_command).fetchall()
        return res

    def ssh_execute(self, command):
        result = []
        self._ssh_connect()  # создаём конект
        stdin, stdout, stderr = self.ssh.exec_command(command)  # исполняем команду
        if stderr.read():
            for line in stderr.readlines():
                print(line)
            self.ssh.close()
        for line in stdout.readlines():
            result.append(line)
            print(line)


def test():
    prj_list = ['bke', 'nova', 'st', 'ggr', 'igs', 'eriell', 'gk']
    for prj in prj_list:
        p = Project(prj)
        p.configurate()
        p._sql_engine()
        p._ssh_connect()
        assert p.name != 'UNSPECIFIED'
        assert p.engine
        assert p.ssh
        p.get_shortcuts()


if __name__ == '__main__':
    test()
