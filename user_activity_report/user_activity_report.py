# -*- coding:utf-8  -*-

from base_models.wits_models import (Wits_user as users)
from projects import project

bke = project.Project('bke')
bke.configurate()
session = bke.sql_sessionmaker()


class User():
    def __init__(self, id, sesion):
        self.id = id
        self.param = session.query(users).filter_by(id=id).first()
        self.last_name = self.param.last_name
        self.first_name = self.param.first_name
        self.patr_name = self.param.patr_name
        self._sessions = {}
        self.logged = False
        # self.id = id
        # self.network_id = user.network_id
        # self.name = user.name
        # self.email = user.email
        # self.group_id = user.group_id
        # self.role = user.role
        # self.session = user.session
        # self.last_name = user.last_name
        # self.first_name = user.first_name
        # self.part_name = user.part_name
        # self.organization = user.organization
        # self.position = user.position
        # self.tel = user.tel

    def __str__(self):
        return '{}'.format(
            '\n'.join('{}:  {} '.format(k, v) for k, v in self.param.__dict__.items() if not k.startswith('_')))

    def info(self):
        return '{id}\n{org}\n{pos} \n{name}'.format(id=user.id,
                                                    pos=user.param.position,
                                                    name=' '.join([user.last_name, user.first_name, user.patr_name]),
                                                    org=user.param.organization)

    def is_logged(self):
        return self.logged

    def login(self):
        self.logged = True

    def logout(self):
        self.logged = False

    def sessions(self):
        return self._sessions

        # todo add method add_session??


class Session():
    pass


user = User(141, session)

print(user.info())
