from sqlalchemy import Column
from sqlalchemy import Integer, String, ForeignKey, Text, BLOB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Meta:
    __tablename__ = None

    def __repr__(self):
        return ('<{}({})>'.format(self.__tablename__, ','.join(
            str(atr) for _, atr in self.__class__.__dict__.items() if not _.startswith('_'))))


class Wits_network(Base, Meta):
    __tablename__ = 'WITS_NETWORK'

    id = Column('id', Integer, primary_key=True, nullable=False,
                autoincrement=True)
    name_ru = Column('name_ru', String(64))
    name_en = Column('name_en', String(64))
    email = Column('email', String(32))
    phone = Column('phone', String(32))
    address = Column('address', String(128))
    logo = Column('logo', BLOB)


class Wits_user(Base, Meta):
    __tablename__ = 'WITS_USER'

    id = Column('id', Integer, primary_key=True, nullable=False,
                autoincrement=True)
    network_id = Column('network_id', Integer, ForeignKey('WITS_NETWORK.id'))
    name = Column('name', String(255))
    password = Column('password', String(32))
    email = Column('email', String(255))
    witsml_user = Column('witsml_user', String(32))
    witsml_password = Column('witsml_password', String(32))
    group_id = Column('group_id', Integer, ForeignKey('WITS_USER_GROUP.id'))
    role = Column('role', Integer)
    session = Column('session', String(128))
    last_name = Column('last_name', String(32))
    first_name = Column('first_name', String(32))
    patr_name = Column('patr_name', String(32))
    lang = Column('lang', String(2))
    organization = Column('organization', String(255))
    position = Column('position', String(255))
    tel = Column('tel', String(32))
    removed = Column('removed', Integer)

    network = relationship("Wits_network", backref="users")
    group = relationship('Wits_user_group', backref='users')


class Wits_user_log(Base, Meta):
    __tablename__ = 'WITS_USER_LOG'

    user_id = Column('user_id', Integer, ForeignKey('WITS_USER.id'), primary_key=True, autoincrement=True)
    event_id = Column('event_id', Integer, ForeignKey('WITS_USER_EVENT.id'))
    date = Column('date', Integer)
    wellbore_id = Column('wellbore_id', Integer)
    data = Column('data', Text)

    event = relationship('Wits_user_event', backref='events')
    sessions = relationship('Wits_user', backref='sessions')


class Wits_user_event(Base, Meta):
    __tablename__ = 'WITS_USER_EVENT'

    id = Column('id', Integer, primary_key=True)
    name_ru = Column('name_ru', String(255))
    name_en = Column('name_en', String(255))


class Wits_user_group(Base, Meta):
    __tablename__ = 'WITS_USER_GROUP'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    network_id = Column('network_id', Integer, ForeignKey('WITS_NETWORK.id'))
    name = Column('name', String(255))


class Table_mapper():
    def __init__(self, engine=None):
        self.meta = Base.metadata
        self.meta.reflect(bind=engine)
        self.tables = self.meta.tables

    def return_mapped_table(self, name=None):
        return self.tables[name]
