from collections import defaultdict
from datetime import datetime

from base_models.wits_models import Wits_user as users


class User:
    def __init__(self, id, session):
        self.id = id
        self.param = session.query(users).filter_by(id=id).first()
        self.last_name = self.param.last_name
        self.first_name = self.param.first_name
        self.patr_name = self.param.patr_name
        self._sessions = {}
        self.logged = False
        self.total_video_time = 0
        self.total_monitoring_time = 0
        self.collisions = []
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
        return '{id}\n{org}\n{pos} \n{name}'.format(id=self.id,
                                                    pos=self.param.position,
                                                    name=' '.join(str(i) for i in
                                                                  [self.last_name, self.first_name, self.patr_name]),
                                                    org=self.param.organization)

    def is_logged(self):
        return self.logged

    def login(self):
        self.logged = True

    def logout(self):
        self.logged = False

    def sessions(self, ses):
        try:
            session = self._sessions[ses]
        except KeyError:
            session = self._sessions[ses] = Session(ses)
        return session

    def session_start(self, session, dt):
        assert not self.sessions(session).status(), 'Пытаемся открыть уже открутую сессию! {}'.format(session)
        self.sessions(session).open(dt)
        self.login()

    def session_stop(self, session: str, dt):
        active_session = self.get_active_session()
        new_session = self.sessions(session)
        assert active_session == new_session, 'Сессия на закрытие не соответствует активной сессии\n' \
                                              'user: {}\n' \
                                              'active: {}\n' \
                                                         'session: {}\n' \
                                                         ''.format(self.info(), active_session, self.sessions(session))
        assert active_session.status(), 'Пытаемся закрыть сессию. Сессия уже закрыта! {}'.format(session)
        # todo ^ Описать решние коллизий в таком случае ^
        active_session.close(dt)
        self.logout()

    def session_store(self, session: str, args: str):
        assert self.sessions(
            session).status(), 'Текущая сессия закрыта.\n{}\nНе можем добавить запись в сессию!'.format(session)
        self.sessions(session).store(args)

    def get_active_session(self):
        session = [session for session in self._sessions.values() if session.status()]
        assert len(session) == 1, 'Активной дожна быть только одна сессия! Найдено актиных сессий: ' \
                                  '\n{} ' \
                                  '\nДля пользователя: ' \
                                  '\n{}'.format(session, self.info())
        # todo ^ Описать решние коллизий в таком случае ^
        return session[-1]

    def close_active_session(self, dt=None):
        active_session = self.get_active_session()
        if not dt:
            dt = active_session.get_cached_date()
        self.session_stop(active_session.ses, dt)

    def calculate_total(self):
        for session in self._sessions.values():
            self.total_video_time += session.total_time_video
            self.total_monitoring_time += session.total_time
        return self.total_monitoring_time, self.total_video_time


class Session:
    def __init__(self, data):
        self.ses = data
        self.total_time = 0
        self.total_time_video = 0
        self.planshet = defaultdict(int)
        self.active = False
        self.cached_data = {'start_session': 0, 'last': 0}
        self.storage = {'all': {'total': 0}, 'video': {'start': 0, 'stop': 0}}

    def __str__(self):
        return '{}.{}'.format(self.ses, self.active)

    def __eq__(self, other):
        if not isinstance(other, Session):
            return False
        else:
            if self.ses == other.ses:
                return True
            else:
                return False

    def close(self, dt):
        self.cached_data['last'] = dt
        self.total_time = self.cached_data['last'] - self.cached_data['start_session']
        self.active = False

    def open(self, dt):
        self.cached_data['start_session'] = dt
        self.active = True

    def store(self, args: str):
        def calculate(start, stop):
            total = stop - start
            self.total_time_video += total
            self.storage['video']['stop'], self.storage['video']['start'] = 0, 0

        time, action, form = args.split('=')
        dt = Dt(time)
        if form.startswith('Camera'):
            start = self.storage['video']['start']
            stop = self.storage['video']['stop']
            if action == 'fopen' and not start:
                self.storage['video']['start'] = dt
            elif action == 'fclose' and start:
                calculate(start, dt)
            elif action == 'fopen' and not stop:
                calculate(start, dt)
                self.storage['video']['start'] = dt
                # Остальные формы игнорим.
                # todo сделать подсчёт по всем формам

    def status(self):
        """
        Возвращает True/False для открытой и закрытой сесси
        :return: Bool 
        """
        return self.active

    def get_cached_date(self):
        return self.cached_data['last'] or self.cached_data['start_session']

    def return_total_time(self):
        return self.total_time

    def return_total_video_time(self):
        return self.total_time_video

    def session_period(self):
        return '{} - {}'.format(self.cached_data['start_session'], self.cached_data['last'])


class Dt:
    formats = {'date': '%Y-%m-%d', 'datetime': '%Y-%m-%d %H:%M:%S'}

    def __init__(self, dt):
        if isinstance(dt, int):
            if len(str(dt)) > 10:
                dt = int(str(dt)[:10])
            self.dt = dt
        if isinstance(dt, datetime):
            self.dt = int(datetime.timestamp(dt))
        if isinstance(dt, str):
            if dt.isdigit():
                self.dt = int(dt[:10])
            elif len(dt) > 10:
                self.dt = datetime.strptime(dt, Dt.formats['datetime']).timestamp()
            else:
                self.dt = datetime.strptime(dt, Dt.formats['date']).timestamp()
        if isinstance(dt, Dt):
            self.dt = dt.to_timestamp()

    def __str__(self):
        return str(Dt.to_string(self))

    def __repr__(self):
        return Dt.to_string(self)

    def __sub__(self, other):
        if isinstance(other, Dt):
            other = other.dt
        return self.dt - other

    def to_string(self):
        return datetime.fromtimestamp(self.dt).strftime(Dt.formats['datetime'])

    def to_timestamp(self):
        return int(self.dt)

    def to_request(self):
        return self.to_timestamp() * 1000
