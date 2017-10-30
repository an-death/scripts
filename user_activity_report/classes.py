from collections import defaultdict
from datetime import datetime, timedelta

from base_models.wits_models import Wits_user as users
from projects import project


def get_connect_to_db(project_name: str):
    prj = project.Project(project_name)
    prj.configurate()
    return prj.sql_sessionmaker()


class User:
    def __init__(self, id, session):
        self.id = id
        self.param = session.query(users).filter_by(id=id).first()
        self.last_name = self.param.last_name
        self.first_name = self.param.first_name
        self.patr_name = self.param.patr_name
        self._sessions = {}
        self.logged = False
        self.total_video_time = Dt(0)
        self.total_monitoring_time = Dt(0)
        self.collisions = []

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '{}.{}'.format(self.param.name, self.logged)

    @property
    def fio(self):
        return ' '.join(str(i) for i in [self.last_name, self.first_name, self.patr_name] or [self.param.name] if i)

    def info(self):
        return '{id}\n{org}\n{pos} \n{name}'.format(id=self.id,
                                                    pos=self.param.position,
                                                    name=self.fio,
                                                    org=self.param.organization)

    @property
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
        assert not self.sessions(session).status, 'Пытаемся открыть уже открутую сессию! {}'.format(session)
        self.sessions(session).open(dt)
        self.login()

    def session_stop(self, session: str, dt):
        active_session = self.get_active_session()
        new_session = self.sessions(session)
        assert active_session is new_session, 'Сессия на закрытие не соответствует активной сессии\n' \
                                              'user: {}\n' \
                                              'active: {}\n' \
                                              'session: {}\n' \
                                              ''.format(self.info(), active_session, self.sessions(session))
        assert active_session.status, 'Пытаемся закрыть сессию. Сессия уже закрыта! {}'.format(session)
        # todo ^ Описать решние коллизий в таком случае ^
        args_to_close_planshet = '{}=fclose='.format(dt)
        self.session_store(session, dt, args_to_close_planshet)
        active_session.close(dt)
        self.logout()

    def session_store(self, session: str, date, args: str):
        assert self.sessions(
            session).status, 'Текущая сессия закрыта.\n{}\nНе можем добавить запись в сессию!'.format(session)
        self.sessions(session).store(date, args)

    def get_active_session(self):
        session = [session for session in self._sessions.values() if session.status]
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


class Session:
    def __init__(self, data):
        self.ses = data
        self.total_time = Dt(0)
        self.total_time_video = Dt(0)
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

    @property
    def status(self):
        """
        Возвращает True/False для открытой и закрытой сесси
        :return: Bool 
        """
        return self.active

    def close(self, dt):
        self.cached_data['last'] = dt
        last = self.cached_data['last']
        start = self.cached_data['start_session']
        assert last >= start, 'В результате вычитания получился отрицательный результат,' \
                              ' что не возможно для времени!\n {} > {} \nsession: {}'.format(last, start, self.ses)
        self.total_time += last - start
        self.active = False

    def open(self, dt):
        self.cached_data['start_session'] = dt
        self.active = True

    def store(self, dt, args: str):
        def calculate(start, last):
            assert last >= start, 'В результате вычитания получился отрицательный результат,' \
                                  ' что не возможно для времени!\n {} > {} \nsession: {}'.format(last, start, self.ses)
            total = last - start
            self.total_time_video += total
            self.storage['video']['stop'], self.storage['video']['start'] = 0, 0

        time, action, *form = args.split('=')
        # todo Придумать решение для это коллизии!!
        # В информации о сессии хранится время большее, чем в самом логе
        # time = Dt(time)
        # if not time.__ge__(self.storage['video']['start']):
        #     dt = time
        #####################################################################
        form = ''.join(form)
        if not form:
            form = 'Camera'  # для закрытия всех планшетов при закрытии сессии
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
        self.cached_data['last'] = dt

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
        if isinstance(dt, Dt):
            self.dt = dt.to_timestamp()
        elif isinstance(dt, int):
            if len(str(dt)) > 10:
                dt = int(str(dt)[:10])
            self.dt = dt
        elif isinstance(dt, datetime):
            self.dt = int(datetime.timestamp(dt))
        elif isinstance(dt, str):
            if dt.isdigit():
                self.dt = int(dt[:10])
            elif len(dt) > 10:
                self.dt = datetime.strptime(dt, Dt.formats['datetime']).timestamp()
                self.dt = int(self.dt)
            else:
                self.dt = datetime.strptime(dt, Dt.formats['date']).timestamp()
                self.dt = int(self.dt)
        else:
            exit('Не получилось распознать dt {}, type({})'.format(dt, type(dt)))

    def __str__(self):
        return self.to_string()

    def __repr__(self):
        return self.__str__()

    def __float__(self):
        return float(self.dt)

    def __sub__(self, other):
        if isinstance(other, Dt):
            other = other.dt
        return Dt(self.dt - other)

    def __add__(self, other):
        if isinstance(other, Dt):
            other = other.dt
        elif isinstance(other, str) and other.isdigit():
            other = int(other)
        return Dt(self.dt + other)

    def __ge__(self, other):
        if isinstance(other, Dt):
            other = other.dt
        return self.dt >= other

    def to_string(self):
        return datetime.fromtimestamp(self.dt).strftime(Dt.formats['datetime'])

    def to_timestamp(self):
        return int(self.dt)

    def to_request(self):
        return self.to_timestamp() * 1000

    def to_human(self):
        return str(timedelta(seconds=self.dt))  # + ' с'
