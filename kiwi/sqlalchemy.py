from threading import Lock
from typing import TYPE_CHECKING, Dict

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

if TYPE_CHECKING:
    from kiwi import Application


class SQLAlchemy:
    app: 'Application'
    sessions: Dict[str, scoped_session]

    def __init__(self):
        self.sessions = {}
        self.session_lock = Lock()

    def init_app(self, app: 'Application'):
        self.app = app

    def session(self, name: str) -> Session:
        with self.session_lock:
            if name not in self.sessions:
                self.sessions[name] = self.create_session(name)
            return self.sessions[name]()

    def create_session(self, name: str) -> scoped_session:
        conf = self.app.config['database'][name]
        url = URL.create(
            drivername=f"{conf['adapter']}+{conf['connector']}",
            host=conf['host'],
            port=conf['port'],
            username=conf['user'],
            password=conf['pass'],
            database=conf['db'],
        )
        engine = create_engine(url, connect_args={'charset': 'utf8'})
        return scoped_session(sessionmaker(engine))

    def remove_sessions(self) -> None:
        with self.session_lock:
            for session in self.sessions.values():
                session.remove()
