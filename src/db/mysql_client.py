from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker


class MySQLClient:
    def __init__(self, host: str, port: int, user: str, password: str, db_name: str):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db_name = db_name
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None

    def create_engine(self) -> Engine:
        if self._engine is None:
            url = f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._db_name}?charset=utf8mb4"
            self._engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600, echo=False, future=True)
        return self._engine

    def create_database_if_not_exists(self):
        # 先连接到不指定库的 MySQL，避免库不存在时报错
        root_url = f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/mysql?charset=utf8mb4"
        root_engine = create_engine(root_url, pool_pre_ping=True, pool_recycle=3600, echo=False, future=True)
        with root_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{self._db_name}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
            conn.commit()

    def session_factory(self) -> sessionmaker:
        if self._session_factory is None:
            engine = self.create_engine()
            self._session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        return self._session_factory
