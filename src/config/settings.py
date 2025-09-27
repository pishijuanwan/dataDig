import os
import yaml
from dataclasses import dataclass
from typing import Optional


@dataclass
class TushareSettings:
    token: str


@dataclass
class DatabaseSettings:
    host: str
    port: int
    user: str
    password: str
    name: str


@dataclass
class IngestSettings:
    start_date: str
    end_date: Optional[str]
    requests_per_minute_limit: int
    chunk_size: int
    sleep_seconds_between_calls: float


@dataclass
class LoggingSettings:
    level: str
    log_dir: str
    log_file: str


@dataclass
class Settings:
    tushare: TushareSettings
    database: DatabaseSettings
    ingest: IngestSettings
    logging: LoggingSettings


def load_settings(config_path: Optional[str] = None) -> Settings:
    """加载配置文件并返回 Settings 对象（包含中文流程日志在调用方）。"""
    if config_path is None:
        # 默认读取项目根目录下的 configs/config.yaml
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(project_root, "configs", "config.yaml")

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    tushare_cfg = TushareSettings(**data["tushare"])
    db_cfg = DatabaseSettings(**data["database"])
    ingest_cfg = IngestSettings(**data["ingest"])
    logging_cfg = LoggingSettings(**data["logging"])    

    return Settings(
        tushare=tushare_cfg,
        database=db_cfg,
        ingest=ingest_cfg,
        logging=logging_cfg,
    )
