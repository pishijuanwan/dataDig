import os
from sqlalchemy.orm import Session

from src.config.settings import load_settings
from src.app_logging.logger import setup_logger
from src.db.mysql_client import MySQLClient
from src.models.daily_price import Base
from src.datasource.tushare_client import TushareClient
from src.repository.daily_repository import DailyRepository
from src.services.daily_ingest_service import DailyIngestService


def main():
    settings = load_settings()
    logger = setup_logger(settings.logging.level, settings.logging.log_dir, settings.logging.log_file)

    logger.info("[流程] 读取配置成功，准备初始化数据库连接与建库建表")

    db = MySQLClient(
        host=settings.database.host,
        port=settings.database.port,
        user=settings.database.user,
        password=settings.database.password,
        db_name=settings.database.name,
    )

    db.create_database_if_not_exists()
    engine = db.create_engine()

    # 自动建表（仅首次）
    Base.metadata.create_all(bind=engine)
    logger.info("[流程] 数据表检查/创建完成")

    # 组装依赖
    ts_client = TushareClient(
        token=settings.tushare.token,
        requests_per_minute_limit=settings.ingest.requests_per_minute_limit,
        sleep_seconds_between_calls=settings.ingest.sleep_seconds_between_calls,
        logger=logger,
    )

    SessionFactory = db.session_factory()
    with SessionFactory() as session:  # type: Session
        repo = DailyRepository(session=session, logger=logger)
        service = DailyIngestService(ts_client=ts_client, repo=repo, logger=logger)

        logger.info("[流程] 启动增量模式：按库内最大交易日续拉，默认起点=%s", settings.ingest.start_date)
        service.ingest_incremental(start_date=settings.ingest.start_date)
        session.commit()
        logger.info("[流程] 全市场日线数据拉取与写入完成")


if __name__ == "__main__":
    main()
