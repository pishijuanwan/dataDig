import os
from sqlalchemy.orm import Session

from src.config.settings import load_settings
from src.app_logging.logger import setup_logger
from src.db.mysql_client import MySQLClient
from src.models.daily_price import Base
from src.datasource.tushare_client import TushareClient
from src.repository.daily_repository import DailyRepository
from src.services.index_ingest_service import IndexIngestService


def main():
    settings = load_settings()
    logger = setup_logger(settings.logging.level, settings.logging.log_dir, "index_ingest.log")

    logger.info("[流程] 指数数据下载程序启动，读取配置成功，准备初始化数据库连接与建库建表")

    db = MySQLClient(
        host=settings.database.host,
        port=settings.database.port,
        user=settings.database.user,
        password=settings.database.password,
        db_name=settings.database.name,
    )

    db.create_database_if_not_exists()
    engine = db.create_engine()

    # 自动建表（仅首次），包含新增的指数表
    Base.metadata.create_all(bind=engine)
    logger.info("[流程] 指数数据表检查/创建完成")

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
        service = IndexIngestService(ts_client=ts_client, repo=repo, logger=logger)

        logger.info("[流程] 启动指数数据增量模式：按库内最大交易日续拉，默认起点=%s", settings.ingest.start_date)
        
        # 首次运行会下载指数基本信息
        logger.info("[流程] 开始下载指数基本信息")
        service.ingest_index_basic_all()
        
        # 增量拉取指数日线数据
        logger.info("[流程] 开始增量拉取指数日线数据")
        service.ingest_incremental(start_date=settings.ingest.start_date)
        
        session.commit()
        logger.info("[流程] 指数日线数据拉取与写入完成")


if __name__ == "__main__":
    main()
