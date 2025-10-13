import sys
from sqlalchemy.orm import Session

from src.config.settings import load_settings
from src.app_logging.logger import setup_logger
from src.db.mysql_client import MySQLClient
from src.models.daily_price import Base
from src.datasource.tushare_client import TushareClient
from src.repository.daily_repository import DailyRepository
from src.services.daily_ingest_service import DailyIngestService


def main():
    """全量下载历史数据的主函数"""
    settings = load_settings()
    logger = setup_logger(settings.logging.level, settings.logging.log_dir, "full_data_ingest.log")

    logger.info("[流程] 全量历史数据下载开始，读取配置成功，准备初始化数据库连接与建库建表")

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

        # 获取命令行参数中的开始日期，默认为配置文件中的值
        start_date = sys.argv[1] if len(sys.argv) > 1 else settings.ingest.start_date
        end_date = sys.argv[2] if len(sys.argv) > 2 else None
        
        logger.info("[流程] 启动全量模式：强制从指定日期开始下载，起始日期=%s，结束日期=%s", start_date, end_date or "今天")
        logger.info("[流程] 警告：全量模式会重新下载所有数据，可能与现有数据产生重复，但数据库使用upsert避免重复插入")
        
        # 强制使用全量下载
        service.ingest_all_from_to(start_date=start_date, end_date=end_date)
        session.commit()
        logger.info("[流程] 全量历史数据下载与写入完成")


if __name__ == "__main__":
    main()


