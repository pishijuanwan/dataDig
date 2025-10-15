#!/usr/bin/env python3
"""
股票基础信息增强脚本
使用AKShare接口获取总股本、流通股、总市值、流通市值信息并更新到数据库
"""

import sys
import os
import argparse
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config.settings import load_settings
from src.datasource.tushare_client import TushareClient
from src.datasource.akshare_client import AKShareClient
from src.db.mysql_client import MySQLClient
from src.repository.daily_repository import DailyRepository
from src.services.stock_basic_enhance_service import StockBasicEnhanceService
from src.app_logging.logger import get_logger


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='股票基础信息增强脚本')
    parser.add_argument('--batch-size', type=int, default=50, help='批处理大小，默认50只股票一批')
    parser.add_argument('--specific-stocks', nargs='+', help='指定要更新的股票代码列表，例如: 000001.SZ 600000.SH')
    parser.add_argument('--sleep-interval', type=float, default=0.8, help='AKShare调用间隔（秒），默认0.8秒')
    
    args = parser.parse_args()
    
    # 初始化日志
    logger = get_logger("update_stock_basic")
    logger.info("[启动] 股票基础信息增强脚本开始执行")
    logger.info("[配置] 批处理大小=%d, AKShare调用间隔=%.2f秒", args.batch_size, args.sleep_interval)
    
    if args.specific_stocks:
        logger.info("[配置] 指定股票模式，目标股票: %s", ', '.join(args.specific_stocks))
    else:
        logger.info("[配置] 全量更新模式")
    
    try:
        # 1. 初始化配置
        logger.info("[初始化] 加载配置信息")
        settings = load_settings()
        db_config = settings.database
        ts_config = settings.tushare
        ingest_config = settings.ingest
        
        # 2. 初始化客户端
        logger.info("[初始化] 创建数据库连接")
        db_client = MySQLClient(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            db_name=db_config.name
        )
        
        logger.info("[初始化] 创建Tushare客户端")
        ts_client = TushareClient(
            token=ts_config.token,
            requests_per_minute_limit=ingest_config.requests_per_minute_limit,
            sleep_seconds_between_calls=ingest_config.sleep_seconds_between_calls,
            logger=logger
        )
        
        logger.info("[初始化] 创建AKShare客户端")
        ak_client = AKShareClient(
            sleep_seconds_between_calls=args.sleep_interval,
            logger=logger
        )
        
        # 3. 初始化服务
        with db_client.get_session() as session:
            logger.info("[初始化] 创建数据仓库和增强服务")
            repo = DailyRepository(session, logger=logger)
            enhance_service = StockBasicEnhanceService(
                ts_client=ts_client,
                ak_client=ak_client,
                repo=repo,
                logger=logger
            )
            
            # 4. 执行增强操作
            start_time = datetime.now()
            
            if args.specific_stocks:
                # 指定股票模式
                enhance_service.enhance_specific_stocks(args.specific_stocks)
            else:
                # 全量更新模式
                enhance_service.enhance_stock_basic_info(batch_size=args.batch_size)
                
            # 5. 提交事务
            session.commit()
            logger.info("[流程] 数据库事务提交成功")
            
            # 6. 统计执行时间
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info("[完成] 股票基础信息增强完成，总耗时: %s", str(duration))
            
    except KeyboardInterrupt:
        logger.info("[中断] 用户取消操作")
        sys.exit(1)
    except Exception as e:
        logger.error("[错误] 执行过程中发生错误: %s", str(e))
        logger.exception("详细错误信息:")
        sys.exit(1)
    
    logger.info("[结束] 股票基础信息增强脚本执行完毕")


if __name__ == "__main__":
    main()
