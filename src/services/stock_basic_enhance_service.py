from typing import Optional
import pandas as pd
from sqlalchemy.orm import Session

from src.datasource.tushare_client import TushareClient
from src.datasource.akshare_client import AKShareClient
from src.repository.daily_repository import DailyRepository


class StockBasicEnhanceService:
    """
    股票基础信息增强服务
    用于获取并更新股票的总股本、流通股、总市值、流通市值信息
    """
    
    def __init__(self, ts_client: TushareClient, ak_client: AKShareClient, repo: DailyRepository, logger=None):
        self._ts = ts_client
        self._ak = ak_client
        self._repo = repo
        self._logger = logger
        
    def enhance_stock_basic_info(self, batch_size: int = 100):
        """
        增强股票基础信息，添加市值相关字段
        
        Args:
            batch_size: 批处理大小，避免一次处理太多股票
        """
        if self._logger:
            self._logger.info("[流程] 开始增强股票基础信息，添加市值相关数据")
            
        # 1. 获取所有上市股票列表
        if self._logger:
            self._logger.info("[流程] 获取股票基础信息列表")
        stock_df = self._ts.query_stock_basic()
        
        if stock_df is None or stock_df.empty:
            if self._logger:
                self._logger.warning("[流程] 未获取到股票基础信息，退出处理")
            return
            
        # 只处理正常上市的股票
        active_stocks = stock_df[stock_df['list_status'] == 'L'].copy()
        if self._logger:
            self._logger.info("[流程] 获取到 %d 只正常上市股票", len(active_stocks))
            
        # 2. 批量获取AKShare个股信息
        ts_codes = active_stocks['ts_code'].tolist()
        
        # 分批处理，避免请求过于频繁
        total_batches = (len(ts_codes) + batch_size - 1) // batch_size
        if self._logger:
            self._logger.info("[流程] 将分 %d 批处理，每批 %d 只股票", total_batches, batch_size)
            
        all_enhanced_data = []
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(ts_codes))
            batch_codes = ts_codes[start_idx:end_idx]
            
            if self._logger:
                self._logger.info("[进度] 处理第 %d/%d 批，包含 %d 只股票", 
                                batch_idx + 1, total_batches, len(batch_codes))
            
            # 获取该批股票的AKShare数据
            enhanced_df = self._ak.batch_get_stock_metrics(batch_codes)
            
            if not enhanced_df.empty:
                # 与原始股票信息合并
                batch_stocks = active_stocks[active_stocks['ts_code'].isin(batch_codes)].copy()
                
                # 将AKShare数据合并到原始数据中
                merged_df = pd.merge(
                    batch_stocks,
                    enhanced_df[['ts_code', 'total_share', 'float_share', 'total_mv', 'circ_mv']],
                    on='ts_code',
                    how='left'
                )
                
                all_enhanced_data.append(merged_df)
                
                if self._logger:
                    self._logger.info("[进度] 第 %d 批处理完成，成功获取 %d 只股票的增强数据", 
                                    batch_idx + 1, len(enhanced_df))
            else:
                if self._logger:
                    self._logger.warning("[进度] 第 %d 批未获取到任何增强数据", batch_idx + 1)
        
        # 3. 合并所有批次的数据
        if all_enhanced_data:
            final_df = pd.concat(all_enhanced_data, ignore_index=True)
            
            # 4. 更新数据库
            if self._logger:
                self._logger.info("[流程] 开始更新数据库，总计 %d 条记录", len(final_df))
            
            self._repo.upsert_stock_basic(final_df)
            
            if self._logger:
                self._logger.info("[流程] 股票基础信息增强完成")
                
            # 5. 统计更新结果
            self._log_enhancement_statistics(final_df)
            
        else:
            if self._logger:
                self._logger.warning("[流程] 未获取到任何增强数据，请检查AKShare接口")
    
    def enhance_specific_stocks(self, ts_codes: list):
        """
        增强指定股票的基础信息
        
        Args:
            ts_codes: 股票代码列表
        """
        if self._logger:
            self._logger.info("[流程] 开始增强指定 %d 只股票的基础信息", len(ts_codes))
            
        # 获取指定股票的基础信息
        stock_df = self._ts.query_stock_basic()
        if stock_df is None or stock_df.empty:
            if self._logger:
                self._logger.warning("[流程] 未获取到股票基础信息")
            return
            
        target_stocks = stock_df[stock_df['ts_code'].isin(ts_codes)].copy()
        if target_stocks.empty:
            if self._logger:
                self._logger.warning("[流程] 未找到指定的股票代码")
            return
            
        # 获取AKShare数据
        enhanced_df = self._ak.batch_get_stock_metrics(ts_codes)
        
        if not enhanced_df.empty:
            # 合并数据
            merged_df = pd.merge(
                target_stocks,
                enhanced_df[['ts_code', 'total_share', 'float_share', 'total_mv', 'circ_mv']],
                on='ts_code',
                how='left'
            )
            
            # 更新数据库
            self._repo.upsert_stock_basic(merged_df)
            
            if self._logger:
                self._logger.info("[流程] 指定股票增强完成，成功处理 %d 只股票", len(merged_df))
                
            self._log_enhancement_statistics(merged_df)
        else:
            if self._logger:
                self._logger.warning("[流程] 未获取到指定股票的增强数据")
    
    def _log_enhancement_statistics(self, df: pd.DataFrame):
        """记录增强数据的统计信息"""
        if df.empty or self._logger is None:
            return
            
        total_stocks = len(df)
        
        # 统计各个字段的有效数据数量
        total_share_count = df['total_share'].notna().sum()
        float_share_count = df['float_share'].notna().sum()
        total_mv_count = df['total_mv'].notna().sum()
        circ_mv_count = df['circ_mv'].notna().sum()
        
        self._logger.info("[统计] 总股票数: %d", total_stocks)
        self._logger.info("[统计] 获得总股本数据: %d (%.1f%%)", 
                        total_share_count, (total_share_count / total_stocks * 100) if total_stocks > 0 else 0)
        self._logger.info("[统计] 获得流通股本数据: %d (%.1f%%)", 
                        float_share_count, (float_share_count / total_stocks * 100) if total_stocks > 0 else 0)
        self._logger.info("[统计] 获得总市值数据: %d (%.1f%%)", 
                        total_mv_count, (total_mv_count / total_stocks * 100) if total_stocks > 0 else 0)
        self._logger.info("[统计] 获得流通市值数据: %d (%.1f%%)", 
                        circ_mv_count, (circ_mv_count / total_stocks * 100) if total_stocks > 0 else 0)
        
        # 显示一些示例数据
        if total_mv_count > 0:
            sample_data = df[df['total_mv'].notna()].head(3)
            for _, row in sample_data.iterrows():
                self._logger.info("[示例] %s %s: 总股本=%.2f万股, 流通股=%.2f万股, 总市值=%.2f万元, 流通市值=%.2f万元",
                                row['ts_code'], row['name'],
                                row['total_share'] or 0, row['float_share'] or 0,
                                row['total_mv'] or 0, row['circ_mv'] or 0)
