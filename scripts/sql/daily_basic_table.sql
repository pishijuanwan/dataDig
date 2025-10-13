-- Tushare每日指标数据表
-- 包含股票的每日基本面指标：市盈率、市净率、换手率、总市值等

CREATE TABLE IF NOT EXISTS `daily_basic` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `ts_code` varchar(20) NOT NULL COMMENT 'TS代码',
  `trade_date` varchar(8) NOT NULL COMMENT '交易日期 YYYYMMDD',
  `close` double DEFAULT NULL COMMENT '当日收盘价',
  `turnover_rate` double DEFAULT NULL COMMENT '换手率（%）',
  `turnover_rate_f` double DEFAULT NULL COMMENT '换手率（自由流通股）（%）',
  `volume_ratio` double DEFAULT NULL COMMENT '量比',
  `pe` double DEFAULT NULL COMMENT '市盈率（总市值/净利润）',
  `pe_ttm` double DEFAULT NULL COMMENT '市盈率（TTM滚动12个月）',
  `pb` double DEFAULT NULL COMMENT '市净率（总市值/净资产）',
  `ps` double DEFAULT NULL COMMENT '市销率',
  `ps_ttm` double DEFAULT NULL COMMENT '市销率（TTM滚动12个月）',
  `dv_ratio` double DEFAULT NULL COMMENT '股息率（%）',
  `dv_ttm` double DEFAULT NULL COMMENT '股息率（TTM）（%）',
  `total_share` double DEFAULT NULL COMMENT '总股本（万股）',
  `float_share` double DEFAULT NULL COMMENT '流通股本（万股）',
  `free_share` double DEFAULT NULL COMMENT '自由流通股本（万股）',
  `total_mv` double DEFAULT NULL COMMENT '总市值（万元）',
  `circ_mv` double DEFAULT NULL COMMENT '流通市值（万元）',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_ts_code_trade_date` (`ts_code`,`trade_date`),
  KEY `idx_trade_date` (`trade_date`),
  KEY `idx_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='每日基本面指标数据表';

-- 为常用查询字段添加索引
CREATE INDEX `idx_pe` ON `daily_basic` (`pe`);
CREATE INDEX `idx_pb` ON `daily_basic` (`pb`);
CREATE INDEX `idx_total_mv` ON `daily_basic` (`total_mv`);
CREATE INDEX `idx_turnover_rate` ON `daily_basic` (`turnover_rate`);


