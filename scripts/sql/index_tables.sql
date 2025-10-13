-- 指数基本信息表
CREATE TABLE IF NOT EXISTS `index_basic` (
  `ts_code` varchar(20) NOT NULL COMMENT '指数代码',
  `name` varchar(100) NOT NULL COMMENT '指数名称',
  `market` varchar(50) DEFAULT NULL COMMENT '市场',
  `publisher` varchar(50) DEFAULT NULL COMMENT '发布方',
  `index_type` varchar(50) DEFAULT NULL COMMENT '指数类别',
  `category` varchar(50) DEFAULT NULL COMMENT '指数分类',
  `base_date` varchar(8) DEFAULT NULL COMMENT '基期',
  `base_point` double DEFAULT NULL COMMENT '基点',
  `list_date` varchar(8) DEFAULT NULL COMMENT '发布日期',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 指数日线数据表
CREATE TABLE IF NOT EXISTS `index_daily` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `ts_code` varchar(20) NOT NULL COMMENT '指数代码',
  `trade_date` varchar(8) NOT NULL COMMENT '交易日期 YYYYMMDD',
  `open` double DEFAULT NULL COMMENT '开盘点位',
  `high` double DEFAULT NULL COMMENT '最高点位',
  `low` double DEFAULT NULL COMMENT '最低点位',
  `close` double DEFAULT NULL COMMENT '收盘点位',
  `pre_close` double DEFAULT NULL COMMENT '昨收盘',
  `change` double DEFAULT NULL COMMENT '涨跌点',
  `pct_chg` double DEFAULT NULL COMMENT '涨跌幅',
  `vol` double DEFAULT NULL COMMENT '成交量（手）',
  `amount` double DEFAULT NULL COMMENT '成交额（千元）',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_index_ts_trade_date` (`ts_code`,`trade_date`),
  KEY `idx_index_trade_date` (`trade_date`),
  KEY `idx_index_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
