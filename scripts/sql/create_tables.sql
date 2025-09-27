CREATE TABLE IF NOT EXISTS `stock_basic` (
  `ts_code` varchar(20) NOT NULL COMMENT 'TS代码',
  `symbol` varchar(20) NOT NULL COMMENT '股票代码',
  `name` varchar(100) NOT NULL COMMENT '股票名称',
  `area` varchar(100) DEFAULT NULL COMMENT '地域',
  `industry` varchar(100) DEFAULT NULL COMMENT '行业',
  `market` varchar(50) DEFAULT NULL COMMENT '市场类别',
  `list_date` varchar(8) DEFAULT NULL COMMENT '上市日期',
  `list_status` varchar(2) DEFAULT NULL COMMENT '上市状态 L/P/D',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `daily_price` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `ts_code` varchar(20) NOT NULL COMMENT 'TS代码',
  `trade_date` varchar(8) NOT NULL COMMENT '交易日期 YYYYMMDD',
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `pre_close` double DEFAULT NULL,
  `change` double DEFAULT NULL,
  `pct_chg` double DEFAULT NULL,
  `vol` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_ts_trade_date` (`ts_code`,`trade_date`),
  KEY `idx_trade_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
