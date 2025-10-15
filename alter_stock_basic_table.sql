-- 为stock_basic表添加市值相关字段
-- 总股本、流通股、总市值、流通市值

USE stock_db;

-- 添加总股本字段（万股）
ALTER TABLE `stock_basic` ADD COLUMN `total_share` double DEFAULT NULL COMMENT '总股本（万股）';

-- 添加流通股本字段（万股） 
ALTER TABLE `stock_basic` ADD COLUMN `float_share` double DEFAULT NULL COMMENT '流通股本（万股）';

-- 添加总市值字段（万元）
ALTER TABLE `stock_basic` ADD COLUMN `total_mv` double DEFAULT NULL COMMENT '总市值（万元）';

-- 添加流通市值字段（万元）
ALTER TABLE `stock_basic` ADD COLUMN `circ_mv` double DEFAULT NULL COMMENT '流通市值（万元）';

-- 查看表结构确认
DESCRIBE `stock_basic`;

-- 查看当前数据样例
SELECT ts_code, symbol, name, total_share, float_share, total_mv, circ_mv 
FROM `stock_basic` 
LIMIT 5;
