# 完整的逆向投资策略分析运行指南

## 🚀 一键运行（推荐）

最简单的方式是使用一键运行脚本，它会自动完成所有步骤：

```bash
./scripts/shell/run_complete_analysis.sh
```

**预计总耗时：2-3小时**（取决于网络速度和数据量）

## 📋 运行步骤详解

### 第1步：数据导入（必需）

#### 1.1 导入股票基础信息和日线价格数据
```bash
./scripts/shell/run_full_data_ingest.sh 2024-01-01
```
- **功能**：下载股票基础信息、日线价格数据
- **数据范围**：2024年1月1日至今
- **预计耗时**：30-60分钟
- **数据量**：约500万条记录

#### 1.2 导入每日指标数据（关键！）
```bash
./scripts/shell/run_full_daily_basic_ingest.sh 2024-01-01
```
- **功能**：下载市盈率、市净率、换手率、总市值等指标
- **重要性**：⭐⭐⭐⭐⭐ 逆向投资策略必需数据
- **预计耗时**：45-90分钟
- **数据量**：约500万条记录

#### 1.3 导入指数数据
```bash
./scripts/shell/run_index_ingest.sh 2024-01-01
```
- **功能**：下载上证指数、深证成指、创业板指、科创50等指数数据
- **重要性**：⭐⭐⭐⭐⭐ 用于判断"大盘涨个股跌"
- **预计耗时**：5-10分钟
- **数据量**：约1000条记录

### 第2步：数据验证
```bash
python scripts/strategy/check_available_data.py
```
- **功能**：检查数据完整性，找出可用的分析日期
- **耗时**：1-2分钟

### 第3步：运行策略分析

#### 3.1 逆向投资策略分析（核心功能）
```bash
python scripts/strategy/contrarian_strategy_analysis.py
```
- **功能**：寻找大盘涨个股跌的投资机会
- **筛选条件**：
  - 对应指数涨幅 ≥ 2%
  - 个股跌幅 ≥ 6%
  - 近20日涨幅 ≤ 20%
- **分析维度**：5天和10天后表现

#### 3.2 通用股票筛选分析（额外福利）
```bash
python scripts/strategy/stock_screening_analysis.py
```
- **功能**：多种投资策略的股票筛选
- **包含策略**：价值投资、动量策略、成长投资

## 📊 输出文件说明

### 分析结果文件（保存在 `results/` 目录）

#### 逆向投资策略结果
- `股票筛选表现分析_[日期]_[时间].csv` - 详细股票表现数据
- `股票筛选统计摘要_[日期]_[时间].txt` - 策略效果摘要

#### 通用筛选策略结果
- 价值投资筛选结果
- 动量投资筛选结果  
- 成长投资筛选结果

### 日志文件（保存在 `logs/` 目录）
- `complete_analysis.log` - 完整流程主日志
- `contrarian_strategy_analysis.log` - 逆向策略详细日志
- `stock_screening_analysis.log` - 通用筛选详细日志

## ⚙️ 配置说明

### 当前配置（`configs/config.yaml`）
```yaml
ingest:
  start_date: "2024-01-01"    # 数据起始日期
  end_date: null              # 数据结束日期（null表示到今天）
  requests_per_minute_limit: 450  # API调用频率限制
  chunk_size: 1000           # 每次处理的数据块大小
  sleep_seconds_between_calls: 0.15  # API调用间隔
```

### 自定义配置
如果需要修改数据范围，编辑 `configs/config.yaml`：
- **更早数据**：修改 `start_date` 为 `"2020-01-01"`
- **特定范围**：设置 `end_date` 为 `"2024-12-31"`

## 🛠️ 故障排除

### 常见问题

#### 1. 数据导入失败
**症状**：显示网络错误或API限制  
**解决**：
```bash
# 检查网络连接
ping tushare.pro

# 降低API调用频率（修改配置文件中的requests_per_minute_limit）
# 重新运行失败的步骤
```

#### 2. 策略分析无结果
**症状**：提示"未找到符合条件的股票"  
**原因**：选择的日期可能没有符合条件的市场情况  
**解决**：
```bash
# 使用简化版本测试
python scripts/strategy/simplified_contrarian_strategy.py

# 或手动指定其他日期进行测试
```

#### 3. 数据库连接失败
**症状**：MySQL连接错误  
**解决**：
```bash
# 检查MySQL服务状态
brew services start mysql

# 检查配置文件中的数据库设置
```

### 调试工具

#### 检查数据状态
```bash
python scripts/strategy/check_available_data.py
```

#### 测试基本功能
```bash
python scripts/strategy/test_contrarian_strategy.py
```

## 📈 使用建议

### 投资决策流程
1. **运行分析**：获取最新的策略筛选结果
2. **查看结果**：重点关注胜率和平均收益率
3. **个股研究**：对筛选出的股票进行基本面分析
4. **风险控制**：设置止损点和仓位限制
5. **跟踪表现**：定期检查持仓股票表现

### 最佳实践
- **运行频率**：建议每周运行1-2次
- **市场时机**：在市场波动较大时运行效果更好
- **结果验证**：结合技术分析和基本面分析
- **风险管理**：单只股票仓位不超过总资金的5%

## 🔄 定期维护

### 数据更新
```bash
# 更新最新数据（增量更新）
./scripts/shell/run_daily_basic_ingest.sh

# 重新运行策略分析
python scripts/strategy/contrarian_strategy_analysis.py
```

### 清理日志
```bash
# 清理过期日志文件
find logs/ -name "*.log" -mtime +30 -delete
```

## 💡 进阶功能

### 自定义筛选条件
编辑 `scripts/strategy/contrarian_strategy_analysis.py` 中的参数：
```python
contrarian_condition = ContrarianCondition(
    min_index_rise=2.0,      # 指数最小涨幅
    max_stock_fall=-6.0,     # 个股最大跌幅
    max_historical_rise=20.0, # 历史最大涨幅
    historical_days=20       # 历史天数
)
```

### 批量分析
```bash
# 分析多个日期的表现
for date in 20241001 20241015 20241101; do
    python scripts/strategy/contrarian_strategy_analysis.py --date $date
done
```

---

## 📞 技术支持

如果遇到问题，请检查：
1. 日志文件（`logs/` 目录）
2. 数据库连接状态
3. 网络连接状态
4. Tushare API额度

**开始使用**：直接运行 `./scripts/shell/run_complete_analysis.sh` 即可！
