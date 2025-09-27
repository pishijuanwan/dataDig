# 策略回测模块

这个模块提供了一个完整的股票策略回测框架，支持自定义策略的开发和回测。

## 目录结构

```
src/strategy/
├── __init__.py
├── README.md                    # 本文档
├── models/                      # 数据模型
│   ├── __init__.py
│   ├── base_strategy.py        # 策略基类
│   └── backtest_result.py      # 回测结果模型
├── engines/                     # 回测引擎
│   ├── __init__.py
│   └── backtest_engine.py      # 核心回测引擎
├── services/                    # 服务层
│   ├── __init__.py
│   └── strategy_service.py     # 策略服务，提供高级接口
├── strategies/                  # 具体策略实现
│   ├── __init__.py
│   └── simple_ma_strategy.py   # 简单移动平均策略示例
└── repository/                  # 数据仓储层（待实现）
    └── __init__.py
```

## 快速开始

### 1. 运行示例策略

```bash
# 进入项目根目录
cd /Users/nxm/PycharmProjects/dataDig

# 运行策略回测脚本
python scripts/strategy/run_backtest.py
```

### 2. 编程方式使用

```python
import sys
sys.path.append('/Users/nxm/PycharmProjects/dataDig')

from src.config.settings import Settings
from src.app_logging.logger import setup_logger
from src.db.mysql_client import MySQLClient
from src.strategy.services.strategy_service import StrategyService
from src.strategy.strategies.simple_ma_strategy import SimpleMAStrategy

# 初始化
settings = Settings()
logger = setup_logger("INFO", "logs", "my_backtest.log")
mysql_client = MySQLClient(settings, logger)
session = mysql_client.get_session()
strategy_service = StrategyService(session, logger)

# 配置策略
strategy_config = {
    'short_window': 5,
    'long_window': 20,
    'initial_cash': 100000.0,
    'max_position_pct': 0.95
}

# 运行回测
result = strategy_service.run_single_strategy_backtest(
    strategy_class=SimpleMAStrategy,
    strategy_config=strategy_config,
    symbols=["000001.SZ", "600036.SH"],
    start_date="20220101",
    end_date="20231231"
)

# 输出结果
strategy_service.print_backtest_summary(result)
```

## 开发自定义策略

### 1. 创建策略类

继承 `BaseStrategy` 类并实现必要的方法：

```python
from src.strategy.models.base_strategy import BaseStrategy, StrategyConfig, TradingSignal
import pandas as pd

class MyStrategy(BaseStrategy):
    def __init__(self, config: StrategyConfig, logger=None):
        super().__init__(config, logger)
        # 初始化策略特有的属性
    
    def initialize(self) -> None:
        """策略初始化，在回测开始前调用一次"""
        if self.logger:
            self.logger.info("[策略初始化] 我的策略初始化完成")
    
    def on_bar(self, symbol: str, bar_data: pd.Series) -> str:
        """
        处理每个K线数据，返回交易信号
        
        Args:
            symbol: 股票代码
            bar_data: K线数据，包含 open, high, low, close, vol 等字段
            
        Returns:
            TradingSignal.BUY, TradingSignal.SELL, 或 TradingSignal.HOLD
        """
        # 实现你的策略逻辑
        # 例如：基于技术指标、基本面数据等做出买卖决策
        
        if 某个买入条件:
            return TradingSignal.BUY
        elif 某个卖出条件:
            return TradingSignal.SELL
        else:
            return TradingSignal.HOLD
    
    def get_position_size(self, symbol: str, price: float) -> int:
        """可选：自定义仓位管理逻辑"""
        # 默认使用基类的仓位计算方法
        return super().get_position_size(symbol, price)
```

### 2. 测试策略

```python
# 创建策略配置
config = StrategyConfig(
    initial_cash=100000.0,
    max_position_pct=0.95,
    # 添加策略特有的参数
    my_param1=10,
    my_param2=0.5
)

# 运行回测
result = strategy_service.run_single_strategy_backtest(
    strategy_class=MyStrategy,
    strategy_config=config.__dict__,
    symbols=["000001.SZ", "600036.SH"],
    start_date="20220101",
    end_date="20231231"
)
```

## 回测结果说明

回测完成后会生成 `BacktestResult` 对象，包含：

### 主要指标
- **总收益率**: 整个回测期间的收益率
- **年化收益率**: 按年计算的收益率
- **最大回撤**: 最大亏损幅度
- **夏普比率**: 风险调整后收益指标
- **波动率**: 收益率的标准差
- **交易次数**: 总交易笔数
- **胜率**: 盈利交易占比

### 详细记录
- **交易记录**: 每笔买卖的详细信息
- **每日收益**: 每个交易日的资产价值变化

### 文件导出
回测结果会自动导出为CSV文件：
- `{策略名}_trades_{时间戳}.csv`: 交易记录
- `{策略名}_daily_{时间戳}.csv`: 每日收益
- `{策略名}_summary_{时间戳}.txt`: 结果摘要

## 策略开发建议

1. **数据验证**: 确保策略逻辑正确处理数据缺失、异常值等情况
2. **参数优化**: 通过多组参数对比找到最优配置
3. **风险管理**: 实现止损、止盈等风险控制机制
4. **性能优化**: 对于复杂策略，注意内存和计算效率
5. **回测验证**: 使用不同时间段验证策略的稳定性

## 内置策略

### SimpleMAStrategy (简单移动平均策略)

**策略逻辑**:
- 金叉（短期均线上穿长期均线）时买入
- 死叉（短期均线下穿长期均线）时卖出

**配置参数**:
- `short_window`: 短期均线窗口（默认5）
- `long_window`: 长期均线窗口（默认20）
- `max_stocks`: 最多持有股票数（默认5）
- `position_per_stock`: 每只股票资金分配比例

## 注意事项

1. 确保数据库中有足够的历史价格数据
2. 回测结果仅供参考，不构成投资建议
3. 实际交易中需要考虑更多因素（滑点、流动性等）
4. 策略过度拟合历史数据可能导致未来表现不佳

## 扩展功能

未来可以添加的功能：
- [ ] 更多技术指标支持
- [ ] 基本面数据集成
- [ ] 实时策略执行
- [ ] 策略组合优化
- [ ] 风险评估模块
- [ ] 回测结果可视化

## 故障排除

1. **数据库连接问题**: 检查 `configs/config.yaml` 中的数据库配置
2. **数据缺失**: 确保已运行数据采集脚本获取历史数据
3. **内存不足**: 对于大规模回测，考虑减少股票数量或时间范围
4. **性能问题**: 检查策略逻辑是否有不必要的重复计算
