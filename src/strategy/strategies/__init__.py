"""
策略模块

提供各种交易策略实现，包括：
- 纯买入策略（buy_strategies）
- 纯卖出策略（sell_strategies） 
- 组合策略（combined_strategies）
"""

# 导出组合策略
from .combined_strategies.simple_ma_strategy import SimpleMAStrategy, SimpleMAStrategyConfig
from .combined_strategies.red_three_soldiers_strategy import RedThreeSoldiersStrategy, RedThreeSoldiersConfig

__all__ = [
    'SimpleMAStrategy',
    'SimpleMAStrategyConfig', 
    'RedThreeSoldiersStrategy',
    'RedThreeSoldiersConfig'
]
