"""
纯买入策略模块

该模块包含只生成买入信号的策略实现
"""

from .red_three_soldiers_strategy import RedThreeSoldiersStrategy, RedThreeSoldiersConfig

__all__ = [
    'RedThreeSoldiersStrategy',
    'RedThreeSoldiersConfig'
]
