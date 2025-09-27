"""
策略类型定义模块

定义了不同类型的策略类型，支持纯买入、纯卖出和组合策略
"""


class StrategyType:
    """策略类型常量定义"""
    BUY_ONLY = "buy_only"      # 纯买入策略：只生成买入信号
    SELL_ONLY = "sell_only"    # 纯卖出策略：只生成卖出信号  
    COMBINED = "combined"      # 组合策略：既可买入又可卖出


class StrategyCapability:
    """策略能力标识"""
    def __init__(self, can_buy: bool = False, can_sell: bool = False):
        self.can_buy = can_buy      # 是否支持买入
        self.can_sell = can_sell    # 是否支持卖出
        
    @classmethod
    def buy_only(cls):
        """创建纯买入能力"""
        return cls(can_buy=True, can_sell=False)
    
    @classmethod
    def sell_only(cls):
        """创建纯卖出能力"""
        return cls(can_buy=False, can_sell=True)
    
    @classmethod
    def combined(cls):
        """创建组合能力"""
        return cls(can_buy=True, can_sell=True)
    
    def get_strategy_type(self) -> str:
        """获取对应的策略类型"""
        if self.can_buy and self.can_sell:
            return StrategyType.COMBINED
        elif self.can_buy:
            return StrategyType.BUY_ONLY
        elif self.can_sell:
            return StrategyType.SELL_ONLY
        else:
            raise ValueError("策略必须至少支持买入或卖出中的一种操作")
