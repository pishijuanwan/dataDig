def annual_return(win_rate=0.5, rr=2, trades=100, fee=0.02):
    """
    win_rate: 胜率 (0~1)
    rr: 盈亏比 (盈利/亏损)
    trades: 年交易次数
    fee: 每次交易手续费比例 (针对本金)
    """
    # 盈利因子：本金 + 盈利 - 手续费
    win_factor = 1 + rr - fee
    # 亏损因子：本金 - 1(亏损) - 手续费
    lose_factor = 1 - 1 - fee + 1  # 本金 - 亏损(1倍本金) + 剩余(0) - 手续费
    lose_factor = 1 - 1 - fee + 0  # = -fee, 不合理，改成亏掉本金再扣手续费
    lose_factor = 1 - 1 - fee      # = -0.02
    # 实际应该限制为不能小于 0
    lose_factor = max(0, 1 - 1 - fee)  # = 0

    # 如果亏损直接归 0，则按保守算法
    # 这里我们更合理的设定是亏损后本金剩余 = 1 - 1 = 0，再扣手续费 -> 0.98 倍
    lose_factor = 1 - fee  # = 0.98

    # 每次期望因子
    expected_factor = win_rate * win_factor + (1 - win_rate) * lose_factor

    # 年度增长
    final_capital = expected_factor ** trades
    annual_yield = final_capital - 1
    return annual_yield, final_capital


if __name__ == "__main__":
    r, cap = annual_return()
    print(f"最终资金增长倍数: {cap:.2e}")
    print(f"年化收益率: {r:.2e}")
