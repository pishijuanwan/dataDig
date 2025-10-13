# trade_sim.py
"""
可直接运行的脚本：计算在指定假设下（满仓进出、次日以 +gain 或 -loss 平仓、每次进出费 fee）
的解析期望、分位数，以及用蒙特卡洛做验证。

运行：python trade_sim.py
"""

import math
import random
import statistics

def analytical_results(initial_capital, win_rate, gain, loss, trades, fee):
    win_mult = (1 + gain) * (1 - fee)
    lose_mult = (1 - loss) * (1 - fee)
    expected_mult_per_trade = win_rate * win_mult + (1 - win_rate) * lose_mult
    expected_final_capital = initial_capital * (expected_mult_per_trade ** trades)
    expected_return_pct = (expected_final_capital / initial_capital - 1) * 100

    # 精确二项分布
    ks = list(range(trades + 1))
    probs = [math.comb(trades, k) * (win_rate ** k) * ((1 - win_rate) ** (trades - k)) for k in ks]
    multipliers = [ (win_mult ** k) * (lose_mult ** (trades - k)) for k in ks ]
    final_values = [initial_capital * m for m in multipliers]
    expected_final_from_dist = sum(v * p for v, p in zip(final_values, probs))

    # 分位数 (5%, 50%, 95%)：通过累积分布查找
    cdf = 0.0
    def find_percentile(pct):
        nonlocal cdf
        cum = 0.0
        target = pct / 100.0
        for k, p, v, m in zip(ks, probs, final_values, multipliers):
            cum += p
            if cum >= target:
                return v, m, k
        return final_values[-1], multipliers[-1], ks[-1]

    p5_val, p5_mult, p5_k = find_percentile(5)
    median_val, median_mult, median_k = find_percentile(50)
    p95_val, p95_mult, p95_k = find_percentile(95)

    return {
        "win_mult": win_mult,
        "lose_mult": lose_mult,
        "expected_mult_per_trade": expected_mult_per_trade,
        "expected_final_capital": expected_final_capital,
        "expected_final_from_dist": expected_final_from_dist,
        "expected_return_pct": expected_return_pct,
        "p5": (p5_val, p5_mult, p5_k),
        "median": (median_val, median_mult, median_k),
        "p95": (p95_val, p95_mult, p95_k),
    }

def monte_carlo_sim(initial_capital, win_rate, win_mult, lose_mult, trades, trials, seed=123456):
    rng = random.Random(seed)
    results = []
    for _ in range(trials):
        wins = 0
        # 用二项模拟每次交易的胜负
        for _ in range(trades):
            if rng.random() < win_rate:
                wins += 1
        final = initial_capital * (win_mult ** wins) * (lose_mult ** (trades - wins))
        results.append(final)
    results.sort()
    mean = statistics.mean(results)
    median = statistics.median(results)
    n = len(results)
    def percentile(list_sorted, pct):
        if pct <= 0:
            return list_sorted[0]
        if pct >= 100:
            return list_sorted[-1]
        # 使用上取整的秩
        rank = int(math.ceil(pct / 100.0 * n)) - 1
        rank = max(0, min(n - 1, rank))
        return list_sorted[rank]
    p5 = percentile(results, 5)
    p95 = percentile(results, 95)
    return {"mean": mean, "median": median, "p5": p5, "p95": p95}

def fmt_money(x):
    return f"{x:,.2f} 元"

def fmt_pct(x):
    return f"{x:,.4f} %"

def main():
    # 默认参数（可修改）
    initial_capital = 100000   # 100 万
    win_rate = 0.5                 # 50%
    gain = 0.20                    # +10%
    loss = 0.05                    # -5%
    trades = 50                    # 50 次交易/年
    fee = 0.002                    # 每次进出总费用 0.2%
    trials = 200000                 # 蒙特卡洛次数

    # 解析结果
    res = analytical_results(initial_capital, win_rate, gain, loss, trades, fee)

    print("=== 假设 ===")
    print(f"初始资金: {fmt_money(initial_capital)}，交易次数: {trades}，胜率: {win_rate*100:.1f}%")
    print(f"盈: +{gain*100:.1f}% 亏: -{loss*100:.1f}%，每次进出费用: {fee*100:.3f}%\n")

    print("=== 每次实际乘数（含手续费） ===")
    print(f"赢时乘数: {res['win_mult']:.6f}")
    print(f"亏时乘数: {res['lose_mult']:.6f}")
    print(f"每次期望乘数 E[M]: {res['expected_mult_per_trade']:.6f} （相当于每次期望增幅 {(res['expected_mult_per_trade']-1)*100:.4f}%）\n")

    print("=== 解析期望 ===")
    print("解析计算的预期终值:", fmt_money(res['expected_final_capital']))
    print("（分布求和验证）:", fmt_money(res['expected_final_from_dist']))
    print("预期总收益率:", fmt_pct(res['expected_return_pct']), "\n")

    p5_val, p5_mult, p5_k = res['p5']
    median_val, median_mult, median_k = res['median']
    p95_val, p95_mult, p95_k = res['p95']

    print("=== 分布信息（精确二项分布） ===")
    print(f"5% 分位最终资金: {fmt_money(p5_val)} （对应胜场数 k = {int(p5_k)}，multiplier = {p5_mult:.6f}）")
    print(f"中位数最终资金: {fmt_money(median_val)} （对应胜场数 k = {int(median_k)}，multiplier = {median_mult:.6f}）")
    print(f"95% 分位最终资金: {fmt_money(p95_val)} （对应胜场数 k = {int(p95_k)}，multiplier = {p95_mult:.6f}）\n")

    # Monte Carlo 验证
    mc = monte_carlo_sim(initial_capital, win_rate, res['win_mult'], res['lose_mult'], trades, trials)
    print("=== Monte Carlo 验证（%d 次） ===" % trials)
    print(f"模拟平均最终资金: {fmt_money(mc['mean'])}")
    print(f"模拟中位数最终资金: {fmt_money(mc['median'])}")
    print(f"模拟 5% / 95% 分位: {fmt_money(mc['p5'])} / {fmt_money(mc['p95'])}")

if __name__ == "__main__":
    main()
