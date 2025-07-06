import math
import numpy as np
import matplotlib.pyplot as plt

def q_values_from_price(p_yes: float, b: float):
    ratio = (1 - p_yes) / p_yes
    delta_q = b * math.log(ratio)
    q_yes = 0
    q_no = delta_q
    return q_yes, q_no

def lmsr_cost(q_yes: float, q_no: float, b: float) -> float:
    return b * math.log(math.exp(q_yes / b) + math.exp(q_no / b))

def cost_to_buy_yes(q_yes: float, q_no: float, b: float, y: float) -> float:
    return lmsr_cost(q_yes + y, q_no, b) - lmsr_cost(q_yes, q_no, b)

def compute_max_total_wager(p_yes_lmsr, p_yes_const, b):
    q_yes, q_no = q_values_from_price(p_yes_lmsr, b)
    k = p_yes_const / (1 - p_yes_const)

    max_profit = 0
    max_x = 0
    best_y = 0

    for y in np.linspace(1, 40000, 500):
        delta_c = cost_to_buy_yes(q_yes, q_no, b, y)
        x = y + (y - delta_c) * (1 - p_yes_const) / p_yes_const
        profit_yes = y - delta_c
        profit_no = (x - y) * (1 / (1 - p_yes_const)) - (x - y)
        profit = min(profit_yes, profit_no)
        if profit >= 0 and x > max_x:
            max_x = x
            max_profit = profit
            best_y = y

    return max_x, max_profit, best_y

def compute_arbitrage_profit(x, p_yes_lmsr, p_yes_const, b):
    q_yes, q_no = q_values_from_price(p_yes_lmsr, b)
    k = p_yes_const / (1 - p_yes_const)

    def delta_C(y):
        return cost_to_buy_yes(q_yes, q_no, b, y)

    y = (k * x + delta_C(0)) / (1 + k)
    for _ in range(5):
        dC = delta_C(y)
        y = (k * x + dC) / (1 + k)

    dC = delta_C(y)
    profit_yes = y - dC
    profit_no = (x - y) * (1 / (1 - p_yes_const)) - (x - y)
    profit = min(profit_yes, profit_no)
    margin = profit / x if x > 0 else 0
    return profit, margin

# Parameters
b = 3000
p_yes_lmsr = 0.268941
p_yes_const = 0.35

x_vals = np.linspace(100, 50000, 300)
profits = []
margins = []

for x in x_vals:
    profit, margin = compute_arbitrage_profit(x, p_yes_lmsr, p_yes_const, b)
    profits.append(profit)
    margins.append(margin)

max_x, max_profit, best_y = compute_max_total_wager(p_yes_lmsr, p_yes_const, b)

# Plotting
plt.figure()
plt.plot(x_vals, profits, color='orange')
plt.axvline(x=max_x, linestyle='--', color='green', label=f'Max Wager ≈ {max_x:.0f}')
plt.title("Profit vs Total Wager")
plt.xlabel("Total Wager (x)")
plt.ylabel("Profit")
plt.legend()
plt.grid(True)
plt.show()

plt.figure()
plt.plot(x_vals, margins, color='orange')
plt.axvline(x=max_x, linestyle='--', color='green', label='Max Wager')
plt.title("Profit Margin vs Total Wager")
plt.xlabel("Total Wager (x)")
plt.ylabel("Profit Margin")
plt.legend()
plt.grid(True)
plt.show()

max_x, max_profit, best_y