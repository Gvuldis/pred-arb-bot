#!/usr/bin/env python3

import math
from typing import List, Tuple

q_yes, q_no = 15544, 10150
b = 3000
fee_rate = 0.02
ada_to_usd = 0.60

order_book_yes: List[Tuple[float, int]] = [
    (0.83, 6000),
    (0.835, 8000),
    (0.84, 9000),
    (0.845, 9000),
]
order_book_no: List[Tuple[float, int]] = [
    (0.15, 5000),
    (0.16, 8000),
    (0.165, 9000),
    (0.17, 9000),
]

#sis ir ja vajag b aprekinat bodega
def infer_b(q_yes: float, q_no: float, price_yes: float) -> float:
    if not (0.0 < price_yes < 1.0):
        raise ValueError("price_yes must be strictly between 0 and 1.")
    diff = q_yes - q_no
    if diff == 0:
        raise ValueError("q_yes equals q_no ⇒ b nedeterminēts (∞).")
    return diff / math.log(price_yes / (1.0 - price_yes))

def compute_price(qy, qn):
    e1, e2 = math.exp(qy / b), math.exp(qn / b)
    return e1 / (e1 + e2)

def lmsr_cost(qy, qn):
    return b * math.log(math.exp(qy / b) + math.exp(qn / b))

def consume_order_book(ob: List[Tuple[float, int]], qty: int):
    bought = cost = 0.0
    for price, avail in ob:
        take = min(qty - bought, avail)
        cost += take * price
        bought += take
        if bought >= qty:
            break
    avg_price = cost / bought if bought else 0.0
    return int(bought), cost, avg_price

p_bod_yes = compute_price(q_yes, q_no)
side = 'YES' if p_bod_yes < order_book_yes[0][0] else 'NO'
x_raw = (b * math.log(order_book_yes[0][0] / (1 - order_book_yes[0][0])) - (q_yes - q_no)) if side == 'YES' else ((q_yes - q_no) - b * math.log(order_book_yes[0][0] / (1 - order_book_yes[0][0])))
x_opt = max(0, int(round(x_raw)))

scenarios = [
    ("Fixed 100 shares", 100),
    ("25% optimum", int(round(0.25 * x_opt))),
    ("50% optimum", int(round(0.50 * x_opt))),
    ("75% optimum", int(round(0.75 * x_opt))),
    ("100% optimum", x_opt),
]

def print_table(title, x):
    if side == 'YES':
        cost_bod = lmsr_cost(q_yes + x, q_no) - lmsr_cost(q_yes, q_no)
        fee_bod = cost_bod * fee_rate
        p_start = compute_price(q_yes, q_no)
        p_end = compute_price(q_yes + x, q_no)
        ob = order_book_no
    else:
        cost_bod = lmsr_cost(q_yes, q_no + x) - lmsr_cost(q_yes, q_no)
        fee_bod = cost_bod * fee_rate
        p_start = 1 - compute_price(q_yes, q_no)
        p_end = 1 - compute_price(q_yes, q_no + x)
        ob = order_book_yes

    y_shares = int(round(x * ada_to_usd))
    filled, cost_poly_ada, avg_poly = consume_order_book(ob, y_shares)

    comb_ada = cost_bod + cost_poly_ada
    comb_usd = comb_ada * ada_to_usd
    profit_ada = x - comb_ada
    profit_usd = profit_ada * ada_to_usd
    margin = profit_ada / comb_ada if comb_ada else 0.0

    print(f"=== {title} ===")
    print("| Market     | Side | StartP | EndP   | Shares | Cost ADA | Fee ADA | AvgPoly | Comb ADA | Comb USD | Profit ADA | Profit USD | Margin | Fill |")
    print("|------------|------|--------|--------|--------|----------|---------|---------|----------|----------|------------|------------|--------|------|")
    print(f"| Bodega     | {side:<4} | {p_start:6.4f} | {p_end:6.4f} | {x:6d} | {cost_bod:8.2f} | {fee_bod:7.2f} | {'':7} | {comb_ada:8.2f} | {comb_usd:8.2f} | {profit_ada:10.2f} | {profit_usd:10.2f} | {margin:6.2%} |      |")
    print(f"| Polymarket | {('NO' if side=='YES' else 'YES'):<4} | {'':6} | {'':6} | {filled:6d} | {cost_poly_ada:8.2f} | {'':7} | {avg_poly:7.4f} | {'':8} | {'':8} | {'':10} | {'':10} | {'':6} | {filled==y_shares} |")
    print()

for title, x in scenarios:
    print_table(title, x)