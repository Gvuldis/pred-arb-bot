import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.special import logsumexp

# ---------- MODEL CONSTANTS ----------
FEE_RATE    = 0.02       # 2% fee on Bodega trades
B           = 3000       # LMSR liquidity parameter
Q_YES       = 17_500     # YES pool size on Bodega
Q_NO        = 23_500     # NO  pool size on Bodega
P_POLY_YES  = 0.20       # YES price on Polymarket (USD)
P_POLY_NO   = 1 - P_POLY_YES
ADA_TO_USD  = 0.6        # 1 ADA = 0.6 USD

# ---------- CORE FUNCTIONS ----------
import math

def compute_price(q_yes, q_no, b=B):
    e_yes = math.exp(q_yes/b)
    e_no  = math.exp(q_no/b)
    return e_yes/(e_yes+e_no)

def lmsr_cost(q_yes, q_no, b=B):
    return b * logsumexp([q_yes/b, q_no/b])

def lmsr_cost_vector(q_yes, q_no, x_vals, b=B):
    arr = np.vstack([
        (q_yes + x_vals)/b,
        np.full_like(x_vals, q_no/b)
    ])
    return b * logsumexp(arr, axis=0)

# ---------- PRE-TRADE STATE ----------
p0    = compute_price(Q_YES, Q_NO)
swap  = (P_POLY_YES < p0)
if swap:
    buy_poly_price = P_POLY_YES
    p_ext          = P_POLY_NO
    q_yes, q_no    = Q_NO, Q_YES
else:
    buy_poly_price = P_POLY_NO
    p_ext          = P_POLY_YES
    q_yes, q_no    = Q_YES, Q_NO

# baseline cost offset
cost0 = lmsr_cost(q_yes, q_no)

# ---------- ANALYTIC OPTIMUM ----------
def optimal_x_with_fee(q_yes, q_no, p_ext, fee=FEE_RATE, b=B):
    p_eff = p_ext/(1+fee)
    if not 0 < p_eff < 1:
        return None
    e_yes = math.exp(q_yes/b)
    e_no  = math.exp(q_no/b)
    y = (p_eff*e_no)/((1-p_eff)*e_yes)
    return None if y<=0 else b*math.log(y)

x_star = optimal_x_with_fee(q_yes, q_no, p_ext)

# ---------- GRID UP TO x* ----------
increments = list(np.arange(0, math.ceil(x_star/100)*100+1, 100))
if not np.isclose(increments[-1], x_star):
    increments.append(x_star)
x_vals = np.array(sorted(set(increments)))

# ---------- COMPUTE METRICS ----------
raw_bod    = lmsr_cost_vector(q_yes, q_no, x_vals) - cost0    # USD before fee
fee_usd    = raw_bod * FEE_RATE                                # USD fee
bod_usd    = raw_bod + fee_usd                                 # USD after fee
poly_usd   = x_vals * buy_poly_price                           # Polymarket USD spend

total_usd  = bod_usd + poly_usd
rev_usd    = x_vals
profit_usd = rev_usd - total_usd

# suppress divide warnings and apply safe formulas
with np.errstate(divide='ignore', invalid='ignore'):
    margin         = np.where(total_usd>0, profit_usd/total_usd, np.nan)
    avg_bod_price  = np.where(x_vals>0, bod_usd/x_vals, np.nan)
    avg_poly_price = np.where(x_vals>0, poly_usd/x_vals, np.nan)
    bod_ada_post   = bod_usd/ADA_TO_USD
    bod_ada_pre    = raw_bod/ADA_TO_USD

# ---------- OPTIMUM SUMMARY ----------
opt_idx = np.where(np.isclose(x_vals, x_star))[0][0]
print("=== OPTIMUM SUMMARY ===")
print(f"x* (nr_of_shares)    = {x_star:.6f}")
print(f"TotalCost_USD        = {total_usd[opt_idx]:.6f}")
print(f"PolyCost_USD         = {poly_usd[opt_idx]:.6f}")
print(f"BodCost_ADA (post)   = {bod_ada_post[opt_idx]:.6f}")
print(f"Profit_USD           = {profit_usd[opt_idx]:.6f}")
print(f"Margin               = {margin[opt_idx]:.6f}")
print("=======================\n")

# ---------- RESULT TABLE ----------
df = pd.DataFrame({
    'nr_of_shares':       x_vals,
    'Poly_USD_cost':      poly_usd,
    'Bod_ADA_cost':  bod_ada_pre,
    'Bod_USD_cost':  raw_bod,
    'TotalCost_USD':      total_usd,
    'Fee_USD':            fee_usd,
    'AvgPrice_Poly_USD':  avg_poly_price,
    'AvgPrice_Bod_USD':   avg_bod_price,
    'Profit_USD':         profit_usd,
    'Margin':             margin
})

# move optimum row to top
star = df.iloc[[opt_idx]]
rest = df.drop(opt_idx).reset_index(drop=True)
df = pd.concat([star, rest], ignore_index=True)

print(df.to_string(index=False, float_format="{:,.6f}".format))

# ---------- VISUALISATION ----------
plt.close('all')
fig, ax1 = plt.subplots()
ax1.plot(x_vals, margin, label='Margin')
ax1.set_xlabel('nr_of_shares')
ax1.set_ylabel('Margin')
ax2 = ax1.twinx()
ax2.plot(x_vals, profit_usd, label='Profit_USD', color='tab:orange')
ax2.set_ylabel('Profit_USD')
if x_star is not None:
    ax1.axvline(x_star, linestyle='--', color='red', label='x*')

# combined legend
lines, labels = ax1.get_legend_handles_labels()
l2, l2l = ax2.get_legend_handles_labels()
ax1.legend(lines + l2, labels + l2l, loc='upper left')
plt.title('Arbitrage Profit & Margin vs nr_of_shares')
plt.show()