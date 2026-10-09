"""
pricing/ — option pricing and volatility kernels. Pure numpy/scipy, no I/O,
every parameter explicit, every random draw from a passed Generator.

    black_scholes   Black-Scholes-Merton price, Greeks, implied vol
    binomial        Cox-Ross-Rubinstein tree, European and American
    monte_carlo     risk-neutral GBM with antithetic variates
    volatility      realised-vol estimators and the vol cone

Plan and measurements: research/option-pricing-plan-2026-10-08.md.
Conventions: T in years ACT/365, r and q continuously compounded decimals,
sigma annual decimal. `right` is "call" or "put".
"""
