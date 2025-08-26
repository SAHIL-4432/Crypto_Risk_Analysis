import numpy as np

def monte_carlo_simulation(start_price, mean_return, volatility, days, simulations):
    results = []
    for _ in range(simulations):
        prices = [start_price]
        for _ in range(days):
            price = prices[-1] * np.exp(np.random.normal(mean_return, volatility))
            prices.append(price)
        results.append(prices)
    return results
