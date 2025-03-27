import pandas as pd

def calculate_moving_average(series, window):
    return series.rolling(window=window).mean()

# Add additional signal functions as needed