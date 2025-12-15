import numpy as np
import talib

print('Testing TA-Lib...')

# 1. Create fake 5-minute candle data (random float values)
# High, Low, Close (must be numpy arrays of type float)
highs = np.random.rand(20) * 100 + 100  # Prices around 100-200
lows = highs - (np.random.rand(20) * 5)  # Low is slightly below High
closes = (highs + lows) / 2  # Close is midpoint

print(f'Generated {len(closes)} candles.')

# 2. Calculate ATR (14)
try:
    atr = talib.ATR(highs, lows, closes, timeperiod=14)
    # The first 14 values will be NaN (because ATR needs 14 previous candles)
    last_atr = atr[-1]

    if np.isnan(last_atr):
        print('❌ ATR returned NaN (Check data size)')
    else:
        print(f'✅ Success! Calculated ATR: {last_atr:.4f}')
        print('TA-Lib is ready for the bot.')

except Exception as e:
    print(f'❌ Critical Error: {e}')
