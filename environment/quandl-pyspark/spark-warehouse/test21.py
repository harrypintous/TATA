import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_finance import candlestick2_ohlc
from mpl_finance import candlestick_ohlc

%matplotlib notebook # for Jupyter

# Format m/d/Y,Open,High,Low,Close,Adj Close,Volume
# csv data does not include NaN, or 'weekend' lines,
# only dates from which prices are recorded
DJIA = pd.read_csv('yourFILE.csv') #Format m/d/Y,Open,High,
    Low,Close,Adj Close,Volume

print(DJIA.head())

fg, ax1 = plt.subplots()

cl =candlestick2_ohlc(ax=ax1,opens=DJIA['Open'],
    highs=DJIA['High'],lows=DJIA['Low'],
    closes=DJIA['Close'],width=0.4, colorup='#77d879',
    colordown='#db3f3f')

ax1.set_xticks(np.arange(len(DJIA)))
ax1.set_xticklabels(DJIA['Date'], fontsize=6, rotation=-90)

plt.show()