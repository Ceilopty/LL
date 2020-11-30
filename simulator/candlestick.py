# Illustration


"""Plotting candlesticks."""


import pandas as pd
import mplfinance as mpf

from simulator.JQ4LL import Jq


def candlestick(data: pd.DataFrame,
                count: int = 100,
                mav: tuple = (5, 10, 20, 45, 60),
                volume: bool = True,
                **kwargs):
    data = data.copy()
    data.index = pd.DatetimeIndex(data['date'])
    for column in ('Open', 'High', 'Low', 'Close', 'Volume'):
        data[column] = data[column.lower()]
    my_color = mpf.make_marketcolors(up='red',
                                     down='green',
                                     edge='black',
                                     wick='black',
                                     volume='blue',
                                     )
    my_style = mpf.make_mpf_style(marketcolors=my_color,
                                  gridaxis='both',
                                  gridstyle='-.',
                                  y_on_right=False)
    mpf.plot(data[-count:],
             type='candle',
             mav=mav,
             volume=volume,
             style=my_style,
             **kwargs)


def test():
    jq = Jq()
    au9999 = jq.get_bars('AU9999.XSGE')
    au9999.index = pd.DatetimeIndex(au9999['date'])
    candlestick(au9999, 200)


if __name__ == "__main__":
    test()
