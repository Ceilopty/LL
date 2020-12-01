from simulator.condition import (Indicator as _Ind,
                                 Flag as _Fl,
                                 Any, All, Not,
                                 Count as Cnt,
                                 Interval,
                                 )

__all__ = [
    # Basic Indicators
    *'hlc''kdj',

    # Ma relative
    'close_gt_ma',

    # Profit & loss
    'potential_loss_for_short',
    'potential_profit_for_long',
    'potential_profit_for_short',
    'potential_loss_for_long',
    'profit_for_long',
    'profit_for_short',

    # CandleStick
    'doji',
    'black',
    'white',

    # Frequently Used
    'Any',
    'All',
    'Not',
    'Interval',
]

_ma_group = (5, 10, 20, 45, 60)

o, h, l, c = map(_Ind, ('open', 'high', 'low', 'close'))
k, d, j = map(_Ind, 'KDJ')
close_gt_ma = Cnt(*((_Ind(f'ma_{p}') < c) for p in _ma_group))
close_lt_ma = Cnt(*((_Ind(f'ma_{p}') > c) for p in _ma_group))
doji = o.eq(c)
black = o > c
white = o < c
dt = _Ind('date')
beginner = _Fl('first')
stopper = _Fl('last')

# positive for earning, negative for pay.
potential_loss_for_long = l.shift(-1) - c
potential_profit_for_long = h.shift(-1) - c

potential_loss_for_short = c - h.shift(-1)
potential_profit_for_short = c - l.shift(-1)

profit_for_long = c.shift(-1) - c
profit_for_short = c - c.shift(-1)

if __name__ == '__main__':
    from simulator.JQ4LL import Jq

    jq = Jq()
    p = jq.get_bars('P9999.XDCE')
    dojis = p[doji]
    blacks = p[black]
    whites = p[white]
    p['loss_for_long'] = p.pipe(potential_loss_for_long)
