# Enter signs

"""Conditions to open a trade.
"""

from simulator.condition import (Action as Act,
                                 Count as Cnt,
                                 Indicator as Ind,
                                 Status as Sat,
                                 )

__all__ = [*'hlc''kdj',
           'close_gt_ma',
           ]

h, l, c = map(Ind, ('high', 'low', 'close'))
k, d, j = map(Ind, 'KDJ')
close_gt_ma = Cnt(*((Ind(f'ma_{p}') < c) for p in (5,
                                                   10,
                                                   20,
                                                   45,
                                                   60,
                                                   )))


class Enters:
    """Standard Demo for enter conditions

    Combination of `class Condition`.
    If any pros and no cons, enter!
    """
    # indicators used
    indicators = []
    # only if all
    pre_conditions = []
    # if any
    pros = []
    # only if none
    cons = []


class Jx0(Enters):
    """"""
