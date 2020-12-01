# Enter signs

"""Conditions to open a trade.
"""

from simulator.condition import (Action as Act,
                                 Count as Cnt,
                                 Indicator as Ind,
                                 Status as Sat,
                                 )

__all__ = [
           ]


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


if __name__ == '__main__':
    pass
