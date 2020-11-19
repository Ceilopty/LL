# Enter signs

"""Conditions to open a trade.
"""


class Condition:
    """single conditions
    Instance is callable, for given series,
    return a same length boolean series.
    """
    indicators = []
    __slots__ = []
    def __call__(self, df):
        if any(indicator in df for indicator in self.indicators):
            pass
        else:
            raise AttributeError(f"Not all attributions in {self.indicators} found in df.")


class XuY(Condition):
    """Indicator `X` come up through a certain level `X` from below to above."""
    indicators =[]
    level = []

    def __call__(self, df):
        target = df[self.indicators]
        pre = target.shift(1)
        return (target >= self.level) & (pre < self.level)


class KDJ0(XuY):
    indicators = 'J'
    level = 0


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
