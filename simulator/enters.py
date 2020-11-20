# Enter signs

"""Conditions to open a trade.
"""

from typing import Union
from pandas import DataFrame as dF, Series


class Condition:
    """single conditions, subjected to be subclassed.

    Instance is callable, for given series, return a same length boolean series.
    """
    indicators = []
    __slots__ = ()

    def __and__(self, other):
        if not isinstance(other, Condition):
            raise TypeError("only conditions can add, got %r"%type(other))

        def comb(df: dF):
            return df.pipe(self).__and__(df.pipe(other))
        return comb


class Touch(Condition):
    """`Indicator` touches `Level` up or down.

    Focus on two bars: the current, and the previous.
    If both bars fulfill specific condition, mark the current bar True."""
    def __init__(self, indicators: str,
                 direction: str,
                 level: Union[int, float],
                 ):
        """Marks True if the particular bar touches (or crosses) a given level.

        :param indicators: The name of the target column in df.
        :param direction: Can be one of the following :
            `cross_up`: from (-inf, level] to (level, inf)
            `touch_up`: from (-inf, level) to [level, inf)
            `cross_down`: from [level, inf) to (-inf, level)
            `touch_down`: from (level, inf) to (-inf, level]
        :param level: The level to determine whether touched or not.

        :return: A DataFrame filter (series of booleans).

        Usage:
        >>> j_touch_up_0 = Touch(indicators='J', direction='touch_up', level=0)
        >>> foo = dF({'J':[-1, 0, 0, 1, 0, 0, -1]})
        >>> foo
           J
        0 -1
        1  0
        2  0
        3  1
        4  0
        5  0
        6 -1
        >>> foo.pipe(j_touch_up_0)
        0    False
        1     True
        2    False
        3    False
        4    False
        5    False
        6    False
        Name: J, dtype: bool
        >>> foo[foo.pipe(j_touch_up_0)]
        1  0
        >>> foo[foo.pipe(Touch('J', 'cross_up', 0))]
        3  1
        >>> foo[foo.pipe(Touch('J', 'cross_down', 0))]
        6 -1
        >>> foo[foo.pipe(Touch('J', 'touch_down', 0))]
        4  0
        """
        self.indicators = indicators
        self.level = level
        # Operators used between (pre or target) and level for each case.
        # In the term of (pre_operator, target_operator).
        dir_map = {'cross_up': ('__le__', '__gt__'),
                   'touch_up': ('__lt__', '__ge__'),
                   'cross_down': ('__ge__', '__lt__'),
                   'touch_down': ('__gt__', '__le__'),
                   }
        self.pre_op, self.tar_op = dir_map[direction]

    def __call__(self, df: dF) -> Series:
        # get the current bar
        # and the bound the method
        target = df[self.indicators]
        tar_meth = getattr(target, self.tar_op)
        # get the previous bar
        # and the bound the method
        pre = target.shift(1)
        pre_meth = getattr(pre, self.pre_op)
        # compute whether both tar and pre are at the right position of level
        return tar_meth(self.level) & pre_meth(self.level)


class At(Condition):
    """`Indicator` on, on or at, under, under at `Level`.

    Focus on only the current bar. Marks True if the Indicator
    is at the given position.
    """
    def __init__(self, indicators: str,
                 direction: str,
                 level: Union[int, float],
                 ):
        """Marks True if the particular bar sits on the right side of a given level.

                :param indicators: The name of the target column in df.
                :param direction: Can be one of the following :
                    `lt`: (-inf, level), less than
                    `le`: (-inf, level], less or equal
                    `ge`: [level, inf), greater or equal
                    `gt`: (level, inf), greater than
                :param level: The level to determine the position.

                :return: A DataFrame filter (series of booleans).

                Usage:
                >>> j_ge_0 = At(indicators='J', direction='ge', level=0)
                >>> foo = dF({'J':[-1, 0, 1]})
                >>> foo
                   J
                0 -1
                1  0
                2  1
                >>> foo.pipe(j_ge_0)
                0    False
                1     True
                2     True
                Name: J, dtype: bool
                >>> foo[foo.pipe(j_ge_0)]
                1  0
                2  1
                >>> foo[foo.pipe(At('J', 'gt' ,0))]
                2  1
                >>> foo[foo.pipe(At('J', 'le', 0))]
                0 -1
                1  0
                >>> foo[foo.pipe(Touch('J','lt', 0))]
                0 -1
                """
        self.indicators = indicators
        self.level = level
        self.op = f'__{direction}__'

    def __call__(self, df: dF):
        # get the current bar
        target = df[self.indicators]
        # and the bound the method
        tar_meth = getattr(target, self.op)
        # compare to the level
        return tar_meth(self.level)


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
