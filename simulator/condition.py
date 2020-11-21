"""Given any condition, return a func that convert a DF to boolean vector.

"""

from typing import Union
from functools import wraps
from pandas import DataFrame as dF, Series

__all__ = ["Touch",
           "At",
           "Alias",
           ]


def operators_conductor(operator_name):
    func = getattr(Series, operator_name)

    @wraps(func)
    def operator_method(self, other=None):
        if other is None:
            def not_(df: dF):
                return func(df.pipe(self))
            return not_
        # if not isinstance(other, Condition):
            # raise TypeError("only conditions can add, got %r" % type(other))

        def comb(df: dF):
            return func(df.pipe(self), df.pipe(other))
        return comb
    return operator_method


class Condition:
    """single conditions, subjected to be subclassed.

    Instance is callable, for given series, return a same length boolean series.
    """
    _indicator = []
    _direction = []
    _level = []
    _collections = {}
    __slots__ = ()

    def __new__(cls, indicator, direction, level):
        hash_ = cls.__name__, indicator, direction, float(level)
        if hash_ not in Condition._collections:
            Condition._collections[hash_] = super().__new__(cls)
        return Condition._collections[hash_]

    __and__ = operators_conductor("__and__")
    __rand__ = __and__
    __or__ = operators_conductor("__or__")
    __ror__ = __or__
    __xor__ = operators_conductor("__xor__")
    __rxor__ = __xor__
    __invert__ = operators_conductor("__invert__")

    def __repr__(self):
        re = (self._indicator, self._direction, self._level)
        re = map(str, re)
        re = ' '.join(re)
        return f'{self.__class__.__name__}[ {re} ]'


class Touch(Condition):
    """`Indicator` touches `Level` up or down.

    Focus on two bars: the current, and the previous.
    If both bars fulfill specific condition, mark the current bar True."""

    def __init__(self, indicator: str,
                 direction: str,
                 level: Union[int, float],
                 ):
        """Marks True if the particular bar touches (or crosses) a given level.

        :param indicator: The name of the target column in df.
        :param direction: Can be one of the following :
            `cross_up`: from (-inf, level] to (level, inf)
            `touch_up`: from (-inf, level) to [level, inf)
            `cross_down`: from [level, inf) to (-inf, level)
            `touch_down`: from (level, inf) to (-inf, level]
        :param level: The level to determine whether touched or not.

        :return: A DataFrame filter (series of booleans).

        Usage:
        >>> j_touch_up_0 = Touch(indicator='J', direction='touch_up', level=0)
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
        self._indicator = indicator
        self._direction = direction.replace('_', ' ')
        self._level = level
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
        target = df[self._indicator]
        tar_meth = getattr(target, self.tar_op)
        # get the previous bar
        # and the bound the method
        pre = target.shift(1)
        pre_meth = getattr(pre, self.pre_op)
        # compute whether both tar and pre are at the right position of level
        return tar_meth(self._level) & pre_meth(self._level)


class At(Condition):
    """`Indicator` on, on or at, under, under at `Level`.

    Focus on only the current bar. Marks True if the Indicator
    is at the given position.
    """

    def __init__(self, indicator: str,
                 direction: str,
                 level: Union[int, float],
                 ):
        """Marks True if the particular bar sits on the right side of a given level.

                :param indicator: The name of the target column in df.
                :param direction: Can be one of the following :
                    `lt`: (-inf, level), less than
                    `le`: (-inf, level], less or equal
                    `ge`: [level, inf), greater or equal
                    `gt`: (level, inf), greater than
                :param level: The level to determine the position.

                :return: A DataFrame filter (series of booleans).

                Usage:
                >>> j_ge_0 = At(indicator='J', direction='ge', level=0)
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
        dir_repr_map = {'lt': '<',
                        'le': '<=',
                        'gt': '>',
                        'ge': '>='}
        self._indicator = indicator
        self._direction = dir_repr_map[direction]
        self._level = level
        self.op = f'__{direction}__'

    def __call__(self, df: dF):
        # get the current bar
        target = df[self._indicator]
        # and the bound the method
        tar_meth = getattr(target, self.op)
        # compare to the level
        return tar_meth(self._level)


class Alias:
    def __new__(cls, con):
        if not isinstance(con, str):
            raise TypeError('Please input a string for Alias')
        cmp_map = {'<': 'lt',
                   '<=': 'le',
                   '>': 'gt',
                   '>=': 'ge',
                   }
        dir_map = {'sc': 'cross_up',    # 上穿
                   'sp': 'touch_up',    # 上碰
                   'xc': 'cross_down',  # 下穿
                   'xp': 'touch_down',  # 下碰
                   }
        try:
            ind, opr, lev = con.split(' ')
        except ValueError as e:
            raise ValueError(f"Too much or less space! got {con}") from e

