"""Given any condition, return a func that convert a DF to boolean vector.

"""

from typing import Union, Iterable
from functools import wraps, total_ordering, reduce
from pandas import DataFrame as dF, Series, Interval as _Iv

__all__ = ["Action",
           "Status",
           "Alias",
           "All",
           "Any",
           "Count",
           "Indicator",
           "Interval",
           ]

# TODO: Given a PosInt `N`, any status should be confirmed when all of the last `N` bars meet the criteria.


def _operators_conductor(operator_name, _bool=None):
    """Return a unbound method for Conditions. Such as +-&|"""
    func = getattr(Series, operator_name)
    if _bool is None:
        # return bool series.
        _pre, _post = bool, bool
    else:
        # return ints.
        _pre, _post = int, int

    @wraps(func)
    def operator_method(self, other=None):
        if other is None:
            # for unary such as pos, neg, invert
            def not_(df: dF):
                return func(df.pipe(self.copy().pop())).apply(_post)

            return not_

        # if not isinstance(other, Condition):
        # raise TypeError("only conditions can add, got %r" % type(other))

        def comb(df: dF) -> Series:
            return func(df.pipe(self).apply(_pre), df.pipe(other).apply(_pre)).apply(_post)

        return comb

    return operator_method


# Divide iter into two groups: the instances and the others.
def _get_all_ins(iterable, cls) -> (list, list):
    instance = []
    others = []
    for may_be_ins in iterable:
        if isinstance(may_be_ins, cls):
            instance.append(may_be_ins)
        else:
            others.append(may_be_ins)
    return instance, others


# Remove duplications
def _unique(iterable: Iterable[set]) -> set:
    res = set()
    for item in iterable:
        res.update(item)
    return res


@total_ordering
class Condition:
    """single conditions, subjected to be subclassed.

    Instance is callable, receive a DataFrame, return a boolean Series.
    """
    _collections = {}
    __slots__ = ("_indicator",
                 "_direction",
                 "_level",
                 )

    def __new__(cls, indicator, direction, level):
        if isinstance(level, str):
            level = Indicator(level)
        elif isinstance(level, (Count, Interval)):
            pass
        else:
            level = float(level)
        hash_ = cls.__name__, indicator, direction, level
        if hash_ not in cls._collections:
            self = super().__new__(cls)
            cls._collections[hash_] = self
            self._indicator = indicator
            self._direction = direction
            self._level = level
        return cls._collections[hash_]

    def __init__(self, *args, **kwargs):
        pass

    def __and__(self, other):
        return All(self, other)
    __rand__ = __and__

    def __or__(self, other):
        return Any(self, other)
    __ror__ = __or__

    __xor__ = _operators_conductor("__xor__")
    __rxor__ = __xor__

    def __invert__(self):
        return Not(self)

    def __repr__(self):
        re = (self._indicator, self._direction.replace('_', ' '), self._level)
        re = map(str, re)
        re = ' '.join(re)
        return f'{self.__class__.__name__}[ {re} ]'

    def __hash__(self):
        hash_ = self.__class__.__name__, self._indicator, self._direction, self._level
        return hash(hash_)

    def __gt__(self, value):
        # Not instance is treated as what it contains.
        if isinstance(value, Not):
            value = value.copy().pop()
        if not isinstance(value, Condition):
            return NotImplemented
        if self.__class__.__name__ != value.__class__.__name__:
            return self.__class__.__name__ > value.__class__.__name__
        if isinstance(self._indicator, Indicator):
            s = self._indicator.ind
        elif isinstance(self._indicator, str):
            s = self._indicator
        else:
            raise TypeError(f'Need Indicator, got {type(self._indicator)}')
        if isinstance(value._indicator, Indicator):
            v = value._indicator.ind
        elif isinstance(value._indicator, str):
            v = value._indicator
        else:
            raise TypeError(f'Need Indicator, got {type(self._indicator)}')
        if s != v:
            return s > v
        if self._direction != value._direction:
            return self._direction > value._direction
        return self._level > value._level


class Action(Condition):
    """`Indicator` touches `Level` up or down.

    Focus on two bars: the current, and the previous.
    If both bars fulfill specific condition, mark the current bar True."""

    __slots__ = ("pre_op",
                 "tar_op",
                 )

    def __init__(self, indicator: Union[str, ],
                 direction: str,
                 level: Union[int, float, str, ],
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
        >>> j_touch_up_0 = Action(indicator='J', direction='touch_up', level=0)
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
        >>> foo[foo.pipe(Action('J', 'cross_up', 0))]
        3  1
        >>> foo[foo.pipe(Action('J', 'cross_down', 0))]
        6 -1
        >>> foo[foo.pipe(Action('J', 'touch_down', 0))]
        4  0
        """
        # Operators used between (pre or target) and level for each case.
        # In the term of (pre_operator, target_operator).
        dir_map = {'cross_up': ('__le__', '__gt__'),
                   'touch_up': ('__lt__', '__ge__'),
                   'cross_down': ('__ge__', '__lt__'),
                   'touch_down': ('__gt__', '__le__'),
                   }
        self.pre_op, self.tar_op = dir_map[direction]
        super(Action, self).__init__(indicator, direction, level)

    def __call__(self, df: dF) -> Series:
        # get the current bar
        # and the bound the method
        if isinstance(self._indicator, str):
            target = df[self._indicator]
        elif isinstance(self._indicator, Count):
            target = df.pipe(self._indicator)
        else:
            target = None
        tar_meth = getattr(target, self.tar_op)
        # get the previous bar
        # and the bound the method
        pre = target.shift(1)
        pre_meth = getattr(pre, self.pre_op)
        if isinstance(self._level, (float, Interval)):
            tar_level = self._level
            pre_level = self._level
        elif isinstance(self._level, str):
            tar_level = df[self._level]
            pre_level = tar_level.shift(1)
        elif isinstance(self._level, Count):
            tar_level = df.pipe(self._level)
            pre_level = tar_level.shift(1)
        else:
            tar_level, pre_level = None, None
        # compute whether both tar and pre are at the right position of level
        return tar_meth(tar_level) & pre_meth(pre_level)


class Status(Condition):
    """`Indicator` on, on or at, under, under at `Level`.

    Focus on only the current bar. Marks True if the Indicator
    is at the given position.
    """
    __slots__ = ("_op",
                 )

    def __init__(self, indicator: Union[str, ],
                 direction: str,
                 level: Union[int, float, str],
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
                >>> j_ge_0 = Status(indicator='J', direction='ge', level=0)
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
                >>> foo[foo.pipe(Status('J', 'gt' ,0))]
                2  1
                >>> foo[foo.pipe(Status('J', 'le', 0))]
                0 -1
                1  0
                >>> foo[foo.pipe(Action('J','lt', 0))]
                0 -1
                """
        super(Status, self).__init__(indicator, direction, level)
        dir_repr_map = {'lt': '<',
                        'le': '<=',
                        'gt': '>',
                        'ge': '>=',
                        'eq': '==',
                        'ne': '!=',
                        }
        self._direction = dir_repr_map[direction]
        self._op = f'__{direction}__'

    def __call__(self, df: dF):
        # get the current bar
        if isinstance(self._indicator, str):
            target = df[self._indicator]
        elif isinstance(self._indicator, Count):
            target = df.pipe(self._indicator)
        else:
            target = None
        # and the bound the method
        tar_meth = getattr(target, self._op)
        # compare to the level
        if isinstance(self._level, str):
            level = df[self._level]
        elif isinstance(self._level, (float, Interval)):
            level = self._level
        elif isinstance(self._level, Count):
            level = df.pipe(self._level)
        else:
            level = None
        return tar_meth(level)


_dir_map = {'sc': 'cross_up',  # 上穿
            'sp': 'touch_up',  # 上碰
            'xc': 'cross_down',  # 下穿
            'xp': 'touch_down',  # 下碰
            }


class _AnyTime(Condition):
    _bool = True
    _collections = None
    __slots__ = ("_level",
                 "_direction",
                 )

    def __new__(cls: type):
        if cls._collections is None:
            cls._collections = object.__new__(cls)
        return cls._collections

    def __init__(self):
        self._level = ""
        self._indicator = repr(self)
        self._direction = ""
        object.__init__(self)

    def __repr__(self):
        return self.__class__.__name__.replace('_', '')

    def __call__(self, df: dF) -> Series:
        return Series((self._bool,) * df.shape[0])

    def __bool__(self):
        return False


class _NoTime(_AnyTime):
    _collections = None
    _bool = False
    __slots__ = ()


AnyTime = _AnyTime()
NoTime = _NoTime()


# TODO shortcuts.
class Alias:
    def __new__(cls, con):
        if not isinstance(con, str):
            raise TypeError('Please input a string for Alias')
        cmp_map = {'<': 'lt',
                   '<=': 'le',
                   '>': 'gt',
                   '>=': 'ge',
                   }
        try:
            ind, opr, lev = con.split(' ')
        except ValueError as e:
            raise ValueError(f"Too much or less space! got {con}") from e
        print(cmp_map, _dir_map, ind, )


class _Set:
    """Container of `Conditions`.


    """
    _collections = {}
    _algorithm = ""
    __slots__ = ('_hash',  # key in `_collection`.
                 '_init',  # args for __init__
                 '_set',   # data
                 )

    def __new__(cls: type, *args, **kw):
        # `_Set`s have at least two members.
        # Else return empty set `AnyTime` or `NoTime`,
        # or the only member directly.
        if not args:
            if issubclass(cls, All):
                return AnyTime
            elif issubclass(cls, Any):
                return NoTime
            raise ValueError(f"{cls.__name__} cannot be empty.")
        # Only `Condition`s or `_Set`s can put together here.
        # `Count` can not be here.
        if not all(isinstance(arg, (Condition, All, Any, Not)) for arg in args):
            raise TypeError("Got something strange. Only `Condition` can be here")
        # Peel the cover like `All(All(blah blah))`.
        # Notice that `_Set`(`Condition`) returns a `Condition` instead of `_Set`.
        if len(args) == 1:
            if isinstance(args[0], Not):
                if issubclass(cls, Not):
                    return args[0].pop()
                else:
                    return args[0]
            elif not issubclass(cls, (Not, Count)):
                return args[0]
        elif issubclass(cls, Not):
            raise ValueError("Too much for `Not`.")
        # It's easy to handle if only `Condition`s in `args`.
        args = tuple(set(args))
        if len(args) == 1 and isinstance(args[0], Condition):
            if not issubclass(cls, (Not, Count)):
                return args[0]
        if all(isinstance(arg, (Condition, Not)) for arg in args):
            not_ins, con_ins = _get_all_ins(args, Not)
            for n in not_ins:
                if n.copy().pop() in con_ins:
                    if issubclass(cls, Any):
                        return AnyTime
                    elif issubclass(cls, All):
                        return NoTime

            # Generate a key witch would be used later.
            # Notice that order in args makes no difference.
            key = (cls.__name__,) + tuple(sorted(args))
        else:
            # While `_Set`s appear, it depends.
            # Multiple `_Set`s of the same type should combine.
            # Also, do all `Condition`s.
            # To begin with, divide Sets from Conditions.
            set_ins, con_ins = _get_all_ins(args, (All, Any))
            # Then take `All`s and `Any`s apart.
            and_ins, or_ins = _get_all_ins(set_ins, All)
            # Remove duplicates
            and_set = _unique(and_ins)
            or_set = _unique(or_ins)
            # Combine `Condition`s to witch type we are generating.
            # Remove `foo` in `Any` from `All(All(foo, bar), Any(_foo_, baz))`
            # Remove `foo` in `All` from `Any(All(_foo_, bar), Any(foo, baz))`
            # Which means when generation `All`, anything in inner `All`
            # should be removed from inner `Any`, vice versa.
            if issubclass(cls, All):
                # Combine.
                and_set.update(con_ins)
                # Remove union.
                or_set.difference_update(and_set & or_set)
                # If `or_set` has only one element now, combine to `and_set`.
                if len(or_set) == 1:
                    and_set.add(or_set.pop())
            elif issubclass(cls, Any):
                # Combine.
                or_set.update(con_ins)
                # Remove union.
                and_set.difference_update(and_set & or_set)
                # Vice versa
                if len(and_set) == 1:
                    or_set.add(and_set.pop())
            # Now we've got simplified `args`.
            args = (All(*and_set), Any(*or_set))
            # If either `All` or `Any` absents, simply return the other.
            if not args[0]:
                return args[1]
            if not args[1]:
                return args[0]
            # If we come here, it seems clear that `args` consists of `All` and `Any`,
            # but there's still chance that `args` is still hashable.
            # It happens when both `and_set` and `or_set` len 1.
            if all(isinstance(arg, Condition) for arg in args):
                if args[0] is args[1]:
                    return args[0]
                key = (cls.__name__, min(args), max(args))
            # Both `All` and `Any` are now in `args`.
            # We use a hashable key.
            # Again notice that `set`s have no order, so we sorted it.
            else:
                key = f'{cls.__name__} {"; ".join(sorted(map(str, args)))}'

        if key not in cls._collections:
            self = super().__new__(cls)
            cls._collections[key] = self
            self._hash = key
            # Store args for initialization here.
            self._init = args
        return cls._collections[key]

    def __init__(self, *args, **kw) -> None:
        if hasattr(self, '_init'):
            self._set = set(self._init)
            del self._init

    def __call__(self, df: dF) -> Series:
        func = _operators_conductor(self._algorithm)
        if len(self) > 1:
            return reduce(func, iter(self))(df)
        return func(self)(df)

    def __len__(self):
        return len(self._set)

    def __iter__(self):
        return iter(self._set)

    def __hash__(self):
        return hash(self._hash)

    def __repr__(self):
        sep = ',\n\t'
        return f'\n{self.__class__.__name__}({sep.join(map(str, self._set))})\n'

    def __and__(self, other):
        return All(self, other)

    def __or__(self, other):
        return Any(self, other)

    def __invert__(self):
        return Not(self)

    def copy(self):
        return self._set.copy()

    def pop(self):
        return self._set.pop()


class All(_Set):
    __slots__ = ()
    _collections = {}
    _algorithm = "__and__"


class Any(_Set):
    __slots__ = ()
    _collections = {}
    _algorithm = "__or__"


class Not(_Set):
    __slots__ = ()
    _collections = {}
    _algorithm = "__invert__"


def _count_comp(comp_name):
    def func(self, other):
        return Status(self, comp_name, other)
    return func


def _count_action(action):
    def func(self, other):
        return Action(self, _dir_map[action], other)
    return func


def _count_add_sub(operator):
    def func(self, other):
        return IndicatorAddSub(self, operator, other)
    return func


class Count(_Set):
    _algorithm = "add"
    __slots__ = ()

    def __call__(self, df: dF) -> Series:
        func = _operators_conductor(self._algorithm, '')
        if len(self) > 1:
            return reduce(func, self)(df)
        return func(self.copy().pop())(df)

    __gt__ = _count_comp("gt")
    __ge__ = _count_comp("ge")
    __lt__ = _count_comp("lt")
    __le__ = _count_comp("le")
    sc = _count_action('sc')
    xc = _count_action('xc')
    sp = _count_action('sp')
    xp = _count_action('xp')
    __add__ = _count_add_sub("add")
    __sub__ = _count_add_sub("sub")
    eq = _count_comp("eq")
    __ne__ = _count_comp("ne")
    __invert__ = None


class Indicator(Count):
    _collections = {}
    __slots__ = ('_indicator',
                 '_shift'
                 )

    def __new__(cls, *args, **kwargs):
        key = args[0], kwargs.get("shift", 0)
        if key not in cls._collections:
            self = object.__new__(cls)
            cls._collections[key] = self
            self._indicator = key[0]
            self._shift = key[1]
        return cls._collections[key]

    def __str__(self):
        return self.ind_shf

    __repr__ = __str__

    def __call__(self, df: dF) -> Series:
        return df[self._indicator].shift(self._shift)

    def __hash__(self):
        return hash(str(self))

    @property
    def ind(self):
        return self._indicator

    @property
    def shf(self):
        return self._shift

    @property
    def ind_shf(self):
        if self.shf:
            return f"{self.ind}.shift({self.shf})"
        return self.ind

    def shift(self, shf):
        return Indicator(self.ind, shift=shf+self.shf)


class IndicatorAddSub(Indicator):
    def __init__(self, self_, operator, other):
        self._self = self_
        if operator not in ('add', 'sub'):
            raise ValueError(f'Only `add` or `sub` is legal for `operator`, got {operator}')
        self._operator = operator
        if isinstance(other, Indicator):
            self._type = "I"
            self._other = other
        elif isinstance(other, (int, float)):
            self._type = "N"
            self._other = float(other)
        else:
            self._other = other
            self._type = "O"
        while False:
            super().__init__()

    def __str__(self):
        value = (self._other.ind_shf if self._type == "I"
                 else self._other if self._type == "N"
                 else self._other)
        operator = "+" if self._operator == "add" else "-"
        return f"{{ {self._self} {operator} {value} }}"

    __repr__ = __str__

    def __hash__(self):
        return hash(self._self) ^ hash(self._operator) ^ hash(self._other)

    def __call__(self, df: dF) -> Series:
        if self._type == "I":
            return getattr(df.pipe(self._self), self._operator)(df.pipe(self._other))
        else:
            return getattr(df.pipe(self._self), self._operator)(self._other)


class Interval(_Iv):
    _comparable = (int, float)

    def __lt__(self, other):
        if isinstance(other, self._comparable):
            if self.closed_right:
                return other > self.right
            else:
                return other >= self.right
        return super(Interval, self).__lt__(other)

    def __le__(self, other):
        if isinstance(other, self._comparable):
            return other >= self.right
        return super(Interval, self).__le__(other)

    def __gt__(self, other):
        if isinstance(other, self._comparable):
            if self.closed_left:
                return other < self.left
            else:
                return other <= self.left
        return super(Interval, self).__gt__(other)

    def __ge__(self, other):
        if isinstance(other, self._comparable):
            return other <= self.left
        return super(Interval, self).__ge__(other)
