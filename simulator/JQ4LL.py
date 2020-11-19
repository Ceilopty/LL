# Getting data from "JQDATA".

"""JQ4LL meant for "JQ for LiangLi"

LIST_PICKLE_PATH: relative path of list.pickle.
It's the contracts info of futures by default.

class Jq: object
"""

__author__ = 'Ceilopty'
__all__ = ['Jq', 'AlwaysDump']

import os

# import numpy as np
import pandas as pd
import jqdatasdk as jq

#
LIST_PICKLE_PATH = './data/futures_list.pkl'
# convert to abspath
LIST_PICKLE_PATH = os.path.join(os.path.abspath(os.path.curdir), LIST_PICKLE_PATH)

# Index format
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'


class Jq:

    def __init__(self):
        self._auth()
        self._get_count()
        self.jq = jq
        try:
            self.list = self._read_pickle()
        except FileNotFoundError:
            print('默认路径不存在')
            if input("是否联机获取列表？Y/N").lower() == "y":
                self._get_list()
                self._dump_pickle()
            else:
                self.list = None

    def _get_list(self, types=None):
        self.list = jq.get_all_securities(types or ["futures"])

    def _get_count(self):
        self._count = jq.get_query_count()

    @staticmethod
    def _auth():
        # Sensitive information is saved separately. Load & Delete to keep them safe.
        from _AUTH import AUTH_ID, AUTH_PASSWORD
        # For many reason such as poor network connection, auth session may fail.
        try:
            jq.auth(AUTH_ID, AUTH_PASSWORD) if not jq.is_auth() else None
        except Exception as e:
            print(f'AUTH FAILED {e}')
        del AUTH_ID, AUTH_PASSWORD

    @staticmethod
    def _read_pickle():
        return pd.read_pickle(LIST_PICKLE_PATH)

    def _dump_pickle(self):
        path_dir = os.path.split(LIST_PICKLE_PATH)[0]
        if not os.path.exists(path_dir):
            os.mkdir(path_dir)
        self.list.to_pickle(LIST_PICKLE_PATH)

    @property
    def count(self):
        try:
            self._get_count()
        except Exception as e:
            print(f"Can't get count {e}")
        else:
            return self._count

    def get_bars(self,
                 security,          # Code. Can be `str` or `tuple` of `str`s.
                 count=1,             # PosInt. Number of bars. Make no sense if too large.
                 unit='15m',        # Time period of a bar. Can be one of '1/5/15/30/60/120m', '1d/w/M' &etc.
                 fields=('date',    # Fields to got.
                         'open',
                         'high',
                         'low',
                         'close',
                         'volume',  # Deal Volume
                         'money',   # Deal Money
                         'open_interest',  # Unclosed Amount
                         'factor'   # FQ factor
                         ),
                 include_now=False,  # Whether including `last` to `now` as an incomplete bar or not.
                 end_dt=None,        # `datetime.datetime` or `None`. `datetime.now()` by default.
                 fq_ref_date=None,   # Reference date for fq.
                 df=True,            # Whether return `dataframe` or not.
                 ):
        if isinstance(security, tuple):
            return {sec: self.get_bars(sec) for sec in security}
        if hasattr(self, security):
            return object.__getattribute__(self, security)
        if os.path.exists(f'./data/securities/{security}.pkl'):
            object.__setattr__(self, security, pd.read_pickle(f'./data/securities/{security}.pkl'))
            return object.__getattribute__(self, security)
        if self.confirm_get(f'bars of {security}'):
            res = jq.get_bars(security, count, unit, fields, include_now, end_dt, fq_ref_date, df)
            object.__setattr__(self, security, res)
            return res
        print('Got Nothing')

    @staticmethod
    def confirm_get(msg=''):
        return input(f'Get {msg} online? (Y/N):').lower() == 'y'


class AlwaysDump:
    def __init__(self, data=None):
        if data is not None:
            from datetime import datetime
            from pickle import dump
            with open(f'./data/dump{datetime.now()}.pkl'.replace(':', '_'), 'wb') as f:
                dump(data, f)

    def __call__(self, func):
        from functools import wraps
        from datetime import datetime
        from pickle import dump
        res = []

        @wraps(func)
        def wrapper(*args, **kwargs):
            res.append(func(*args, **kwargs))
            with open(f'./data/dump{datetime.now()}.pkl'.replace(':', '_'), 'wb') as f:
                dump(res[0], f)
            return res[0]

        return wrapper


# Make sure path is exist. If not, mkdir for upper level.
def exist_path(path):
    path = os.path.abspath(path)
    if os.path.exists(path):
        return path
    upper, curr = os.path.split(path)
    if os.path.exists(upper):
        os.mkdir(path)
    return os.path.join(exist_path(upper), curr)


# jq.get_bars('P9999.XDCE',1,'15m',('date', 'open', 'close', 'high', 'low', 'volume', 'money','open_interest'))
