# 模块注释

"""文档字符串

文档字符串
"""

__author__ = 'Ceilopty'
__all__ = ['Jq', 'AlwaysDump']


import os

# import numpy as np
import pandas as pd
import jqdatasdk as jq

from _AUTH import AUTH_ID, AUTH_PASSWORD


LIST_PICKLE_PATH = './data/futures_list.pkl'
# convert to abspath
LIST_PICKLE_PATH = os.path.join(os.path.abspath(os.path.curdir), LIST_PICKLE_PATH)


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
        jq.auth(AUTH_ID, AUTH_PASSWORD) if not jq.is_auth() else None

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
        return self._count


class AlwaysDump:
    def __init__(self, data=None):
        if data:
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
