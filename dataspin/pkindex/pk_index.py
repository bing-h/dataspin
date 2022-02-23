import os
from basepy.log import logger
import json

from dataspin.utils.file import DataFileReader


class PKIndexCache:

    def __init__(self,storage,pk_keys) -> None:
        self._cache = set()
        self._storage = storage
        self._pk_keys = pk_keys
        
    def update(self):
        pass

    def load(self):
        for filepath in self._storage.fetch_files():
            reader = DataFileReader(filepath)
            for data,line in reader.readlines():
                pk = []
                for pk_key in self._pk_keys:
                    pk.append(data[pk_key])
                self._cache.add(''.join(pk))

    def expire(self):
        pass

    def get(self,**kargs):
        pass

    

