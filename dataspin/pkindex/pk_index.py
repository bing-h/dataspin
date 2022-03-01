import os
import time


from dataspin.utils.file import DataFileReader


class PKIndexCache:

    def __init__(self, expire_time: int, pk_keys: list) -> None:
        """
        when current_time - baseline_time larger than expire_time,cache use precache and precache refresh a new set
        """
        self._cache = set()
        self._precache = set()
        self._pk_keys = pk_keys
        self._expire_time = expire_time
        self._baseline_time = int(time.time())

    def update_pk_file(self, file_path):
        self._expire()
        reader = DataFileReader(file_path)
        for data, line in reader.readlines():
            self._update_cache_value(data)

    def _update_cache_value(self, data):
        pk_value = []
        for pk_key in self._pk_keys:
            pk_value.append(data[pk_key])
        pk_value = tuple(pk_value)
        self._cache.add(pk_value)
        self._precache.add(pk_value)

    def update_expire_time(self, expire_time):
        self._expire_time = expire_time

    def load(self, file_lists):
        for filepath in file_lists:
            self.update_pk_file(filepath)

    def _expire(self):
        current_time = int(time.time())
        if (current_time - self._baseline_time) >= self._expire_time:
            self._cache = self._precache
            self._precache = set()
            self._baseline_time = current_time

    def is_exists(self, pk_entrys: dict):
        """
        pk_entrys {"app_id":"","event_id":""}
        """
        self._expire()
        pk_value = []
        for k in self._pk_keys:
            pk_value.append(pk_entrys[k])
        pk_value = tuple(pk_value)
        if pk_value not in self._cache:
            self._update_cache_value(pk_entrys)
            return False
        return True
