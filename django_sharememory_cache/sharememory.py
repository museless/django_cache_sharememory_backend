#-*- coding:utf8 -*-

""" 
Intro: Multi process safe share memory based cache backend 
Author: Muse

"""

import os
import time
import mmap
import struct

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

try:
    from django.utils.six.moves import cPickle as pickle
except ImportError:
    import pickle


class SharememCache(BaseCache):
    mmap_file_maxsize = 4 * 1024 * 1024 * 1024
    byte_order = "little"
    size_length = 8

    def __init__(self, name, params):
        BaseCache.__init__(self, params)
        self._make_mmap(name)

    def add(self, key, value, timeout = DEFAULT_TIMEOUT, version = None):
        key = self.make_key(key, version = version)
        self.validate_key(key)

        pickled = pickle.dumps(value, pickle.HIGHEST_PROTOCOL)

        if self._validate_size(pickled):
            if self._has_expired(key):
                self._set(key, pickled)

        return  False

    def set(self, key, value, timeout = DEFAULT_TIMEOUT, version = None):
        return  self.add(key, value, timeout = timeout, version = version)

    def get(self, key, default = None, version = None, acquire_lock = True):
        key = self.make_key(key, version = version)
        self.validate_key(key)

        if not self._has_expired(key):
            pickled = _caches.get(key, None)

            if pickled:
                return  pickle.loads(pickled)

        return  default

    def _has_expired(self, key):
        expire = _expire_info.get(key, None)

        if not expire or expire > int(time.time()):
            return  True

        return  False

    def _validate_size(self, pickled):
        if len(pickled) > self.mmap_file_maxsize:
            return  False

        return  True

    def _make_mmap(self, name):
        need_initialize = False

        if not os.path.exists(name):
            need_initialize = True

        self._file = open(name, "a+b")
        fd = self._file.fileno()

        st = os.stat(fd)

        if st.st_size < self.mmap_file_maxsize:
            self._file.truncate(self.mmap_file_maxsize)

        self._mmap = mmap.mmap(fd, mmap_file_maxsize)

        if need_initialize:
            self._write_mmap({"cache": {}, "expire": {}})

    def _read_mmap(self):
        size = struct.unpack("<q", self._mmap[: self.size_length])
        pickled = self._mmap[self.size_length: size]

        return  pickle.loads(pickled)

    def _write_mmap(self, data):
        self._mmap.seek(0)
        pickled = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
        datalen = len(pickled)

        self._mmap.write(datalen.to_byte(self.size_length, self.byte_order))
        self._mmap.write(pickled)

