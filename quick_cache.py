#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2016-04-15

@author: Joschi <josua.krause@gmail.com>

This package provides a quick to use caching mechanism that is thread-safe.
However, it cannot protect cache files from being modified by other processes.
"""

import os
import zlib
import pickle
import shutil
import hashlib
import threading

class QuickCache(object):
    def __init__(self, base_file, temp="tmp"):
        """Creates a new cache. It is recommended to only use one cache object
           for all cache operations of a program.

        Parameters
        ----------
        base_file : filename
            The data source file the caching is based on.

        temp : folder (optional; default="tmp")
            The folder to store the cache files.
        """
        self._own = threading.RLock()
        self._locks = {}
        with open(base_file, 'rb') as f:
            self._base = hashlib.sha1(f.read()).hexdigest()
        self._temp = temp
        self._full_base = os.path.join(self._temp, self._base)

    def clean_cache(self):
        if not os.path.exists(self._full_base):
            return
        shutil.rmtree(self._full_base)

    def clean_all_caches(self):
        if not os.path.exists(self._temp):
            return
        shutil.rmtree(self._temp)

    def get_file(self, cache_id_obj):
        cache_id = "{:08x}".format(zlib.crc32("&".join(sorted([str(k) + "=" + str(v) for k, v in cache_id_obj.iteritems()]))) & 0xffffffff)
        return os.path.join(self._full_base, os.path.join("{0}".format(cache_id[:2]), "{0}.tmp".format(cache_id[2:])))

    def get_hnd(self, cache_id_obj):
        """Gets a handle for the given cache file with exclusive access. The handle
           is meant to be used in a resource block.
        """
        cache_file = self.get_file(cache_id_obj)
        if cache_file not in self._locks:
            try:
                while not self._own.acquire(True):
                    pass
                if cache_file not in self._locks:
                    self._locks[cache_file] = _CacheLock(cache_file, cache_id_obj)
            finally:
                self._own.release()
        return self._locks[cache_file]

class _CacheLock(object):
    def __init__(self, cache_file, cache_id_obj):
        """Creates a handle for the given cache file."""
        self._cache_file = cache_file
        self._cache_id_obj = cache_id_obj
        self._lock = threading.RLock()

    def name(self):
        """The cache file."""
        return self._cache_file

    def has(self):
        """Whether the cache file exists in the file system."""
        return os.path.exists(self._cache_file)

    def read(self):
        """Reads the cache file as pickle file."""
        with open(self._cache_file, 'rb') as f_in:
            (cache_id_obj, res) = pickle.load(f_in)
            if cache_id_obj != self._cache_id_obj:
                raise ValueError("cache mismatch")
            return res

    def write(self, obj):
        """Writes the given object to the cache file as pickle. The cache file with
             its path is created if needed.
        """
        if not os.path.exists(os.path.dirname(self._cache_file)):
            os.makedirs(os.path.dirname(self._cache_file))
        with open(self._cache_file, 'w') as f_out:
            pickle.dump((self._cache_id_obj, obj), f_out)

    def __enter__(self):
        while not self._lock.acquire(True):
            pass
        return self

    def __exit__(self, _type, _value, _traceback):
        self._lock.release()
        return False
