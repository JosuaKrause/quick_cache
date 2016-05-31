#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2016-04-15

@author: Joschi <josua.krause@gmail.com>

This package provides a quick to use caching mechanism that is thread-safe.
However, it cannot protect cache files from being modified by other processes.
"""

import os
import sys
import json
import time
import zlib
import cPickle
import shutil
import hashlib
import threading
import collections

class QuickCache(object):
    def __init__(self, base_file, temp="tmp", warnings=None):
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
        self._warnings = warnings

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
                    self._locks[cache_file] = _CacheLock(cache_file, cache_id_obj, self._warnings)
            finally:
                self._own.release()
        return self._locks[cache_file]

class _CacheLock(object):
    def __init__(self, cache_file, cache_id_obj, warnings):
        """Creates a handle for the given cache file."""
        self._cache_file = cache_file
        self._cache_id_obj = cache_id_obj
        self._lock = threading.RLock()
        self._warnings = warnings
        self._start_time = None

    def name(self):
        """The cache file."""
        return self._cache_file

    def has(self):
        """Whether the cache file exists in the file system."""
        return os.path.exists(self._cache_file)

    def read(self):
        """Reads the cache file as pickle file."""
        with open(self._cache_file, 'rb') as f_in:
            (cache_id_obj, elapsed_time, res) = cPickle.load(f_in)
            if cache_id_obj != self._cache_id_obj:
                raise ValueError("cache mismatch")
            if self._start_time is not None and elapsed_time is not None:
                current_time = time.time() - self._start_time
                if self._warnings is not None and elapsed_time < current_time:

                    def convert(v):
                        if isinstance(v, basestring):
                            return v
                        if isinstance(v, dict):
                            return "{..{0}}".format(len(v.keys()))
                        if isinstance(v, collections.Iterable):
                            return "[..{0}]".format(len(v))
                        return str(v)

                    desc = "[{0}]".format(", ".join([ "{0}={1}".format(k, convert(v)) for (k, v) in self._cache_id_obj.items() ]))
                    self._warnings("reading cache takes longer than computing! {0}: {1} < {2}".format(desc, elapsed_time, current_time))
            return res

    def write(self, obj):
        """Writes the given object to the cache file as pickle. The cache file with
           its path is created if needed.
        """
        if not os.path.exists(os.path.dirname(self._cache_file)):
            os.makedirs(os.path.dirname(self._cache_file))
        with open(self._cache_file, 'wb') as f_out:
            cPickle.dump((self._cache_id_obj, (time.time() - self._start_time) if self._start_time is not None else None, obj), f_out, -1)

    def __enter__(self):
        while not self._lock.acquire(True):
            pass
        self._start_time = time.time()
        return self

    def __exit__(self, _type, _value, _traceback):
        self._lock.release()
        self._start_time = None
        return False
