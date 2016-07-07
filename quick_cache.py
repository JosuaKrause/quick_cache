#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2016-04-15

@author: Joschi <josua.krause@gmail.com>

This package provides a quick to use caching mechanism that is thread-safe.
However, it cannot protect cache files from being modified by other processes.
"""
from __future__ import division

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
    def __init__(self, base_file=None, quota=None, temp="tmp", warnings=None):
        """Creates a new cache. It is recommended to only use one cache object
           for all cache operations of a program.

        Parameters
        ----------
        base_file : filename (optional; default=None)
            The data source file the caching is based on or None for a general cache.

        quota : size (optional; default=None)
            The maximum cache size in MB. Longest untouched files are removed first.
            Files larger than quota are not written. Quota is base file specific.

        temp : folder (optional; default="tmp")
            The folder to store the cache files.

        warnings : function (optional; default=None)
            Used to print warnings. Arguments are formatting string and then
            *args and **kwargs.
        """
        self._own = threading.RLock()
        self._locks = {}
        self._temp = temp
        self._quota = None if quota is None else float(quota)
        if base_file is not None:
            with open(base_file, 'rb') as f:
                base = hashlib.sha1(f.read()).hexdigest()
            self._full_base = os.path.join(self._temp, base)
        else:
            self._full_base = self._temp
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
                    self._locks[cache_file] = _CacheLock(cache_file, cache_id_obj, self._full_base, self._quota, self._warnings)
            finally:
                self._own.release()
        return self._locks[cache_file]

class _CacheLock(object):
    def __init__(self, cache_file, cache_id_obj, base, quota, warnings):
        """Creates a handle for the given cache file."""
        self._cache_file = cache_file
        self._cache_id_obj = cache_id_obj
        self._lock = threading.RLock()
        self._base = base
        self._quota = quota
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
                    self._warnings("reading cache takes longer than computing! {0}: {1} < {2}", desc, elapsed_time, current_time)
            return res

    def write(self, obj):
        """Writes the given object to the cache file as pickle. The cache file with
           its path is created if needed.
        """
        out = cPickle.dumps((self._cache_id_obj, (time.time() - self._start_time) if self._start_time is not None else None, obj), -1)
        own_size = len(out) / 1024.0 / 1024.0
        if self._quota is not None and own_size > self._quota:
            if self._warnings is not None:
                self._warnings("single file exceeds quota: {0}MB > {1}MB", own_size, self._quota)
            return obj # file exceeds quota

        def get_size(start_path):
            total_size = 0
            for dirpath, _dirnames, filenames in os.walk(start_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    total_size += os.path.getsize(fp)
            return total_size / 1024.0 / 1024.0

        while self._quota is not None and get_size(self._base) + own_size > self._quota:
            oldest_fp = None
            oldest_time = None
            for dirpath, _dirnames, filenames in os.walk(self._base):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    cur_time = os.path.getatime(fp)
                    if oldest_time is None or cur_time < oldest_time:
                        oldest_time = cur_time
                        oldest_fp = fp
            if oldest_fp is None:
                if self._warnings is not None:
                    self._warnings("cannot free enough space for quota ({0}MB > {1}MB)!", get_size(self._base) + own_size, self._quota)
                return # cannot free enough space
            if self._warnings is not None:
                self._warnings("removing old cache file: '{0}'", oldest_fp)
            os.remove(oldest_fp)

        if not os.path.exists(os.path.dirname(self._cache_file)):
            os.makedirs(os.path.dirname(self._cache_file))
        try:
            with open(self._cache_file, 'wb') as f_out:
                f_out.write(out)
        except:
            # better remove everything written if an exception
            # occurs during I/O -- we don't want partial files
            if os.path.exists(self._cache_file):
                os.remove(self._cache_file)
            raise
        return obj

    def __enter__(self):
        while not self._lock.acquire(True):
            pass
        self._start_time = time.time()
        return self

    def __exit__(self, _type, _value, _traceback):
        self._lock.release()
        self._start_time = None
        return False
