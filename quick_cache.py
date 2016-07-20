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

if hasattr(time, "process_time"):
    get_time = lambda: time.process_time()
else:
    get_time = lambda: time.clock()

def _write_str(id_obj, elapsed, data):
    obj = json.dumps([ id_obj, elapsed ], sort_keys=True, allow_nan=True)
    return str(len(obj)) + ';' + obj + data

def _read_str(f_id):
    txt = f_id.read()
    len_ix, rest = txt.split(";", 1)
    length = int(len_ix)
    id_obj, elapsed = json.loads(rest[:length])
    return id_obj, elapsed, rest[length:]

methods = {
    "pickle": (
        lambda id_obj, elapsed, data: cPickle.dumps((id_obj, elapsed, data), -1),
        lambda f_id: cPickle.load(f_id),
    ),
    "json": (
        lambda id_obj, elapsed, data: json.dumps([ id_obj, elapsed, data ], sort_keys=True, allow_nan=True),
        lambda f_id: tuple(json.load(f_id)),
    ),
    "string": (_write_str, _read_str),
}
class QuickCache(object):
    def __init__(self, base_file=None, base_string=None, quota=None, temp="tmp", warnings=None, method="pickle"):
        """Creates a new cache. It is recommended to only use one cache object
           for all cache operations of a program.

        Parameters
        ----------
        base_file : filename (optional; default=None)
            The data source file the caching is based on or None for a general cache.
            Only one of base_file or base_string can be non-None.

        base_string : string (optional; default=None)
            The string the caching is based on (if the string changes so does the
            cache) or None for a general cache.
            Only one of base_file or base_string can be non-None.

        quota : size (optional; default=None)
            The maximum cache size in MB. Longest untouched files are removed first.
            Files larger than quota are not written. Quota is base specific.

        temp : folder (optional; default="tmp")
            The folder to store the cache files.

        warnings : function (optional; default=None)
            Used to print warnings. Arguments are formatting string and then
            *args and **kwargs.

        method : string (optional; default="pickle")
            The method to read/write data to the disk. The stored content must
            be convertible without loss. Available methods are:
                "pickle",
                "json",
                "string",

        Attributes
        ----------
        lock_index_size : size
            Target size of the lock index. The size can not be guaranteed. Default
            value is 100.
        """
        self._own = threading.RLock()
        self._method = methods.get(method, None)
        if self._method is None:
            raise ValueError("unknown method: '{0}'".format(method))
        self._locks = {}
        self._temp = temp
        self._quota = None if quota is None else float(quota)
        if base_file is not None:
            if base_string is not None:
                raise ValueError("of base_file and base_string only one can be non-None: {0} {1}".format(base_file, base_string))
            with open(base_file, 'rb') as f:
                base = hashlib.sha1(f.read()).hexdigest()
            self._full_base = os.path.join(self._temp, base)
        elif base_string is not None:
            base = hashlib.sha1(base_string).hexdigest()
            self._full_base = os.path.join(self._temp, base)
        else:
            self._full_base = self._temp
        self._warnings = warnings
        self.lock_index_size = 100

    def clean_cache(self):
        """Cleans the cache of this cache object."""
        if not os.path.exists(self._full_base):
            return
        shutil.rmtree(self._full_base)

    def clean_all_caches(self):
        """Clean all caches in the `temp` path."""
        if not os.path.exists(self._temp):
            return
        shutil.rmtree(self._temp)

    def get_file(self, cache_id_obj):
        """Returns the file path for the given cache object."""
        cache_id = "{:08x}".format(zlib.crc32("&".join(sorted([str(k) + "=" + str(v) for k, v in cache_id_obj.iteritems()]))) & 0xffffffff)
        return os.path.join(self._full_base, os.path.join("{0}".format(cache_id[:2]), "{0}.tmp".format(cache_id[2:])))

    def _remove_lock(self, k):
        try:
            del self._locks[k]
        except KeyError:
            pass

    def try_enforce_index_size(self, save):
        """Tries to remove finished locks from the index."""
        if len(self._locks) > self.lock_index_size:
            for (k, v) in self._locks.items():
                if save != k and v.is_done():
                    self._remove_lock(k)


    def get_hnd(self, cache_id_obj, method=None):
        """Gets a handle for the given cache file with exclusive access. The handle
           is meant to be used in a resource block.

        Parameters
        ----------
        cache_id_obj : object
            An object uniquely identifying the cached resource. Note that some
            methods require the cache id object to be json serializable. The
            string representation of each element, however, has to reflect its
            content in a lossless way.

        method : string (optional; default=None)
            Defines the method used to encode the cached content. If None the
            default method of this cache is used. The method must be consistent
            between multiple accesses of the same cache resource.
        """
        cache_file = self.get_file(cache_id_obj)
        if cache_file not in self._locks:
            try:
                while not self._own.acquire(True):
                    pass
                if cache_file not in self._locks:
                    m = self._method if method is None else methods.get(method, None)
                    if m is None:
                        raise ValueError("unknown method: '{0}'".format(method))
                    res = _CacheLock(cache_file, cache_id_obj, self._full_base, self._quota, self._warnings, m)
                    self._locks[cache_file] = res
                else:
                    res = None
            finally:
                self._own.release()
        else:
            res = None
        if res is None:
            res = self._locks[cache_file]
            res.ensure_cache_id(cache_id_obj)
        self.try_enforce_index_size(cache_file)
        return res

class _CacheLock(object):
    def __init__(self, cache_file, cache_id_obj, base, quota, warnings, method):
        """Creates a handle for the given cache file."""
        self._cache_file = cache_file
        self._cache_id_obj = cache_id_obj
        self._lock = threading.RLock()
        self._base = base
        self._quota = quota
        self._warnings = warnings
        self._start_time = None
        self._write, self._read = method
        self._done = False

    def ensure_cache_id(self, cache_id_obj):
        """Ensure the integrity of the cache id object."""
        if cache_id_obj != self._cache_id_obj:
            raise ValueError("cache mismatch")

    def name(self):
        """The cache file."""
        return self._cache_file

    def is_done(self):
        """Conservatively determine whether this cache is ready and can safely be
           removed from the lock index.
        """
        return self._done

    def has(self):
        """Whether the cache file exists in the file system."""
        self._done = os.path.exists(self._cache_file)
        return self._done

    def read(self):
        """Reads the cache file as pickle file."""

        def convert(v):
            if isinstance(v, basestring):
                return v
            if isinstance(v, dict):
                return "{..{0}}".format(len(v.keys()))
            if isinstance(v, collections.Iterable):
                return "[..{0}]".format(len(v))
            return str(v)

        def warn(msg, elapsed_time, current_time):
            desc = "[{0}]".format(", ".join([ "{0}={1}".format(k, convert(v)) for (k, v) in self._cache_id_obj.items() ]))
            self._warnings("{0} {1}: {2}s < {3}s", msg, desc, elapsed_time, current_time)

        file_time = get_time()
        with open(self._cache_file, 'rb') as f_in:
            (cache_id_obj, elapsed_time, res) = self._read(f_in)
            self.ensure_cache_id(cache_id_obj)
            real_time = get_time() - file_time
            if self._warnings is not None and elapsed_time is not None and real_time > elapsed_time:
                warn("reading cache from disk takes longer than computing!", elapsed_time, real_time)
            elif self._start_time is not None and elapsed_time is not None:
                current_time = get_time() - self._start_time
                if self._warnings is not None and elapsed_time < current_time:
                    warn("reading cache takes longer than computing!", elapsed_time, current_time)
            return res

    def write(self, obj):
        """Writes the given object to the cache file as pickle. The cache file with
           its path is created if needed.
        """
        out = self._write(self._cache_id_obj, (get_time() - self._start_time) if self._start_time is not None else None, obj)
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
                return obj # cannot free enough space
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
        self._done = True
        return obj

    def __enter__(self):
        while not self._lock.acquire(True):
            pass
        self._start_time = get_time()
        return self

    def __exit__(self, _type, _value, _traceback):
        self._lock.release()
        self._start_time = None
        return False
