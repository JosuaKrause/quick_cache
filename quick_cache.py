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
import atexit
import shutil
import hashlib
import threading
import collections

try:
    import cPickle
except ImportError:
    import pickle as cPickle

try:
    unicode = unicode
except NameError:
    # python 3
    str = str
    unicode = str
    bytes = bytes
    basestring = (str, bytes)
else:
    # python 2
    str = str
    unicode = unicode
    bytes = str
    basestring = basestring

if hasattr(time, "monotonic"):

    def _get_monotonic_time():
        return time.monotonic()

    get_time = _get_monotonic_time
else:

    def _get_clock_time():
        return time.clock()

    get_time = _get_clock_time

__version__ = "0.3.0"


def _write_str(id_obj, elapsed, data):
    obj = json.dumps([id_obj, elapsed], sort_keys=True, allow_nan=True)
    return (str(len(obj)) + ';' + obj + data).encode('utf-8')


def _read_str(txt):
    len_ix, rest = txt.decode('utf-8').split(";", 1)
    length = int(len_ix)
    id_obj, elapsed = json.loads(rest[:length])
    return id_obj, elapsed, rest[length:]


CHUNK_SIZE = 1024 * 1024 * 1024
methods = {
    "pickle": (
        lambda id_obj, elapsed, data: cPickle.dumps(
            (id_obj, elapsed, data), -1),
        lambda txt: cPickle.loads(txt),
    ),
    "json": (
        lambda id_obj, elapsed, data: json.dumps(
            [id_obj, elapsed, data], sort_keys=True, allow_nan=True),
        lambda txt: tuple(json.loads(txt)),
    ),
    "string": (_write_str, _read_str),
}


class QuickCache(object):
    def __init__(self, base_file=None, base_string=None, quota=None,
                 ram_quota=0, temp="tmp", warnings=None, method="pickle"):
        """Creates a new cache. It is recommended to only use one cache object
           for all cache operations of a program.

        Parameters
        ----------
        base_file : filename (optional; default=None)
            The data source file the caching is based on or None for a general
            cache. Only one of base_file or base_string can be non-None.

        base_string : string (optional; default=None)
            The string the caching is based on (if the string changes so does
            the cache) or None for a general cache.
            Only one of base_file or base_string can be non-None.

        quota : size (optional; default=None)
            The maximum cache size in MB. Longest untouched files are removed
            first. Files larger than quota are not written. Quota is base
            specific.

        ram_quota : size (optional; default=0)
            The maximum RAM cache size in MB. Longest unused caches are written
            to disk first. Caches larger than the RAM quota are written to disk
            immediately if disk quota allows.

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
        verbose : bool (default=False)
            Whether to log non warning messages.
        """
        self._own = threading.RLock()
        self._method = methods.get(method, None)
        if self._method is None:
            raise ValueError("unknown method: '{0}'".format(method))
        self._locks = {}
        self._temp = temp
        self._quota = None if quota is None else float(quota)
        self._ram_quota = None if ram_quota is None else float(ram_quota)
        if base_file is not None:
            if base_string is not None:
                err_msg = "of base_file and base_string only one " + \
                          "can be non-None: {0} {1}".format(
                              base_file, base_string)
                raise ValueError(err_msg)
            with open(base_file, 'rb') as f:
                base = hashlib.sha1(f.read()).hexdigest()
            self._full_base = os.path.join(self._temp, base)
        elif base_string is not None:
            base = hashlib.sha1(base_string.encode('utf8')).hexdigest()
            self._full_base = os.path.join(self._temp, base)
        else:
            self._full_base = self._temp

        def no_msg(message, *args, **kwargs):
            pass

        self._warnings = warnings if warnings is not None else no_msg
        self.verbose = False
        atexit.register(lambda: self.remove_all_locks())

    def clean_cache(self, section=None):
        """Cleans the cache of this cache object."""
        self.remove_all_locks()
        if section is not None and "/" in section:
            raise ValueError("invalid section '{0}'".format(section))
        if section is not None:
            path = os.path.join(self._full_base, section)
        else:
            path = self._full_base
        if not os.path.exists(path):
            return
        shutil.rmtree(path)

    def list_sections(self):
        """List all sections."""
        if not os.path.exists(self._full_base):
            return []
        return [
            name for name in os.listdir(self._full_base)
            if os.path.isdir(os.path.join(self._full_base, name))
        ]

    def get_file(self, cache_id_obj, section=None):
        """Returns the file path for the given cache object."""
        section = "default" if section is None else section
        if "/" in section:
            raise ValueError("invalid section '{0}'".format(section))
        cache_id = "{:08x}".format(
            zlib.crc32(b"&".join(sorted([
                str(k).encode('utf8') + b"=" + str(v).encode('utf8')
                for k, v in cache_id_obj.items()
            ]))) & 0xffffffff)
        return os.path.join(self._full_base,
                            os.path.join(section,
                                         os.path.join(
                                             "{0}".format(cache_id[:2]),
                                             "{0}.tmp".format(cache_id[2:]))))

    def _remove_lock(self, k):
        try:
            self._locks[k].remove()
            del self._locks[k]
        except KeyError:
            pass

    def enforce_ram_quota(self):
        locks = self._locks.values()
        full_size = sum([l.get_size() for l in locks])
        ram_quota = self._ram_quota
        if full_size > ram_quota:
            locks.sort(key=lambda l: l.get_last_access())
            for l in locks:
                old_size = l.get_size()
                l.force_to_disk()
                new_size = l.get_size()
                full_size -= old_size - new_size
                if full_size <= ram_quota:
                    break

    def remove_all_locks(self):
        """Removes all locks and ensures their content is written to disk."""
        locks = list(self._locks.items())
        locks.sort(key=lambda l: l[1].get_last_access())
        for l in locks:
            self._remove_lock(l[0])

    def get_hnd(self, cache_id_obj, section=None, method=None):
        """Gets a handle for the given cache file with exclusive access.
           The handle is meant to be used in a resource block.

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
        cache_file = self.get_file(cache_id_obj, section)
        if cache_file not in self._locks:
            try:
                while not self._own.acquire(True):
                    pass
                if cache_file not in self._locks:
                    if method is None:
                        m = self._method
                    else:
                        m = methods.get(method, None)
                    if m is None:
                        raise ValueError(
                            "unknown method: '{0}'".format(method))
                    res = _CacheLock(cache_file, cache_id_obj, self._full_base,
                                     self._quota, self._ram_quota,
                                     self._warnings, self.verbose, m)
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
        self.enforce_ram_quota()
        return res


class _CacheLock(object):
    def __init__(self, cache_file, cache_id_obj, base, quota,
                 ram_quota, warnings, verbose, method):
        """Creates a handle for the given cache file."""
        self._cache_file = cache_file
        self._lock = threading.RLock()
        self._base = base
        self._quota = quota
        self._ram_quota = ram_quota
        self._warnings = warnings
        self._start_time = None
        self._last_access = get_time()
        self._write, self._read = method
        self._cache_id_obj = self._get_canonical_id(cache_id_obj)
        self._out = None
        self._done = False
        self.verbose = verbose

    def _get_canonical_id(self, cache_id_obj):
        return self._read(self._write(cache_id_obj, 0, ""))[0]

    def ensure_cache_id(self, cache_id_obj):
        """Ensure the integrity of the cache id object."""
        cache_id = self._get_canonical_id(cache_id_obj)
        if cache_id != self._cache_id_obj:
            raise ValueError(
                "cache mismatch {0} != {1}".format(
                    cache_id, self._cache_id_obj))

    def name(self):
        """The cache file."""
        return self._cache_file

    def is_done(self):
        """Conservatively determine whether this cache is ready and can safely
           be removed from the lock index.
        """
        return self._done or self._out is not None

    def has(self):
        """Whether the cache file exists in the file system."""
        self._done = os.path.exists(self._cache_file)
        return self._done or self._out is not None

    def _cache_id_desc(self):

        def convert(v):
            if isinstance(v, basestring):
                return v
            if isinstance(v, dict):
                return "{..{0}}".format(len(v.keys()))
            if isinstance(v, collections.Iterable):
                return "[..{0}]".format(len(v))
            return str(v)

        return "[{0}]".format(", ".join([
            "{0}={1}".format(k, convert(v))
            for (k, v) in self._cache_id_obj.items()
        ]))

    def read(self):
        """Reads the cache file as pickle file."""

        def warn(msg, elapsed_time, current_time):
            desc = self._cache_id_desc()
            self._warnings(
                "{0} {1}: {2}s < {3}s", msg, desc, elapsed_time, current_time)

        file_time = get_time()
        out = self._out
        if out is None:
            if self.verbose:
                self._warnings("reading {0} from disk", self._cache_id_desc())
            with open(self._cache_file, 'rb') as f_in:
                out = None
                while True:
                    t_out = f_in.read(CHUNK_SIZE)
                    if not len(t_out):
                        break
                    if out is not None:
                        out += t_out
                    else:
                        out = t_out
                self._out = out
        (cache_id_obj, elapsed_time, res) = self._read(out)
        self.ensure_cache_id(cache_id_obj)
        real_time = get_time() - file_time
        if elapsed_time is not None and real_time > elapsed_time:
            warn("reading cache from disk takes longer than computing!",
                 elapsed_time, real_time)
        elif self._start_time is not None and elapsed_time is not None:
            current_time = get_time() - self._start_time
            if elapsed_time < current_time:
                warn("reading cache takes longer than computing!",
                     elapsed_time, current_time)
        self._last_access = get_time()
        return res

    def write(self, obj):
        """Writes the given object to the cache file as pickle. The cache file with
           its path is created if needed.
        """
        if self.verbose:
            self._warnings("cache miss for {0}", self._cache_id_desc())
        if self._start_time is not None:
            elapsed = get_time() - self._start_time
        else:
            elapsed = None
        out = self._write(self._cache_id_obj, elapsed, obj)
        self._out = out
        self.force_to_disk(self.get_size() > self._ram_quota)
        self._last_access = get_time()
        return self._read(out)[2]

    def get_size(self):
        out = self._out
        return len(out) / 1024.0 / 1024.0 if out is not None else 0.0

    def get_last_access(self):
        return self._last_access

    def force_to_disk(self, removeMem=True):
        out = self._out
        if out is None:
            return
        cache_file = self._cache_file
        if os.path.exists(cache_file):
            self._done = True
            if removeMem:
                self._out = None
                if self.verbose:
                    self._warnings("free memory of {0}", self._cache_id_desc())
            return
        own_size = len(out) / 1024.0 / 1024.0
        quota = self._quota
        if quota is not None and own_size > quota:
            self._warnings(
                "single file exceeds quota: {0}MB > {1}MB", own_size, quota)
            return  # file exceeds quota

        def get_size(start_path):
            total_size = 0
            for dirpath, _dirnames, filenames in os.walk(start_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    total_size += os.path.getsize(fp)
            return total_size / 1024.0 / 1024.0

        base = self._base
        while quota is not None and get_size(base) + own_size > quota:
            oldest_fp = None
            oldest_time = None
            for dirpath, _dirnames, filenames in os.walk(base):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    cur_time = os.path.getatime(fp)
                    if oldest_time is None or cur_time < oldest_time:
                        oldest_time = cur_time
                        oldest_fp = fp
            if oldest_fp is None:
                self._warnings(
                    "cannot free enough space for quota ({0}MB > {1}MB)!",
                    get_size(base) + own_size, quota)
                return  # cannot free enough space
            self._warnings("removing old cache file: {0}", oldest_fp)
            os.remove(oldest_fp)

        if not os.path.exists(os.path.dirname(cache_file)):
            os.makedirs(os.path.dirname(cache_file))
        try:
            if self.verbose:
                self._warnings(
                    "writing cache to disk: {0}", self._cache_id_desc())
            with open(cache_file, 'wb') as f_out:
                cur_chunk = 0
                while cur_chunk < len(out):
                    next_chunk = cur_chunk + CHUNK_SIZE
                    f_out.write(out[cur_chunk:next_chunk])
                    cur_chunk = next_chunk
        except:
            # better remove everything written if an exception
            # occurs during I/O -- we don't want partial files
            if os.path.exists(cache_file):
                os.remove(cache_file)
            raise
        self._done = True
        if removeMem:
            self._out = None
            if self.verbose:
                self._warnings("free memory of {0}", self._cache_id_desc())

    def remove(self):
        self.force_to_disk()

    def __enter__(self):
        while not self._lock.acquire(True):
            pass
        self._start_time = get_time()
        return self

    def __exit__(self, _type, _value, _traceback):
        self._lock.release()
        self._start_time = None
        return False
