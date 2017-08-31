quick\_cache
============

A quick and easy to use python caching system.

You can install *quick\_cache* via

.. code:: bash

    pip install --user quick_cache

and import it in python using:

.. code:: python

    from quick_cache import QuickCache

Create the cache object as follows:

.. code:: python

    def msg(message, *args, **kwargs):
        print(message.format(*args, **kwargs), file=sys.stderr)

    cache = QuickCache(base_file, quota=500, ram_quota=100, warnings=msg)

where ``base_file`` is an optional file whose *content* invalidates the
cache (ie., when the content of the file changes the cache is
invalidated; for large files it might be desirable to use the *mtime* in
the cache object below) and ``msg`` is an optional formatting function
that prints warnings (by default it's ``None`` which doesn't print
anything; warnings are emitted when the actual computation is faster
than reading the results from the cache or if other exceptional
situations occur). ``quota`` and ``ram_quota`` are optional maximal
cache sizes, both in RAM and on disk, in MB.

The caching functionality can then be used via:

.. code:: python

    with cache.get_hnd({
        # object identifying the task to cache
        # can be any combination of keys and values
        "param_a": 5,
        "input_file_c": os.path.getmtime(input_file_c), # for file change time
        ...
    }) as hnd:
        if not hnd.has():
            res = hnd.write(do_compute()) # compute your result here
        else:
            res = hnd.read()
    # your result is in res

The cache object used for creating the handle uniquely defines the task.
The object should contain all parameters of the task and the task
computation itself should be deterministic.
