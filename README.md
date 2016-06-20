# quick_cache
A quick and easy to use python caching system.

Create the cache object as follows:
```python
def msg(message, *args, **kwargs):
    print(message.format(*args, **kwargs), file=sys.stderr)

cache = QuickCache(base_file, quota=500, warnings=msg)
```
where `base_file` is an optional file whose *content* invalidates
the cache (ie., when the content of the file changes the cache is invalidated;
for large files it might be desirable to use the *mtime* in the cache object below)
and `msg` is an optional formatting function that prints warnings
(by default it's `None` which doesn't print anything;
warnings are emitted when the actual computation is faster than
reading the results from the cache or if other exceptional situations occur).
`quota` is an optional maximal cache size in MB.

The caching functionality can then be used via:
```python
with cache.get_hnd({
    # object identifying the task to cache
    # can be any combination of keys and values
    "param_a": 5,
    "input_file_c": os.path.getmtime(input_file_c), # for file change time
    ...
}) as hnd:
    if not hnd.has():
        res = do_compute() # compute your result here
        hnd.write(res)
    else:
        res = hnd.read()
# your result is in res
```
The cache object used for creating the handle uniquely defines
the task. The object should contain all parameters of the task
and the task computation itself should be deterministic.

You can add *quick_cache* to your git project via submodules.
```bash
git submodule add https://github.com/JosuaKrause/quick_cache.git lib/quick_cache/
# when checking out the project later at a different location do
git submodule update --init --recursive
```

and import it in python via:
```python
import sys

sys.path.append('lib')
from quick_cache.quick_cache import QuickCache
```
