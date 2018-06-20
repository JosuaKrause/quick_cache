# -*- coding: utf-8 -*-
"""
Created on 2016-09-10

@author: joschi <josua.krause@gmail.com>

This package provides a quick to use caching mechanism that is thread-safe.
However, it cannot protect cache files from being modified by other processes.
"""

from setuptools import setup

from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# NOTE! steps to distribute:
# $ python setup.py sdist bdist_wheel
# $ twine upload dist/... <- here be the new version!

setup(
    name='quick_cache',
    version="0.3.0",
    description='QuickCache is a quick to use and ' +
                'easy to set up cache implementation.',
    long_description=long_description,
    url='https://github.com/JosuaKrause/quick_cache',
    author='Josua Krause',
    author_email='josua.krause@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='cache quick easy',
    py_modules=['quick_cache'],
    install_requires=[],
    extras_require={
        'dev': [],
        'test': [],
    },
    data_files=[],
    entry_points={
        'console_scripts': [
        ],
    },
)
