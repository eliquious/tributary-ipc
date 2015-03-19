#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
import ipc

with open('README.md', 'r') as f:
    long_description = f.read()

with open('requirements.txt', 'r') as f:
    install_requires = list(f)

with open('test-requirements.txt', 'r') as f:
    test_requires = [x for x in list(f) if x[0:2] != '-r']

setup(
    name='tributary_ipc',
    description=ipc.__doc__,
    long_description=long_description,
    maintainer='eliquious',
    maintainer_email='eliquious@gmail.com',
    packages=['ipc'],
    # data_files=[('share/doc/tributary', ['README.rst'])],
    entry_points={
        'tributary.ext': '.ipc = ipc'
    },
    setup_requires=['vcversioner'],
    vcversioner={
        'version_module_paths': ['ipc/_version.py'],
        'vcs_args': ['git', '--git-dir', '%(root)s/.git', 'describe',
                  '--tags', '--long'],
    },
    install_requires=install_requires,
    tests_require=test_requires,
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Natural Language :: English',
    ],
)
