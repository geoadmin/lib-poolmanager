# -*- coding: utf-8 -*-

import sys
from nose import with_setup
from nose.tools import raises
from poolmanager import PoolManager

PY3 = sys.version_info[0] == 3


if PY3:
    xrange = range


def add(x):
    return x + 1


def error_func(x):
    if x == 10:
        d = []
        d.pop()
    return x


def setup_func_no_store():
    global pm
    pm = PoolManager(numProcs=2)
    pm.imap_unordered(add, xrange(0, 100), 2)


def setup_func_store():
    global pm_store
    pm_store = PoolManager(numProcs=2, store=True)
    pm_store.imap_unordered(add, xrange(0, 100), 2)


@raises(Exception)
def setup_func_error():
    global pm_error
    pm_error = PoolManager(numProcs=2, factor=2, store=True)
    pm_error.imap_unordered(add, 'notvalid', 2)


@raises(Exception)
def setup_func_error_within_subprocess():
    global pm_error_sub
    pm_error_sub = PoolManager(numProcs=2, store=True)
    pm_error_sub.imap_unordered(error_func, xrange(0, 100), 2)


def setup_func_with_callback():
    global pm_c
    global callback

    def callback(counter, res):
        print(counter)
        print(res)
    pm_c = PoolManager(numProcs=2, store=True)
    pm_c.imap_unordered(add, xrange(0, 100), 2, callback=callback)


@with_setup(setup_func_no_store)
def test_no_store():
    assert len(pm.results) == 0
    assert pm.nbOfProcesses == 2
    assert pm.nbOfProcessesAlive == 0


@with_setup(setup_func_store)
def test_store():
    expected_result = [add(r) for r in xrange(0, 100)]
    assert len(pm_store.results) == 100
    assert len(set(expected_result).difference(set(pm_store.results))) == 0
    assert pm_store.nbOfProcesses == 2
    assert pm_store.nbOfProcessesAlive == 0


@with_setup(setup_func_error)
def test_invalid_iterable():
    assert len(pm_error.results) == 0
    assert pm_error.nbOfProcesses == 4
    assert pm_error.nbOfProcessesAlive == 0


@with_setup(setup_func_error_within_subprocess)
def test_invalid_function():
    # We can't predict the result here
    # We excpect to raise an exception and stop all subprocesses
    assert len(pm_error_sub.results) >= 0
    assert pm_error_sub.nbOfProcesses == 2
    assert pm_error_sub.nbOfProcessesAlive == 0


@with_setup(setup_func_with_callback)
def test_func_with_callback():
    assert len(pm_c.results) == 100
    assert pm_c.nbOfProcesses == 2
    assert pm_c.nbOfProcessesAlive == 0
