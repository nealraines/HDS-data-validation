# -*- coding: UTF-8 -*-
# Author: Neal Raines
# Create Date: 7/1/2025
# Description: Example test file

import nose

def setup_func():
    print("set up test fixtures")

def teardown_func():
    print("tear down test fixtures")

@nose.with_setup(setup_func, teardown_func)
def test():
    assert False

def test_evens():
    for i in range(0, 5):
        yield check_even, i, i*3

def check_even(n, nn):
    assert n % 2 == 0 or nn % 2 == 0