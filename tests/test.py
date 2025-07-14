# -*- coding: UTF-8 -*-
# Author: Neal Raines
# Create Date: 7/1/2025
# Description: Merkle_2 test file

import pytest
import os

import pandas as pd
import pyodbc as odbc

import Merkle_2.exempt_pcat
from Merkle_2.queries import duplicate_upc
from Merkle_2.utils import upc_collapse, alt_modulus, format_df

class SimpleCache:
    def __init__(self):
        self.store = {}

    def set(self, key, value):
        self.store[key] = value

    def get(self, key):
        return self.store.get(key, None)

@pytest.fixture
def cache():
    # setup
    test_cache = SimpleCache()
    yield test_cache
    # teardown
    test_cache.store.clear()

def test_cache_set_and_get(cache):
    cache.set("test_key", "test_value")
    assert cache.get("test_key") == "test_value", "Get value from cashe"

def test_cache_miss_returns_none(cache):
    assert cache.get("nonexistent_key") is None, "Return none on missed key"
