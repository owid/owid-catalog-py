#
#  test_catalogs.py
#

import tempfile
from typing import Optional, Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest  # noqa

from owid.catalog import RemoteCatalog, LocalCatalog, Table

from .test_datasets import create_temp_dataset


_catalog: Optional[RemoteCatalog] = None


def load_catalog() -> RemoteCatalog:
    global _catalog

    if _catalog is None:
        _catalog = RemoteCatalog()

    return _catalog


def test_remote_catalog_loads():
    load_catalog()


def test_remote_find_returns_all():
    c = load_catalog()
    assert len(c.find()) == len(c.frame)


def test_remote_find_one():
    c = load_catalog()
    t = c.find_one("population", dataset="key_indicators", namespace="owid")
    assert isinstance(t, Table)


def test_find_from_local_catalog():
    with mock_catalog(3) as catalog:
        matches = catalog.find()
        assert len(matches.dataset.unique()) == 3


def test_load_from_local_catalog():
    with mock_catalog(1) as catalog:
        catalog.find().iloc[0].load()


@contextmanager
def mock_catalog(n: int = 3) -> Iterator[LocalCatalog]:
    with tempfile.TemporaryDirectory() as dirname:
        path = Path(dirname)
        for i in range(n):
            create_temp_dataset(path / f"dataset{i}")
        yield LocalCatalog(path)
