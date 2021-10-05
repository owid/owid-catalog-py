#
#  test_catalogs.py
#

from typing import Optional

from owid.catalog.catalogs import RemoteCatalog
from owid.catalog.tables import Table


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
