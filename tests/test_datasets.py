#
#  test_datasets.py
#

import tempfile
from os.path import join, exists
from os import rmdir
import json
import shutil
from contextlib import contextmanager
from typing import Iterator, Union
import random
from glob import glob
from pathlib import Path

import pytest

from owid.catalog import Dataset, DatasetMeta
from .test_tables import mock_table
from .mocking import mock


def test_dataset_fails_to_load_empty_folder():
    with temp_dataset_dir(create=True) as dirname:
        with pytest.raises(Exception):
            Dataset(dirname)


def test_create_empty():
    with temp_dataset_dir(create=True) as dirname:
        shutil.rmtree(dirname)

        Dataset.create_empty(dirname)

        assert exists(join(dirname, "index.json"))
        with open(join(dirname, "index.json")) as istream:
            doc = json.load(istream)
        assert doc == {"is_public": True}


def test_create_fails_if_non_dataset_dir_exists():
    with temp_dataset_dir(create=True) as dirname:
        with pytest.raises(Exception):
            Dataset.create_empty(dirname)


def test_create_overwrites_entire_folder():
    with temp_dataset_dir(create=True) as dirname:
        with open(join(dirname, "index.json"), "w") as ostream:
            ostream.write('{"clam": "chowder"}')

        with open(join(dirname, "hallo-thar.txt"), "w") as ostream:
            ostream.write("Hello")

    d = Dataset.create_empty(dirname)

    # this should have been deleted
    assert not exists(join(dirname, "hallo-thar.txt"))

    assert open(d._index_file).read().strip() == '{\n  "is_public": true\n}'


def test_add_table():
    t = mock_table()

    with temp_dataset_dir() as dirname:
        # make a dataset
        ds = Dataset.create_empty(dirname)
        ds.metadata = DatasetMeta(short_name="bob")

        # add the table, it should be on disk now
        ds.add(t)
        assert t.metadata.dataset == ds.metadata

        # check that it's really on disk
        table_files = [
            join(dirname, t.metadata.checked_name + ".feather"),
            join(dirname, t.metadata.checked_name + ".meta.json"),
        ]
        for filename in table_files:
            assert exists(filename)

        # load a fresh copy from disk
        t2 = ds[t.metadata.checked_name]
        assert id(t2) != id(t)

        # the fresh copy from disk should be identical to the copy we added
        assert t2.metadata.primary_key == t.metadata.primary_key
        assert t2.equals_table(t)
        assert t2.metadata.dataset == ds.metadata


def test_add_table_csv():
    t = mock_table()

    with temp_dataset_dir() as dirname:
        # make a dataset
        ds = Dataset.create_empty(dirname)

        # add the table, it should be on disk now
        ds.add(t, format="csv")

        # check that it's really on disk
        table_files = [
            join(dirname, t.metadata.checked_name + ".csv"),
            join(dirname, t.metadata.checked_name + ".meta.json"),
        ]
        for filename in table_files:
            assert exists(filename)

        # load a fresh copy from disk
        t2 = ds[t.metadata.checked_name]
        assert id(t2) != id(t)

        # the fresh copy from disk should be identical to the copy we added
        assert t2.equals_table(t)


def test_metadata_roundtrip():
    with temp_dataset_dir() as dirname:
        d = Dataset.create_empty(dirname)
        d.metadata = mock(DatasetMeta)
        d.save()

        d2 = Dataset(dirname)
        assert d2.metadata == d.metadata


def test_dataset_size():
    with mock_dataset() as d:
        n_expected = len(glob(join(d.path, "*.feather")))
        assert len(d) == n_expected


def test_dataset_iteration():
    with mock_dataset() as d:
        i = 0
        for table in d:
            i += 1
        assert i == len(d)


def test_dataset_hash_changes_with_data_changes():
    with mock_dataset() as d:
        c1 = d.checksum()

        t = mock_table()
        d.add(t)
        c2 = d.checksum()

        assert c1 != c2


def test_dataset_hash_invariant_to_copying():
    # make a mock dataset
    with mock_dataset() as d1:

        # make a copy of it
        with temp_dataset_dir() as dirname:
            d2 = Dataset.create_empty(dirname)
            d2.metadata = d1.metadata
            d2.save()

            for t in d1:
                d2.add(t)

            # the copy should have the same checksum
            assert d2.checksum() == d1.checksum()


def test_snake_case_dataset():
    with mock_dataset() as d:
        # short_name of a dataset must be snake_case
        d.metadata.short_name = "camelCase"
        with pytest.raises(NameError):
            d.save()


def test_snake_case_table():
    with mock_dataset() as d:
        # short_name of a table must be snake_case
        t = mock_table()
        t.metadata.short_name = "camelCase"
        with pytest.raises(NameError):
            d.add(t)

        # short_name of a dataset must be snake_case
        t = mock_table()
        t["camelCase"] = 1
        with pytest.raises(NameError):
            d.add(t)

        # short_name of columns and index names must be snake_case
        t = mock_table()
        t.index.names = ["Country"]
        with pytest.raises(NameError):
            d.add(t)


@contextmanager
def temp_dataset_dir(create: bool = False) -> Iterator[str]:
    with tempfile.TemporaryDirectory() as dirname:
        if not create:
            rmdir(dirname)
        yield dirname


def create_temp_dataset(dirname: Union[Path, str]) -> Dataset:
    d = Dataset.create_empty(dirname)
    d.metadata = mock(DatasetMeta)
    d.metadata.short_name = Path(dirname).name
    d.metadata.is_public = True
    d.save()
    for _ in range(random.randint(2, 5)):
        t = mock_table()
        d.add(t)
    return d


@contextmanager
def mock_dataset() -> Iterator[Dataset]:
    with temp_dataset_dir() as dirname:
        d = create_temp_dataset(dirname)
        yield d
