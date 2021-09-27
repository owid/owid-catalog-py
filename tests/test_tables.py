#
#  test_tables.py
#

from owid.catalog.variables import Variable
import tempfile
from os.path import join, exists, splitext
import json

import jsonschema
import pytest
import pandas as pd

from owid.catalog.tables import Table, SCHEMA
from owid.catalog.meta import VariableMeta, TableMeta
from .mocking import mock


def test_create():
    t = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    assert list(t.gdp) == [100, 102, 104]
    assert list(t.country) == ["AU", "SE", "CH"]


def test_add_table_metadata():
    t = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})

    # write some metadata
    t.metadata.short_name = "my_table"
    t.metadata.title = "My table indeed"
    t.metadata.description = (
        "## Well...\n\nI discovered this table in the Summer of '63..."
    )

    # metadata persists with slicing
    t2 = t.iloc[:2]
    assert t2.metadata == t.metadata


def test_read_empty_table_metadata():
    t = Table()
    assert t.metadata == TableMeta()


def test_table_schema_is_valid():
    jsonschema.Draft7Validator.check_schema(SCHEMA)


def test_add_field_metadata():
    t = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    title = "GDP per capita in 2011 international $"

    assert t.gdp.metadata == VariableMeta()

    t.gdp.title = title

    # check single field access
    assert t.gdp.title == title

    # check entire metadata access
    assert t.gdp.metadata == VariableMeta(title=title)

    # check field-level metadata persists across slices
    assert t.iloc[:1].gdp.title == title


def test_saving_empty_table_fails():
    t = Table()

    with pytest.raises(Exception):
        t.to_feather("/tmp/example.feather")


def test_round_trip_no_metadata():
    t1 = Table({"gdp": [100, 102, 104], "countries": ["AU", "SE", "CH"]})
    with tempfile.TemporaryDirectory() as path:
        filename = join(path, "table.feather")
        t1.to_feather(filename)

        assert exists(filename)
        assert exists(splitext(filename)[0] + ".meta.json")

        t2 = Table.read_feather(filename)
        assert_tables_eq(t1, t2)


def test_round_trip_with_index():
    t1 = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t1.set_index("country", inplace=True)
    with tempfile.TemporaryDirectory() as path:
        filename = join(path, "table.feather")
        t1.to_feather(filename)

        assert exists(filename)
        assert exists(splitext(filename)[0] + ".meta.json")

        t2 = Table.read_feather(filename)
        assert_tables_eq(t1, t2)


def test_round_trip_with_metadata():
    t1 = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t1.set_index("country", inplace=True)
    t1.title = "A very special table"
    t1.description = "Something something"

    with tempfile.TemporaryDirectory() as path:
        filename = join(path, "table.feather")
        t1.to_feather(filename)

        assert exists(filename)
        assert exists(splitext(filename)[0] + ".meta.json")

        t2 = Table.read_feather(filename)
        assert_tables_eq(t1, t2)


def test_field_metadata_copied_between_tables():
    t1 = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t2 = Table({"hdi": [73, 92, 45], "country": ["AU", "SE", "CH"]})

    t1.gdp.description = "A very important measurement"

    t2["gdp"] = t1.gdp
    assert t2.gdp.metadata == t1.gdp.metadata


def test_field_metadata_serialised():
    t1 = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t1.gdp.description = "Something grand"

    with tempfile.TemporaryDirectory() as dirname:
        filename = join(dirname, "test.feather")
        t1.to_feather(filename)

        t2 = Table.read_feather(filename)
        assert_tables_eq(t1, t2)


def test_tables_from_dataframes_have_variable_columns():
    df = pd.DataFrame({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]})
    t = Table(df)
    assert isinstance(t.gdp, Variable)

    t.gdp.metadata.title = "test"


def test_tables_always_list_fields_in_metadata():
    df = pd.DataFrame(
        {
            "gdp": [100, 102, 104],
            "country": ["AU", "SE", "CH"],
            "french_fries": ["yes", "no", "yes"],
        }
    )
    t = Table(df.set_index("country"))
    with tempfile.TemporaryDirectory() as temp_dir:
        t.to_feather(join(temp_dir, "example.feather"))
        m = json.load(open(join(temp_dir, "example.meta.json")))

    assert m["primary_key"] == ["country"]
    assert m["fields"] == {"country": {}, "gdp": {}, "french_fries": {}}


def assert_tables_eq(lhs: Table, rhs: Table) -> None:
    assert lhs.to_dict() == rhs.to_dict()
    assert lhs.metadata == rhs.metadata
    assert lhs._fields == rhs._fields


def mock_table() -> Table:
    t: Table = Table({"gdp": [100, 102, 104], "country": ["AU", "SE", "CH"]}).set_index(
        "country"
    )  # type: ignore
    t.metadata = mock(TableMeta)
    for col in t.all_columns:
        t._fields[col] = mock(VariableMeta)

    return t
