#
#  test_meta.py
#

from typing import Optional, Dict, Any
from dataclasses import dataclass

from dataclasses_json import dataclass_json

from owid.catalog import meta


def test_dict_mixin():
    @meta.pruned_json
    @dataclass_json
    @dataclass
    class Dog:
        name: Optional[str] = None
        age: Optional[int] = None

        def to_dict(self) -> Dict[str, Any]:
            ...

    assert Dog(name="fred").to_dict() == {"name": "fred"}
    assert Dog(age=10).to_dict() == {"age": 10}


def test_empty_dataset_metadata():
    d1 = meta.DatasetMeta()
    assert d1.to_dict() == {"is_public": True}


def test_dataset_version():
    s1 = meta.Source(name="s1", publication_date="2022-01-01")
    s2 = meta.Source(name="s2", publication_date="2022-01-02")

    assert meta.DatasetMeta(version="1").version == "1"
    assert meta.DatasetMeta(sources=[s1]).version == "2022-01-01"
    assert meta.DatasetMeta(sources=[s1, s2]).version is None
    assert meta.DatasetMeta(version="1", sources=[s1]).version == "1"


def test_to_json():
    meta.Source(name="s1", publication_date="2022-01-01").to_json()  # type: ignore
