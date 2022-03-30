#
#  meta.py
#
#  Metadata helpers.
#

from pathlib import Path
from typing import Optional, TypeVar, Dict, Any, List, Union
from dataclasses import dataclass, field
import json
import yaml

from dataclasses_json import dataclass_json

T = TypeVar("T")


def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    cls.to_dict = lambda self: {  # type: ignore
        k: v
        for k, v in orig(self).items()
        if not k.startswith("_") and v not in [None, [], {}]
    }

    return cls


@pruned_json
@dataclass_json
@dataclass
class Source:
    """Notes on importing sources to grapher:
    - Field `source.description` gets mapped to `Internal notes`, but we rather use it for `additional_info`
    - The most important fields are `published_by`, `publisher_source` and `additional_info`
    - In admin for dataset (i.e. /admin/datasets/1234) only the first source of a dataset is shown and
        can be edited. The other ones are not visible.
    """

    name: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    source_data_url: Optional[str] = None
    owid_data_url: Optional[str] = None
    date_accessed: Optional[str] = None
    publication_date: Optional[str] = None
    publication_year: Optional[int] = None
    # specific fields for grapher
    # NOTE: it's not clear how to map description & name to fields in grapher, so
    # we're keeping both for the time being. We might consolidate them in the future
    published_by: Optional[str] = None
    publisher_source: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        ...


@pruned_json
@dataclass_json
@dataclass
class License:
    name: Optional[str]
    url: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        ...


@pruned_json
@dataclass_json
@dataclass
class VariableMeta:
    title: Optional[str] = None
    description: Optional[str] = None
    sources: List[Source] = field(default_factory=list)
    licenses: List[Source] = field(default_factory=list)
    unit: Optional[str] = None
    short_unit: Optional[str] = None
    display: Optional[Dict[str, Any]] = None
    additional_info: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "VariableMeta":
        ...


@pruned_json
@dataclass_json
@dataclass
class DatasetMeta:
    """
    The metadata for this entire dataset kept in JSON (e.g. mydataset/index.json).

    The number of fields is limited, but should handle everything that we get from
    Walden. There is a lot more opportunity to store more metadata at the table and
    the variable level.
    """

    namespace: Optional[str] = None
    # NOTE: short_name should be underscore and validate in setter, however this
    # is nontrivial to do with `dataclass_json` (see https://github.com/lidatong/dataclasses-json/issues/176)
    short_name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    sources: List[Source] = field(default_factory=list)
    licenses: List[License] = field(default_factory=list)
    is_public: bool = True
    additional_info: Optional[Dict[str, Any]] = None

    # an md5 checksum of the ingredients used to make this dataset
    source_checksum: Optional[str] = None

    def save(self, filename: Union[str, Path]) -> None:
        filename = Path(filename).as_posix()
        with open(filename, "w") as ostream:
            json.dump(self.to_dict(), ostream, indent=2)

    @classmethod
    def load(cls, filename: str) -> "DatasetMeta":
        with open(filename) as istream:
            return cls.from_dict(json.load(istream))

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DatasetMeta":
        ...

    @property
    def version(self) -> Optional[str]:
        if len(self.sources) == 1:
            (source,) = self.sources
            if source.publication_date:
                return source.publication_date

            return str(source.publication_year)

        return None

    def update_from_yaml(self, path: Union[Path, str]) -> None:
        """The main reason for wanting to do this is to manually override what goes into Grapher before an export."""
        with open(path) as istream:
            annot = yaml.safe_load(istream)

        # update sources of dataset
        for source_annot in annot["dataset"].get("sources", []):
            # if there's an existing source, update it
            ds_sources = [s for s in self.sources if s.name == source_annot["name"]]
            if ds_sources:
                for k, v in source_annot.items():
                    setattr(ds_sources[0], k, v)
            # otherwise create new source
            else:
                self.sources.append(Source(**source_annot))

        # update dataset
        for k, v in annot["dataset"].items():
            if k != "sources":
                setattr(self, k, v)


@pruned_json
@dataclass_json
@dataclass
class TableMeta:
    # data about this table
    short_name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None

    # a reference back to the dataset
    dataset: Optional[DatasetMeta] = field(compare=False, default=None)
    primary_key: List[str] = field(default_factory=list)

    @property
    def checked_name(self) -> str:
        if not self.short_name:
            raise Exception("table has no short_name")

        return self.short_name

    @staticmethod
    def from_dict(dict: Dict[str, Any]) -> "TableMeta":
        ...
