#
#  datasets.py
#

import hashlib
import json
import shutil
import warnings
from dataclasses import dataclass
from glob import glob
from os import mkdir
from os.path import join
from pathlib import Path
from typing import Any, Iterator, List, Literal, Optional, Union

import pandas as pd
import yaml

from . import tables, utils
from .meta import DatasetMeta, TableMeta
from .properties import metadata_property

FileFormat = Literal["csv", "feather", "parquet"]

# the formats we can serialise and deserialise; in some cases they
# will be tried in this order if we don't specify one explicitly
SUPPORTED_FORMATS: List[FileFormat] = ["feather", "parquet", "csv"]

# the formats we generate by default
DEFAULT_FORMATS: List[FileFormat] = ["feather", "parquet"]

# the format we use by default if we only need one
PREFERRED_FORMAT: FileFormat = "feather"

# sanity checks
assert set(DEFAULT_FORMATS).issubset(SUPPORTED_FORMATS)
assert PREFERRED_FORMAT in DEFAULT_FORMATS
assert SUPPORTED_FORMATS[0] == PREFERRED_FORMAT


@dataclass
class Dataset:
    """
    A dataset is a folder full of data tables, with metadata available at `index.json`.
    """

    path: str
    metadata: "DatasetMeta"

    def __init__(self, path: Union[str, Path]) -> None:
        # for convenience, accept Path objects directly
        if isinstance(path, Path):
            self.path = path.as_posix()
        else:
            self.path = path

        self.metadata = DatasetMeta.load(self._index_file)

    @classmethod
    def create_empty(cls, path: Union[str, Path], metadata: Optional["DatasetMeta"] = None) -> "Dataset":
        path = Path(path)

        if path.is_dir():
            if not (path / "index.json").exists():
                raise Exception(f"refuse to overwrite non-dataset dir at: {path}")
            shutil.rmtree(path)

        mkdir(path)

        metadata = metadata or DatasetMeta()

        index_file = path / "index.json"
        metadata.save(index_file)

        return Dataset(path.as_posix())

    def add(
        self,
        table: tables.Table,
        formats: List[FileFormat] = DEFAULT_FORMATS,
        repack: bool = True,
    ) -> None:
        """
        Add this table to the dataset by saving it in the dataset's folder. By default we
        save in multiple formats, but if you need a specific one (e.g. CSV for explorers)
        you can specify it.

        :param repack: if True, try to cast column types to the smallest possible type (e.g. float64 -> float32)
            to reduce binary file size. Consider using False when your dataframe is large and the repack is failing.
        """

        utils.validate_underscore(table.metadata.short_name, "Table's short_name")
        for col in list(table.columns) + list(table.index.names):
            utils.validate_underscore(col, "Variable's name")

        # copy dataset metadata to the table
        table.metadata.dataset = self.metadata

        for format in formats:
            if format not in SUPPORTED_FORMATS:
                raise Exception(f"Format '{format}'' is not supported")

            table_filename = join(self.path, table.metadata.checked_name + f".{format}")
            table.to(table_filename, repack=repack)

    def __getitem__(self, name: str) -> tables.Table:
        stem = self.path / Path(name)

        for format in SUPPORTED_FORMATS:
            path = stem.with_suffix(f".{format}")
            if path.exists():
                return tables.Table.read(path)

        raise KeyError(f"Table `{name}` not found, available tables: {', '.join(self.table_names)}")

    def __contains__(self, name: str) -> bool:
        return any((Path(self.path) / name).with_suffix(f".{format}").exists() for format in SUPPORTED_FORMATS)

    def save(self) -> None:
        assert self.metadata.short_name, "Missing dataset short_name"
        utils.validate_underscore(self.metadata.short_name, "Dataset's short_name")

        if not self.metadata.namespace:
            warnings.warn(f"Dataset {self.metadata.short_name} is missing namespace")

        self.metadata.save(self._index_file)

        # Update the copy of this datasets metadata in every table in the set.
        for table_name in self.table_names:
            table = self[table_name]
            table.metadata.dataset = self.metadata
            table._save_metadata(join(self.path, table.metadata.checked_name + f".meta.json"))

    def update_metadata(self, metadata_path: Path) -> None:
        self.metadata.update_from_yaml(metadata_path, if_source_exists="replace")

        with open(metadata_path) as istream:
            metadata = yaml.safe_load(istream)
            for table_name in metadata["tables"].keys():
                table = self[table_name]
                table.update_metadata_from_yaml(metadata_path, table_name)
                table._save_metadata(join(self.path, table.metadata.checked_name + f".meta.json"))

    def index(self, catalog_path: Path = Path("/")) -> pd.DataFrame:
        """
        Return a DataFrame describing the contents of this dataset, one row per table.
        """
        base = {
            "namespace": self.metadata.namespace,
            "dataset": self.metadata.short_name,
            "version": self.metadata.version,
            "checksum": self.checksum(),
            "is_public": self.metadata.is_public,
        }
        rows = []
        for metadata_file in self._metadata_files:
            with open(metadata_file) as istream:
                metadata = TableMeta.from_dict(json.load(istream))

            row = base.copy()

            assert metadata.short_name
            row["table"] = metadata.short_name

            row["dimensions"] = json.dumps(metadata.primary_key)

            table_path = Path(self.path) / metadata.short_name
            relative_path = table_path.relative_to(catalog_path)
            row["path"] = relative_path.as_posix()
            row["channel"] = relative_path.parts[0]

            row["formats"] = [f for f in SUPPORTED_FORMATS if table_path.with_suffix(f".{f}").exists()]  # type: ignore

            rows.append(row)

        return pd.DataFrame.from_records(rows)

    @property
    def _index_file(self) -> str:
        return join(self.path, "index.json")

    def __len__(self) -> int:
        return len(self.table_names)

    def __iter__(self) -> Iterator[tables.Table]:
        for name in self.table_names:
            yield self[name]

    @property
    def _data_files(self) -> List[str]:
        files = []
        for format in SUPPORTED_FORMATS:
            pattern = join(self.path, f"*.{format}")
            files.extend(glob(pattern))

        return sorted(files)

    @property
    def table_names(self) -> List[str]:
        return sorted(set(Path(f).stem for f in self._data_files))

    @property
    def _metadata_files(self) -> List[str]:
        return sorted(glob(join(self.path, "*.meta.json")))

    def checksum(self) -> str:
        "Return a MD5 checksum of all data and metadata in the dataset."
        _hash = hashlib.md5()
        _hash.update(checksum_file(self._index_file).digest())

        for data_file in self._data_files:
            _hash.update(checksum_file(data_file).digest())

            metadata_file = Path(data_file).with_suffix(".meta.json").as_posix()
            _hash.update(checksum_file(metadata_file).digest())

        return _hash.hexdigest()


for k in DatasetMeta.__dataclass_fields__:
    if hasattr(Dataset, k):
        raise Exception(f'metadata field "{k}" would overwrite a Dataset built-in')

    setattr(Dataset, k, metadata_property(k))


def checksum_file(filename: str) -> Any:
    "Return the MD5 checksum of a given file."
    chunk_size = 2**20  # 1MB
    checksum = hashlib.md5()
    with open(filename, "rb") as istream:
        chunk = istream.read(chunk_size)
        while chunk:
            checksum.update(chunk)
            chunk = istream.read(chunk_size)

    return checksum
