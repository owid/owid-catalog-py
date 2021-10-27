#
#  datasets.py
#

from os.path import join, isdir, exists
from os import mkdir
from dataclasses import dataclass
import shutil
from typing import Any, Iterator, List, Literal, Optional, Union
from glob import glob
import hashlib
from pathlib import Path
import json

import pandas as pd

from . import tables
from .properties import metadata_property
from .meta import DatasetMeta, TableMeta


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
    def create_empty(
        cls, path: str, metadata: Optional["DatasetMeta"] = None
    ) -> "Dataset":
        if isdir(path):
            if not exists(join(path, "index.json")):
                raise Exception(f"refuse to overwrite non-dataset dir at: {path}")
            shutil.rmtree(path)

        mkdir(path)

        index_file = join(path, "index.json")
        DatasetMeta().save(index_file)

        return Dataset(path)

    def add(
        self, table: tables.Table, format: Literal["csv", "feather"] = "feather"
    ) -> None:
        """Add this table to the dataset by saving it in the dataset's folder. Defaults to
        feather format but you can override this to csv by passing 'csv' for the format"""

        # copy dataset metadata to the table
        table.metadata.dataset = self.metadata

        allowed_formats = ["feather", "csv"]
        if format not in allowed_formats:
            raise Exception(f"Format '{format}'' is not supported")
        table_filename = join(self.path, table.metadata.checked_name + f".{format}")
        if format == "feather":
            table.to_feather(table_filename)
        else:
            table.to_csv(table_filename)

    def __getitem__(self, name: str) -> tables.Table:
        table_filename = join(self.path, name + ".feather")
        if exists(table_filename):
            return tables.Table.read_feather(table_filename)
        table_filename = join(self.path, name + ".csv")
        if exists(table_filename):
            return tables.Table.read_csv(table_filename)
        raise KeyError(name)

    def __contains__(self, name: str) -> bool:
        feather_table_filename = join(self.path, name + ".feather")
        csv_table_filename = join(self.path, name + ".csv")
        return exists(feather_table_filename) or exists(csv_table_filename)

    def save(self) -> None:
        self.metadata.save(self._index_file)
        self._update_table_metadata()

    def _update_table_metadata(self) -> None:
        "Update the copy of this dataset's metadata in every table in the set."
        dataset_meta = self.metadata.to_dict()

        for metadata_file in glob(join(self.path, "*.meta.json")):
            with open(metadata_file) as istream:
                table_meta = json.load(istream)

            table_meta["dataset"] = dataset_meta

            with open(metadata_file, "w") as ostream:
                json.dump(table_meta, ostream, indent=2)

    def index(self, catalog_path: Path) -> pd.DataFrame:
        """
        Return a DataFrame describing the contents of this dataset, one row per table.
        """
        base = {
            "namespace": self.metadata.namespace,
            "dataset": self.metadata.short_name,
            "version": self.metadata.version,
            "checksum": self.checksum(),
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
            row["path"] = table_path.relative_to(catalog_path).as_posix()

            if table_path.with_suffix(".feather").exists():
                row["format"] = "feather"
            elif table_path.with_suffix(".csv").exists():
                row["format"] = "csv"

            rows.append(row)

        return pd.DataFrame.from_records(rows)

    @property
    def _index_file(self) -> str:
        return join(self.path, "index.json")

    def __len__(self) -> int:
        return len(self._data_files)

    def __iter__(self) -> Iterator[tables.Table]:
        for filename in self._data_files:
            if filename.endswith(".feather"):
                yield tables.Table.read_feather(filename)

            elif filename.endswith(".csv"):
                yield tables.Table.read_csv(filename)

            else:
                raise Exception(f"don't know how to read table: {filename}")

    @property
    def _data_files(self) -> List[str]:
        feather_pattern = join(self.path, "*.feather")
        csv_pattern = join(self.path, "*.csv")
        return sorted(glob(feather_pattern) + glob(csv_pattern))

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


for k in DatasetMeta.__dataclass_fields__:  # type: ignore
    if hasattr(Dataset, k):
        raise Exception(f'metadata field "{k}" would overwrite a Dataset built-in')

    setattr(Dataset, k, metadata_property(k))


def checksum_file(filename: str) -> Any:
    "Return the MD5 checksum of a given file."
    chunk_size = 2 ** 20  # 1MB
    checksum = hashlib.md5()
    with open(filename, "rb") as istream:
        chunk = istream.read(chunk_size)
        while chunk:
            checksum.update(chunk)
            chunk = istream.read(chunk_size)

    return checksum
