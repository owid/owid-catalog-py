#
#  datasets.py
#

from os.path import join, isdir, exists
from os import mkdir
from dataclasses import dataclass
import shutil
from typing import Iterator, List, Literal, Optional
from glob import glob

from . import tables
from .properties import metadata_property
from .meta import DatasetMeta


@dataclass
class Dataset:
    """
    A dataset is a folder full of data tables, with metadata available at `index.json`.
    """

    path: str
    metadata: "DatasetMeta"

    def __init__(self, path: str) -> None:
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

    @property
    def _index_file(self) -> str:
        return join(self.path, "index.json")

    def save(self) -> None:
        self.metadata.save(self._index_file)

    def __len__(self) -> int:
        return len(self._data_files)

    def __iter__(self) -> Iterator[tables.Table]:
        for filename in self._data_files:
            yield tables.Table.read_feather(filename)

    @property
    def _data_files(self) -> List[str]:
        feather_pattern = join(self.path, "*.feather")
        csv_pattern = join(self.path, "*.csv")
        return glob(feather_pattern) + glob(csv_pattern)


for k in DatasetMeta.__dataclass_fields__:  # type: ignore
    if hasattr(Dataset, k):
        raise Exception(f'metadata field "{k}" would overwrite a Dataset built-in')

    setattr(Dataset, k, metadata_property(k))