#
#  catalog.py
#  owid-catalog-py
#

from pathlib import Path
from typing import Optional, Iterator, Union, Any
import json

import pandas as pd
import numpy as np

from .datasets import Dataset
from .tables import Table

OWID_CATALOG_URI = "https://owid-catalog.nyc3.digitaloceanspaces.com/"
REMOTE_CATALOG: Optional["RemoteCatalog"] = None


class Catalog:
    frame: "CatalogFrame"

    def find(
        self,
        table: Optional[str] = None,
        namespace: Optional[str] = None,
        dataset: Optional[str] = None,
    ) -> "CatalogFrame":
        criteria = np.ones(len(self.frame), dtype=bool)

        if table:
            criteria &= self.frame.table.apply(lambda t: table in t)

        if namespace:
            criteria &= self.frame.namespace == namespace

        if dataset:
            criteria &= self.frame.dataset == dataset

        return self.frame[criteria].drop(columns=["checksum"])  # type: ignore

    def find_one(self, *args: Optional[str], **kwargs: Optional[str]) -> Table:
        return self.find(*args, **kwargs).load()


class LocalCatalog(Catalog):
    path: Path
    frame: "CatalogFrame"

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        if self._catalog_file.exists():
            self.frame = CatalogFrame(pd.read_feather(self._catalog_file.as_posix()))
        else:
            self.frame = CatalogFrame.create_empty()

    @property
    def _catalog_file(self) -> Path:
        return self.path / "catalog.feather"

    def iter_datasets(self) -> Iterator[Dataset]:
        to_search = [self.path]
        while to_search:
            dir = to_search.pop()
            if (dir / "index.json").exists():
                yield Dataset(dir)
                continue

            for child in dir.iterdir():
                if child.is_dir():
                    to_search.append(child)

    def reindex(self) -> None:
        # walk the directory tree, generate a namespace/version/dataset/table frame
        # save it to feather
        rows = []
        for ds in self.iter_datasets():
            base = {
                "namespace": ds.metadata.namespace,
                "dataset": ds.metadata.short_name,
                "version": ds.metadata.version,
                "checksum": ds.checksum(),
            }
            for table in ds:
                row = base.copy()
                assert table.metadata.short_name
                row["table"] = table.metadata.short_name

                row["dimensions"] = json.dumps(table.primary_key)

                table_path = Path(ds.path) / table.metadata.short_name
                row["path"] = table_path.relative_to(self.path).as_posix()

                if table_path.with_suffix(".feather").exists():
                    row["format"] = "feather"
                elif table_path.with_suffix(".csv").exists():
                    row["format"] = "csv"

                rows.append(row)

        df = pd.DataFrame.from_records(rows)
        df.to_feather(self._catalog_file)

        self.frame = CatalogFrame(df)


class RemoteCatalog(Catalog):
    uri: str
    frame: "CatalogFrame"

    def __init__(self, uri: str = OWID_CATALOG_URI) -> None:
        self.uri = uri
        self.frame = CatalogFrame(pd.read_feather(self.uri + "catalog.feather"))
        self.frame._base_uri = uri

    @property
    def datasets(self) -> pd.DataFrame:
        return self.frame[["namespace", "version", "dataset"]].drop_duplicates()


class CatalogFrame(pd.DataFrame):
    """
    DataFrame helper, meant only for displaying catalog results.
    """

    _base_uri: Optional[str] = None

    _metadata = ["_base_uri"]

    @property
    def _constructor(self) -> type:
        return CatalogFrame

    @property
    def _constructor_sliced(self) -> Any:
        # ensure that when we pick a series we still have the URI
        def build(*args: Any, **kwargs: Any) -> Any:
            c = CatalogSeries(*args, **kwargs)
            c._base_uri = self._base_uri
            return c

        return build

    def load(self) -> Table:
        if len(self) == 1:
            return self.iloc[0].load()  # type: ignore

        raise ValueError("only one table can be loaded at once")

    @staticmethod
    def create_empty() -> "CatalogFrame":
        return CatalogFrame(
            {
                "namespace": [],
                "version": [],
                "table": [],
                "dimensions": [],
                "path": [],
                "format": [],
            }
        )


class CatalogSeries(pd.Series):
    _metadata = ["_base_uri"]

    @property
    def _constructor(self) -> type:
        return CatalogSeries

    def load(self) -> Table:
        if self.path and self.format and self._base_uri:
            uri = self._base_uri + self.path
            if self.format == "feather":
                return Table.read_feather(uri + ".feather")
            elif self.format == "csv":
                return Table.read_csv(uri + ".csv")
            else:
                raise ValueError("unknown format")

        raise ValueError("series is not a table spec")


def find(
    table: Optional[str] = None,
    namespace: Optional[str] = None,
    dataset: Optional[str] = None,
) -> "CatalogFrame":
    global REMOTE_CATALOG

    if not REMOTE_CATALOG:
        REMOTE_CATALOG = RemoteCatalog()

    return REMOTE_CATALOG.find(table=table, namespace=namespace, dataset=dataset)


def find_one(*args: Optional[str], **kwargs: Optional[str]) -> Table:
    return find(*args, **kwargs).load()
