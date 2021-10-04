#
#  catalog.py
#  owid-catalog-py
#

from pathlib import Path
from typing import Protocol, Iterator, Union, Any
import json

import pandas as pd

from .datasets import Dataset
from .tables import Table

OWID_CATALOG_URI = "https://owid-catalog.nyc3.digitaloceanspaces.com/"


class Catalog(Protocol):
    def __iter__(self) -> Iterator[Dataset]:
        ...

    def __len__(self) -> int:
        ...

    def search(self, query: str) -> pd.DataFrame:
        return self.frame[self.frame.table.apply(lambda t: query in t)]  # type: ignore


class LocalCatalog(Catalog):
    path: Path

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)

    def __iter__(self) -> Iterator[Dataset]:
        to_search = [self.path]
        while to_search:
            dir = to_search.pop()
            if (dir / "index.json").exists():
                yield Dataset(dir)
                continue

            for child in dir.iterdir():
                if child.is_dir():
                    to_search.append(child)

    def __len__(self) -> int:
        return sum(1 for ds in self)

    @property
    def frame(self) -> pd.DataFrame:
        return pd.read_feather(self.path / "catalog.feather")  # type: ignore

    def reindex(self) -> None:
        # walk the directory tree, generate a namespace/version/dataset/table frame
        # save it to feather
        rows = []
        for ds in self:
            base = {
                "namespace": ds.metadata.namespace,
                "dataset": ds.metadata.short_name,
                "version": ds.metadata.version,
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
        df.to_feather(self.path / "catalog.feather")


class RemoteCatalog(Catalog):
    uri: str
    frame: pd.DataFrame

    def __init__(self, uri: str = OWID_CATALOG_URI) -> None:
        self.uri = uri
        self.frame = CatalogFrame(pd.read_feather(self.uri + "catalog.feather"))
        self.frame._base_uri = uri


class CatalogFrame(pd.DataFrame):
    """
    DataFrame helper, meant only for displaying catalog results.
    """

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
