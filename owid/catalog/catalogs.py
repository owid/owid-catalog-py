#
#  catalog.py
#  owid-catalog-py
#

from pathlib import Path
from typing import Dict, Optional, Iterator, Union, Any, cast
import json
import os
import heapq

import pandas as pd
import numpy as np
import requests
import tempfile
from urllib.parse import urlparse
import numpy.typing as npt

from .datasets import Dataset
from .tables import Table
from . import s3_utils

# increment this on breaking changes to require clients to update
OWID_CATALOG_VERSION = 1

# location of the default remote catalog
OWID_CATALOG_URI = "https://catalog.ourworldindata.org/"

# S3 location for private files
S3_OWID_URI = "s3://owid-catalog"

# global copy cached after first request
REMOTE_CATALOG: Optional["RemoteCatalog"] = None


class CatalogMixin:
    """
    Abstract data catalog API, encapsulates finding and loading data.
    """

    frame: "CatalogFrame"

    def find(
        self,
        table: Optional[str] = None,
        namespace: Optional[str] = None,
        dataset: Optional[str] = None,
    ) -> "CatalogFrame":
        criteria: npt.ArrayLike = np.ones(len(self.frame), dtype=bool)

        if table:
            criteria &= self.frame.table.apply(lambda t: table in t)

        if namespace:
            criteria &= self.frame.namespace == namespace

        if dataset:
            criteria &= self.frame.dataset == dataset

        matches = self.frame[criteria]
        if "checksum" in matches.columns:
            matches = matches.drop(columns=["checksum"])

        return cast(CatalogFrame, matches)

    def find_one(self, *args: Optional[str], **kwargs: Optional[str]) -> Table:
        return self.find(*args, **kwargs).load()


class LocalCatalog(CatalogMixin):
    """
    A data catalog that's on disk. On-disk catalogs do not need an index file, since
    you can simply walk the directory. However, they support a `reindex()` method
    which can create such an index.
    """

    path: Path
    frame: "CatalogFrame"

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        if self._catalog_file.exists():
            df = pd.read_feather(self._catalog_file.as_posix())
            self.frame = CatalogFrame(df)
            self.frame._base_uri = self.path.as_posix() + "/"
        else:
            # could take a while to generate if there are many datasets
            self.reindex()

        # ensure the frame knows where to load data from

    @property
    def _catalog_file(self) -> Path:
        return self.path / "catalog.feather"

    @property
    def _metadata_file(self) -> Path:
        return self.path / "catalog.meta.json"

    def iter_datasets(self) -> Iterator[Dataset]:
        to_search = [self.path]
        while to_search:
            dir = heapq.heappop(to_search)
            if (dir / "index.json").exists():
                yield Dataset(dir)
                continue

            for child in dir.iterdir():
                if child.is_dir():
                    heapq.heappush(to_search, child)

    def reindex(self) -> None:
        self._save_metadata({"format_version": OWID_CATALOG_VERSION})

        # walk the directory tree, generate a namespace/version/dataset/table frame
        # save it to feather
        frames = []
        for ds in self.iter_datasets():
            frames.append(ds.index(self.path))

        df = pd.concat(frames, ignore_index=True)

        keys = ["table", "dataset", "version", "namespace", "is_public"]
        columns = keys + [c for c in df.columns if c not in keys]

        df.sort_values(keys, inplace=True)
        df = df[columns]
        df.reset_index(drop=True, inplace=True)
        df.to_feather(self._catalog_file)

        self.frame = CatalogFrame(df)
        self.frame._base_uri = self.path.as_posix() + "/"

    def _save_metadata(self, contents: Dict[str, Any]) -> None:
        with open(self._metadata_file, "w") as ostream:
            json.dump(contents, ostream, indent=2)


class RemoteCatalog(CatalogMixin):
    uri: str
    frame: "CatalogFrame"

    def __init__(self, uri: str = OWID_CATALOG_URI) -> None:
        self.uri = uri
        self.metadata = self._read_metadata(self.uri + "catalog.meta.json")
        if self.metadata["format_version"] > OWID_CATALOG_VERSION:
            raise PackageUpdateRequired(
                f"library supports api version {OWID_CATALOG_VERSION}, "
                f'but the remote catalog has version {self.metadata["version"]} '
                "-- please update"
            )

        self.frame = CatalogFrame(pd.read_feather(self.uri + "catalog.feather"))
        self.frame._base_uri = uri

    @property
    def datasets(self) -> pd.DataFrame:
        return self.frame[["namespace", "version", "dataset"]].drop_duplicates()

    @staticmethod
    def _read_metadata(uri: str) -> Dict[str, Any]:
        """
        Read the metadata JSON blob for this repo.
        """
        resp = requests.get(uri)
        resp.raise_for_status()
        return cast(Dict[str, Any], resp.json())


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
            with tempfile.TemporaryDirectory() as tmpdir:
                uri = self._base_uri + self.path + "." + self.format

                # download the data locally first if the file is private
                # keep backward compatibility
                if not getattr(self, "is_public", True):
                    uri = _download_private_file(uri, tmpdir)

                if self.format == "feather":
                    return Table.read_feather(uri)
                elif self.format == "csv":
                    return Table.read_csv(uri)
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


def _download_private_file(uri: str, tmpdir: str) -> str:
    parsed = urlparse(uri)
    base, ext = os.path.splitext(parsed.path)
    s3_utils.download(
        S3_OWID_URI + base + ".meta.json",
        tmpdir + "/data.meta.json",
    )
    s3_utils.download(
        S3_OWID_URI + base + ext,
        tmpdir + "/data" + ext,
    )
    return tmpdir + "/data" + ext


class PackageUpdateRequired(Exception):
    pass
