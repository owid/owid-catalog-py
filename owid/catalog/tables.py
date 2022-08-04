#
#  tables.py
#

from os.path import join, dirname, splitext
import json
import copy
import yaml
from typing import Any, Literal, Optional, List, Dict, Union, cast
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import requests

from . import variables
from .meta import VariableMeta, TableMeta
from .frames import repack_frame

from pandas.util._decorators import (
    rewrite_axis_style_signature,
)

SCHEMA = json.load(open(join(dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])


class Table(pd.DataFrame):
    # metdata about the entire table
    metadata: TableMeta

    # metadata about individual columns
    # NOTE: the name _fields is also on the Variable class, pandas will propagate this to
    #       any slices, which is how they get access to their metadata
    _fields: Dict[str, VariableMeta]

    # propagate all these fields on every slice or copy
    _metadata = ["metadata", "_fields"]

    # slicing and copying creates tables
    @property
    def _constructor(self) -> type:
        return Table

    @property
    def _constructor_sliced(self) -> Any:
        return variables.Variable

    def __init__(
        self, *args: Any, metadata: Optional[TableMeta] = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)

        # empty table metadata by default
        self.metadata = metadata or TableMeta()

        # all columns have empty metadata by default
        assert not hasattr(self, "_fields")
        self._fields = defaultdict(VariableMeta)

    @property
    def primary_key(self) -> List[str]:
        return [n for n in self.index.names if n]

    def to(self, path: Union[str, Path], repack: bool = True) -> None:
        """
        Save this table in one of our SUPPORTED_FORMATS.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if path.endswith(".csv"):
            # ignore repacking
            return self.to_csv(path)

        elif path.endswith(".feather"):
            return self.to_feather(path, repack=repack)

        elif path.endswith(".parquet"):
            return self.to_parquet(path, repack=repack)

        else:
            raise ValueError(f"could not detect a suitable format to save to: {path}")

    @classmethod
    def read(cls, path: Union[str, Path]) -> "Table":
        if isinstance(path, Path):
            path = path.as_posix()

        if path.endswith(".csv"):
            return cls.read_csv(path)

        elif path.endswith(".feather"):
            return cls.read_feather(path)

        elif path.endswith(".parquet"):
            return cls.read_parquet(path)

        raise ValueError(f"could not detect a suitable format to read from: {path}")

    # Mypy complaints about this not matching the defintiion of NDFrame.to_csv but I don't understand why
    def to_csv(self, path: Any, **kwargs: Any) -> None:  # type: ignore
        """
        Save this table as a csv file plus accompanying JSON metadata file.
        If the table is stored at "mytable.csv", the metadata will be at
        "mytable.meta.json".
        """
        if not isinstance(path, str) or not path.endswith(".csv"):
            raise ValueError(f'filename must end in ".csv": {path}')

        df = pd.DataFrame(self)
        # if the dataframe uses the default index then we don't want to store it (would be a column of row numbers)
        save_index = self.primary_key != []
        df.to_csv(path, index=save_index, **kwargs)

        metadata_filename = splitext(path)[0] + ".meta.json"
        self._save_metadata(metadata_filename)

    def to_feather(
        self,
        path: Any,
        repack: bool = True,
        compression: Literal["zstd", "lz4", "uncompressed"] = "zstd",
        **kwargs: Any,
    ) -> None:
        """
        Save this table as a feather file plus accompanying JSON metadata file.
        If the table is stored at "mytable.feather", the metadata will be at
        "mytable.meta.json".
        """
        if not isinstance(path, str) or not path.endswith(".feather"):
            raise ValueError(f'filename must end in ".feather": {path}')

        # feather can't store the index
        df = pd.DataFrame(self)
        if self.primary_key:
            overlapping_names = set(self.index.names) & set(self.columns)
            if overlapping_names:
                raise ValueError(
                    f"index names are overlapping with column names: {overlapping_names}"
                )
            df = df.reset_index()

        if repack:
            # use smaller data types wherever possible
            # NOTE: this can be slow for large dataframes
            df = repack_frame(df)

        df.to_feather(path, compression=compression, **kwargs)

        metadata_filename = splitext(path)[0] + ".meta.json"
        self._save_metadata(metadata_filename)

    def to_parquet(self, path: Any, repack: bool = True) -> None:  # type: ignore
        """
        Save this table as a parquet file with embedded metadata in the table schema.

        NOTE: we save the metadata for fields in the table scheme, but it might be
              possible with Parquet to store it in the fields themselves somehow
        """
        if not isinstance(path, str) or not path.endswith(".parquet"):
            raise ValueError(f'filename must end in ".parquet": {path}')

        # parquet can store the index, but repacking is wasted on index columns so
        # we get rid of the index first
        df = pd.DataFrame(self)
        if self.primary_key:
            df = df.reset_index()

        if repack:
            # use smaller data types wherever possible
            # NOTE: this can be slow for large dataframes
            df = repack_frame(df)

        # create a pyarrow table with metadata in the schema
        # (some metadata gets auto-generated to help pandas deserialise better, we want to keep that)
        t = pyarrow.Table.from_pandas(df)
        new_metadata = {
            b"owid_table": json.dumps(self.metadata.to_dict(), default=str),  # type: ignore
            b"owid_fields": json.dumps(self._get_fields_as_dict(), default=str),
            b"primary_key": json.dumps(self.primary_key),
            **t.schema.metadata,
        }
        schema = t.schema.with_metadata(new_metadata)
        t = t.cast(schema)

        # write the combined table to disk
        pq.write_table(t, path)

    def _save_metadata(self, filename: str) -> None:
        # write metadata
        with open(filename, "w") as ostream:
            metadata = self.metadata.to_dict()  # type: ignore
            metadata["primary_key"] = self.primary_key
            metadata["fields"] = self._get_fields_as_dict()
            json.dump(metadata, ostream, indent=2, default=str)

    @classmethod
    def read_csv(cls, path: Union[str, Path]) -> "Table":
        """
        Read the table from csv plus accompanying JSON sidecar.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if not path.endswith(".csv"):
            raise ValueError(f'filename must end in ".csv": {path}')

        # load the data
        df = Table(
            pd.read_csv(path, index_col=False, na_values=[""], keep_default_na=False)
        )

        # load the metadata
        metadata = cls._read_metadata(path)

        primary_key = metadata.pop("primary_key") if "primary_key" in metadata else []
        fields = metadata.pop("fields") if "fields" in metadata else {}

        df.metadata = TableMeta(**metadata)
        df._fields = defaultdict(
            VariableMeta, {k: VariableMeta.from_dict(v) for k, v in fields.items()}
        )

        if primary_key:
            df.set_index(primary_key, inplace=True)

        return df

    def set_index(  # type: ignore
        self,
        keys: Union[str, List[str]],
        inplace: bool = False,
        drop: bool = True,
        append: bool = False,
        verify_integrity: bool = False,
    ) -> Optional[pd.DataFrame]:
        if isinstance(keys, str):
            keys = [keys]

        if inplace:
            super().set_index(
                keys,
                inplace=True,
                drop=drop,
                append=append,
                verify_integrity=verify_integrity,
            )
            self.metadata.primary_key = keys
            return None

        t = super().set_index(
            keys,
            inplace=False,
            drop=drop,
            append=append,
            verify_integrity=verify_integrity,
        )
        t.metadata.primary_key = keys
        return t

    @classmethod
    def read_feather(cls, path: Union[str, Path]) -> "Table":
        """
        Read the table from feather plus accompanying JSON sidecar.

        The path may be a local file path or a URL.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if not path.endswith(".feather"):
            raise ValueError(f'filename must end in ".feather": {path}')

        # load the data
        df = Table(pd.read_feather(path))

        # load the metadata
        metadata = cls._read_metadata(path)

        primary_key = metadata.get("primary_key", [])
        fields = metadata.pop("fields") if "fields" in metadata else {}

        df.metadata = TableMeta.from_dict(metadata)
        df._set_fields_from_dict(fields)

        if primary_key:
            df.set_index(primary_key, inplace=True)

        return df

    @classmethod
    def read_parquet(cls, path: Union[str, Path]) -> "Table":
        """
        Read the table from a parquet file, and unpack the schema metadata.

        The path may be a local file path or a URL.
        """
        if isinstance(path, Path):
            path = path.as_posix()

        if not path.endswith(".parquet"):
            raise ValueError(f'filename must end in ".parquet": {path}')

        # load the data as a pyarrow table
        t = pq.read_table(path)
        df = Table(t.to_pandas())

        # look for embedded table and field metadata in the table schema
        if b"owid_table" in t.schema.metadata:
            df.metadata = TableMeta.from_json(t.schema.metadata[b"owid_table"])  # type: ignore
        if b"owid_fields" in t.schema.metadata:
            fields = json.loads(t.schema.metadata[b"owid_fields"])
            df._set_fields_from_dict(fields)
        if b"primary_key" in t.schema.metadata:
            primary_key = json.loads(t.schema.metadata[b"primary_key"])
            if primary_key:
                df.set_index(primary_key, inplace=True)

        return df

    def _get_fields_as_dict(self) -> Dict[str, Any]:
        return {col: self._fields[col].to_dict() for col in self.all_columns}

    def _set_fields_from_dict(self, fields: Dict[str, Any]) -> None:
        self._fields = defaultdict(
            VariableMeta, {k: VariableMeta.from_dict(v) for k, v in fields.items()}
        )

    @staticmethod
    def _read_metadata(data_path: str) -> Dict[str, Any]:
        metadata_path = splitext(data_path)[0] + ".meta.json"

        if metadata_path.startswith("http"):
            return cast(Dict[str, Any], requests.get(metadata_path).json())

        with open(metadata_path, "r") as istream:
            return cast(Dict[str, Any], json.load(istream))

    def __setitem__(self, key: Any, value: Any) -> Any:
        super().__setitem__(key, value)

        # propagate metadata when we add a series to a table
        if isinstance(key, str):
            if isinstance(value, variables.Variable):
                self._fields[key] = value.metadata
            else:
                self._fields[key] = VariableMeta()

    def equals_table(self, rhs: "Table") -> bool:
        return (
            isinstance(rhs, Table)
            and self.metadata == rhs.metadata
            and self.to_dict() == rhs.to_dict()
        )

    @rewrite_axis_style_signature(
        "mapper",
        [("copy", True), ("inplace", False), ("level", None), ("errors", "ignore")],
    )
    def rename(self, *args: Any, **kwargs: Any) -> Optional["Table"]:
        """Rename columns while keeping their metadata."""
        inplace = kwargs.get("inplace")
        old_cols = self.all_columns
        new_table = super().rename(*args, **kwargs)

        if inplace:
            new_table = self

        # construct new _fields attribute
        fields = {
            new_col: self._fields[old_col] if inplace
            # avoid deepcopy if inplace to make it faster
            else copy.deepcopy(self._fields[old_col])
            for old_col, new_col in zip(old_cols, new_table.all_columns)
        }

        new_table._fields = defaultdict(VariableMeta, fields)

        if inplace:
            return None
        else:
            return cast(Table, new_table)

    @property
    def all_columns(self) -> List[str]:
        "Return names of all columns in the dataset, including the index."
        combined: List[str] = filter(None, list(self.index.names) + list(self.columns))  # type: ignore
        return combined

    def update_metadata_from_yaml(
        self, path: Union[Path, str], table_name: str
    ) -> None:
        """Update metadata of table and variables from a YAML file."""
        with open(path) as istream:
            annot = yaml.safe_load(istream)

        self.metadata.short_name = table_name

        t_annot = annot["tables"][table_name]

        # update variables
        for v_short_name, v_annot in (t_annot["variables"] or {}).items():
            for k, v in v_annot.items():
                setattr(self[v_short_name].metadata, k, v)

        # update table attributes
        for k, v in t_annot.items():
            if k != "variables":
                setattr(self.metadata, k, v)

    def prune_metadata(self) -> "Table":
        """Prune metadata for columns that are not in the table. This can happen after slicing
        the table by columns."""
        self._fields = {col: self._fields[col] for col in self.all_columns}
        return self
