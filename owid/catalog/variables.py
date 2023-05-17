#
#  variables.py
#

import json
from os import path
from typing import Any, Dict, List, Optional, Union, cast

import pandas as pd
import structlog
from pandas._typing import Scalar
from pandas.core.series import Series

from .meta import License, Source, VariableMeta
from .properties import metadata_property

log = structlog.get_logger()

SCHEMA = json.load(open(path.join(path.dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])


class Variable(pd.Series):
    _name: Optional[str] = None
    _fields: Dict[str, VariableMeta]

    def __init__(
        self,
        data: Any = None,
        index: Any = None,
        _fields: Optional[Dict[str, VariableMeta]] = None,
        **kwargs: Any,
    ) -> None:
        self._fields = _fields or {}

        # silence warning
        if data is None and not kwargs.get("dtype"):
            kwargs["dtype"] = "object"

        super().__init__(data=data, index=index, **kwargs)

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        # None name does not modify _fields, it is usually triggered on pandas operations
        if name is not None:
            # move metadata when you rename a field
            if self._name and self._name in self._fields:
                self._fields[name] = self._fields.pop(self._name)

            # make sure there is always a placeholder metadata object
            if name not in self._fields:
                self._fields[name] = VariableMeta()

        self._name = name

    @property
    def checked_name(self) -> str:
        if not self.name:
            raise ValueError("variable must be named to have metadata")

        return self.name

    # which fields should pandas propagate on slicing, etc?
    _metadata = ["_fields", "_name"]

    @property
    def _constructor(self) -> type:
        return Variable

    @property
    def _constructor_expanddim(self) -> type:
        # XXX lazy circular import
        from . import tables

        return tables.Table

    @property
    def metadata(self) -> VariableMeta:
        return self._fields[self.checked_name]

    @metadata.setter
    def metadata(self, meta: VariableMeta) -> None:
        self._fields[self.checked_name] = meta

    def astype(self, *args: Any, **kwargs: Any) -> "Variable":
        # To fix: https://github.com/owid/owid-catalog-py/issues/12
        v = super().astype(*args, **kwargs)
        v.name = self.name
        return cast(Variable, v)

    def __add__(self, other: Union[Scalar, Series]) -> Series:
        variable = super().__add__(other)
        # TODO: Is there any better solution to the names issue?
        #  * If variable.name is None, an error is raised.
        #  * If variable.name = self.checked_name then the metadata of the first variable summed is modified.
        #  * If variable.name is always a random string (that does not coincide with an existing variable) then
        #    when replacing a variable (e.g. tb["a"] += 1) it loses its metadata.
        if variable.name is None:
            variable.name = "**TEMPORARY UNNAMED VARIABLE**"
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="+", name=self.name)  # type: ignore
        # TODO: Currently, the processing log is not catching the name of the new variable, but the one being added.

        return variable

    def add(self, other: Union[Scalar, Series]) -> Series:
        return self.__add__(other=other)


# dynamically add all metadata properties to the class
for k in VariableMeta.__dataclass_fields__:
    if hasattr(Variable, k):
        raise Exception(f'metadata field "{k}" would overwrite a Pandas built-in')

    setattr(Variable, k, metadata_property(k))


def _combine_variable_units_or_short_units(variables: List[Variable], operation, unit_or_short_unit) -> Optional[str]:
    # Gather units (or short units) of all variables.
    units_or_short_units = pd.unique([getattr(variable.metadata, unit_or_short_unit) for variable in variables])
    # Initialise the unit (or short unit) of the output variable.
    unit_or_short_unit_combined = None
    if operation in ["+", "-"]:
        # If units (or short units) do not coincide among all variables, raise a warning and assign None.
        if len(units_or_short_units) != 1:
            log.warning(f"Different values of '{unit_or_short_unit}' detected among variables: {units_or_short_units}")
            unit_or_short_unit_combined = None
        else:
            # Otherwise, assign the common unit.
            unit_or_short_unit_combined = units_or_short_units[0]
    elif operation == "*":
        # TODO: Define.
        pass
    elif operation == "/":
        # TODO: Define.
        pass

    return unit_or_short_unit_combined


def combine_variables_units(variables: List[Variable], operation: str) -> Optional[str]:
    return _combine_variable_units_or_short_units(variables=variables, operation=operation, unit_or_short_unit="unit")


def combine_variables_short_units(variables: List[Variable], operation) -> Optional[str]:
    return _combine_variable_units_or_short_units(
        variables=variables, operation=operation, unit_or_short_unit="short_unit"
    )


def _combine_variables_titles_and_descriptions(variables: List[Variable], title_or_description: str) -> Optional[str]:
    # Keep the title only if all variables have exactly the same title.
    # Otherwise we assume that the variable has a different meaning, and its title should be manually handled.
    titles_or_descriptions = pd.unique([getattr(variable.metadata, title_or_description) for variable in variables])
    if len(titles_or_descriptions) == 1:
        title_or_description_combined = titles_or_descriptions[0]
    else:
        title_or_description_combined = None

    return title_or_description_combined


def combine_variables_titles(variables: List[Variable]) -> Optional[str]:
    return _combine_variables_titles_and_descriptions(variables=variables, title_or_description="title")


def combine_variables_descriptions(variables: List[Variable]) -> Optional[str]:
    return _combine_variables_titles_and_descriptions(variables=variables, title_or_description="description")


def get_unique_sources_from_variables(variables: List[Variable]) -> List[Source]:
    # Make a list of all sources of all variables.
    sources = sum([variable.metadata.sources for variable in variables], [])

    # Get unique array of tuples of source fields (respecting the order).
    unique_sources_array = pd.unique([tuple(source.to_dict().items()) for source in sources])

    # Make a list of sources.
    unique_sources = [Source.from_dict(dict(source)) for source in unique_sources_array]  # type: ignore

    return unique_sources


def get_unique_licenses_from_variables(variables: List[Variable]) -> List[License]:
    # Make a list of all licenses of all variables.
    licenses = sum([variable.metadata.licenses for variable in variables], [])

    # Get unique array of tuples of license fields (respecting the order).
    unique_licenses_array = pd.unique([tuple(license.to_dict().items()) for license in licenses])

    # Make a list of licenses.
    unique_licenses = [License.from_dict(dict(license)) for license in unique_licenses_array]

    return unique_licenses


def combine_variables_processing_logs(variables: List[Variable]):
    # Make a list with all entries in the processing log of all variables.
    processing_log = sum(
        [
            variable.metadata.processing_log if variable.metadata.processing_log is not None else []
            for variable in variables
        ],
        [],
    )

    return processing_log


def combine_variables_metadata(variables: List[Any], operation: str, name: str = "variable") -> VariableMeta:
    # Initialise an empty metadata.
    metadata = VariableMeta()

    # Skip other objects passed in variables that may not contain metadata (e.g. a scalar).
    variables_only = [variable for variable in variables if hasattr(variable, "metadata")]

    # Combine each metadata field using the logic of the specified operation.
    metadata.title = combine_variables_titles(variables=variables_only)
    metadata.description = combine_variables_descriptions(variables=variables_only)
    metadata.unit = combine_variables_units(variables=variables_only, operation=operation)
    metadata.short_unit = combine_variables_short_units(variables=variables_only, operation=operation)
    metadata.sources = get_unique_sources_from_variables(variables=variables_only)
    metadata.licenses = get_unique_licenses_from_variables(variables=variables_only)
    metadata.processing_log = combine_variables_processing_logs(variables=variables_only)
    metadata.processing_log.extend(
        [{"variable": name, "parents": [variable.name for variable in variables_only], "operation": operation}]
    )

    return metadata
