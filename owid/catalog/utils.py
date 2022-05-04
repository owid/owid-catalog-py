import re
from typing import Optional, List
from unidecode import unidecode
import pandas as pd

from .tables import Table
from .variables import Variable


def underscore(name: Optional[str], validate: bool = True) -> Optional[str]:
    """Convert arbitrary string to under_score. This was fine tuned on WDI bank column names.
    This function might evolve in the future, so make sure to have your use cases in tests
    or rather underscore your columns yourself.
    """
    if name is None:
        return None

    orig_name = name

    name = (
        name.replace(" ", "_")
        .replace("-", "_")
        .replace("—", "_")
        .replace("–", "_")
        .replace(",", "_")
        .replace(".", "_")
        .replace("\t", "_")
        .replace("?", "_")
        .replace('"', "")
        .replace("\xa0", "_")
        .replace("’", "")
        .replace("`", "")
        .replace("−", "_")
        .replace("“", "")
        .replace("”", "")
        .replace("#", "")
        .replace("^", "")
        .lower()
    )

    # replace special separators
    name = (
        name.replace("(", "__")
        .replace(")", "__")
        .replace(":", "__")
        .replace(";", "__")
        .replace("[", "__")
        .replace("]", "__")
    )

    # replace special symbols
    name = name.replace("/", "_")
    name = name.replace("=", "_")
    name = name.replace("%", "pct")
    name = name.replace("+", "plus")
    name = name.replace("us$", "usd")
    name = name.replace("$", "dollar")
    name = name.replace("&", "_and_")
    name = name.replace("<", "_lt_")
    name = name.replace(">", "_gt_")

    # replace quotes
    name = name.replace("'", "")

    # shrink triple underscore
    name = re.sub("__+", "__", name)

    # convert special characters to ASCII
    name = unidecode(name)

    # strip leading and trailing underscores
    name = name.strip("_")

    # if the first letter is number, prefix it with underscore
    if re.match("^[0-9]", name):
        name = f"_{name}"

    # make sure it's under_score now, if not then raise NameError
    if validate:
        validate_underscore(name, f"`{orig_name}`")

    return name


def underscore_table(t: Table) -> Table:
    """Convert column and index names to underscore."""
    t = t.rename(columns=underscore)

    t.index.names = [underscore(e) for e in t.index.names]
    t.metadata.primary_key = t.primary_key
    t.metadata.short_name = underscore(t.metadata.short_name)
    return t


def validate_underscore(name: Optional[str], object_name: str) -> None:
    """Raise error if name is not snake_case."""
    if name is not None and not re.match("^[a-z_][a-z0-9_]*$", name):
        raise NameError(
            f"{object_name} must be snake_case. Change `{name}` to `{underscore(name, validate=False)}`"
        )


def concat_variables(variables: List[Variable]) -> Table:
    """Concatenate variables into a single table keeping all metadata."""
    t = Table(pd.concat(variables, axis=1))
    for v in variables:
        if v.name:
            t._fields[v.name] = v.metadata
    return t
