import re
from typing import Optional
from unidecode import unidecode

from .tables import Table


def underscore(name: Optional[str], validate: bool = True) -> Optional[str]:
    """Convert arbitrary string to under_score. This was fine tuned on WDI bank column names.
    This function might evolve in the future, so make sure to have your use cases in tests
    or rather underscore your columns yourself.
    """
    if name is None:
        return None

    name = (
        name.replace(" ", "_")
        .replace("-", "_")
        .replace(",", "_")
        .replace(".", "_")
        .lower()
    )

    # replace special separators
    name = (
        name.replace("(", "__").replace(")", "__").replace(":", "__").replace(";", "__")
    )

    # replace special symbols
    name = name.replace("/", "_")
    name = name.replace("=", "_")
    name = name.replace("%", "pct")
    name = name.replace("+", "plus")
    name = name.replace("us$", "usd")
    name = name.replace("$", "dollar")
    name = name.replace("&", "_and_")

    # replace quotes
    name = name.replace("'", "")

    # shrink triple underscore
    name = re.sub("__+", "__", name)

    # convert special characters to ASCII
    name = unidecode(name)

    # strip leading and trailing underscores
    name = name.strip("_")

    # make sure it's under_score now, if not then raise NameError
    if validate:
        validate_underscore(name, "Name")

    return name


def underscore_table(t: Table) -> Table:
    """Convert column and index names to underscore."""
    t = t.rename(columns=underscore)

    t.index.names = [underscore(e) for e in t.index.names]
    t.metadata.primary_key = t.primary_key
    return t


def validate_underscore(name: Optional[str], object_name: str) -> None:
    """Raise error if name is not snake_case."""
    if name is not None and not re.match("^[a-z][a-z0-9_]*$", name):
        raise NameError(
            f"{object_name} must be snake_case. Change `{name}` to `{underscore(name, validate=False)}`"
        )
