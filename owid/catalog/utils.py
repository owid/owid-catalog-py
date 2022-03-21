from typing import Optional
from .tables import Table


def underscore(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None

    name = name.replace(" ", "_").replace("-", "_").lower()

    if "(" in name:
        raise NameError(f"{name} contains forbidden character `(`")
    return name


def underscore_table(t: Table) -> Table:
    """Convert column and index names to underscore."""
    t.columns = [underscore(e) for e in t.columns]
    t.index.names = [underscore(e) for e in t.index.names]
    return t
