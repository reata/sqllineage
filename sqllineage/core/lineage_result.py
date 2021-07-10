from typing import Set, TYPE_CHECKING, Tuple

from sqllineage.models import Table


class LineageResult:
    """
    Statement Level Lineage Result.

    LineageResult will hold attributes like read, write, rename, drop, intermediate.

    Each of them is a Set[:class:`sqllineage.models.Table`] except for rename.

    For rename, it a Set[Tuple[:class:`sqllineage.models.Table`, :class:`sqllineage.models.Table`]], with the first
    table being original table before renaming and the latter after renaming.

    This is the most atomic representation of lineage result.
    """

    __slots__ = ["read", "write", "rename", "drop", "intermediate"]
    if TYPE_CHECKING:
        read = write = drop = intermediate = set()  # type: Set[Table]
        rename = set()  # type: Set[Tuple[Table, Table]]

    def __init__(self) -> None:
        for attr in self.__slots__:
            setattr(self, attr, set())

    def __add__(self, other):
        lineage_result = LineageResult()
        for attr in self.__slots__:
            setattr(
                lineage_result, attr, getattr(self, attr).union(getattr(other, attr))
            )
        return lineage_result

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in self.__slots__
        )

    def __repr__(self):
        return str(self)
