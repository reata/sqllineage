import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from enum import Enum

    class StrEnum(str, Enum):
        """Compatibility implementation of enum.StrEnum for Python 3.10."""

        def __str__(self) -> str:
            return str(self.value)
