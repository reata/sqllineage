from sqlparse.sql import Token

from sqllineage.core.lineage_result import LineageResult


class NextTokenBaseHandler:
    """
    This is to address an extract pattern when a specified token indicates we should extract something from next token.
    """

    def __init__(self) -> None:
        self.indicator = False

    def _indicate(self, token: Token) -> bool:
        """
        Whether current token indicates a following token to be handled or not.
        """
        raise NotImplementedError

    def _handle(self, token: Token, lineage_result: LineageResult) -> None:
        """
        Handle the indicated token, and update the lienage result accordingly
        """
        raise NotImplementedError

    def indicate(self, token: Token):
        """
        Set indicator to True only when _indicate returns True
        """
        indicator = self._indicate(token)
        if indicator:
            self.indicator = True

    def handle(self, token: Token, lineage_result: LineageResult):
        """
        Handle and set indicator back to False
        """
        if self.indicator:
            self._handle(token, lineage_result)
            self.indicator = False


class CurrentTokenBaseHandler:
    """
    This is to address an extract pattern when we should extract something from current token
    """

    def handle(self, token: Token, lineage_result: LineageResult) -> None:
        raise NotImplementedError
