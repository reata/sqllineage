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


class SectionTokenBaseHandler:
    """
    This is to address an extract pattern when we should extract something from a section of tokens
    """

    def __init__(self) -> None:
        self.start_indicator = False
        self.end_indicator = False

    def _indicate_start(self, token: Token) -> bool:
        raise NotImplementedError

    def _handle_start(self, token: Token, lineage_result: LineageResult) -> None:
        raise NotImplementedError

    def _indicate_end(self, token: Token) -> bool:
        raise NotImplementedError

    def _handle_end(self, token: Token, lineage_result: LineageResult) -> None:
        raise NotImplementedError

    def indicate_start(self, token: Token) -> None:
        indicator = self._indicate_start(token)
        if indicator:
            self.start_indicator = True

    def handle_start(self, token: Token, lineage_result: LineageResult) -> None:
        if self.start_indicator:
            self._handle_start(token, lineage_result)
            self.start_indicator = False

    def indicate_end(self, token: Token) -> None:
        indicator = self._indicate_end(token)
        if indicator:
            self.end_indicator = True

    def handle_end(self, token: Token, lineage_result: LineageResult) -> None:
        if self.end_indicator:
            self._handle_end(token, lineage_result)
            self.end_indicator = False
