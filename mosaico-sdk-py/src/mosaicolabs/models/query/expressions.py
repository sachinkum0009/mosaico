from typing import Any, Dict, List


class _QueryExpression:
    """
    Represents a single, atomic comparison (e.g., "field > 10").
    This is the final product of a _QueryableField method like .gt(10).
    """

    def __init__(self, full_path: str, op: str, value: Any):
        """
        Initializes the atomic comparison.

        Args:
            full_path (str): The complete, dot-separated path to the field (e.g., "GPS.position.x").
            op (str): The short string for the operation (e.g., "eq", "gt").
            value (Any): The value to compare against.
        """
        # self.key is the full path used in the final query dict, e.g., "GPS.status"
        self.key = full_path

        self.op = op
        self.value = value

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts this comparison into its dictionary format.
        Example: {"GPS.status": {"eq": 0}}
        """
        return {self.key: {self.op: self.value}}


# --- Logical Combinators --


class _QueryCombinator:
    """
    Abstract base class for logical operators that combine other expressions
    (e.g., AND, OR).
    """

    def __init__(
        self,
        expressions: List[_QueryExpression],
        # op: str = "$and",
    ):
        """
        Initializes the logical query.

        Args:
            expressions (List[_QueryExpression]): A list of expressions to combine.
        """
        # self.op = op
        self.expressions = expressions

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts this logical group into its dictionary format.
        """
        if not self.expressions:
            return {}
        # return {self.op: [expr.to_dict() for expr in self.expressions]}
        return {
            key: val for expr in self.expressions for key, val in expr.to_dict().items()
        }


class _QueryTopicExpression(_QueryExpression):
    """
    Represents a single, atomic comparison within the topic context.
    Inherits from _QueryExpression without modification.
    """

    pass


class _QuerySequenceExpression(_QueryExpression):
    """
    Represents a single, atomic comparison within the sequence context.
    Inherits from _QueryExpression without modification.
    """

    pass


class _QueryCatalogExpression(_QueryExpression):
    """
    Represents a single, atomic comparison within the data catalog context.
    Inherits from _QueryExpression without modification.
    """

    pass
