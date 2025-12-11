from typing import Any, Dict, Optional, Protocol, Tuple, Type
from .expressions import _QueryExpression


class _QueryableMixinProtocol(Protocol):
    __mixin_supported_types__: tuple[type, ...]


class QueryableProtocol(Protocol):
    """
    Protocol for any class that can be part of a top-level Query.

    A class implicitly satisfies this protocol if it implements
    both a `name()` and a `to_dict()` method.
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...]

    def with_expression(self, expr: _QueryExpression) -> "QueryableProtocol": ...

    def name(self) -> str: ...

    def to_dict(self) -> Dict[str, Any]: ...


class FieldMapperProtocol(Protocol):
    """
    Protocol for a stateless field mapper.
    Its job is to inspect a class and return a nested dictionary
    (a "field map") of all queryable paths.
    """

    def build_map(
        self,
        class_type: Type,
        query_expression_type: Type[_QueryExpression],
        path_prefix: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Builds the queryable field map for a given class.

        Args:
            class_type: The Pydantic or Arrow class to inspect.
            query_expression_type: The _QueryExpression class (e.g., _QueryTopicExpression)
                                   to inject into the final _QueryableField.
            path_prefix: The current path prefix.
        """
        ...
