"""
Sequence Catalog Entity.

This module defines the `Sequence` class, which represents a read-only view of a
Sequence's metadata. A Sequence is a logical grouping of multiple Topics.
"""

from typing import Any, List
from pydantic import PrivateAttr


from ..query.generation.api import queryable
from ..query.generation.pydantic_mapper import PydanticFieldMapper
from ..query.expressions import _QuerySequenceExpression

from .platform_base import PlatformBase


@queryable(
    mapper_type=PydanticFieldMapper,
    prefix="",
    query_expression_type=_QuerySequenceExpression,
)
class Sequence(PlatformBase):
    """
    Represents a Sequence entity within the platform catalog.

    A Sequence serves as a container for related Topics (e.g., a single recording
    session containing Camera, IMU, and GPS topics). This class provides access
    to the list of contained topics and sequence-level metadata.
    It is decorated with `@queryable` to enable fluid query syntax generation.

    **NOTE:** This version of the SDK allows the direct queryablity of the sole 'user_metadata'
    field via 'Q' query proxy. All the other entities can be queried via the
    'with_*' functions of the QueryTopic() class

    """

    # --- Private Fields ---
    _topics: List[str] = PrivateAttr(default_factory=list)

    # --- Factory Method ---
    @classmethod
    def from_flight_info(
        cls, name: str, metadata: Any, sys_info: Any, topics: List[str]
    ) -> "Sequence":
        """
        Factory method to construct a Sequence from Flight protocol objects.

        Args:
            name (str): The name of the sequence.
            metadata (Any): The decoded `SequenceMetadata` object.
            sys_info (Any): The `_DoActionResponseSysInfo` object containing stats.
            topics (List[str]): A list of topic names contained within this sequence.

        Returns:
            Sequence: An initialized, read-only Sequence model.
        """
        instance = cls(
            user_metadata=metadata.user_metadata,
        )

        # Set private attributes explicitly
        instance._init_base_private(
            name=name,
            created_datetime=sys_info.created_datetime,
            is_locked=sys_info.is_locked,
            total_size_bytes=sys_info.total_size_bytes,
        )

        # Set local private attributes
        instance._topics = topics
        return instance

    # --- Properties ---
    @property
    def topics(self) -> List[str]:
        """
        Returns the list of names of the topics contained in this sequence.

        Note: This returns string names, not `Topic` objects. To interact with
        a topic, use `MosaicoClient.topic_handler()`.
        """
        return self._topics
