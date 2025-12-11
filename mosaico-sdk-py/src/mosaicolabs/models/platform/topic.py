"""
Topic Catalog Entity.

This module defines the `Topic` class, which represents a read-only view of a
Topic's metadata in the platform catalog. It is used primarily for inspection
(listing topics) and query construction.
"""

from typing import Any, Optional

from pydantic import PrivateAttr
from ..query.generation.api import queryable
from ..query.generation.pydantic_mapper import PydanticFieldMapper
from ..query.expressions import _QueryTopicExpression

from .platform_base import PlatformBase


@queryable(
    mapper_type=PydanticFieldMapper,
    prefix="",
    query_expression_type=_QueryTopicExpression,
)
class Topic(PlatformBase):
    """
    Represents a Topic entity within the platform catalog.

    This class provides access to topic-specific system metadata, such as
    the ontology tag (e.g., 'imu', 'camera') and the serialization format.
    It is decorated with `@queryable` to enable fluid query syntax generation.

    **NOTE:** This version of the SDK allows the direct queryablity of the sole 'user_metadata'
    field via 'Q' query proxy. All the other entities can be queried via the
    'with_*' functions of the QueryTopic() class
    """

    # --- Private Fields (Internal State) ---
    _sequence_name: str = PrivateAttr()
    _ontology_tag: str = PrivateAttr()
    _serialization_format: str = PrivateAttr()
    _chunks_number: Optional[int] = PrivateAttr(default=None)

    # --- Factory Method ---
    @classmethod
    def from_flight_info(
        cls, sequence_name: str, name: str, metadata: Any, sys_info: Any
    ) -> "Topic":
        """
        Factory method to construct a Topic from Flight protocol objects.

        This acts as a bridge (adapter) between the low-level `TopicMetadata` /
        `_DoActionResponseSysInfo` objects (from `comm.metadata` and `comm.do_action`)
        and this high-level catalog model.

        Args:
            name (str): The full resource name of the topic.
            metadata (Any): The decoded `TopicMetadata` object containing user properties.
            sys_info (Any): The `_DoActionResponseSysInfo` object containing system stats.

        Returns:
            Topic: An initialized, read-only Topic model.
        """
        # Create the instance with public fields.
        # Note: metadata.user_metadata comes flat from the server; we unflatten it
        # to restore nested dictionary structures for the user.
        instance = cls(
            user_metadata=metadata.user_metadata,
        )

        # Set private attributes explicitly via the base helper
        instance._init_base_private(
            name=name,
            created_datetime=sys_info.created_datetime,
            is_locked=sys_info.is_locked,
            total_size_bytes=sys_info.total_size_bytes,
        )

        # Set local private attributes
        instance._sequence_name = sequence_name
        instance._ontology_tag = metadata.properties.ontology_tag
        instance._serialization_format = metadata.properties.serialization_format
        instance._chunks_number = sys_info.chunks_number

        return instance

    # --- Properties ---
    @property
    def ontology_tag(self) -> str:
        """
        Returns the ontology type identifier (e.g., 'imu', 'gnss').
        Corresponds to the `__ontology_tag__` in the `Serializable` class registry.
        """
        return self._ontology_tag

    @property
    def sequence_name(self) -> str:
        """
        Returns the parent sequence name.
        """
        return self._sequence_name

    @property
    def chunks_number(self) -> Optional[int]:
        """
        Returns the number of data chunks stored for this topic.
        May be None if the server did not provide detailed storage stats.
        """
        return self._chunks_number

    @property
    def serialization_format(self) -> str:
        """
        Returns the format used to serialize data (e.g., 'arrow', 'image').
        Corresponds to the `SerializationFormat` enum.
        """
        return self._serialization_format
