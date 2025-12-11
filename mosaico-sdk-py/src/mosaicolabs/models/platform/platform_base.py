"""
Platform Entity Base Module.

This module defines `PlatformBase`, the foundational class for the main catalog entities,
Sequences and Topics, within the SDK. It consolidates shared system attributes
(like creation time, locks, and size) and integrates with the Pydantic validation
system and the internal Query API.
"""

import datetime
from typing import Any, Dict

from pydantic import PrivateAttr
import pydantic
from ..query.generation.api import _QueryableModel


class PlatformBase(pydantic.BaseModel, _QueryableModel):
    """
    Abstract base class for Mosaico Sequence and Topic catalog entities.

    This class serves two purposes:
    1.  **Data Container:** It holds standard system attributes (`created_datetime`,
        `total_size_bytes`) that are common to both Sequences and Topics.
    2.  **Query Interface:** By inheriting from `_QueryableModel`, it allows specific
        fields (like `user_metadata`) to be used in client-side query construction
        (e.g., `Sequence.Q.user_metadata["project"] == "Apollo"`).

    Note:
        Instances of this class are typically created via factory methods processing
        server responses, rather than direct instantiation by the user.
    """

    user_metadata: Dict[str, Any]
    """Custom user-defined key-value pairs associated with the entity."""

    # --- Private Attributes ---
    # These fields are managed internally and populated via _init_base_private.
    # They are excluded from the standard Pydantic __init__ to prevent users
    # from manually setting system-controlled values.
    _is_locked: bool = PrivateAttr(default=False)
    _total_size_bytes: int = PrivateAttr()
    _created_datetime: datetime.datetime = PrivateAttr()
    _name: str = PrivateAttr()

    def _init_base_private(
        self,
        *,
        name: str,
        total_size_bytes: int,
        created_datetime: datetime.datetime,
        is_locked: bool = False,
    ) -> None:
        """
        Internal helper to populate system-controlled private attributes.

        This is used by factory methods (`from_flight_info`) to set attributes
        that are strictly read-only for the user.

        Args:
            name: The unique resource name.
            total_size_bytes: The storage size on the server.
            created_datetime: The UTC timestamp of creation.
            is_locked: Whether the resource is currently locked (e.g., during writing).
        """
        self._is_locked = is_locked
        self._total_size_bytes = total_size_bytes
        self._created_datetime = created_datetime or datetime.datetime.utcnow()
        self._name = name or ""

    # --- Shared Properties ---
    @property
    def name(self) -> str:
        """Returns the unique resource name."""
        return self._name

    @property
    def created_datetime(self) -> datetime.datetime:
        """Returns the UTC datetime when the resource was created on the server."""
        return self._created_datetime

    @property
    def is_locked(self) -> bool:
        """Returns True if the resource is currently locked (e.g., being written to)."""
        return self._is_locked

    @property
    def total_size_bytes(self) -> int:
        """Returns the total storage size of the resource in bytes."""
        return self._total_size_bytes
