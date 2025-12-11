"""
Enumerations Module.

Defines the state machines and policy options used throughout the client library.
"""

from enum import Enum


class SequenceStatus(Enum):
    """
    Represents the lifecycle state of a Sequence during the writing process.
    """

    Null = "null"  # Not yet initialized or registered on server.
    Pending = "pending"  # Registered on server; accepting data; not yet finalized.
    Finalized = "finalized"  # Successfully closed; data is immutable.
    Error = "error"  # Aborted or failed state.


class OnErrorPolicy(Enum):
    """
    Defines the behavior when an exception occurs during a sequence write.
    """

    Report = "report"  # Notify the server of the error but keep partial data.
    Delete = "delete"  # Abort the sequence and instruct server to discard all data.
