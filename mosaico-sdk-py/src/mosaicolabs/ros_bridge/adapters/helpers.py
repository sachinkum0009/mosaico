from typing import Dict, Optional, Type

from ..adapter_base import ROSAdapterBase
from ..ros_bridge import ROSBridge
from mosaicolabs.models.header import Header


def _make_header(ros_head_dict: Optional[Dict]) -> Optional[Header]:
    # Mandatory Field Conversions (as before)
    # Extract metadata
    if ros_head_dict is None:
        return None
    return Header(**ros_head_dict)


def _validate_msgdata(
    cls: Type[ROSAdapterBase], ros_data: dict, case_insensitive: bool = False
):
    missing_keys = [
        key
        for key in cls._REQUIRED_KEYS
        if key not in ros_data.keys()
        and (
            not case_insensitive
            or (
                key.lower() not in ros_data.keys()
                and key.upper() not in ros_data.keys()
            )
        )
    ]

    if missing_keys:
        raise ValueError(
            f"Malformed ROS message {cls.ros_msgtype}: missing required keys {missing_keys}. "
            f"Available keys: {list(ros_data.keys())}"
        )


def register_adapter(cls):
    """
    Decorator to register a ROSAdapter in the ROSBridge.
    """
    ROSBridge.register_adapter(cls)
    return cls
