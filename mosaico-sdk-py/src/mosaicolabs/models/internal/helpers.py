def _fix_empty_dicts(obj):
    """
    Recursively replaces dictionaries where all values are None
    with a single None value.

    Notes:
        Fixes a schema issue with Parquet v2 deserialization:
        Fields defined as `Vector3d = None` may be incorrectly
        deserialized as `Vector3d(x=None, y=None, z=None)`.
        This function cleans up that structure back to `None`.
    """
    if isinstance(obj, dict):
        # Recursively fix all values in the dictionary
        fixed = {k: _fix_empty_dicts(v) for k, v in obj.items()}

        # If all values in the fixed dict are None, return None
        if all(v is None for v in fixed.values()):
            return None
        # Otherwise, return the fixed dictionary
        return fixed
    # If not a dict, return the object unchanged
    return obj
