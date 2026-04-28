"""Permission and collision-policy helpers.

Both ``FsspecStorage`` (top-level) and ``NestedFileSystem`` (per sub-fs)
expose the same shape:

- ``permissions``: dict with ``allow_read`` / ``allow_write`` / ``allow_delete``,
  defaulting to ``True``.
- ``on_collision``: one of ``"overwrite"`` / ``"rename"`` / ``"raise"``,
  defaulting to ``"overwrite"``.

When both layers carry a value, the *most restrictive* one wins (booleans
combine with ``AND``; ``on_collision`` is ranked
``raise > rename > overwrite``).
"""

from __future__ import annotations

from typing import Mapping
from typing import Optional

from django.core.exceptions import ImproperlyConfigured

DEFAULT_PERMISSIONS: dict[str, bool] = {
    "allow_read": True,
    "allow_write": True,
    "allow_delete": True,
}
ALLOWED_PERMISSION_KEYS = frozenset(DEFAULT_PERMISSIONS)

DEFAULT_ON_COLLISION = "overwrite"
ALLOWED_ON_COLLISION = ("overwrite", "rename", "raise")
_COLLISION_SEVERITY = {"overwrite": 0, "rename": 1, "raise": 2}


def normalize_permissions(permissions: Optional[Mapping[str, bool]]) -> dict[str, bool]:
    """Validate ``permissions`` and apply defaults.

    Parameters
    ----------
    permissions : Mapping[str, bool] or None
        Mapping with any subset of ``allow_read`` / ``allow_write`` /
        ``allow_delete``. Missing keys default to ``True``. ``None`` returns
        the all-``True`` default.

    Returns
    -------
    dict[str, bool]
        A new dict with all three keys populated.

    Raises
    ------
    django.core.exceptions.ImproperlyConfigured
        When ``permissions`` is not a mapping or contains unknown keys.
    """
    out = dict(DEFAULT_PERMISSIONS)
    if permissions is None:
        return out
    if not isinstance(permissions, Mapping):
        raise ImproperlyConfigured(f"'permissions' must be a mapping, got {type(permissions).__name__}")
    unknown = set(permissions) - ALLOWED_PERMISSION_KEYS
    if unknown:
        raise ImproperlyConfigured(
            f"Unknown 'permissions' keys: {sorted(unknown)}. Allowed: {sorted(ALLOWED_PERMISSION_KEYS)}."
        )
    out.update({k: bool(v) for k, v in permissions.items()})
    return out


def normalize_on_collision(value: Optional[str]) -> str:
    """Validate ``on_collision`` and apply the default.

    Parameters
    ----------
    value : str or None
        One of ``"overwrite"`` / ``"rename"`` / ``"raise"``, or ``None``.

    Returns
    -------
    str
        The validated value, or ``"overwrite"`` when ``value`` is ``None``.

    Raises
    ------
    django.core.exceptions.ImproperlyConfigured
        When ``value`` is not one of the allowed strings.
    """
    if value is None:
        return DEFAULT_ON_COLLISION
    if value not in ALLOWED_ON_COLLISION:
        raise ImproperlyConfigured(f"on_collision must be one of {ALLOWED_ON_COLLISION}, got {value!r}")
    return value


def combine_permissions(parent: Mapping[str, bool], child: Mapping[str, bool]) -> dict[str, bool]:
    """AND-merge two normalized permission dicts (most restrictive wins)."""
    return {k: bool(parent[k]) and bool(child[k]) for k in DEFAULT_PERMISSIONS}


def combine_on_collision(parent: str, child: str) -> str:
    """Pick the most restrictive of two ``on_collision`` values.

    Severity order: ``raise`` > ``rename`` > ``overwrite``.
    """
    return parent if _COLLISION_SEVERITY[parent] >= _COLLISION_SEVERITY[child] else child
