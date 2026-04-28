"""Shared pytest fixtures and Django bootstrap.

Two responsibilities:

1. Ensure ``tests/`` is on ``sys.path`` so ``test_app`` imports as a
   top-level package (Django's INSTALLED_APPS uses the bare name, not
   ``tests.test_app``).
2. Configure Django settings exactly once for the whole test session.
   Doing this here (instead of inside an individual test module) keeps
   the suite extensible: any new Django-aware test file can rely on the
   settings already being configured rather than redoing
   ``settings.configure(...)`` and risking the "called twice" error.
"""

import sys
from pathlib import Path

import django
from django.conf import settings
from django.core.management import call_command

_TESTS_DIR = Path(__file__).parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

_TEST_DATA_DIR = _TESTS_DIR / "tmp"

if not settings.configured:
    settings.configure(
        DEBUG=True,
        INSTALLED_APPS=["test_app"],
        ROOT_URLCONF="urls",
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
            }
        },
        STORAGES={
            "default": {
                "BACKEND": "django_fsspec.FsspecStorage",
                "OPTIONS": {
                    # Some tests in this fixture rely on the rename-on-collision
                    # behavior (e.g. test_functions_part_two checks that a
                    # duplicate save returns a unique name). The package
                    # default is `overwrite`, so we make it explicit here.
                    "on_collision": "rename",
                    "storage_config": {
                        "protocol": "dir",
                        "path": _TEST_DATA_DIR,
                        "target_protocol": "local",
                        "target_options": {
                            "auto_mkdir": True,
                        },
                    },
                },
            }
        },
    )
    django.setup()
    call_command("migrate", "--run-syncdb", verbosity=0)
