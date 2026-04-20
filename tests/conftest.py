"""Ensure ``tests/`` is on ``sys.path`` so ``test_app`` imports as a top-level
package (Django's INSTALLED_APPS uses the bare name, not ``tests.test_app``).
"""

import sys
from pathlib import Path

_TESTS_DIR = Path(__file__).parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))
