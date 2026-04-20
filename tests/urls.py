"""Root URLconf used by ``tests/test_fsspec_storage.py``.

Wraps ``test_app.urls`` in an ``include(...)`` so the ``test_app``
namespace (declared via ``app_name`` in ``test_app/urls.py``) resolves
through ``reverse('test_app:...')`` in tests.
"""

from django.urls import include
from django.urls import path

urlpatterns = [
    path("", include("test_app.urls")),
]
