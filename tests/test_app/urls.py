from django.urls import path

from tests.test_app.views import FileUploadView

urlpatterns = [
    # test_app:file_upload
    path("test_app/file_upload/", FileUploadView.as_view(), name="file_upload"),
    path("test_app/file_upload/<int:pk>/", FileUploadView.as_view(), name="file_download")
]
