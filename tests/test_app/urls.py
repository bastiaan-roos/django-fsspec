from django.urls import path
from test_app.views import FileUploadView

app_name = "test_app"

urlpatterns = [
    # test_app:file_upload
    path("test_app/file_upload/", FileUploadView.as_view(), name="file_upload"),
    path("test_app/file_upload/<int:pk>/", FileUploadView.as_view(), name="file_download"),
]
