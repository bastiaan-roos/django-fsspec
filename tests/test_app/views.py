from django.http import JsonResponse, FileResponse
from django.views.generic import View

from tests.test_app.models import FieldTestModel


class FileUploadView(View):
    """
    A view that handles file uploads.
    """

    def post(self, request, *args, **kwargs):
        """
        Handles the POST request for file uploads.
        """
        # Handle file upload logic here
        requested_file = request.FILES.get("file")
        if not requested_file:
            return JsonResponse({"error": "No file provided."}, status=400)
        # Save the file to the desired location
        file_name = request.POST.get("filename", requested_file.name)
        # create instance FieldTestModel and save file to field file
        # using chunked upload
        test_model = FieldTestModel()
        test_model.file.save(file_name, requested_file, save=True)

        return JsonResponse({
            "message": "File uploaded successfully.",
            "id": test_model.id,
            "file_path": test_model.file.path,
        }, status=201)

    def get(self, pk, request, *args, **kwargs):
        """
        Handles the GET request for file uploads.
        """
        # Handle file upload logic here
        test_model = FieldTestModel.objects.get(pk=pk)
        if not test_model:
            return JsonResponse({"error": "Object does not exists"}, status=400)
        # Save the file to the desired location
        file_name = test_model.file.name
        # create instance FieldTestModel and save file to field file
        # using chunked upload

        return FileResponse(
            test_model.file,
            content_type="application/octet-stream",
            as_attachment=True,
            filename=file_name,
        )