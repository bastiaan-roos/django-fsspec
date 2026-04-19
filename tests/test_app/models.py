from django.db import models


app_label = "test_app"


class FieldTestModel(models.Model):
    """Test model for testing fields."""

    file = models.FileField(upload_to="test/")
    extra_file = models.ImageField(upload_to="extra/")

    def __str__(self):
        return self.file.name

