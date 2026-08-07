from django.urls import path

from plugins.activity_pulse import views

app_name = "activity_pulse"

urlpatterns = [path("", views.index, name="index")]
