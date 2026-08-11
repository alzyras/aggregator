from django.urls import path

from plugins.daily_brief import views


app_name = "daily_brief"

urlpatterns = [path("", views.index, name="index")]
