from django.urls import path

from plugins.workload_radar import views


app_name = "workload_radar"

urlpatterns = [path("", views.index, name="index")]
