from django.urls import path

from plugins.data_chat import views

app_name = "data_chat"

urlpatterns = [
    path("", views.index, name="index"),
    path("ask/", views.ask, name="ask"),
]
