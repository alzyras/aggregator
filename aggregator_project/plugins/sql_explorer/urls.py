from django.urls import path

from plugins.sql_explorer import views

app_name = "sql_explorer"

urlpatterns = [
    path("", views.index, name="index"),
    path("query/", views.run_query, name="query"),
]
