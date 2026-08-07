from __future__ import annotations

from django.urls import path

from intelligence import views

app_name = "intelligence"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("settings/", views.ai_settings, name="settings"),
    path("analyze/", views.queue_analysis, name="queue_analysis"),
    path("tags/", views.tag_catalog, name="tags"),
    path("task/<int:item_id>/tags", views.update_task_tags, name="task_tags"),
    path("chat/", views.chat, name="chat"),
    path("chat/ask", views.chat_ask, name="chat_ask"),
    path("chat/thread/<int:thread_id>/delete", views.delete_chat_thread, name="delete_chat_thread"),
]
