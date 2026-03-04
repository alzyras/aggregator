from __future__ import annotations

from django.contrib.auth.models import AnonymousUser
from django.http import HttpResponseForbidden

from workspaces.models import Workspace, WorkspaceMember


class WorkspaceMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request.workspace = None
        user = request.user
        if isinstance(user, AnonymousUser) or not user.is_authenticated:
            return self.get_response(request)

        member = (
            WorkspaceMember.objects.select_related("workspace")
            .filter(user=user)
            .order_by("created_at")
            .first()
        )
        if not member:
            workspace = Workspace.objects.create(name=f"{user.username}'s workspace")
            member = WorkspaceMember.objects.create(
                workspace=workspace,
                user=user,
                role=WorkspaceMember.ROLE_OWNER,
            )

        request.workspace = member.workspace
        if not request.workspace:
            return HttpResponseForbidden("Workspace membership required.")

        return self.get_response(request)
