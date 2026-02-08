from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.shortcuts import get_object_or_404, render
from django.utils.dateparse import parse_date

from events.models import Event
from ingestion.providers import get_provider_choices


@login_required
def event_list(request):
    events = (
        Event.objects.for_workspace(request.workspace)
        .order_by("-start_time", "-created_at")
    )

    source = request.GET.get("source")
    source_entity_type = request.GET.get("type")
    start_date = parse_date(request.GET.get("start", ""))
    end_date = parse_date(request.GET.get("end", ""))

    if source:
        events = events.filter(source=source)
    if source_entity_type:
        events = events.filter(source_entity_type=source_entity_type)
    if start_date:
        events = events.filter(start_time__date__gte=start_date)
    if end_date:
        events = events.filter(start_time__date__lte=end_date)

    paginator = Paginator(events, 25)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    context = {
        "page_obj": page_obj,
        "source_choices": get_provider_choices(),
        "filters": {
            "source": source or "",
            "type": source_entity_type or "",
            "start": start_date.isoformat() if start_date else "",
            "end": end_date.isoformat() if end_date else "",
        },
    }
    return render(request, "events_list.html", context)


@login_required
def event_detail(request, pk):
    event = get_object_or_404(
        Event.objects.for_workspace(request.workspace), pk=pk
    )
    context = {
        "event": event,
    }
    return render(request, "events_detail.html", context)
