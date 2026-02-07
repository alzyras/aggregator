from __future__ import annotations

import json

from django import template

register = template.Library()


@register.filter
def pretty_json(value) -> str:
    try:
        return json.dumps(value, indent=2, sort_keys=True)
    except TypeError:
        return str(value)
