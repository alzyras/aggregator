def slugify(value: str) -> str:
    """Simple slugify helper (keeps dependencies light)."""
    return (
        value.lower()
        .replace(" ", "-")
        .replace("/", "-")
        .replace("_", "-")
    )
