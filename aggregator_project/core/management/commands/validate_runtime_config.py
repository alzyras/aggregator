from __future__ import annotations

import os

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Validate production runtime configuration for web and worker roles."

    def add_arguments(self, parser):
        parser.add_argument("--strict", action="store_true")

    def handle(self, *args, **options):
        strict = bool(options["strict"])
        errors: list[str] = []
        warnings: list[str] = []
        role = os.getenv("APP_ROLE", "web")
        pg_host = os.getenv("PGHOST") or os.getenv("POSTGRES_HOST") or ""
        pg_user = os.getenv("PGUSER", "")
        pg_password = os.getenv("PGPASSWORD", "")
        db_mode = os.getenv("DB_DEPLOYMENT_MODE", "").lower()
        ssl_redirect = (os.getenv("DJANGO_SECURE_SSL_REDIRECT", "1").lower() in {"1", "true", "yes", "on"})
        trust_proxy_ssl = (
            os.getenv("DJANGO_TRUST_PROXY_SSL_HEADER", "0").lower() in {"1", "true", "yes", "on"}
        )
        cookie_secure = (
            os.getenv("DJANGO_SESSION_COOKIE_SECURE", "1").lower() in {"1", "true", "yes", "on"}
        )

        if role not in {"web", "worker"}:
            errors.append("APP_ROLE must be either 'web' or 'worker'.")
        if not os.getenv("ENCRYPTION_KEY"):
            errors.append("ENCRYPTION_KEY is required and must stay stable.")
        if not settings.DEBUG and settings.SECRET_KEY == "dev-insecure-secret-key":
            warnings.append("DJANGO_SECRET_KEY is using the development default while DJANGO_DEBUG=false.")
        if not pg_host:
            warnings.append("PGHOST/POSTGRES_HOST is not set explicitly; database selection may be ambiguous.")
        if db_mode == "external" and pg_host in {"", "postgres", "localhost", "127.0.0.1"}:
            errors.append("DB_DEPLOYMENT_MODE=external requires PGHOST to point at the external database host.")
        if db_mode == "built_in" and pg_host not in {"postgres", ""}:
            warnings.append("DB_DEPLOYMENT_MODE=built_in usually expects PGHOST=postgres.")
        if not db_mode:
            warnings.append("DB_DEPLOYMENT_MODE is not set; use 'built_in' or 'external' to document the DB choice.")
        if not settings.DEBUG and pg_user == "postgres" and pg_password == "postgres":
            warnings.append("Production deploy is using the default postgres/postgres database credentials.")
        if not settings.DEBUG and not ssl_redirect:
            warnings.append("DJANGO_SECURE_SSL_REDIRECT is disabled while DJANGO_DEBUG=false.")
        if ssl_redirect and not trust_proxy_ssl:
            warnings.append(
                "DJANGO_SECURE_SSL_REDIRECT is enabled but DJANGO_TRUST_PROXY_SSL_HEADER is disabled."
            )
        if ssl_redirect and not cookie_secure:
            warnings.append("Session cookies are not marked secure while HTTPS redirect is enabled.")

        for warning in warnings:
            self.stdout.write(self.style.WARNING(f"Warning: {warning}"))
        if errors or (strict and warnings):
            for error in errors:
                self.stderr.write(self.style.ERROR(f"Error: {error}"))
            if strict and warnings and not errors:
                raise CommandError("Runtime config warnings are fatal in strict mode.")
            raise CommandError("Runtime config validation failed.")
        self.stdout.write(self.style.SUCCESS("Runtime config validation passed."))
