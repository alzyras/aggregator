from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BASE_DIR.parent

load_dotenv(REPO_ROOT / ".env")


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    value = _env(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


SECRET_KEY = _env("DJANGO_SECRET_KEY", "dev-insecure-secret-key")
DEBUG = _env_bool("DJANGO_DEBUG", default=False)

ALLOWED_HOSTS = [h.strip() for h in _env("DJANGO_ALLOWED_HOSTS", "").split(",") if h.strip()] or [
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
]
CSRF_TRUSTED_ORIGINS = [
    origin.strip()
    for origin in _env("DJANGO_CSRF_TRUSTED_ORIGINS", "").split(",")
    if origin.strip()
]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.sites",
    "allauth",
    "allauth.account",
    "workspaces",
    "core",
    "connectors",
    "ingestion",
    "events",
    "planner",
    "providers.asana.apps.AsanaProviderConfig",
    "providers.todoist.apps.TodoistProviderConfig",
    "providers.google_fit.apps.GoogleFitProviderConfig",
    "providers.habitica.apps.HabiticaProviderConfig",
    "providers.jira.apps.JiraProviderConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "allauth.account.middleware.AccountMiddleware",
    "core.middleware.WorkspaceMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "aggregator_project.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    }
]

WSGI_APPLICATION = "aggregator_project.wsgi.application"
ASGI_APPLICATION = "aggregator_project.asgi.application"


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _env("PGDATABASE") or _env("POSTGRES_DB", "aggregator"),
        "USER": _env("PGUSER") or _env("POSTGRES_USER", "postgres"),
        "PASSWORD": _env("PGPASSWORD") or _env("POSTGRES_PASSWORD", ""),
        "HOST": _env("PGHOST") or _env("POSTGRES_HOST", "localhost"),
        "PORT": _env("PGPORT") or _env("POSTGRES_PORT", "5432"),
    }
}


AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {
        "BACKEND": (
            "django.contrib.staticfiles.storage.StaticFilesStorage"
            if DEBUG
            else "whitenoise.storage.CompressedManifestStaticFilesStorage"
        )
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

ENCRYPTION_KEY = _env("ENCRYPTION_KEY")

JOB_MAX_CONCURRENCY = int(_env("JOB_MAX_CONCURRENCY", "4") or "4")
JOB_MAX_ATTEMPTS = int(_env("JOB_MAX_ATTEMPTS", "3") or "3")
JOB_STALE_RUNNING_SECONDS = int(_env("JOB_STALE_RUNNING_SECONDS", "900") or "900")
PLANNER_STATUS_WRITEBACK_MAX_RETRIES = int(_env("PLANNER_STATUS_WRITEBACK_MAX_RETRIES", "3") or "3")

PLANNER_AUTO_CREATE = _env("PLANNER_AUTO_CREATE", "true").lower() in {"1", "true", "yes"}
PLANNER_AUTO_COMPLETE = _env("PLANNER_AUTO_COMPLETE", "false").lower() in {"1", "true", "yes"}

SITE_ID = 1

AUTHENTICATION_BACKENDS = [
    "django.contrib.auth.backends.ModelBackend",
    "allauth.account.auth_backends.AuthenticationBackend",
]

LOGIN_URL = "account_login"
LOGIN_REDIRECT_URL = "plugins_view"
LOGOUT_REDIRECT_URL = "account_login"

ACCOUNT_AUTHENTICATION_METHOD = "username_email"
ACCOUNT_EMAIL_REQUIRED = True
ACCOUNT_USERNAME_REQUIRED = True
ACCOUNT_EMAIL_VERIFICATION = "none"
ACCOUNT_LOGOUT_ON_GET = False

USE_X_FORWARDED_HOST = _env_bool("DJANGO_USE_X_FORWARDED_HOST", default=False)

if _env_bool("DJANGO_TRUST_PROXY_SSL_HEADER", default=False):
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

SECURE_SSL_REDIRECT = _env_bool("DJANGO_SECURE_SSL_REDIRECT", default=not DEBUG)
SESSION_COOKIE_SECURE = _env_bool("DJANGO_SESSION_COOKIE_SECURE", default=SECURE_SSL_REDIRECT)
CSRF_COOKIE_SECURE = _env_bool("DJANGO_CSRF_COOKIE_SECURE", default=SECURE_SSL_REDIRECT)
SECURE_HSTS_SECONDS = int(_env("DJANGO_SECURE_HSTS_SECONDS", "0") or "0")
SECURE_HSTS_INCLUDE_SUBDOMAINS = _env_bool("DJANGO_SECURE_HSTS_INCLUDE_SUBDOMAINS", default=False)
SECURE_HSTS_PRELOAD = _env_bool("DJANGO_SECURE_HSTS_PRELOAD", default=False)
SECURE_REFERRER_POLICY = _env("DJANGO_SECURE_REFERRER_POLICY", "same-origin") or "same-origin"
SECURE_CONTENT_TYPE_NOSNIFF = _env_bool("DJANGO_SECURE_CONTENT_TYPE_NOSNIFF", default=True)

if DEBUG:
    EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
