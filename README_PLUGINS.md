# Connectors and Plugins

Aggregator has two deliberately separate extension systems.

- **Connectors** communicate with external services. They authenticate, fetch,
  normalize, and optionally write task changes back to the source.
- **Plugins** are workspace tools that operate on data already stored in
  Aggregator. Enabling a plugin can add its own navigation entry and UI.

## Connectors

Connector availability is controlled by `ENABLED_CONNECTORS`. Leave it empty to
enable every installed connector, or use a comma-separated allowlist such as:

```dotenv
ENABLED_CONNECTORS=asana,todoist,jira,habitica,github,linear,clickup,trello
```

Users add and edit accounts from `/connectors/`; credentials are encrypted with
`ENCRYPTION_KEY`. The current connectors are:

| Connector | Ingests tasks | Status writeback | Description writeback |
| --- | --- | --- | --- |
| Asana | Yes | Yes | Yes |
| Todoist | Yes | Yes | Yes |
| Jira Cloud / Server | Yes | Yes | Yes |
| Habitica | Yes | Yes | Yes |
| GitHub Issues | Yes | Open / close | Yes |
| Linear | Yes | Workflow-state mapping | Yes |
| ClickUp | Yes | Configured list statuses | Yes |
| Trello | Yes | Archive / configured lists | Yes |
| Google Fit | Activity events | No | No |

Every provider is an installed Django app under
`aggregator_project/providers/<provider>/`. Its `AppConfig.ready()` registers a
single `ProviderSpec`. A complete task provider owns these capabilities inside
its folder:

```text
providers/example/
  apps.py                  # registers ProviderSpec
  client.py                # fetches source records
  forms.py                 # account configuration
  normalizer.py            # emits canonical events
  settings.py              # credentials, preferences, source links
  verify.py                # verifies credentials
  sanitizer.py             # strips unsafe raw payload data
  status_writer.py         # status and description writeback
  planner_badges.py        # source-specific planner context
  templates/providers/example/
```

The central ingestion and connector apps consume the contract; they do not need
provider-specific branches. Provider tests should assert normalization,
pagination and filtering, exact writeback requests, verification, sanitization,
and connector form behavior.

## Workspace Intelligence

The first-class workspace intelligence area is not a plugin. It lives at
`/insights/` and keeps the cross-provider taxonomy in one place:

- imported tasks receive deterministic rule tags immediately;
- an owner or admin can select an OpenAI Responses model or a reachable
  OpenAI-compatible local Qwen endpoint for summaries and richer tags;
- `task_enrichment` jobs only send the current task content and use a content
  hash so stale jobs are ignored;
- manual tags are preserved when rules or AI classify a task again;
- `/insights/chat/` persists conversations per workspace and user, grounded in
  that user's visible tasks, tags, Planner state, and aggregate insight data.

OpenAI Responses requests set `store=false`. The local backend calls an
OpenAI-compatible `/v1/chat/completions` endpoint, so it works with common
Qwen serving stacks without embedding a model runtime in this application.

## Plugins

Plugin availability comes from installed `plugin.json` manifests. Installation
does not enable a plugin: a workspace owner enables each tool from `/plugins/`,
and its navigation entry appears only in that workspace.

### Built-in Plugins

**SQL Explorer** creates an isolated, read-only SQLite snapshot of the current
workspace's tasks. Queries are limited to one read statement and cannot access
the application database or another workspace.

**Data Chat** sends a size-capped task snapshot to the OpenAI Responses API only
after a user submits a question. Requests use `store=false`; task text is marked
as untrusted data. Configure it with:

```dotenv
OPENAI_API_KEY=...
OPENAI_CHAT_MODEL=...
```

**Activity Pulse** calculates provider distribution, planner workload,
completion, and task-aging signals locally without an external service.

### Plugin Contract

A plugin is self-contained under `aggregator_project/plugins/<plugin>/`:

```text
plugins/example/
  plugin.json
  apps.py
  urls.py
  views.py
  services.py
  templates/plugins/example/
  static/plugins/example/
  tests/
```

`plugin.json` identifies the Django app config. `AppConfig.ready()` registers a
`PluginSpec` with an ID, label, description, URL name, icon, order, and optional
configuration check. The platform discovers the manifest, installs the app,
mounts its URL configuration, enforces workspace activation, and builds the
navigation entry from that spec.

Plugin code must scope every query to `request.workspace`. Plugins should not
import another plugin's implementation or add special cases to the platform.
Cross-plugin capability belongs in a stable core service instead.

## Adding an Extension

For a connector:

1. Create the provider folder and `AppConfig`.
2. Implement and register one complete `ProviderSpec`.
3. Add the source choice and install the app.
4. Add provider-focused sync, lifecycle, writeback, and UI contract tests.

For a plugin:

1. Create the self-contained plugin folder and `plugin.json`.
2. Register one `PluginSpec` from its `AppConfig`.
3. Add routes, templates, static assets, and workspace-isolation tests inside
   that folder.
4. Confirm enable, disable, navigation, and direct-URL access behavior.
