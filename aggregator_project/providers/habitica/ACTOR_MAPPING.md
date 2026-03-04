# Habitica Actor Mapping

## Definition

`external_actor_id` is **provider-scoped**. Habitica user ids are stored directly without cross-provider identity unification.

## Habitica population

- Source: `GET /api/v3/user`
- `external_actor_id`: `data.id`
- `external_actor_display_name`: `data.profile.name` (fallback: `data.auth.local.username`)
- `external_actor_type`: `user`
- `external_actor_raw`: the raw Habitica user payload returned by `/user`

## Design note

Identity unification across providers is intentionally deferred by design.
