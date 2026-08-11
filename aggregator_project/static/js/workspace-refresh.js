(() => {
  const POLL_INTERVAL_MS = 2500;
  const POLL_TIMEOUT_MS = 120000;
  const activePolls = new WeakMap();

  function csrfToken() {
    const value = document.cookie
      .split(";")
      .map((part) => part.trim())
      .find((part) => part.startsWith("csrftoken="))
      ?.split("=")[1];
    return decodeURIComponent(value || "");
  }

  async function requestJson(url, options = {}) {
    const response = await fetch(url, {
      cache: "no-store",
      headers: {
        Accept: "application/json",
        ...options.headers,
      },
      ...options,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.error || "Unable to refresh sources.");
    return payload;
  }

  function plural(count, singular, pluralForm = `${singular}s`) {
    return `${count} ${count === 1 ? singular : pluralForm}`;
  }

  function presenceLabel(payload) {
    if (payload.is_refreshing || payload.refreshing_count) {
      return `Refreshing ${plural(Number(payload.refreshing_count || 0), "source")}`;
    }
    if (payload.failed_count) {
      return `${plural(Number(payload.failed_count), "source")} needs attention`;
    }
    if (payload.all_checked_at) return "Updated just now";
    if (payload.has_connected_sources) return "Awaiting first refresh";
    return "Connect a source to refresh";
  }

  function updatePresence(root, payload) {
    const presence = root?.querySelector("[data-refresh-presence]");
    if (!presence) return;
    const isStale = Boolean(payload.stale_count || payload.failed_count || !payload.has_connected_sources);
    presence.classList.toggle("is-stale", isStale);
    presence.setAttribute("aria-live", "polite");
    if (payload.all_checked_at) presence.title = payload.all_checked_at;
    const label = presence.querySelector("span:last-child");
    if (label) label.textContent = presenceLabel(payload);
  }

  function notify(message, tone = "success") {
    window.dispatchEvent(new CustomEvent("app:toast", {detail: {message, tone}}));
  }

  function reloadWhenSafe(root) {
    if (root.dataset.refreshReloadScheduled === "true") return;
    const openDialog = root.querySelector("dialog[open]");
    if (openDialog) {
      root.dataset.refreshReady = "true";
      notify("Sources updated. Close the editor to load the latest data.");
      openDialog.addEventListener(
        "close",
        () => {
          if (root.dataset.refreshReady === "true") reloadWhenSafe(root);
        },
        {once: true},
      );
      return;
    }
    root.dataset.refreshReloadScheduled = "true";
    window.setTimeout(() => window.location.reload(), 350);
  }

  function finishRefresh(root, payload, initialCacheVersion) {
    updatePresence(root, payload);
    root.dataset.refreshCacheVersion = String(payload.cache_version || initialCacheVersion);
    const dataChanged = Number(payload.cache_version || 0) > Number(initialCacheVersion || 0);
    if (dataChanged) {
      notify("Sources updated. Loading the latest data.");
      if (root.dataset.refreshAutoload === "true") reloadWhenSafe(root);
      return;
    }
    if (payload.failed_count) {
      notify("Refresh finished with a source that needs attention.", "error");
    } else {
      notify("Sources are up to date.");
    }
  }

  function pollRefresh(root, initialCacheVersion) {
    const existing = activePolls.get(root);
    if (existing) return existing;
    const stateUrl = root?.dataset.refreshStateUrl;
    if (!stateUrl) return Promise.resolve(null);

    const poll = (async () => {
      const deadline = Date.now() + POLL_TIMEOUT_MS;
      while (Date.now() < deadline) {
        await new Promise((resolve) => window.setTimeout(resolve, POLL_INTERVAL_MS));
        try {
          const payload = await requestJson(stateUrl);
          updatePresence(root, payload);
          if (!payload.is_refreshing) {
            finishRefresh(root, payload, initialCacheVersion);
            return payload;
          }
        } catch (error) {
          notify(error.message || "Unable to check refresh progress.", "error");
          return null;
        }
      }
      notify("Refresh is still running. The latest data will appear when it finishes.");
      return null;
    })();
    activePolls.set(root, poll);
    void poll.then(
      () => activePolls.delete(root),
      () => activePolls.delete(root),
    );
    return poll;
  }

  async function queueAndTrack(root, button) {
    if (!root?.dataset.refreshUrl) return null;
    const initialCacheVersion = Number(root.dataset.refreshCacheVersion || 0);
    if (button) {
      button.disabled = true;
      button.classList.add("is-loading");
      button.setAttribute("aria-busy", "true");
    }
    try {
      const payload = await requestJson(root.dataset.refreshUrl, {
        method: "POST",
        headers: {"X-CSRFToken": csrfToken()},
      });
      updatePresence(root, payload);
      if (payload.queued) {
        notify("Refresh queued.");
        void pollRefresh(root, initialCacheVersion);
      } else if (payload.is_refreshing || payload.refreshing_count) {
        notify("Refresh is already in progress.");
        void pollRefresh(root, initialCacheVersion);
      } else if (!payload.has_connected_sources) {
        notify("Connect a source before refreshing.", "error");
      } else {
        finishRefresh(root, payload, initialCacheVersion);
      }
      return payload;
    } catch (error) {
      notify(error.message || "Unable to refresh sources.", "error");
      return null;
    } finally {
      if (button) {
        button.disabled = false;
        button.classList.remove("is-loading");
        button.removeAttribute("aria-busy");
      }
    }
  }

  window.workspaceRefresh = {queueAndTrack, updatePresence, pollRefresh};
})();
