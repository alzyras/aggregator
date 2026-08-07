(() => {
  const root = document.querySelector("[data-planner-root]");
  if (!root) return;

  const csrfToken = (document.cookie.match(/csrftoken=([^;]+)/) || [])[1] || "";
  const tabs = Array.from(root.querySelectorAll("[data-tab]"));
  const panels = Array.from(root.querySelectorAll("[data-tab-panel]"));
  const searchInput = root.querySelector("[data-task-search]");
  const sourceFilter = root.querySelector("[data-source-filter]");
  const sortControl = root.querySelector("[data-sort-control]");
  const clearFiltersButton = root.querySelector("[data-clear-filters]");
  const resultCount = root.querySelector("[data-result-count]");
  const batchSize = Number(root.dataset.batchSize || 32);
  const visibleLimits = new Map();
  const pendingReorders = new Map();
  let reorderTimer = null;
  let dragItem = null;
  let dragList = null;
  let dragOriginalNext = null;
  let iconRenderQueued = false;

  function renderIconsSoon() {
    if (iconRenderQueued) return;
    iconRenderQueued = true;
    window.requestAnimationFrame(() => {
      iconRenderQueued = false;
      window.renderLucideIcons?.();
    });
  }

  function toast(message, tone = "success") {
    window.dispatchEvent(new CustomEvent("app:toast", {detail: {message, tone}}));
  }

  async function postJson(url, payload = {}) {
    const response = await fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": csrfToken,
        "Accept": "application/json",
      },
      body: JSON.stringify(payload),
    });
    let body = {};
    try {
      body = await response.json();
    } catch (_error) {
      body = {};
    }
    if (!response.ok) {
      throw new Error(body.error || "The update could not be saved.");
    }
    return body;
  }

  function activeStatus() {
    return tabs.find((tab) => tab.classList.contains("is-active"))?.dataset.tab || "inbox";
  }

  function listForStatus(status) {
    return root.querySelector(`.planner-list[data-status='${status}']`);
  }

  function filtersActive() {
    return Boolean(searchInput?.value.trim() || sourceFilter?.value);
  }

  function rowMatches(row) {
    const query = searchInput?.value.trim().toLocaleLowerCase() || "";
    const source = sourceFilter?.value || "";
    if (source && row.dataset.source !== source) return false;
    if (query && !(row.dataset.search || "").includes(query)) return false;
    return true;
  }

  function compareRows(a, b, status) {
    const sort = sortControl?.value || "default";
    if (sort === "title") {
      return (a.dataset.title || "").localeCompare(b.dataset.title || "", undefined, {sensitivity: "base"});
    }
    if (sort === "source") {
      const sourceOrder = (a.dataset.source || "").localeCompare(b.dataset.source || "");
      return sourceOrder || (a.dataset.title || "").localeCompare(b.dataset.title || "");
    }
    if (sort === "oldest" || sort === "newest" || (sort === "default" && status === "inbox")) {
      const direction = sort === "oldest" ? 1 : -1;
      const dateA = Number(a.dataset.createdSort || 0);
      const dateB = Number(b.dataset.createdSort || 0);
      if (dateA !== dateB) return (dateA - dateB) * direction;
      return (Number(a.dataset.itemId) - Number(b.dataset.itemId)) * direction;
    }
    return 0;
  }

  function sortList(list) {
    if (!list) return;
    const status = list.dataset.status;
    const sort = sortControl?.value || "default";
    if (sort === "default" && status !== "inbox") return;
    const rows = Array.from(list.querySelectorAll(".planner-row"));
    rows.sort((a, b) => compareRows(a, b, status));
    rows.forEach((row) => list.appendChild(row));
  }

  function updateDraggability() {
    const canReorder = (sortControl?.value || "default") === "default" && !filtersActive();
    root.querySelectorAll(".planner-row").forEach((row) => {
      const enabled = canReorder && row.dataset.status !== "inbox" && Boolean(row.dataset.stateId);
      row.draggable = enabled;
      row.classList.toggle("is-reorderable", enabled);
      const handle = row.querySelector("[data-drag-handle]");
      if (handle) handle.disabled = !enabled;
    });
  }

  function refreshList(status, {resetLimit = false} = {}) {
    const list = listForStatus(status);
    if (!list) return {eligible: 0, total: 0};
    sortList(list);
    const rows = Array.from(list.querySelectorAll(".planner-row"));
    if (resetLimit || !visibleLimits.has(status)) visibleLimits.set(status, batchSize);
    const limit = visibleLimits.get(status);
    let eligibleIndex = 0;
    rows.forEach((row) => {
      const matches = rowMatches(row);
      row.classList.toggle("is-filtered-out", !matches);
      if (!matches) {
        row.classList.remove("is-deferred");
        return;
      }
      row.classList.toggle("is-deferred", eligibleIndex >= limit);
      eligibleIndex += 1;
    });

    const panel = root.querySelector(`[data-tab-panel='${status}']`);
    const empty = panel?.querySelector("[data-empty-status]");
    const more = panel?.querySelector("[data-load-more]");
    empty?.classList.toggle("is-hidden", eligibleIndex > 0);
    if (more) {
      const remaining = Math.max(eligibleIndex - limit, 0);
      more.classList.toggle("is-hidden", remaining === 0);
      more.textContent = remaining ? `Show ${Math.min(batchSize, remaining)} more` : "";
    }
    return {eligible: eligibleIndex, total: rows.length};
  }

  function refreshAll(options = {}) {
    const counts = new Map();
    panels.forEach((panel) => {
      const status = panel.dataset.tabPanel;
      counts.set(status, refreshList(status, options));
    });
    const hasFilters = filtersActive();
    tabs.forEach((tab) => {
      const count = counts.get(tab.dataset.tab) || {eligible: 0, total: 0};
      const badge = tab.querySelector("[data-count-status]");
      if (!badge) return;
      const nextText = hasFilters ? `${count.eligible}/${count.total}` : String(count.total);
      if (badge.textContent.trim() !== nextText) {
        badge.textContent = nextText;
        badge.classList.remove("count-bump");
        void badge.offsetWidth;
        badge.classList.add("count-bump");
      }
    });
    const current = counts.get(activeStatus()) || {eligible: 0};
    if (resultCount) resultCount.textContent = `${current.eligible} ${current.eligible === 1 ? "task" : "tasks"}`;
    clearFiltersButton?.classList.toggle("is-hidden", !hasFilters && (sortControl?.value || "default") === "default");
    updateDraggability();
  }

  function activateTab(status, {persist = true} = {}) {
    if (!tabs.some((tab) => tab.dataset.tab === status)) status = "inbox";
    tabs.forEach((tab) => {
      const active = tab.dataset.tab === status;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", String(active));
      tab.tabIndex = active ? 0 : -1;
    });
    panels.forEach((panel) => panel.classList.toggle("is-active", panel.dataset.tabPanel === status));
    if (persist) {
      window.sessionStorage.setItem("aggregator:planner-tab", status);
      history.replaceState(null, "", `${window.location.pathname}${window.location.search}#${status}`);
    }
    refreshAll();
  }

  function showPlannerError(message) {
    const errorBox = root.querySelector("[data-planner-error]");
    if (errorBox) {
      errorBox.textContent = message;
      errorBox.classList.add("is-visible");
    }
    toast(message, "error");
  }

  function clearPlannerError() {
    const errorBox = root.querySelector("[data-planner-error]");
    if (!errorBox) return;
    errorBox.textContent = "";
    errorBox.classList.remove("is-visible");
  }

  function moveRow(row, list, atTop = true) {
    if (!row || !list) return;
    if (atTop) list.prepend(row);
    else list.appendChild(row);
  }

  function restoreRow(row, list, next) {
    if (!row || !list) return;
    if (next?.parentElement === list) list.insertBefore(row, next);
    else list.appendChild(row);
  }

  function statusLabel(status) {
    return {inbox: "Inbox", backlog: "To do", doing: "In progress", done: "Done"}[status] || status;
  }

  function syncStatusControls(row, status) {
    row.querySelectorAll("[data-status-choice]").forEach((button) => {
      const active = button.dataset.statusChoice === status;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    const select = row.querySelector("[data-planner-status-id]");
    if (select) select.value = status;
  }

  function updateWriteback(row, status, message = "") {
    const normalized = status || "none";
    row.dataset.writebackStatus = normalized;
    const badge = row.querySelector("[data-writeback-badge]");
    const messageElement = row.querySelector("[data-writeback-message]");
    const labels = {pending: "Saving", failed: "Sync failed", unsupported: "Local only", none: "Local", synced: ""};
    if (badge) {
      badge.className = `task-sync-state ${normalized}`;
      badge.textContent = labels[normalized] ?? labels.none;
      badge.classList.toggle("is-hidden", normalized === "synced");
    }
    if (messageElement) {
      const visibleMessage = normalized === "failed" ? message : "";
      messageElement.textContent = visibleMessage;
      messageElement.classList.toggle("is-visible", Boolean(visibleMessage));
    }
    row.querySelector("[data-retry-id]")?.classList.toggle("is-hidden", normalized !== "failed");
    row.querySelector("[data-revert-id]")?.classList.toggle("is-hidden", normalized !== "failed");
  }

  function applyStatusPayload(row, payload) {
    if (payload.item_id) row.dataset.itemId = String(payload.item_id);
    if (payload.state_id) row.dataset.stateId = String(payload.state_id);
    if (payload.planner_status) {
      row.dataset.status = payload.planner_status;
      syncStatusControls(row, payload.planner_status);
    }
    if (typeof payload.pinned === "boolean") row.dataset.pinned = String(payload.pinned);
    row.querySelectorAll("[data-item-action]").forEach((element) => {
      element.dataset.itemAction = row.dataset.itemId;
    });
    const pin = row.querySelector("[data-pin-id]");
    const handle = row.querySelector("[data-drag-handle]");
    if (pin && row.dataset.stateId) pin.disabled = false;
    if (handle && row.dataset.stateId) handle.disabled = false;
    updatePinButton(row);
    updateWriteback(row, payload.writeback_status, payload.writeback_message);
  }

  async function persistStatusMove(row, targetStatus) {
    if (!row || row.dataset.busy === "true" || row.dataset.status === targetStatus) return;
    const originalList = row.parentElement;
    const originalNext = row.nextElementSibling;
    const originalStatus = row.dataset.status;
    const originalWriteback = row.dataset.writebackStatus || "none";
    const originalMessage = row.querySelector("[data-writeback-message]")?.textContent || "";
    const targetList = listForStatus(targetStatus);
    if (!targetList) return;

    row.dataset.busy = "true";
    row.setAttribute("aria-busy", "true");
    row.dataset.status = targetStatus;
    syncStatusControls(row, targetStatus);
    moveRow(row, targetList);
    row.classList.add("just-moved");
    updateWriteback(row, "pending");
    clearPlannerError();
    refreshAll();

    try {
      const payload = await postJson(`/planner/item/${row.dataset.itemId}/planner-status`, {planner_status: targetStatus});
      applyStatusPayload(row, payload);
      queueReorder(targetList, row.dataset.pinned === "true");
      if (originalList !== targetList) queueReorder(originalList, row.dataset.pinned === "true");
      toast(`Moved to ${statusLabel(targetStatus)}`);
    } catch (error) {
      row.dataset.status = originalStatus;
      syncStatusControls(row, originalStatus);
      restoreRow(row, originalList, originalNext);
      updateWriteback(row, originalWriteback, originalMessage);
      showPlannerError(error.message);
    } finally {
      row.dataset.busy = "false";
      row.removeAttribute("aria-busy");
      window.setTimeout(() => row.classList.remove("just-moved"), 360);
      refreshAll();
    }
  }

  function blockPayload(list, pinned) {
    if (!list) return null;
    const ids = Array.from(list.querySelectorAll(".planner-row"))
      .filter((row) => row.dataset.stateId)
      .filter((row) => (row.dataset.pinned === "true") === pinned)
      .map((row) => row.dataset.stateId);
    if (!ids.length) return null;
    return {block_order: ids, planner_status: list.dataset.status};
  }

  function queueReorder(list, pinned) {
    const payload = blockPayload(list, pinned);
    if (!payload) return;
    pendingReorders.set(`${payload.planner_status}:${pinned}`, payload);
    window.clearTimeout(reorderTimer);
    reorderTimer = window.setTimeout(flushReorders, 250);
  }

  async function flushReorders() {
    const payloads = Array.from(pendingReorders.values());
    pendingReorders.clear();
    for (const payload of payloads) {
      try {
        await postJson(root.dataset.reorderUrl, payload);
      } catch (error) {
        showPlannerError(error.message);
      }
    }
  }

  function updateDescriptionWriteback(row, status, message = "") {
    const normalized = status || "none";
    const badge = row.querySelector("[data-description-writeback-badge]");
    const messageElement = row.querySelector("[data-description-writeback-message]");
    const labels = {pending: "Saving", failed: "Sync failed", unsupported: "Local only", none: "", synced: "Saved"};
    if (badge) {
      badge.className = `task-sync-state description ${normalized}`;
      badge.textContent = labels[normalized] ?? "";
      badge.classList.toggle("is-hidden", normalized === "none");
      if (normalized === "synced") window.setTimeout(() => badge.classList.add("is-hidden"), 1500);
    }
    if (messageElement) {
      const visibleMessage = normalized === "failed" ? message : "";
      messageElement.textContent = visibleMessage;
      messageElement.classList.toggle("is-visible", Boolean(visibleMessage));
    }
    row.querySelector("[data-description-retry-id]")?.classList.toggle("is-hidden", normalized !== "failed");
  }

  function autoSize(textarea) {
    textarea.style.height = "auto";
    textarea.style.height = `${Math.min(Math.max(textarea.scrollHeight, 72), 280)}px`;
  }

  function setDescriptionDirty(row) {
    const textarea = row.querySelector("[data-description-input]");
    const save = row.querySelector("[data-description-save-id]");
    if (!textarea || !save) return;
    const dirty = textarea.value !== (textarea.dataset.originalValue || "");
    save.disabled = !dirty;
    save.classList.toggle("is-dirty", dirty);
    row.classList.toggle("has-unsaved-description", dirty);
    const preview = row.querySelector(".task-description-preview");
    if (preview) {
      const normalized = textarea.value.trim().replace(/\s+/g, " ");
      preview.textContent = normalized ? `${normalized.slice(0, 96)}${normalized.length > 96 ? "..." : ""}` : "Add description";
    }
    row.dataset.search = [row.dataset.title, row.dataset.source, row.dataset.connector, textarea.value]
      .filter(Boolean)
      .join(" ")
      .toLocaleLowerCase();
  }

  async function saveDescription(row) {
    const textarea = row?.querySelector("[data-description-input]");
    const save = row?.querySelector("[data-description-save-id]");
    if (!row || !textarea || !save || save.disabled) return;
    save.disabled = true;
    row.dataset.descriptionBusy = "true";
    updateDescriptionWriteback(row, "pending");
    try {
      const payload = await postJson(`/planner/item/${row.dataset.itemId}/description`, {description: textarea.value});
      textarea.value = payload.description ?? textarea.value;
      textarea.dataset.originalValue = textarea.value;
      row.classList.remove("has-unsaved-description");
      updateDescriptionWriteback(row, payload.description_writeback_status, payload.description_writeback_message);
      toast("Description saved");
    } catch (error) {
      updateDescriptionWriteback(row, "failed", error.message);
      showPlannerError(error.message);
    } finally {
      row.dataset.descriptionBusy = "false";
      setDescriptionDirty(row);
    }
  }

  function cleanManualTags(value) {
    const tags = value
      .split(",")
      .map((tag) => tag.trim().replace(/\s+/g, " "))
      .filter(Boolean);
    return Array.from(new Map(tags.map((tag) => [tag.toLocaleLowerCase(), tag])).values());
  }

  function setTagsDirty(row) {
    const input = row.querySelector("[data-tags-input]");
    const save = row.querySelector("[data-tags-save-id]");
    if (!input || !save) return;
    const normalized = cleanManualTags(input.value).join(", ");
    const original = cleanManualTags(input.dataset.originalValue || "").join(", ");
    const dirty = normalized !== original;
    save.disabled = !dirty;
    row.classList.toggle("has-unsaved-tags", dirty);
  }

  function renderTaskTags(row, tags) {
    const target = row.querySelector("[data-task-tags]");
    if (!target) return;
    target.replaceChildren();
    (tags || []).forEach((tag) => {
      const element = document.createElement("span");
      element.className = "task-tag";
      element.textContent = tag.tag__name || "";
      element.style.setProperty("--task-tag-color", tag.tag__color || "#477a64");
      target.append(element);
    });
    target.classList.toggle("is-hidden", !target.childElementCount);
  }

  async function saveManualTags(row) {
    const input = row?.querySelector("[data-tags-input]");
    const save = row?.querySelector("[data-tags-save-id]");
    const message = row?.querySelector("[data-tags-message]");
    if (!row || !input || !save || save.disabled) return;
    const tags = cleanManualTags(input.value);
    save.disabled = true;
    if (message) message.textContent = "";
    try {
      const payload = await postJson(`/insights/task/${row.dataset.itemId}/tags`, {tags});
      input.value = tags.join(", ");
      input.dataset.originalValue = input.value;
      row.classList.remove("has-unsaved-tags");
      renderTaskTags(row, payload.tags);
      row.dataset.search = [
        row.dataset.title,
        row.dataset.source,
        row.dataset.connector,
        row.querySelector("[data-description-input]")?.value,
        ...(payload.tags || []).map((tag) => tag.tag__name),
      ]
        .filter(Boolean)
        .join(" ")
        .toLocaleLowerCase();
      toast("Tags saved");
    } catch (error) {
      if (message) message.textContent = error.message;
      showPlannerError(error.message);
    } finally {
      setTagsDirty(row);
    }
  }

  function updatePinButton(row) {
    const button = row.querySelector("[data-pin-id]");
    if (!button) return;
    const pinned = row.dataset.pinned === "true";
    button.classList.toggle("is-active", pinned);
    button.innerHTML = `<i data-lucide="${pinned ? "pin-off" : "pin"}" aria-hidden="true">${pinned ? "★" : "☆"}</i>`;
    button.title = pinned ? "Unpin task" : "Pin task";
    button.setAttribute("aria-label", button.title);
    renderIconsSoon();
  }

  async function togglePin(row) {
    const button = row?.querySelector("[data-pin-id]");
    if (!row || !button || button.disabled) return;
    const previous = row.dataset.pinned === "true";
    row.dataset.pinned = String(!previous);
    updatePinButton(row);
    if (!previous) row.parentElement?.prepend(row);
    refreshAll();
    button.disabled = true;
    try {
      const payload = await postJson(`/planner/item/${row.dataset.itemId}/pin`);
      row.dataset.pinned = String(payload.pinned);
      updatePinButton(row);
      toast(payload.pinned ? "Task pinned" : "Task unpinned");
    } catch (error) {
      row.dataset.pinned = String(previous);
      updatePinButton(row);
      showPlannerError(error.message);
    } finally {
      button.disabled = false;
      refreshAll();
    }
  }

  async function retryStatus(row) {
    updateWriteback(row, "pending");
    try {
      const payload = await postJson(`/planner/item/${row.dataset.itemId}/writeback/retry`);
      applyStatusPayload(row, payload);
      toast("Sync queued");
    } catch (error) {
      updateWriteback(row, "failed", error.message);
      showPlannerError(error.message);
    }
  }

  async function revertStatus(row) {
    try {
      const payload = await postJson(`/planner/item/${row.dataset.itemId}/writeback/revert`);
      const target = listForStatus(payload.planner_status);
      moveRow(row, target);
      applyStatusPayload(row, payload);
      refreshAll();
      toast("Status reverted");
    } catch (error) {
      showPlannerError(error.message);
    }
  }

  async function retryDescription(row) {
    updateDescriptionWriteback(row, "pending");
    try {
      const payload = await postJson(`/planner/item/${row.dataset.itemId}/description/writeback/retry`);
      updateDescriptionWriteback(row, payload.description_writeback_status, payload.description_writeback_message);
      toast("Description sync queued");
    } catch (error) {
      updateDescriptionWriteback(row, "failed", error.message);
      showPlannerError(error.message);
    }
  }

  root.addEventListener("click", async (event) => {
    const tab = event.target.closest("[data-tab]");
    if (tab) {
      activateTab(tab.dataset.tab);
      return;
    }
    const statusButton = event.target.closest("[data-status-choice]");
    if (statusButton) {
      await persistStatusMove(statusButton.closest(".planner-row"), statusButton.dataset.statusChoice);
      return;
    }
    const loadMore = event.target.closest("[data-load-more]");
    if (loadMore) {
      const status = loadMore.closest("[data-tab-panel]")?.dataset.tabPanel;
      visibleLimits.set(status, (visibleLimits.get(status) || batchSize) + batchSize);
      refreshAll();
      return;
    }
    const clear = event.target.closest("[data-clear-filters]");
    if (clear) {
      searchInput.value = "";
      sourceFilter.value = "";
      sortControl.value = "default";
      refreshAll({resetLimit: true});
      searchInput.focus();
      return;
    }
    const save = event.target.closest("[data-description-save-id]");
    if (save) {
      await saveDescription(save.closest(".planner-row"));
      return;
    }
    const tagsSave = event.target.closest("[data-tags-save-id]");
    if (tagsSave) {
      await saveManualTags(tagsSave.closest(".planner-row"));
      return;
    }
    const pin = event.target.closest("[data-pin-id]");
    if (pin) {
      await togglePin(pin.closest(".planner-row"));
      return;
    }
    const retry = event.target.closest("[data-retry-id]");
    if (retry) {
      await retryStatus(retry.closest(".planner-row"));
      return;
    }
    const revert = event.target.closest("[data-revert-id]");
    if (revert) {
      await revertStatus(revert.closest(".planner-row"));
      return;
    }
    const descriptionRetry = event.target.closest("[data-description-retry-id]");
    if (descriptionRetry) await retryDescription(descriptionRetry.closest(".planner-row"));
  });

  root.addEventListener("change", async (event) => {
    if (event.target.matches("[data-planner-status-id]")) {
      await persistStatusMove(event.target.closest(".planner-row"), event.target.value);
    }
  });

  root.addEventListener("input", (event) => {
    if (event.target.matches("[data-description-input]")) {
      autoSize(event.target);
      setDescriptionDirty(event.target.closest(".planner-row"));
      return;
    }
    if (event.target.matches("[data-tags-input]")) {
      setTagsDirty(event.target.closest(".planner-row"));
      return;
    }
    if (event.target.matches("[data-task-search]")) refreshAll({resetLimit: true});
  });

  root.addEventListener("keydown", async (event) => {
    if (event.target.matches("[data-description-input]") && (event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      await saveDescription(event.target.closest(".planner-row"));
    }
    if (event.target.matches("[data-tags-input]") && (event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      await saveManualTags(event.target.closest(".planner-row"));
    }
    if (event.target.matches("[data-tab]") && ["ArrowLeft", "ArrowRight"].includes(event.key)) {
      event.preventDefault();
      const index = tabs.indexOf(event.target);
      const offset = event.key === "ArrowRight" ? 1 : -1;
      const next = tabs[(index + offset + tabs.length) % tabs.length];
      activateTab(next.dataset.tab);
      next.focus();
    }
  });

  sourceFilter?.addEventListener("change", () => refreshAll({resetLimit: true}));
  sortControl?.addEventListener("change", () => refreshAll({resetLimit: true}));

  document.addEventListener("keydown", (event) => {
    const typing = ["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement?.tagName);
    if (event.key === "/" && !typing) {
      event.preventDefault();
      searchInput?.focus();
    }
    if (event.key === "Escape" && document.activeElement === searchInput && searchInput.value) {
      searchInput.value = "";
      refreshAll({resetLimit: true});
    }
  });

  root.querySelectorAll(".planner-list").forEach((list) => {
    list.addEventListener("dragstart", (event) => {
      const row = event.target.closest(".planner-row.is-reorderable");
      if (!row) {
        event.preventDefault();
        return;
      }
      dragItem = row;
      dragList = list;
      dragOriginalNext = row.nextElementSibling;
      row.classList.add("dragging");
      tabs.forEach((item) => item.classList.add("is-dragging"));
      event.dataTransfer?.setData("text/plain", row.dataset.stateId);
      if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
    });
    list.addEventListener("dragover", (event) => {
      if (!dragItem || dragList !== list) return;
      const target = event.target.closest(".planner-row.is-reorderable");
      if (!target || target === dragItem || target.dataset.pinned !== dragItem.dataset.pinned) return;
      event.preventDefault();
      const after = event.clientY > target.getBoundingClientRect().top + target.offsetHeight / 2;
      target.parentElement.insertBefore(dragItem, after ? target.nextSibling : target);
    });
    list.addEventListener("drop", (event) => {
      if (!dragItem || dragList !== list) return;
      event.preventDefault();
      queueReorder(list, dragItem.dataset.pinned === "true");
    });
    list.addEventListener("dragend", () => {
      dragItem?.classList.remove("dragging");
      tabs.forEach((item) => item.classList.remove("is-dragging", "is-drop-target"));
      dragItem = null;
      dragList = null;
      dragOriginalNext = null;
    });
  });

  tabs.forEach((tab) => {
    tab.addEventListener("dragover", (event) => {
      if (!dragItem) return;
      event.preventDefault();
      tab.classList.add("is-drop-target");
      if (event.dataTransfer) event.dataTransfer.dropEffect = "move";
    });
    tab.addEventListener("dragleave", () => tab.classList.remove("is-drop-target"));
    tab.addEventListener("drop", async (event) => {
      event.preventDefault();
      tab.classList.remove("is-drop-target");
      if (!dragItem || dragItem.dataset.status === tab.dataset.tab) return;
      await persistStatusMove(dragItem, tab.dataset.tab);
    });
  });

  root.querySelectorAll("[data-description-input]").forEach((textarea) => {
    textarea.dataset.originalValue = textarea.value;
    autoSize(textarea);
    setDescriptionDirty(textarea.closest(".planner-row"));
  });
  root.querySelectorAll("[data-tags-input]").forEach((input) => {
    input.dataset.originalValue = input.value;
    setTagsDirty(input.closest(".planner-row"));
  });
  root.querySelectorAll(".planner-row").forEach(updatePinButton);

  const refreshButton = root.querySelector("[data-refresh-sources]");
  refreshButton?.addEventListener("click", async () => {
    refreshButton.disabled = true;
    refreshButton.classList.add("is-loading");
    try {
      const payload = await postJson(root.dataset.refreshUrl);
      toast(payload.added ? `${payload.added} new ${payload.added === 1 ? "task" : "tasks"}` : "Inbox is up to date");
      window.setTimeout(() => window.location.reload(), 250);
    } catch (error) {
      refreshButton.disabled = false;
      refreshButton.classList.remove("is-loading");
      showPlannerError(error.message);
    }
  });

  if ("IntersectionObserver" in window) {
    const observer = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting || entry.target.classList.contains("is-hidden")) return;
        const status = entry.target.closest("[data-tab-panel]")?.dataset.tabPanel;
        if (status !== activeStatus()) return;
        visibleLimits.set(status, (visibleLimits.get(status) || batchSize) + batchSize);
        refreshAll();
      });
    }, {rootMargin: "240px"});
    root.querySelectorAll("[data-load-more]").forEach((button) => observer.observe(button));
  }

  const initialParams = new URLSearchParams(window.location.search);
  const initialSource = initialParams.get("source");
  const initialQuery = initialParams.get("q");
  const initialSort = initialParams.get("sort");
  if (initialSource && sourceFilter?.querySelector(`option[value='${CSS.escape(initialSource)}']`)) sourceFilter.value = initialSource;
  if (initialQuery && searchInput) searchInput.value = initialQuery;
  if (initialSort && sortControl?.querySelector(`option[value='${CSS.escape(initialSort)}']`)) sortControl.value = initialSort;

  const hashStatus = window.location.hash.replace("#", "");
  const rememberedStatus = window.sessionStorage.getItem("aggregator:planner-tab");
  activateTab(hashStatus || rememberedStatus || "inbox", {persist: false});
  refreshAll({resetLimit: true});
})();
