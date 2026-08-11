(() => {
  const root = document.querySelector("[data-planner-root]");
  if (!root) return;

  const csrfToken = (document.cookie.match(/csrftoken=([^;]+)/) || [])[1] || "";
  const searchInput = root.querySelector("[data-task-search]");
  const sourceFilter = root.querySelector("[data-source-filter]");
  const sortControl = root.querySelector("[data-sort-control]");
  const clearFiltersButton = root.querySelector("[data-clear-filters]");
  const quickAddDialog = root.querySelector("[data-quick-add-dialog]");
  const quickAddForm = root.querySelector("[data-quick-add-form]");
  const taskDrawer = root.querySelector("[data-task-drawer]");
  const timelineEvents = root.querySelector("[data-timeline-events]");
  const batchSize = Math.max(Number(root.dataset.batchSize || 32), 1);
  const visibleLimits = new Map();
  const statusLabels = {inbox: "Gather", backlog: "Planned", doing: "Now", done: "Done"};
  const listKeyStatus = {
    gather: "inbox",
    now: "doing",
    later: "backlog",
    upcoming: "backlog",
    unscheduled: "backlog",
    done: "done",
    "kanban-gather": "inbox",
    "kanban-planned": "backlog",
    "kanban-done": "done",
  };
  let activeView = window.sessionStorage.getItem("aggregator:planner-view") || "list";
  let activeCollection = "";
  let dragItem = null;
  let drawerRow = null;
  let reorderTimer = null;

  function renderIconsSoon() {
    window.requestAnimationFrame(() => window.renderLucideIcons?.());
  }

  function toast(message, tone = "success") {
    window.dispatchEvent(new CustomEvent("app:toast", {detail: {message, tone}}));
  }

  function showError(message) {
    const errorBox = root.querySelector("[data-planner-error]");
    if (errorBox) {
      errorBox.textContent = message;
      errorBox.classList.add("is-visible");
    }
    toast(message, "error");
  }

  function clearError() {
    const errorBox = root.querySelector("[data-planner-error]");
    if (!errorBox) return;
    errorBox.textContent = "";
    errorBox.classList.remove("is-visible");
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
    if (!response.ok) throw new Error(body.error || "The update could not be saved.");
    return body;
  }

  function endpoint(templateKey, itemId) {
    return (root.dataset[templateKey] || "").replace("/0/", `/${itemId}/`);
  }

  function rows() {
    return Array.from(root.querySelectorAll("[data-task-row]"));
  }

  function visibleRows() {
    return rows().filter((row) => !row.classList.contains("is-filtered-out")
      && !row.classList.contains("is-deferred"));
  }

  function parseDate(value) {
    if (!value) return null;
    const date = new Date(value);
    return Number.isNaN(date.valueOf()) ? null : date;
  }

  function formatLocalDateTime(value) {
    const date = parseDate(value);
    if (!date) return "";
    const offset = date.getTimezoneOffset();
    return new Date(date.getTime() - offset * 60_000).toISOString().slice(0, 16);
  }

  function isToday(value) {
    const date = parseDate(value);
    if (!date) return false;
    const now = new Date();
    return date.getFullYear() === now.getFullYear()
      && date.getMonth() === now.getMonth()
      && date.getDate() === now.getDate();
  }

  function isFuture(value) {
    const date = parseDate(value);
    if (!date) return false;
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    return date > today && !isToday(value);
  }

  function listKeyForRow(row) {
    const status = row.dataset.status || "inbox";
    if (activeView === "kanban") {
      if (status === "inbox") return "kanban-gather";
      if (status === "done") return "kanban-done";
      return "kanban-planned";
    }
    if (status === "inbox") return "gather";
    if (status === "doing") return "now";
    if (status === "done") return "done";
    if (isToday(row.dataset.plannedStart)) return "later";
    if (isFuture(row.dataset.plannedStart)) return "upcoming";
    return "unscheduled";
  }

  function listForKey(key) {
    return root.querySelector(`.planner-list[data-list-key="${CSS.escape(key)}"]`);
  }

  function placeRow(row, {prepend = false, reveal = false} = {}) {
    const list = listForKey(listKeyForRow(row));
    if (!list) return;
    if (prepend) list.prepend(row);
    else list.appendChild(row);
    if (reveal) revealRow(row);
  }

  function listRows(list) {
    return Array.from(list?.querySelectorAll(":scope > [data-task-row]") || []);
  }

  function displayListKeys() {
    if (activeView === "kanban") {
      return ["kanban-gather", "kanban-planned", "kanban-done"];
    }
    return ["gather", "now", "later", "upcoming", "unscheduled", "done"];
  }

  function revealRow(row) {
    const key = listKeyForRow(row);
    const index = listRows(listForKey(key))
      .filter((candidate) => !candidate.classList.contains("is-filtered-out"))
      .indexOf(row);
    if (index < 0) return;
    visibleLimits.set(key, Math.max(visibleLimits.get(key) || batchSize, index + 1));
  }

  function filtersActive() {
    return Boolean(searchInput?.value.trim() || sourceFilter?.value || activeCollection);
  }

  function rowMatches(row) {
    const query = searchInput?.value.trim().toLocaleLowerCase() || "";
    const source = sourceFilter?.value || "";
    const collection = activeCollection.toLocaleLowerCase();
    if (source && row.dataset.source !== source) return false;
    if (collection && (row.dataset.collection || "").toLocaleLowerCase() !== collection) return false;
    return !query || (row.dataset.search || "").toLocaleLowerCase().includes(query);
  }

  function compareRows(a, b) {
    const sort = sortControl?.value || "default";
    if (sort === "title") return (a.dataset.title || "").localeCompare(b.dataset.title || "", undefined, {sensitivity: "base"});
    if (sort === "source") {
      const sourceOrder = (a.dataset.sourceLabel || a.dataset.source || "").localeCompare(b.dataset.sourceLabel || b.dataset.source || "");
      return sourceOrder || (a.dataset.title || "").localeCompare(b.dataset.title || "");
    }
    if (sort === "newest" || sort === "oldest") {
      const direction = sort === "oldest" ? 1 : -1;
      const importedAtA = Number(a.dataset.createdSort || 0);
      const importedAtB = Number(b.dataset.createdSort || 0);
      const dateA = Number(a.dataset.sourceCreatedSort || 0) || importedAtA;
      const dateB = Number(b.dataset.sourceCreatedSort || 0) || importedAtB;
      if (dateA !== dateB) return (dateA - dateB) * direction;
      if (importedAtA !== importedAtB) return (importedAtA - importedAtB) * direction;
      return (Number(a.dataset.itemId || 0) - Number(b.dataset.itemId || 0)) * direction;
    }
    return 0;
  }

  function sortLists() {
    const sort = sortControl?.value || "default";
    if (sort === "default") return;
    root.querySelectorAll(`.planner-list[data-list-key^="${activeView === "kanban" ? "kanban-" : ""}"]`).forEach((list) => {
      const sorted = listRows(list).sort(compareRows);
      sorted.forEach((row) => list.appendChild(row));
    });
  }

  function updateDraggability() {
    const enabled = (sortControl?.value || "default") === "default" && !filtersActive();
    rows().forEach((row) => {
      row.draggable = enabled;
      row.classList.toggle("is-reorderable", enabled);
      const handle = row.querySelector("[data-drag-handle]");
      if (handle) handle.disabled = !enabled;
    });
  }

  function updateCounts() {
    const all = rows();
    const countFor = (predicate) => all.filter(predicate).length;
    root.querySelectorAll("[data-section-count]").forEach((node) => {
      const key = node.dataset.sectionCount;
      const count = all.filter((row) => listKeyForSection(row, key)).length;
      node.textContent = String(count);
    });
    root.querySelector("[data-sidebar-count='gather']")?.replaceChildren(document.createTextNode(String(countFor((row) => row.dataset.status === "inbox"))));
    root.querySelectorAll("[data-source-count]").forEach((node) => {
      node.textContent = String(countFor((row) => row.dataset.source === node.dataset.sourceCount));
    });
    root.querySelectorAll("[data-collection-count]").forEach((node) => {
      const collection = node.dataset.collectionCount || "";
      node.textContent = String(countFor((row) => (row.dataset.collection || "") === collection));
    });
    const statuses = {
      inbox: countFor((row) => row.dataset.status === "inbox"),
      planned: countFor((row) => ["backlog", "doing"].includes(row.dataset.status)),
      done: countFor((row) => row.dataset.status === "done"),
    };
    Object.entries(statuses).forEach(([key, count]) => {
      const node = root.querySelector(`[data-kanban-count="${key}"]`);
      if (node) node.textContent = String(count);
    });
    const plannedRows = all.filter((row) => ["backlog", "doing"].includes(row.dataset.status));
    const plannedMinutes = plannedRows.reduce((total, row) => total + Number(row.dataset.estimatedMinutes || 0), 0);
    const taskSummary = root.querySelector("[data-planner-task-summary]");
    if (taskSummary) taskSummary.textContent = `${plannedRows.length} task${plannedRows.length === 1 ? "" : "s"} in your plan`;
    const timeSummary = root.querySelector("[data-planner-time-summary]");
    if (timeSummary) timeSummary.textContent = plannedMinutes ? `${plannedMinutes} min of focus planned` : "Keep the day spacious";
  }

  function listKeyForSection(row, key) {
    const status = row.dataset.status;
    if (key === "gather") return status === "inbox";
    if (key === "now") return status === "doing";
    if (key === "done") return status === "done";
    if (status !== "backlog") return false;
    if (key === "later") return isToday(row.dataset.plannedStart);
    if (key === "upcoming") return isFuture(row.dataset.plannedStart);
    return key === "unscheduled" && !isToday(row.dataset.plannedStart) && !isFuture(row.dataset.plannedStart);
  }

  function updateEmptyStates() {
    root.querySelectorAll("[data-empty-section]").forEach((empty) => {
      const key = empty.dataset.emptySection;
      const list = listForKey(key);
      const hasVisible = listRows(list).some((row) => !row.classList.contains("is-filtered-out"));
      empty.classList.toggle("is-hidden", hasVisible);
    });
  }

  function refreshVisibleRows({resetBatch = false} = {}) {
    const activeListKeys = new Set(displayListKeys());
    root.querySelectorAll("[data-load-more-list]").forEach((button) => {
      if (!activeListKeys.has(button.dataset.loadMoreList)) {
        button.classList.add("is-hidden");
      }
    });

    activeListKeys.forEach((key) => {
      const list = listForKey(key);
      if (!list) return;
      if (resetBatch || !visibleLimits.has(key)) visibleLimits.set(key, batchSize);
      const limit = visibleLimits.get(key);
      let matchingCount = 0;
      listRows(list).forEach((row) => {
        if (row.classList.contains("is-filtered-out")) {
          row.classList.remove("is-deferred");
          return;
        }
        row.classList.toggle("is-deferred", matchingCount >= limit);
        matchingCount += 1;
      });

      const button = root.querySelector(
        `[data-load-more-list="${CSS.escape(key)}"]`,
      );
      if (!button) return;
      const remaining = Math.max(matchingCount - limit, 0);
      button.classList.toggle("is-hidden", remaining === 0);
      button.textContent = remaining ? `Show ${Math.min(batchSize, remaining)} more` : "";
    });
  }

  function refreshPlanner({resetBatch = false} = {}) {
    rows().forEach((row) => row.classList.toggle("is-filtered-out", !rowMatches(row)));
    sortLists();
    refreshVisibleRows({resetBatch});
    updateDraggability();
    updateCounts();
    updateEmptyStates();
    clearFiltersButton?.classList.toggle("is-hidden", !filtersActive() && (sortControl?.value || "default") === "default");
    renderTimeline();
  }

  function setView(view, {persist = true, resetBatch = false} = {}) {
    if (!["list", "kanban"].includes(view)) view = "list";
    activeView = view;
    root.querySelectorAll("[data-planner-view]").forEach((node) => {
      node.classList.toggle("is-hidden", node.dataset.plannerView !== view);
    });
    root.querySelectorAll("[data-view-toggle]").forEach((button) => {
      const selected = button.dataset.viewToggle === view;
      button.classList.toggle("is-active", selected);
      button.setAttribute("aria-selected", String(selected));
    });
    rows().forEach((row) => placeRow(row));
    if (persist) window.sessionStorage.setItem("aggregator:planner-view", view);
    refreshPlanner({resetBatch});
  }

  function updateRowStatus(row, status, payload = {}) {
    row.dataset.status = status;
    if (payload.state_id) row.dataset.stateId = String(payload.state_id);
    if (payload.item_id) row.dataset.itemId = String(payload.item_id);
    if (payload.writeback_status) row.dataset.writebackStatus = payload.writeback_status;
    row.classList.toggle("is-complete", status === "done");
    row.querySelectorAll("[data-status-choice]").forEach((button) => {
      button.classList.toggle("is-active", button.dataset.statusChoice === status);
    });
    const pin = row.querySelector("[data-pin-id]");
    if (pin && row.dataset.stateId) pin.disabled = false;
  }

  async function persistStatusMove(row, status, {announce = true} = {}) {
    if (!row || row.dataset.busy === "true") return false;
    if (row.dataset.status === status) return true;
    const original = {
      status: row.dataset.status,
      parent: row.parentElement,
      next: row.nextElementSibling,
      stateId: row.dataset.stateId,
    };
    row.dataset.busy = "true";
    row.setAttribute("aria-busy", "true");
    updateRowStatus(row, status);
    placeRow(row, {prepend: status === "doing" || status === "done", reveal: true});
    refreshPlanner();
    clearError();
    try {
      const payload = await postJson(endpoint("statusUrlTemplate", row.dataset.itemId), {planner_status: status});
      updateRowStatus(row, payload.planner_status || status, payload);
      placeRow(row, {prepend: status === "doing" || status === "done", reveal: true});
      if (announce) toast(`Moved to ${statusLabels[status] || status}`);
      return true;
    } catch (error) {
      updateRowStatus(row, original.status, {state_id: original.stateId});
      if (original.next?.parentElement === original.parent) original.parent.insertBefore(row, original.next);
      else original.parent?.appendChild(row);
      showError(error.message);
      return false;
    } finally {
      row.dataset.busy = "false";
      row.removeAttribute("aria-busy");
      refreshPlanner();
    }
  }

  function slotDate(hour) {
    const date = new Date();
    date.setHours(Number(hour), 0, 0, 0);
    return date;
  }

  function durationForRow(row) {
    const start = parseDate(row.dataset.plannedStart);
    const end = parseDate(row.dataset.plannedEnd);
    if (start && end && end > start) return Math.round((end - start) / 60_000);
    return Number(row.dataset.estimatedMinutes || 30) || 30;
  }

  async function saveSchedule(row, start, {announce = true} = {}) {
    if (!row) return false;
    if (row.dataset.status === "inbox") {
      const moved = await persistStatusMove(row, "backlog", {announce: false});
      if (!moved) return false;
    }
    const previousStart = row.dataset.plannedStart;
    const previousEnd = row.dataset.plannedEnd;
    const end = start ? new Date(start.getTime() + durationForRow(row) * 60_000) : null;
    row.dataset.plannedStart = start ? start.toISOString() : "";
    row.dataset.plannedEnd = end ? end.toISOString() : "";
    placeRow(row, {reveal: true});
    refreshPlanner();
    try {
      const payload = await postJson(endpoint("scheduleUrlTemplate", row.dataset.itemId), {
        planned_start: start ? start.toISOString() : null,
        planned_end: end ? end.toISOString() : null,
      });
      row.dataset.stateId = String(payload.state_id || row.dataset.stateId || "");
      row.dataset.plannedStart = payload.planned_start || "";
      row.dataset.plannedEnd = payload.planned_end || "";
      placeRow(row, {reveal: true});
      if (announce) toast(start ? "Added to your day" : "Removed from your schedule");
      return true;
    } catch (error) {
      row.dataset.plannedStart = previousStart;
      row.dataset.plannedEnd = previousEnd;
      placeRow(row, {reveal: true});
      showError(error.message);
      return false;
    } finally {
      refreshPlanner();
    }
  }

  async function moveToListKey(row, key) {
    const targetStatus = listKeyStatus[key] || "backlog";
    if (key === "later") {
      const moved = await persistStatusMove(row, targetStatus, {announce: false});
      if (moved) await saveSchedule(row, slotDate(15), {announce: false});
      toast("Moved to Later today");
      return;
    }
    if (key === "upcoming") {
      const moved = await persistStatusMove(row, targetStatus, {announce: false});
      if (moved) {
        const tomorrow = slotDate(9);
        tomorrow.setDate(tomorrow.getDate() + 1);
        await saveSchedule(row, tomorrow, {announce: false});
      }
      toast("Moved to Upcoming");
      return;
    }
    if (key === "unscheduled") {
      const moved = await persistStatusMove(row, targetStatus, {announce: false});
      if (moved) await saveSchedule(row, null, {announce: false});
      toast("Moved to Unscheduled");
      return;
    }
    await persistStatusMove(row, targetStatus);
  }

  function allRowsForStatus(status, pinned) {
    return rows().filter((row) => row.dataset.status === status
      && row.dataset.pinned === String(pinned)
      && row.dataset.stateId);
  }

  function queueReorder(row) {
    if (!row || !row.dataset.stateId || !root.dataset.reorderUrl) return;
    window.clearTimeout(reorderTimer);
    reorderTimer = window.setTimeout(async () => {
      const block = allRowsForStatus(row.dataset.status, row.dataset.pinned === "true")
        .map((item) => Number(item.dataset.stateId));
      if (!block.length) return;
      try {
        await postJson(root.dataset.reorderUrl, {
          planner_status: row.dataset.status,
          block_order: block,
        });
      } catch (error) {
        showError(error.message);
      }
    }, 220);
  }

  async function togglePin(row) {
    const button = row?.querySelector("[data-pin-id]");
    if (!row || !button || button.disabled) return;
    button.disabled = true;
    const previous = row.dataset.pinned === "true";
    row.dataset.pinned = String(!previous);
    button.classList.toggle("is-active", !previous);
    button.setAttribute("aria-label", `${!previous ? "Unpin" : "Pin"} ${row.dataset.title || "task"}`);
    try {
      const payload = await postJson(endpoint("pinUrlTemplate", row.dataset.itemId));
      row.dataset.pinned = String(payload.pinned);
      button.classList.toggle("is-active", payload.pinned);
      toast(payload.pinned ? "Task pinned" : "Task unpinned");
    } catch (error) {
      row.dataset.pinned = String(previous);
      button.classList.toggle("is-active", previous);
      showError(error.message);
    } finally {
      button.disabled = false;
      refreshPlanner();
    }
  }

  function escapeHtml(value) {
    return String(value || "").replace(/[&<>'\"]/g, (character) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", "\"": "&quot;",
    }[character]));
  }

  function createTaskRow(task) {
    const row = document.createElement("article");
    row.className = `planner-row source-${task.source || "manual"}`;
    row.tabIndex = 0;
    row.dataset.taskRow = "";
    row.dataset.stateId = String(task.state_id || "");
    row.dataset.itemId = String(task.item_id);
    row.dataset.pinned = "false";
    row.dataset.status = task.planner_status || "backlog";
    row.dataset.source = task.source || "manual";
    row.dataset.sourceLabel = task.source_label || "Personal";
    row.dataset.sourceUrl = task.source_url || "";
    row.dataset.connector = "";
    row.dataset.title = task.title || "";
    row.dataset.description = task.description || "";
    row.dataset.notes = task.notes || "";
    row.dataset.collection = task.collection || "";
    row.dataset.tags = "";
    row.dataset.estimatedMinutes = task.estimated_minutes || "";
    row.dataset.plannedStart = task.planned_start || "";
    row.dataset.plannedEnd = task.planned_end || "";
    row.dataset.search = `${task.title || ""} ${task.description || ""} ${task.source || "manual"} ${task.collection || ""}`;
    row.dataset.createdSort = String(Date.parse(task.created_at || "") || Date.now());
    row.dataset.writebackStatus = task.writeback_status || "none";
    row.innerHTML = `
      <button class="task-check" type="button" data-complete-task aria-label="Mark ${escapeHtml(task.title)} complete" title="Mark complete"><span aria-hidden="true"></span></button>
      <button class="task-drag-handle" type="button" data-drag-handle title="Drag task" aria-label="Drag ${escapeHtml(task.title)}"><i data-lucide="grip-vertical" aria-hidden="true"></i></button>
      <button class="task-open" type="button" data-open-task aria-label="Open ${escapeHtml(task.title)}">
        <span class="task-open-title">${escapeHtml(task.title)}</span>
        <span class="task-meta"><span class="task-source"><span class="provider-dot source-${escapeHtml(task.source || "manual")}" aria-hidden="true"></span>${escapeHtml(task.source_label || "Personal")}</span>${task.collection ? `<span>${escapeHtml(task.collection)}</span>` : ""}${task.estimated_minutes ? `<span>${escapeHtml(task.estimated_minutes)} min</span>` : ""}<span class="task-created">Created today</span></span>
      </button>
      <div class="task-row-actions">
        <button class="task-pin" type="button" data-pin-id="${escapeHtml(task.item_id)}" aria-label="Pin ${escapeHtml(task.title)}" title="Pin"><i data-lucide="pin" aria-hidden="true"></i></button>
        <button class="task-more" type="button" data-open-task aria-label="More details for ${escapeHtml(task.title)}"><i data-lucide="ellipsis" aria-hidden="true"></i></button>
      </div>
      <div class="task-hidden-controls" aria-hidden="true"><button type="button" data-status-choice="inbox">Gather</button><button type="button" data-status-choice="backlog">Planned</button><button type="button" data-status-choice="doing">Now</button><button type="button" data-status-choice="done">Done</button><textarea data-description-input="${escapeHtml(task.item_id)}"></textarea><input data-tags-input="${escapeHtml(task.item_id)}" /><span data-writeback-badge></span><span data-writeback-message></span></div>`;
    updateRowStatus(row, row.dataset.status, task);
    return row;
  }

  function updateRowMeta(row) {
    const meta = row.querySelector(".task-meta");
    if (!meta) return;
    const collection = row.dataset.collection ? `<span>${escapeHtml(row.dataset.collection)}</span>` : "";
    const minutes = row.dataset.estimatedMinutes ? `<span>${escapeHtml(row.dataset.estimatedMinutes)} min</span>` : "";
    const created = meta.querySelector(".task-created")?.textContent || "Created today";
    meta.innerHTML = `<span class="task-source"><span class="provider-dot source-${escapeHtml(row.dataset.source || "manual")}" aria-hidden="true"></span>${escapeHtml(row.dataset.sourceLabel || "Personal")}</span>${collection}${minutes}<span class="task-created">${escapeHtml(created)}</span>`;
    row.dataset.search = [
      row.dataset.title,
      row.dataset.description,
      row.dataset.source,
      row.dataset.connector,
      row.dataset.collection,
      row.dataset.tags,
    ].filter(Boolean).join(" ");
  }

  function openQuickAdd() {
    if (!quickAddDialog) return;
    if (typeof quickAddDialog.showModal === "function") quickAddDialog.showModal();
    else quickAddDialog.setAttribute("open", "");
    window.setTimeout(() => quickAddForm?.elements.title?.focus(), 30);
  }

  function closeQuickAdd() {
    if (!quickAddDialog) return;
    if (typeof quickAddDialog.close === "function") quickAddDialog.close();
    else quickAddDialog.removeAttribute("open");
  }

  function drawerFields() {
    return {
      source: taskDrawer?.querySelector("[data-drawer-source]"),
      title: taskDrawer?.querySelector("[data-drawer-title]"),
      collection: taskDrawer?.querySelector("[data-drawer-collection]"),
      estimate: taskDrawer?.querySelector("[data-drawer-estimate]"),
      schedule: taskDrawer?.querySelector("[data-drawer-schedule]"),
      notes: taskDrawer?.querySelector("[data-drawer-notes]"),
      description: taskDrawer?.querySelector("[data-drawer-description]"),
      tags: taskDrawer?.querySelector("[data-drawer-tags-input]"),
      sourceLink: taskDrawer?.querySelector("[data-drawer-source-link]"),
      sync: taskDrawer?.querySelector("[data-drawer-sync]"),
      pin: taskDrawer?.querySelector("[data-drawer-pin]"),
    };
  }

  function renderDrawer(row) {
    const fields = drawerFields();
    if (!fields.title) return;
    fields.title.textContent = row.dataset.title || "Untitled task";
    fields.source.textContent = row.dataset.sourceLabel || "Personal";
    fields.collection.value = row.dataset.collection || "";
    fields.estimate.value = row.dataset.estimatedMinutes || "";
    fields.schedule.value = formatLocalDateTime(row.dataset.plannedStart);
    fields.notes.value = row.dataset.notes || "";
    fields.description.value = row.dataset.description || "";
    fields.tags.value = row.dataset.tags || "";
    fields.sync.textContent = row.dataset.writebackStatus === "failed" ? "This task needs a sync retry." : row.dataset.writebackStatus === "unsupported" || row.dataset.source === "manual" ? "Personal planning details stay here." : "Changes sync quietly when this source supports them.";
    fields.pin?.classList.toggle("is-active", row.dataset.pinned === "true");
    fields.pin?.querySelector("span")?.remove();
    root.querySelectorAll("[data-drawer-status]").forEach((button) => {
      const selected = button.dataset.drawerStatus === row.dataset.status;
      button.classList.toggle("is-active", selected);
      button.setAttribute("aria-pressed", String(selected));
    });
    if (fields.sourceLink) {
      fields.sourceLink.replaceChildren();
      if (row.dataset.sourceUrl) {
        const link = document.createElement("a");
        link.href = row.dataset.sourceUrl;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = `Open in ${row.dataset.sourceLabel || "source"}`;
        fields.sourceLink.appendChild(link);
      }
    }
  }

  function openDrawer(row) {
    if (!taskDrawer || !row) return;
    drawerRow = row;
    renderDrawer(row);
    if (typeof taskDrawer.showModal === "function") taskDrawer.showModal();
    else taskDrawer.setAttribute("open", "");
    window.setTimeout(() => taskDrawer.querySelector("[data-drawer-collection]")?.focus(), 30);
  }

  function closeDrawer() {
    if (!taskDrawer) return;
    if (typeof taskDrawer.close === "function") taskDrawer.close();
    else taskDrawer.removeAttribute("open");
    drawerRow = null;
  }

  async function saveDrawer() {
    if (!drawerRow || !taskDrawer) return;
    const row = drawerRow;
    const fields = drawerFields();
    const save = taskDrawer.querySelector("[data-save-task-drawer]");
    save.disabled = true;
    clearError();
    try {
      const detailsPayload = {
        collection: fields.collection.value.trim(),
        estimated_minutes: fields.estimate.value || null,
        notes: fields.notes.value,
      };
      const details = await postJson(endpoint("detailsUrlTemplate", row.dataset.itemId), detailsPayload);
      const task = details.task || {};
      row.dataset.collection = task.collection || detailsPayload.collection;
      row.dataset.estimatedMinutes = task.estimated_minutes || "";
      row.dataset.notes = task.notes || detailsPayload.notes;
      if (fields.description.value !== (row.dataset.description || "")) {
        const description = await postJson(endpoint("descriptionUrlTemplate", row.dataset.itemId), {description: fields.description.value});
        row.dataset.description = description.description || "";
      }
      const tags = fields.tags.value.split(",").map((tag) => tag.trim()).filter(Boolean);
      const previousTags = (row.dataset.tags || "").split(",").map((tag) => tag.trim()).filter(Boolean);
      if (tags.join("|").toLocaleLowerCase() !== previousTags.join("|").toLocaleLowerCase()) {
        const tagPayload = await postJson(endpoint("tagsUrlTemplate", row.dataset.itemId), {tags});
        row.dataset.tags = (tagPayload.tags || []).map((tag) => tag.tag__name).join(", ");
      }
      const scheduleValue = fields.schedule.value;
      const requestedSchedule = scheduleValue ? new Date(scheduleValue) : null;
      const currentSchedule = formatLocalDateTime(row.dataset.plannedStart);
      if (scheduleValue !== currentSchedule) await saveSchedule(row, requestedSchedule, {announce: false});
      updateRowMeta(row);
      renderDrawer(row);
      refreshPlanner();
      toast("Task details saved");
    } catch (error) {
      showError(error.message);
    } finally {
      save.disabled = false;
    }
  }

  function renderTimeline() {
    if (!timelineEvents) return;
    timelineEvents.replaceChildren();
    const scheduled = rows().filter((row) => isToday(row.dataset.plannedStart) && row.dataset.status !== "done");
    scheduled.forEach((row) => {
      const start = parseDate(row.dataset.plannedStart);
      if (!start) return;
      const block = document.createElement("button");
      const minutesFromEight = (start.getHours() - 8) * 60 + start.getMinutes();
      const height = Math.max(32, durationForRow(row) * 0.72);
      block.type = "button";
      block.className = "planner-timeline-task";
      block.style.top = `${Math.max(0, minutesFromEight * 0.72)}px`;
      block.style.height = `${height}px`;
      block.dataset.timelineTaskId = row.dataset.itemId;
      block.innerHTML = `<span>${escapeHtml(row.dataset.title)}</span><small>${start.toLocaleTimeString([], {hour: "numeric", minute: "2-digit"})}</small>`;
      timelineEvents.appendChild(block);
    });
  }

  function focusSection(key) {
    if (key === "today") {
      activeCollection = "";
      if (sourceFilter) sourceFilter.value = "";
      if (searchInput) searchInput.value = "";
      setView("list", {resetBatch: true});
      root.querySelector(".planner-main")?.scrollTo?.({top: 0, behavior: "smooth"});
      return;
    }
    setView("list");
    const section = root.querySelector(`[data-planner-section="${CSS.escape(key)}"]`);
    section?.scrollIntoView({behavior: "smooth", block: "start"});
  }

  function refreshButton() {
    return root.querySelector("[data-refresh-sources]");
  }

  root.addEventListener("click", async (event) => {
    const openAdd = event.target.closest("[data-open-quick-add]");
    if (openAdd) {
      openQuickAdd();
      return;
    }
    if (event.target.closest("[data-close-quick-add]")) {
      closeQuickAdd();
      return;
    }
    if (event.target.closest("[data-close-task-drawer]")) {
      closeDrawer();
      return;
    }
    const viewButton = event.target.closest("[data-view-toggle]");
    if (viewButton) {
      setView(viewButton.dataset.viewToggle);
      return;
    }
    if (event.target.closest("[data-clear-filters]")) {
      if (searchInput) searchInput.value = "";
      if (sourceFilter) sourceFilter.value = "";
      if (sortControl) sortControl.value = "default";
      activeCollection = "";
      refreshPlanner({resetBatch: true});
      searchInput?.focus();
      return;
    }
    const loadMore = event.target.closest("[data-load-more-list]");
    if (loadMore) {
      const key = loadMore.dataset.loadMoreList;
      visibleLimits.set(key, (visibleLimits.get(key) || batchSize) + batchSize);
      refreshPlanner();
      return;
    }
    const focusButton = event.target.closest("[data-sidebar-focus]");
    if (focusButton) {
      root.querySelectorAll("[data-sidebar-focus]").forEach((button) => button.classList.toggle("is-active", button === focusButton));
      focusSection(focusButton.dataset.sidebarFocus);
      return;
    }
    const sourceButton = event.target.closest("[data-source-nav]");
    if (sourceButton) {
      if (sourceFilter) sourceFilter.value = sourceButton.dataset.sourceNav || "";
      activeCollection = "";
      refreshPlanner({resetBatch: true});
      return;
    }
    const collectionButton = event.target.closest("[data-collection-nav]");
    if (collectionButton) {
      activeCollection = activeCollection === collectionButton.dataset.collectionNav ? "" : collectionButton.dataset.collectionNav || "";
      root.querySelectorAll("[data-collection-nav]").forEach((button) => button.classList.toggle("is-active", button === collectionButton && Boolean(activeCollection)));
      refreshPlanner({resetBatch: true});
      return;
    }
    if (event.target.closest("[data-refresh-sources]")) {
      await window.workspaceRefresh?.queueAndTrack(root, refreshButton());
      return;
    }
    if (event.target.closest("[data-focus-timeline]")) {
      root.querySelector(".planner-timeline")?.scrollIntoView({behavior: "smooth", block: "start"});
      return;
    }
    const complete = event.target.closest("[data-complete-task]");
    if (complete) {
      await persistStatusMove(complete.closest("[data-task-row]"), "done");
      return;
    }
    const pin = event.target.closest("[data-pin-id]");
    if (pin) {
      await togglePin(pin.closest("[data-task-row]"));
      return;
    }
    const status = event.target.closest("[data-status-choice]");
    if (status) {
      await persistStatusMove(status.closest("[data-task-row]"), status.dataset.statusChoice);
      return;
    }
    const openTask = event.target.closest("[data-open-task]");
    if (openTask) {
      openDrawer(openTask.closest("[data-task-row]"));
      return;
    }
    const timelineTask = event.target.closest("[data-timeline-task-id]");
    if (timelineTask) openDrawer(root.querySelector(`[data-task-row][data-item-id="${CSS.escape(timelineTask.dataset.timelineTaskId)}"]`));
  });

  quickAddForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(quickAddForm);
    const submit = quickAddForm.querySelector("button[type='submit']");
    submit.disabled = true;
    try {
      const payload = await postJson(root.dataset.createUrl, {
        title: formData.get("title"),
        collection: formData.get("collection"),
        estimated_minutes: formData.get("estimated_minutes") || null,
        planner_status: "backlog",
      });
      const row = createTaskRow(payload.task);
      placeRow(row, {prepend: true, reveal: true});
      closeQuickAdd();
      quickAddForm.reset();
      refreshPlanner();
      renderIconsSoon();
      toast("Task added to Unscheduled");
    } catch (error) {
      showError(error.message);
    } finally {
      submit.disabled = false;
    }
  });

  taskDrawer?.addEventListener("click", async (event) => {
    const status = event.target.closest("[data-drawer-status]");
    if (status && drawerRow) {
      await persistStatusMove(drawerRow, status.dataset.drawerStatus);
      renderDrawer(drawerRow);
      return;
    }
    if (event.target.closest("[data-drawer-pin]") && drawerRow) {
      await togglePin(drawerRow);
      renderDrawer(drawerRow);
      return;
    }
    if (event.target.closest("[data-save-task-drawer]")) await saveDrawer();
  });

  searchInput?.addEventListener("input", () => refreshPlanner({resetBatch: true}));
  sourceFilter?.addEventListener("change", () => {
    activeCollection = "";
    refreshPlanner({resetBatch: true});
  });
  sortControl?.addEventListener("change", () => refreshPlanner({resetBatch: true}));

  root.addEventListener("dragstart", (event) => {
    const row = event.target.closest("[data-task-row]");
    if (!row || !row.draggable) return;
    dragItem = row;
    row.classList.add("is-dragging");
    event.dataTransfer?.setData("text/plain", row.dataset.itemId || "");
    if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
  });

  root.addEventListener("dragover", (event) => {
    if (!dragItem) return;
    const slot = event.target.closest("[data-timeline-slot]");
    const list = event.target.closest(".planner-list");
    if (!slot && !list) return;
    event.preventDefault();
    if (slot) slot.classList.add("is-drag-over");
    if (list) {
      list.classList.add("is-drag-over");
      const target = event.target.closest("[data-task-row]");
      if (target && target !== dragItem && target.parentElement === list) {
        const after = event.clientY > target.getBoundingClientRect().top + target.offsetHeight / 2;
        list.insertBefore(dragItem, after ? target.nextSibling : target);
      }
    }
  });

  root.addEventListener("dragleave", (event) => {
    event.target.closest("[data-timeline-slot]")?.classList.remove("is-drag-over");
    event.target.closest(".planner-list")?.classList.remove("is-drag-over");
  });

  root.addEventListener("drop", async (event) => {
    if (!dragItem) return;
    const row = dragItem;
    const slot = event.target.closest("[data-timeline-slot]");
    const list = event.target.closest(".planner-list");
    if (!slot && !list) return;
    event.preventDefault();
    slot?.classList.remove("is-drag-over");
    list?.classList.remove("is-drag-over");
    if (slot) {
      await saveSchedule(row, slotDate(slot.dataset.hour));
      return;
    }
    const key = list.dataset.listKey;
    const desiredStatus = listKeyStatus[key];
    if (key && desiredStatus) {
      if (row.dataset.status !== desiredStatus || ["later", "upcoming", "unscheduled"].includes(key)) {
        await moveToListKey(row, key);
      } else {
        queueReorder(row);
        refreshPlanner();
      }
    }
  });

  root.addEventListener("dragend", () => {
    if (dragItem) dragItem.classList.remove("is-dragging");
    root.querySelectorAll(".is-drag-over").forEach((node) => node.classList.remove("is-drag-over"));
    dragItem = null;
  });

  document.addEventListener("keydown", async (event) => {
    const tagName = document.activeElement?.tagName;
    const typing = ["INPUT", "TEXTAREA", "SELECT"].includes(tagName) || document.activeElement?.isContentEditable;
    if (event.key === "Escape") {
      if (taskDrawer?.open) {
        closeDrawer();
        return;
      }
      if (quickAddDialog?.open) {
        closeQuickAdd();
        return;
      }
      if (searchInput === document.activeElement && searchInput.value) {
        searchInput.value = "";
        refreshPlanner({resetBatch: true});
      }
      return;
    }
    if (typing) return;
    if (event.key === "/") {
      event.preventDefault();
      searchInput?.focus();
      return;
    }
    if (event.key.toLocaleLowerCase() === "n") {
      event.preventDefault();
      openQuickAdd();
      return;
    }
    const focused = document.activeElement?.closest?.("[data-task-row]");
    if (event.key === "Enter" && focused) {
      event.preventDefault();
      openDrawer(focused);
      return;
    }
    if ((event.key === "x" || event.key === " ") && focused) {
      event.preventDefault();
      await persistStatusMove(focused, "done");
      return;
    }
    if (event.key.toLocaleLowerCase() === "s" && focused) {
      event.preventDefault();
      openDrawer(focused);
      window.setTimeout(() => taskDrawer?.querySelector("[data-drawer-schedule]")?.focus(), 40);
      return;
    }
    if (["j", "k", "ArrowDown", "ArrowUp"].includes(event.key)) {
      const available = visibleRows();
      if (!available.length) return;
      event.preventDefault();
      const index = focused ? available.indexOf(focused) : -1;
      const direction = ["j", "ArrowDown"].includes(event.key) ? 1 : -1;
      available[(index + direction + available.length) % available.length].focus();
    }
  });

  root.querySelectorAll("[data-timeline-slot]").forEach((slot) => {
    slot.addEventListener("dragenter", () => slot.classList.add("is-drag-over"));
  });

  setView(activeView, {persist: false});
  refreshPlanner();
  renderIconsSoon();
})();
