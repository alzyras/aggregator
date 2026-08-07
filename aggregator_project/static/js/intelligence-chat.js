(() => {
  const root = document.querySelector("[data-chat-root]");
  if (!root) return;

  const form = root.querySelector("[data-chat-form]");
  const input = root.querySelector("[data-chat-input]");
  const messages = root.querySelector("[data-chat-messages]");
  const error = root.querySelector("[data-chat-error]");
  const submit = form?.querySelector("button[type='submit']");
  const csrfToken = (document.cookie.match(/csrftoken=([^;]+)/) || [])[1] || "";

  function setError(message = "") {
    if (error) error.textContent = message;
  }

  function scrollToLatest() {
    if (!messages) return;
    messages.scrollTop = messages.scrollHeight;
  }

  function renderIconsSoon() {
    window.requestAnimationFrame(() => window.renderLucideIcons?.());
  }

  function appendMessage(role, content, {model = "", pending = false} = {}) {
    const startState = messages?.querySelector("[data-chat-start-state]");
    startState?.remove();
    const article = document.createElement("article");
    article.className = `chat-message ${role}${pending ? " is-pending" : ""}`;

    const roleLabel = document.createElement("span");
    roleLabel.className = "chat-message-role";
    roleLabel.textContent = role === "assistant" ? "Workspace analyst" : "You";
    const body = document.createElement("div");
    body.className = "chat-message-content";
    body.textContent = content;
    article.append(roleLabel, body);
    if (model) {
      const modelLabel = document.createElement("span");
      modelLabel.className = "chat-message-model";
      modelLabel.textContent = model;
      article.append(modelLabel);
    }
    messages?.append(article);
    scrollToLatest();
    return article;
  }

  async function ask(message) {
    const response = await fetch(root.dataset.askUrl, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": csrfToken,
        "Accept": "application/json",
      },
      body: JSON.stringify({message, thread_id: root.dataset.threadId || null}),
    });
    let payload = {};
    try {
      payload = await response.json();
    } catch (_error) {
      payload = {};
    }
    if (!response.ok) throw new Error(payload.error || "The answer could not be generated.");
    return payload;
  }

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const message = input?.value.trim();
    if (!message || !input || !submit) return;

    setError();
    appendMessage("user", message);
    const pending = appendMessage("assistant", "Thinking from your workspace data...", {pending: true});
    input.value = "";
    input.disabled = true;
    submit.disabled = true;

    try {
      const payload = await ask(message);
      root.dataset.threadId = String(payload.thread_id);
      pending.remove();
      appendMessage("assistant", payload.answer.content, {model: payload.model || ""});
      if (payload.thread_url) window.history.replaceState(null, "", payload.thread_url);
    } catch (requestError) {
      pending.remove();
      setError(requestError.message || "The answer could not be generated.");
      input.value = message;
    } finally {
      input.disabled = false;
      submit.disabled = false;
      input.focus();
      renderIconsSoon();
    }
  });

  root.querySelectorAll("[data-chat-suggestion]").forEach((button) => {
    button.addEventListener("click", () => {
      if (!input || input.disabled) return;
      input.value = button.textContent.trim();
      input.focus();
    });
  });

  input?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      form?.requestSubmit();
    }
  });

  scrollToLatest();
})();
