(() => {
  const root = document.querySelector("[data-chat-root]");
  if (!root) return;

  const form = root.querySelector("[data-chat-form]");
  const input = root.querySelector("[data-chat-input]");
  const send = root.querySelector("[data-chat-send]");
  const thread = root.querySelector("[data-chat-thread]");
  const empty = root.querySelector("[data-chat-empty]");
  const history = [];
  const csrfToken = (document.cookie.match(/csrftoken=([^;]+)/) || [])[1] || "";

  function resizeInput() {
    input.style.height = "auto";
    input.style.height = `${Math.min(Math.max(input.scrollHeight, 42), 150)}px`;
  }

  function appendMessage(role, content, {pending = false, error = false} = {}) {
    empty?.classList.add("is-hidden");
    const message = document.createElement("div");
    message.className = `data-chat-message ${role}${pending ? " is-pending" : ""}${error ? " is-error" : ""}`;
    message.dataset.chatMessage = role;
    const label = document.createElement("span");
    label.className = "data-chat-message-label";
    label.textContent = role === "user" ? "You" : "Data Chat";
    const body = document.createElement("div");
    body.className = "data-chat-message-body";
    body.textContent = content;
    message.append(label, body);
    thread.appendChild(message);
    thread.scrollTop = thread.scrollHeight;
    return message;
  }

  async function submitQuestion(value) {
    const question = value.trim();
    if (!question || send.disabled) return;
    appendMessage("user", question);
    input.value = "";
    resizeInput();
    send.disabled = true;
    input.disabled = true;
    const pending = appendMessage("assistant", "Thinking", {pending: true});
    try {
      const response = await fetch(root.dataset.chatUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: {"Content-Type": "application/json", "X-CSRFToken": csrfToken, "Accept": "application/json"},
        body: JSON.stringify({message: question, history: history.slice(-8)}),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Data Chat could not answer.");
      pending.remove();
      appendMessage("assistant", payload.answer);
      history.push({role: "user", content: question}, {role: "assistant", content: payload.answer});
    } catch (error) {
      pending.remove();
      appendMessage("assistant", error.message, {error: true});
    } finally {
      send.disabled = false;
      input.disabled = false;
      input.focus();
    }
  }

  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    void submitQuestion(input.value);
  });
  input?.addEventListener("input", resizeInput);
  input?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      void submitQuestion(input.value);
    }
  });
  root.querySelectorAll("[data-chat-suggestion]").forEach((button) => {
    button.addEventListener("click", () => void submitQuestion(button.dataset.chatSuggestion || ""));
  });
})();
