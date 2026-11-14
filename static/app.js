// Plain vanilla JS - no build step, no framework. Polls the job status every
// couple of seconds while a phase is running; that's simple and reliable
// enough for a small team's occasional audit runs.

let jobId = null;
let pollTimer = null;
let savedSettings = { scrape_do_key: null, gemini_key: null };

const el = (id) => document.getElementById(id);

const sections = {
  upload: el("upload-section"),
  configure: el("configure-section"),
  progress: el("progress-section"),
  error: el("error-section"),
  approval: el("approval-section"),
  done: el("done-section"),
};

const STEP_FOR_SECTION = { upload: 1, configure: 2, progress: 3, approval: 4, done: 5 };

function show(...names) {
  for (const key of Object.keys(sections)) {
    sections[key].classList.toggle("hidden", !names.includes(key));
  }
  updateSteps(Math.max(...names.map((n) => STEP_FOR_SECTION[n] || 0)));
}

function updateSteps(activeStep) {
  document.querySelectorAll("#steps .step").forEach((li) => {
    const step = Number(li.dataset.step);
    li.classList.toggle("done", step < activeStep);
    li.classList.toggle("active", step === activeStep);
  });
}

// ---------------------------------------------------------------- modals

function openModal(id) {
  el(id).classList.remove("hidden");
}

function closeModal(id) {
  el(id).classList.add("hidden");
}

document.querySelectorAll(".modal-close").forEach((btn) => {
  btn.addEventListener("click", () => closeModal(btn.dataset.close));
});

document.querySelectorAll(".modal-overlay").forEach((overlay) => {
  overlay.addEventListener("click", (e) => {
    if (e.target === overlay) overlay.classList.add("hidden");
  });
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    document.querySelectorAll(".modal-overlay").forEach((o) => o.classList.add("hidden"));
  }
});

// ---------------------------------------------------------------- settings

async function loadSettings() {
  const res = await fetch("/api/settings");
  if (res.ok) savedSettings = await res.json();
  return savedSettings;
}

function applySettingsToForm() {
  if (savedSettings.scrape_do_key) el("scrape-do-key").value = savedSettings.scrape_do_key;
  if (savedSettings.gemini_key) el("gemini-key").value = savedSettings.gemini_key;
}

el("settings-btn").addEventListener("click", async () => {
  await loadSettings();
  el("settings-scrape-do-key").value = savedSettings.scrape_do_key || "";
  el("settings-gemini-key").value = savedSettings.gemini_key || "";
  el("settings-status").textContent = "";
  openModal("settings-modal");
});

el("settings-save-btn").addEventListener("click", async () => {
  const scrape_do_key = el("settings-scrape-do-key").value.trim() || null;
  const gemini_key = el("settings-gemini-key").value.trim() || null;
  const res = await fetch("/api/settings", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scrape_do_key, gemini_key }),
  });
  if (res.ok) {
    savedSettings = { scrape_do_key, gemini_key };
    applySettingsToForm();
    updateStartPilotState();
    el("settings-status").textContent = "Saved.";
    setTimeout(() => closeModal("settings-modal"), 500);
  } else {
    el("settings-status").textContent = "Could not save settings.";
  }
});

// ---------------------------------------------------------------- history

const STATUS_LABELS = {
  uploaded: "Not started",
  pilot_running: "Running pilot",
  full_running: "Running full batch",
  awaiting_approval: "Awaiting approval",
  completed: "Completed",
  failed: "Failed",
};

function formatDate(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" }) +
    " · " + d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

function downloadIcon() {
  return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v12"/><path d="M7 11l5 5 5-5"/><path d="M5 21h14"/></svg>';
}

function resumeIcon() {
  return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="4" y1="12" x2="19" y2="12"/><polyline points="13 6 19 12 13 18"/></svg>';
}

async function loadHistory() {
  const list = el("history-list");
  list.innerHTML = '<p class="history-empty">Loading…</p>';
  const res = await fetch("/api/jobs");
  if (!res.ok) {
    list.innerHTML = '<p class="history-empty">Could not load history.</p>';
    return;
  }
  const jobs = await res.json();
  if (!jobs.length) {
    list.innerHTML = '<p class="history-empty">No runs yet - upload a file to get started.</p>';
    return;
  }

  list.innerHTML = "";
  for (const job of jobs) {
    const row = document.createElement("div");
    row.className = "history-item";

    const main = document.createElement("div");
    main.className = "history-item-main";
    main.innerHTML = `
      <span class="status-dot ${job.status}"></span>
      <div class="history-text">
        <div class="history-filename">${job.filename}</div>
        <div class="history-meta">${STATUS_LABELS[job.status] || job.status} · ${formatDate(job.created_at)}</div>
      </div>`;

    const actions = document.createElement("div");
    actions.className = "history-actions";
    if (job.has_pilot_output) {
      actions.innerHTML += `<a class="icon-btn" title="Download pilot file" href="/api/jobs/${job.id}/pilot/download" download>${downloadIcon()}</a>`;
    }
    if (job.has_full_output) {
      actions.innerHTML += `<a class="icon-btn" title="Download audited workbook" href="/api/jobs/${job.id}/full/download" download>${downloadIcon()}</a>`;
    }
    if (job.status === "awaiting_approval") {
      const resumeBtn = document.createElement("button");
      resumeBtn.className = "icon-btn";
      resumeBtn.title = "Open";
      resumeBtn.type = "button";
      resumeBtn.innerHTML = resumeIcon();
      resumeBtn.addEventListener("click", () => resumeJob(job.id));
      actions.appendChild(resumeBtn);
    }

    row.append(main, actions);
    list.appendChild(row);
  }
}

el("history-btn").addEventListener("click", () => {
  openModal("history-modal");
  loadHistory();
});

async function resumeJob(id) {
  jobId = id;
  closeModal("history-modal");
  clearInterval(pollTimer);
  const res = await fetch(`/api/jobs/${jobId}`);
  if (!res.ok) return;
  applyJobState(await res.json());
}

// ---------------------------------------------------------------- dropzone

const dropzone = el("dropzone");
const fileInput = el("file-input");
const fileNameEl = el("file-name");
const uploadBtn = el("upload-btn");

function setChosenFile(file) {
  if (!file) return;
  fileNameEl.textContent = file.name;
  uploadBtn.disabled = false;
}

fileInput.addEventListener("change", () => setChosenFile(fileInput.files[0]));

["dragenter", "dragover"].forEach((evt) =>
  dropzone.addEventListener(evt, (e) => {
    e.preventDefault();
    dropzone.classList.add("drag-over");
  }),
);
["dragleave", "drop"].forEach((evt) =>
  dropzone.addEventListener(evt, (e) => {
    e.preventDefault();
    dropzone.classList.remove("drag-over");
  }),
);
dropzone.addEventListener("drop", (e) => {
  const file = e.dataTransfer.files[0];
  if (!file) return;
  fileInput.files = e.dataTransfer.files;
  setChosenFile(file);
});

el("upload-btn").addEventListener("click", async () => {
  if (!fileInput.files.length) {
    el("upload-status").textContent = "Choose an .xlsx file first.";
    return;
  }
  const form = new FormData();
  form.append("file", fileInput.files[0]);
  uploadBtn.disabled = true;
  uploadBtn.textContent = "Uploading...";
  el("upload-status").textContent = "";

  try {
    const res = await fetch("/api/upload", { method: "POST", body: form });
    if (!res.ok) throw new Error((await res.json()).detail || "Upload failed");
    const data = await res.json();
    jobId = data.job_id;

    const select = el("sheet-select");
    select.innerHTML = "";
    for (const sheet of data.sheets) {
      const opt = document.createElement("option");
      opt.value = sheet;
      opt.textContent = sheet;
      select.appendChild(opt);
    }
    el("pilot-size").value = data.suggested_pilot_size;
    el("pilot-size").max = data.row_count;
    el("upload-status").textContent = `${data.filename} — ${data.row_count} products detected`;
    applySettingsToForm();
    updateStartPilotState();
    show("upload", "configure");
  } catch (e) {
    el("upload-status").textContent = e.message;
  } finally {
    uploadBtn.disabled = false;
    uploadBtn.textContent = "Upload";
  }
});

function updateStartPilotState() {
  const hasKey = el("gemini-key").value.trim().length > 0;
  el("start-pilot-btn").disabled = !hasKey;
  el("start-full-btn").disabled = !hasKey;
  el("gemini-key").classList.toggle("field-missing", !hasKey);
}

el("gemini-key").addEventListener("input", updateStartPilotState);

el("start-pilot-btn").addEventListener("click", async () => {
  const geminiKey = el("gemini-key").value.trim();
  if (!geminiKey) {
    updateStartPilotState();
    el("gemini-key").focus();
    return;
  }
  const body = {
    sheet: el("sheet-select").value,
    pilot_size: Number(el("pilot-size").value) || 100,
    scrape_do_key: el("scrape-do-key").value.trim() || null,
    gemini_key: geminiKey,
  };

  const res = await fetch(`/api/jobs/${jobId}/pilot`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    alert((await res.json()).detail || "Could not start pilot");
    return;
  }
  el("progress-title").textContent = "Running pilot...";
  el("log-box").textContent = "";
  show("progress");
  startPolling();
});

el("start-full-btn").addEventListener("click", async () => {
  const geminiKey = el("gemini-key").value.trim();
  if (!geminiKey) {
    updateStartPilotState();
    el("gemini-key").focus();
    return;
  }
  if (!confirm("Run the full audit on every product now, without a pilot review first?")) return;

  el("start-full-btn").disabled = true;
  const res = await fetch(`/api/jobs/${jobId}/full`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sheet: el("sheet-select").value,
      scrape_do_key: el("scrape-do-key").value.trim() || null,
      gemini_key: geminiKey,
    }),
  });
  if (!res.ok) {
    alert((await res.json()).detail || "Could not start full audit");
    updateStartPilotState();
    return;
  }
  el("progress-title").textContent = "Running full batch...";
  el("log-box").textContent = "";
  show("progress");
  startPolling();
});

el("approve-btn").addEventListener("click", async () => {
  el("approve-btn").disabled = true;
  const res = await fetch(`/api/jobs/${jobId}/approve`, { method: "POST" });
  if (!res.ok) {
    alert((await res.json()).detail || "Could not start full run");
    el("approve-btn").disabled = false;
    return;
  }
  el("progress-title").textContent = "Running full batch...";
  el("log-box").textContent = "";
  show("progress");
  startPolling();
});

el("revise-btn").addEventListener("click", async () => {
  const feedback = el("pilot-feedback").value.trim();
  if (!feedback) {
    el("pilot-feedback").focus();
    return;
  }
  if (!savedSettings.gemini_key) {
    alert("No Gemini key found - open Settings and save one first.");
    return;
  }
  el("revise-btn").disabled = true;
  const res = await fetch(`/api/jobs/${jobId}/pilot/revise`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      feedback,
      scrape_do_key: savedSettings.scrape_do_key || null,
      gemini_key: savedSettings.gemini_key,
    }),
  });
  el("revise-btn").disabled = false;
  if (!res.ok) {
    alert((await res.json()).detail || "Could not re-run the pilot");
    return;
  }
  el("pilot-feedback").value = "";
  el("progress-title").textContent = "Re-running pilot with your feedback...";
  el("log-box").textContent = "";
  show("progress");
  startPolling();
});

function startPolling() {
  clearInterval(pollTimer);
  pollTimer = setInterval(pollStatus, 2000);
  pollStatus();
}

const STAT_META = {
  rows_covered: { label: "Rows covered", tone: "" },
  corrected: { label: "Corrected", tone: "warn" },
  filled: { label: "Filled", tone: "good" },
  confirmed_ok: { label: "Confirmed OK", tone: "good" },
  unconfirmed: { label: "Unconfirmed", tone: "warn" },
  errors: { label: "Errors", tone: "bad" },
  load_errors: { label: "Load errors", tone: "bad" },
};

function animateNumber(node, target) {
  const duration = 600;
  const start = performance.now();
  function tick(now) {
    const progress = Math.min(1, (now - start) / duration);
    const eased = 1 - Math.pow(1 - progress, 3);
    node.textContent = Math.round(target * eased).toLocaleString();
    if (progress < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

function renderStats(container, stats) {
  container.innerHTML = "";
  for (const [key, meta] of Object.entries(STAT_META)) {
    if (!(key in stats)) continue;
    const div = document.createElement("div");
    div.className = `stat ${meta.tone}`;
    const value = document.createElement("div");
    value.className = "value";
    value.textContent = "0";
    const label = document.createElement("div");
    label.className = "label";
    label.textContent = meta.label;
    div.append(value, label);
    container.appendChild(div);
    animateNumber(value, stats[key]);
  }
}

function renderFeedbackHistory(history) {
  const box = el("feedback-history");
  const list = el("feedback-history-list");
  if (!history || !history.length) {
    box.classList.add("hidden");
    list.innerHTML = "";
    return;
  }
  list.innerHTML = "";
  history.forEach((text) => {
    const li = document.createElement("li");
    li.textContent = text;
    list.appendChild(li);
  });
  box.classList.remove("hidden");
}

function applyJobState(job) {
  el("progress-phase").textContent = job.phase ? `Phase: ${job.phase}` : "";
  el("log-box").textContent = (job.log || []).join("\n");
  el("log-box").scrollTop = el("log-box").scrollHeight;

  if (job.status === "pilot_running" || job.status === "full_running") {
    el("progress-title").textContent = job.status === "pilot_running" ? "Running pilot..." : "Running full batch...";
    show("progress");
    return;
  }

  if (job.status === "failed") {
    clearInterval(pollTimer);
    el("error-message").textContent = job.error || "Unknown error";
    show("error");
    return;
  }

  if (job.status === "awaiting_approval") {
    clearInterval(pollTimer);
    renderStats(el("pilot-stats"), job.stats_pilot);
    renderFeedbackHistory(job.feedback_history);
    el("pilot-download").href = `/api/jobs/${jobId}/pilot/download`;
    el("approve-btn").disabled = false;
    el("revise-btn").disabled = false;
    el("pilot-feedback").value = "";
    show("approval");
    return;
  }

  if (job.status === "completed") {
    clearInterval(pollTimer);
    renderStats(el("full-stats"), job.stats_full);
    el("full-download").href = `/api/jobs/${jobId}/full/download`;
    show("done");
    return;
  }
}

async function pollStatus() {
  const res = await fetch(`/api/jobs/${jobId}`);
  if (!res.ok) return;
  applyJobState(await res.json());
}

loadSettings();
