"use strict";
const el = (id) => document.getElementById(id);
const state = {
  workspace: null,
  schedule: null,
  epoch: 0,
  readEpoch: 0,
  versionEpoch: 0,
  previewEpoch: 0,
  busy: false,
  pending: null,
  template: null,
  workspaces: [],
  workspaceCursor: null,
  workflows: [],
  workflowCursor: null,
  schedules: [],
  scheduleCursor: null,
  view: null,
  version: null,
  preview: null,
};
const pendingKey = "dagrivane-calendar-pending";
const scopeKey = "dagrivane-calendar-scope";
const utc = (value) =>
  new Date(value * 1000).toISOString().replace(".000Z", "Z");
function node(tag, text) {
  const value = document.createElement(tag);
  if (text !== undefined && text !== null) value.textContent = String(text);
  return value;
}
function status(text) {
  el("calendar-status").textContent = text;
}
function error(text) {
  el("calendar-error").textContent = text || "";
  el("calendar-error").hidden = !text;
}
function handler(fn) {
  return async (event) => {
    event?.preventDefault();
    error("");
    try {
      await fn(event);
    } catch (failure) {
      error(failure.message);
    }
  };
}
async function request(action, fields = {}) {
  const response = await fetch("/api/evaluate", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Local-Request": "1" },
    body: JSON.stringify({ mode: "calendar", action, ...fields }),
  });
  let value;
  try {
    value = await response.json();
  } catch (_) {
    throw new Error(
      "The response was interrupted. Recover the saved response before submitting again.",
    );
  }
  if (!response.ok) {
    const failure = new Error(
      value.error || `Request failed (${response.status})`,
    );
    failure.rejected = true;
    throw failure;
  }
  return value;
}
function syncButtons() {
  document.querySelectorAll("[data-write]").forEach((button) => {
    button.disabled = state.busy || Boolean(state.pending);
  });
  el("create-schedule").disabled =
    state.busy || Boolean(state.pending) || !state.preview || !state.version;
}
function pending(meta) {
  try {
    if (meta) sessionStorage.setItem(pendingKey, JSON.stringify(meta));
    else sessionStorage.removeItem(pendingKey);
  } catch (_) {
    throw new Error(
      "Tab storage is unavailable; the new request was not sent.",
    );
  }
  state.pending = meta;
  el("pending-operation").hidden = !meta;
  el("pending-description").textContent = meta
    ? `A ${meta.action} response may already be saved for workspace ${meta.workspace}. Recover it before submitting another operation.`
    : "";
  syncButtons();
}
function download(value) {
  const url = URL.createObjectURL(
    new Blob([value.content], {
      type: "application/json",
    }),
  );
  const link = node("a");
  link.href = url;
  link.download = `dagrivane-schedule-${value.schedule.id}-g${value.schedule.generation}.json`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
async function execute(action, fields = {}) {
  if (state.busy || state.pending)
    throw new Error("Finish or recover the current operation first.");
  if (!state.workspace) throw new Error("Choose a workspace first.");
  const epoch = state.epoch;
  const payload = {
    workspace: state.workspace,
    request_key: crypto.randomUUID(),
    ...fields,
  };
  const meta = {
    action,
    workspace: state.workspace,
    schedule: state.schedule,
    fields: payload,
  };
  pending(meta);
  state.busy = true;
  syncButtons();
  status("Saving calendar operation");
  try {
    const value = await request(action, payload);
    pending(null);
    if (epoch !== state.epoch) {
      status(
        "Operation saved in the previous selection. Open it to inspect the result.",
      );
      return null;
    }
    try {
      await refreshSchedules();
    } catch (failure) {
      error(`Operation saved. Refresh failed: ${failure.message}`);
    }
    status("Calendar operation saved");
    return value;
  } catch (failure) {
    if (failure.rejected) pending(null);
    throw failure;
  } finally {
    state.busy = false;
    syncButtons();
  }
}
function invalidatePreview() {
  state.previewEpoch++;
  state.preview = null;
  el("new-preview").hidden = true;
  syncButtons();
}
function signature() {
  return JSON.stringify({
    definition: buildDefinition(),
    workflow: el("workflow").value,
    version: el("workflow-version").value,
  });
}
function buildDefinition() {
  const trigger =
    el("trigger-kind").value === "interval"
      ? { kind: "interval", seconds: Number(el("interval-seconds").value) }
      : {
          kind: "cron",
          expression: el("cron-expression").value.trim(),
          timezone: el("timezone").value.trim(),
          fold: el("fold").value,
          gap: "skip",
          day_match: el("day-match").value,
        };
  return {
    version: 1,
    name: el("new-name").value.trim(),
    trigger,
    start_at: el("starts-at").value.trim(),
    end_at: el("ends-at").value.trim() || null,
    misfire: {
      policy: el("misfire").value,
      max_lateness_seconds:
        el("max-lateness").value === ""
          ? null
          : Number(el("max-lateness").value),
    },
    overlap: el("overlap").value,
    priority: Number(el("priority").value),
  };
}
function slots(tableId, items) {
  const body = el(tableId).querySelector("tbody");
  body.replaceChildren();
  for (const item of items) {
    const row = node("tr");
    row.append(
      node("td", item.utc),
      node("td", item.local || "UTC interval"),
      node("td", item.fold ?? "—"),
    );
    body.append(row);
  }
  if (!items.length) {
    const row = node("tr");
    const cell = node("td", "No remaining slots in the declared range");
    cell.colSpan = 3;
    row.append(cell);
    body.append(row);
  }
}
function renderSchedules() {
  const root = el("schedule-list");
  root.replaceChildren();
  if (!state.schedules.length)
    root.append(
      node(
        "p",
        "No schedules saved in this workspace. Preview a new schedule below.",
      ),
    );
  for (const item of state.schedules) {
    const button = node("button");
    button.type = "button";
    button.dataset.schedule = item.id;
    button.setAttribute("aria-pressed", String(item.id === state.schedule));
    button.append(
      node("span", item.definition.name),
      node(
        "small",
        `${item.enabled ? "Enabled" : "Paused"} / v${item.version} / ${item.next_due === null ? "Clock exhausted" : utc(item.next_due)}`,
      ),
    );
    button.addEventListener(
      "click",
      handler(() => selectSchedule(item.id)),
    );
    root.append(button);
  }
}
async function compatiblePage(value) {
  if (
    value.schedule.generation !== state.view.schedule.generation ||
    value.schedule.plan_sha256 !== state.view.schedule.plan_sha256 ||
    value.audit.passed !== state.view.audit.passed
  ) {
    await refreshSchedules();
    status("Schedule changed while paging; the saved view was refreshed");
    return false;
  }
  return true;
}

function renderEvents(items, append = false) {
  const root = el("decision-events");
  if (!append) root.replaceChildren();
  for (const event of items) {
    const item = node("li", event.kind.replaceAll("_", " "));
    item.append(node("small", `${event.sequence} / ${utc(event.created)}`));
    const details = node("details");
    details.append(
      node("summary", "Recorded decision"),
      node("pre", JSON.stringify(event.payload, null, 2)),
    );
    item.append(details);
    root.append(item);
  }
}
function renderOccurrences(items, append = false) {
  const root = el("slot-outcomes").querySelector("tbody");
  if (!append) root.replaceChildren();
  for (const item of items) {
    const row = node("tr");
    row.append(node("td", utc(item.due)), node("td", item.outcome));
    const cell = node("td");
    if (item.run_id) {
      const button = node("button", `Inspect ${item.run_id.slice(0, 8)}`);
      button.type = "button";
      button.addEventListener(
        "click",
        handler(async () => {
          const epoch = state.epoch;
          const value = await request("run", {
            workspace: state.workspace,
            schedule: item.schedule_id,
            run: item.run_id,
          });
          if (epoch !== state.epoch) return;
          el("run-json").textContent = JSON.stringify(value, null, 2);
          el("run-inspection").hidden = false;
          el("run-inspection").open = true;
        }),
      );
      cell.append(button);
    } else cell.append(node("span", "No run queued"));
    row.append(cell);
    root.append(row);
  }
}
function renderView(value) {
  state.view = value;
  const schedule = value.schedule;
  const previous = state.schedules.findIndex((item) => item.id === schedule.id);
  if (previous < 0) state.schedules.push(schedule);
  else state.schedules[previous] = schedule;
  renderSchedules();
  el("schedule-detail").hidden = false;
  el("schedule-history").hidden = false;
  el("schedule-name").textContent = schedule.definition.name;
  el("schedule-meta").textContent =
    `${schedule.enabled ? "Enabled" : "Paused"} / generation ${schedule.generation} / ${value.workflow.name} v${schedule.version}`;
  el("schedule-audit").textContent = value.audit.passed
    ? "Complete retained decision audit passed. Worker effects are checked separately."
    : `Decision audit unavailable: ${value.audit.error}`;
  el("schedule-audit").className = value.audit.passed ? "muted" : "audit-error";
  slots("retained-slots", value.preview.items);
  el("slots-caption").textContent =
    `Next retained slots from the saved cursor; observed at ${utc(value.observed_at)}${schedule.enabled ? "" : ". Paused schedules preserve their cursor"}`;
  el("toggle-enabled").textContent = schedule.enabled
    ? "Pause schedule"
    : "Resume schedule";
  el("saved-definition").textContent = JSON.stringify(
    {
      definition: schedule.definition,
      plan_sha256: schedule.plan_sha256,
      workflow: value.workflow,
    },
    null,
    2,
  );
  renderEvents(value.events.items);
  renderOccurrences(value.occurrences.items);
  el("more-events").hidden = !value.events.has_more;
  el("more-occurrences").hidden = !value.occurrences.has_more;
}
async function refreshSchedules() {
  if (!state.workspace) return;
  const epoch = state.epoch;
  const readEpoch = ++state.readEpoch;
  const [listing, view] = await Promise.all([
    request("schedules", { workspace: state.workspace }),
    state.schedule
      ? request("view", {
          workspace: state.workspace,
          schedule: state.schedule,
        })
      : Promise.resolve(null),
  ]);
  if (epoch !== state.epoch || readEpoch !== state.readEpoch) return;
  state.schedules = listing.items;
  state.scheduleCursor = listing.next_cursor;
  el("more-schedules").hidden = !listing.has_more;
  renderSchedules();
  if (view) renderView(view);
}
async function selectSchedule(id) {
  state.epoch++;
  state.schedule = id;
  state.view = null;
  el("schedule-detail").hidden = true;
  el("schedule-history").hidden = true;
  el("run-inspection").hidden = true;
  sessionStorage.setItem(
    scopeKey,
    JSON.stringify({ workspace: state.workspace, schedule: id }),
  );
  await refreshSchedules();
}
function workflowOptions() {
  const chosen = el("workflow").value;
  el("workflow").replaceChildren();
  for (const item of state.workflows) {
    const option = node("option", item.name);
    option.value = item.id;
    el("workflow").append(option);
  }
  if (state.workflows.some((item) => item.id === chosen))
    el("workflow").value = chosen;
}
async function loadVersion() {
  invalidatePreview();
  state.version = null;
  syncButtons();
  if (!el("workflow").value) return;
  const epoch = state.epoch;
  const token = ++state.versionEpoch;
  const workflow = el("workflow").value;
  const version = Number(el("workflow-version").value);
  const value = await request("version", {
    workspace: state.workspace,
    workflow,
    version,
  });
  if (
    epoch !== state.epoch ||
    token !== state.versionEpoch ||
    workflow !== el("workflow").value ||
    version !== Number(el("workflow-version").value)
  )
    return;
  state.version = value;
  el("workflow-pin").textContent =
    `${value.name} / pinned v${value.version} / ${value.specification.tasks.length} tasks / ${value.specification.execution_policy || "in-place execution"}`;
  syncButtons();
}
async function chooseWorkspace(id, preferredSchedule = null) {
  state.epoch++;
  state.workspace = id;
  state.schedule = preferredSchedule;
  state.schedules = [];
  state.view = null;
  state.workflows = [];
  state.version = null;
  invalidatePreview();
  el("workspace-content").hidden = !id;
  el("schedule-detail").hidden = true;
  el("schedule-history").hidden = true;
  el("run-inspection").hidden = true;
  el("dispatch-result").hidden = true;
  el("workspace-id").textContent = id || "";
  sessionStorage.setItem(
    scopeKey,
    JSON.stringify({ workspace: id, schedule: preferredSchedule }),
  );
  if (!id) return;
  const epoch = state.epoch;
  const [workflows] = await Promise.all([
    request("workflows", { workspace: id }),
    refreshSchedules(),
  ]);
  if (epoch !== state.epoch) return;
  state.workflows = workflows.items;
  state.workflowCursor = workflows.next_cursor;
  el("more-workflows").hidden = !workflows.has_more;
  workflowOptions();
  el("workflow-version").value =
    state.workflows.find((item) => item.id === el("workflow").value)?.version ||
    1;
  await loadVersion();
  if (!state.schedule && state.schedules.length)
    await selectSchedule(state.schedules[0].id);
  if (!state.schedules.length) el("new-schedule").open = true;
}
async function openWorkspaces(preferred = null, preferredSchedule = null) {
  const listing = await request("overview");
  state.workspaces = listing.workspaces;
  state.workspaceCursor = listing.next_cursor;
  state.template = listing.template;
  el("more-workspaces").hidden = !listing.has_more;
  el("initialize-section").hidden =
    listing.initialized && listing.workspaces.length > 0;
  if (!el("decision-clock").value)
    el("decision-clock").value = utc(listing.now);
  if (!el("starts-at").value) el("starts-at").value = listing.template.start_at;
  if (preferred && !state.workspaces.some((item) => item.id === preferred))
    state.workspaces.push(await request("workspace", { workspace: preferred }));
  el("workspace").replaceChildren();
  for (const item of state.workspaces) {
    const option = node("option", item.name);
    option.value = item.id;
    el("workspace").append(option);
  }
  const selected = preferred || state.workspaces[0]?.id || null;
  if (selected) el("workspace").value = selected;
  await chooseWorkspace(selected, preferredSchedule);
}
function dispatchResult(value) {
  el("dispatch-result").hidden = false;
  el("dispatch-summary").textContent =
    `${value.outcome_count} slot outcomes at ${utc(value.now)}. ${value.due_remaining ? "Due work remains; review and dispatch another bounded batch." : "No enabled due slots remain at this clock."}`;
  el("dispatch-json").textContent = JSON.stringify(value, null, 2);
}

el("workspace").addEventListener(
  "change",
  handler(() => chooseWorkspace(el("workspace").value)),
);
el("initialize-calendar").addEventListener(
  "click",
  handler(async () => {
    if (state.busy || state.pending)
      throw new Error("Recover the current operation first.");
    const value = await request("initialize");
    await openWorkspaces(value.workspace_id);
    status("Reviewed order workflow available");
  }),
);
el("refresh-calendar").addEventListener(
  "click",
  handler(async () => {
    await refreshSchedules();
    status("Saved schedule view refreshed");
  }),
);
el("current-clock").addEventListener(
  "click",
  handler(async () => {
    const value = await request("overview");
    el("decision-clock").value = utc(value.now);
    status("Decision clock set from the local server");
  }),
);
el("dispatch-form").addEventListener(
  "submit",
  handler(async () => {
    const value = await execute("dispatch", {
      now: el("decision-clock").value.trim(),
      limit: Number(el("dispatch-limit").value),
    });
    if (value) dispatchResult(value);
  }),
);
el("workflow").addEventListener(
  "change",
  handler(async () => {
    el("workflow-version").value = state.workflows.find(
      (item) => item.id === el("workflow").value,
    ).version;
    await loadVersion();
  }),
);
el("workflow-version").addEventListener("input", () => {
  state.version = null;
  invalidatePreview();
});
el("workflow-version").addEventListener("change", handler(loadVersion));
for (const id of [
  "new-name",
  "trigger-kind",
  "interval-seconds",
  "cron-expression",
  "timezone",
  "fold",
  "day-match",
  "starts-at",
  "ends-at",
  "misfire",
  "max-lateness",
  "overlap",
  "priority",
])
  el(id).addEventListener("input", invalidatePreview);
el("trigger-kind").addEventListener("change", () => {
  const cron = el("trigger-kind").value === "cron";
  el("cron-fields").hidden = !cron;
  el("interval-field").hidden = cron;
  invalidatePreview();
});
el("preview-schedule").addEventListener(
  "click",
  handler(async () => {
    if (state.busy) throw new Error("Wait for the current operation.");
    const epoch = state.epoch;
    const token = ++state.previewEpoch;
    const original = signature();
    const value = await request("preview", {
      definition: buildDefinition(),
      limit: 8,
    });
    if (
      epoch !== state.epoch ||
      token !== state.previewEpoch ||
      original !== signature()
    )
      return;
    state.preview = value;
    slots("preview-slots", value.items);
    el("preview-pin").textContent =
      `Reviewed clock ${value.plan_sha256.slice(0, 12)}${value.definition.trigger.kind === "cron" ? ` / retained ${value.definition.trigger.timezone.name} rules` : " / UTC interval"}`;
    el("new-preview").hidden = false;
    syncButtons();
    status("Retained clock preview ready");
  }),
);
el("schedule-form").addEventListener(
  "submit",
  handler(async () => {
    if (!state.preview || !state.version)
      throw new Error("Review the clock and a saved workflow version first.");
    const value = await execute("create", {
      workflow: state.version.workflow,
      version: state.version.version,
      workflow_sha256: state.version.fingerprint,
      definition: state.preview.definition,
      preview_sha256: state.preview.plan_sha256,
      now: el("decision-clock").value.trim(),
    });
    if (value) {
      el("new-schedule").open = false;
      invalidatePreview();
      await selectSchedule(value.id);
    }
  }),
);
el("enabled-form").addEventListener(
  "submit",
  handler(async () => {
    if (!state.view) return;
    await execute("set-enabled", {
      schedule: state.schedule,
      enabled: !state.view.schedule.enabled,
      generation: state.view.schedule.generation,
      reason: el("enabled-reason").value.trim(),
      now: el("decision-clock").value.trim(),
    });
  }),
);
el("export-schedule").addEventListener(
  "click",
  handler(async () => {
    const value = await execute("export", { schedule: state.schedule });
    if (value) download(value);
  }),
);
el("more-events").addEventListener(
  "click",
  handler(async () => {
    const epoch = state.epoch;
    const cursor = state.view.events.next_cursor;
    const value = await request("view", {
      workspace: state.workspace,
      schedule: state.schedule,
      event_after: cursor,
    });
    if (epoch !== state.epoch || cursor !== state.view.events.next_cursor)
      return;
    if (!(await compatiblePage(value))) return;
    renderEvents(value.events.items, true);
    state.view.events = value.events;
    el("more-events").hidden = !value.events.has_more;
  }),
);
el("more-occurrences").addEventListener(
  "click",
  handler(async () => {
    const epoch = state.epoch;
    const cursor = state.view.occurrences.next_cursor;
    const value = await request("view", {
      workspace: state.workspace,
      schedule: state.schedule,
      due_after: cursor,
    });
    if (epoch !== state.epoch || cursor !== state.view.occurrences.next_cursor)
      return;
    if (!(await compatiblePage(value))) return;
    renderOccurrences(value.occurrences.items, true);
    state.view.occurrences = value.occurrences;
    el("more-occurrences").hidden = !value.occurrences.has_more;
  }),
);
el("more-schedules").addEventListener(
  "click",
  handler(async () => {
    const epoch = state.epoch;
    const cursor = state.scheduleCursor;
    const value = await request("schedules", {
      workspace: state.workspace,
      after: cursor,
    });
    if (epoch !== state.epoch || cursor !== state.scheduleCursor) return;
    for (const item of value.items)
      if (!state.schedules.some((existing) => existing.id === item.id))
        state.schedules.push(item);
    state.scheduleCursor = value.next_cursor;
    el("more-schedules").hidden = !value.has_more;
    renderSchedules();
  }),
);
el("more-workflows").addEventListener(
  "click",
  handler(async () => {
    const epoch = state.epoch;
    const cursor = state.workflowCursor;
    const value = await request("workflows", {
      workspace: state.workspace,
      after: cursor,
    });
    if (epoch !== state.epoch || cursor !== state.workflowCursor) return;
    for (const item of value.items)
      if (!state.workflows.some((existing) => existing.id === item.id))
        state.workflows.push(item);
    state.workflowCursor = value.next_cursor;
    el("more-workflows").hidden = !value.has_more;
    workflowOptions();
  }),
);
el("more-workspaces").addEventListener(
  "click",
  handler(async () => {
    const cursor = state.workspaceCursor;
    const value = await request("overview", { after: cursor });
    if (cursor !== state.workspaceCursor) return;
    for (const item of value.workspaces)
      if (!state.workspaces.some((existing) => existing.id === item.id)) {
        state.workspaces.push(item);
        const option = node("option", item.name);
        option.value = item.id;
        el("workspace").append(option);
      }
    state.workspaceCursor = value.next_cursor;
    el("more-workspaces").hidden = !value.has_more;
  }),
);
el("recover-operation").addEventListener(
  "click",
  handler(async () => {
    const meta = state.pending;
    if (!meta) return;
    if (meta.workspace !== state.workspace)
      throw new Error(
        "Open the pending request's workspace before recovering it.",
      );
    const epoch = state.epoch;
    let value;
    try {
      value = await request("receipt", {
        workspace: meta.workspace,
        request_key: meta.fields.request_key,
      });
    } catch (failure) {
      if (!failure.rejected || !failure.message.includes("response not found"))
        throw failure;
      try {
        value = await request(meta.action, meta.fields);
      } catch (retry) {
        if (retry.rejected) pending(null);
        throw retry;
      }
    }
    pending(null);
    if (epoch !== state.epoch) {
      status(
        "Response recovered for the previous selection; refresh that selection to inspect it.",
      );
      return;
    }
    if (meta.action === "create") {
      invalidatePreview();
      el("new-schedule").open = false;
      await selectSchedule(value.id);
    } else {
      if (meta.action === "export") download(value);
      else if (meta.action === "dispatch") dispatchResult(value);
      try {
        await refreshSchedules();
      } catch (failure) {
        error(`Response recovered. Refresh failed: ${failure.message}`);
      }
    }
    status("Original request resolved with its retained identity");
  }),
);

handler(async () => {
  const retained = sessionStorage.getItem(pendingKey);
  if (retained) pending(JSON.parse(retained));
  const saved = JSON.parse(sessionStorage.getItem(scopeKey) || "null");
  await openWorkspaces(saved?.workspace, saved?.schedule);
  syncButtons();
})();
