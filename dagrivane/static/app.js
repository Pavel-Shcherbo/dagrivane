const $ = (id) => document.getElementById(id);
const clear = (node) => node.replaceChildren();
function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}
function error(message) {
  $("error").hidden = !message;
  $("error").textContent = message || "";
}
async function evaluate(params) {
  const response = await fetch("/api/evaluate", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Local-Request": "1" },
    body: JSON.stringify(params),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(result.error);
  return result;
}
function download(name, value) {
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(value, null, 2)], { type: "application/json" }),
  );
  const a = element("a");
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

let tasks = [];
function draw() {
  const svg = $("graph");
  clear(svg);
  svg.setAttribute(
    "aria-label",
    tasks.map((task) => task.id + " " + task.status).join(", "),
  );
  const ns = "http://www.w3.org/2000/svg";
  tasks.forEach((task, i) => {
    const gap = tasks.length > 1 ? 708 / (tasks.length - 1) : 0;
    const x = 20 + i * gap;
    if (i) {
      const line = document.createElementNS(ns, "line");
      Object.entries({
        x1: x - gap + 192,
        y1: 110,
        x2: x,
        y2: 110,
        class: "gridline",
      }).forEach(([k, v]) => line.setAttribute(k, v));
      svg.append(line);
    }
    const group = document.createElementNS(ns, "g");
    const rect = document.createElementNS(ns, "rect");
    Object.entries({
      x,
      y: 68,
      width: 192,
      height: 84,
      rx: 8,
      class: "graphnode " + task.status,
    }).forEach(([k, v]) => rect.setAttribute(k, v));
    group.append(rect);
    for (const [text, y, cls] of [
      [task.id, 100, "nodelabel"],
      [task.status, 126, "nodestate"],
    ]) {
      const label = document.createElementNS(ns, "text");
      label.setAttribute("x", x + 16);
      label.setAttribute("y", y);
      label.setAttribute("class", cls);
      label.textContent = text;
      group.append(label);
    }
    svg.append(group);
  });
}
function render() {
  clear($("rows"));
  for (const task of tasks) {
    const row = element("tr", undefined, "taskrow");
    row.tabIndex = 0;
    row.setAttribute("aria-label", task.id + " " + task.status);
    row.append(
      element("td", task.id, "mono"),
      element("td", task.status, "status " + task.status),
      element("td", String(task.attempts), "number"),
    );
    function select() {
      document
        .querySelectorAll(".taskrow")
        .forEach((x) => x.classList.remove("selected"));
      row.classList.add("selected");
      $("logtitle").textContent = task.id + " output";
      $("logs").textContent = task.output || "No output recorded";
    }
    row.onclick = select;
    row.onkeydown = (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        select();
      }
    };
    $("rows").append(row);
  }
  draw();
}
$("run").onclick = async () => {
  error("");
  $("run").disabled = true;
  $("fail").disabled = true;
  $("mode").disabled = true;
  $("runstate").textContent = "Running";
  try {
    const result = await evaluate({
      fail_quality: $("fail").checked,
      mode: $("mode").value,
    });
    tasks = result.tasks;
    render();
    $("runstate").textContent = result.succeeded
      ? "All gates passed"
      : "Release blocked";
    $("runstate").className =
      "status " + (result.succeeded ? "succeeded" : "failed");
    $("runid").textContent = "Run " + result.run_id;
    $("count").textContent =
      tasks.filter((x) => x.status === "succeeded").length +
      " of " +
      tasks.length +
      " succeeded";
    $("rows").firstElementChild?.click();
  } catch (e) {
    error(e.message);
    $("runstate").textContent = "Run could not start";
  } finally {
    $("run").disabled = false;
    $("fail").disabled = false;
    $("mode").disabled = false;
  }
};
$("mode").disabled = true;
$("run").disabled = true;
fetch("/api/example")
  .then((r) => r.json())
  .then((spec) => {
    tasks = spec.tasks.map((t) => ({
      ...t,
      status: "pending",
      attempts: 0,
      output: "",
    }));
    render();
  })
  .catch((e) => error(e.message))
  .finally(() => {
    $("mode").disabled = false;
    $("run").disabled = false;
  });

$("mode").onchange = () => {
  const ids =
    $("mode").value === "dataflow"
      ? ["validate", "aggregate", "release-check"]
      : ["extract", "quality", "aggregate", "release-check"];
  tasks = ids.map((id) => ({ id, status: "pending", attempts: 0, output: "" }));
  render();
  $("count").textContent = tasks.length + " tasks";
  $("runstate").textContent = "Not started";
  $("runstate").className = "status";
  $("runid").textContent = "";
  $("logs").textContent =
    "Run the pipeline, then select a task to inspect its output";
  $("logtitle").textContent = "Task output";
};

let releaseResult = null;
function resetRelease() {
  releaseResult = null;
  $("release-export").disabled = true;
  $("release-error").hidden = true;
  $("release-status").textContent = "Scenario changed; run the check";
  $("release-reason").textContent =
    "The control builds an executable package against the synthetic order summary";
  for (const id of [
    "release-summary",
    "release-lineage",
    "release-environments",
    "release-identity",
  ])
    $(id).textContent = "";
  clear($("release-steps"));
  clear($("release-artifacts"));
}
$("release-scenario").addEventListener("input", resetRelease);
$("release-run").onclick = async () => {
  resetRelease();
  $("release-status").textContent = "Building and verifying";
  $("release-run").disabled = true;
  $("release-scenario").disabled = true;
  try {
    releaseResult = await evaluate({
      mode: "release",
      scenario: $("release-scenario").value,
    });
    const report = releaseResult;
    $("release-status").textContent = report.passed
      ? "Local release verified"
      : "Local release blocked";
    for (const step of report.steps) {
      const row = element("tr");
      row.append(
        element("td", step.name),
        element(
          "td",
          step.status,
          "status " + (step.status === "passed" ? "succeeded" : "failed"),
        ),
      );
      $("release-steps").append(row);
    }
    $("release-reason").textContent =
      report.blocked_reason ||
      "Content, contracts and local prerequisite pointers passed";
    $("release-summary").textContent =
      "Executable package returned " +
      report.application.result.orders +
      " orders; USD " +
      report.application.result.currencies.USD;
    for (const artifact of report.artifacts) {
      const row = element("tr");
      const name = element("td", undefined, "artifact-name");
      name.append(element("span", artifact.name));
      const details = element("details", undefined, "artifact-mobile-digest");
      details.append(
        element("summary", "SHA-256"),
        element("code", artifact.sha256),
      );
      name.append(details);
      row.append(
        name,
        element("td", artifact.kind),
        element(
          "td",
          artifact.sha256,
          "mono digest-cell artifact-digest-column",
        ),
        element(
          "td",
          artifact.verification.passed ? "Verified" : "Changed",
          "status " + (artifact.verification.passed ? "succeeded" : "failed"),
        ),
      );
      $("release-artifacts").append(row);
    }
    const names = new Map(
      report.lineage.nodes.map((node) => [node.id, node.name]),
    );
    $("release-lineage").textContent = report.lineage.edges
      .map(
        (edge) =>
          names.get(edge.child) + " depends on " + names.get(edge.parent),
      )
      .join("; ");
    $("release-environments").textContent = report.environments
      .map(
        (environment) =>
          environment.name +
          ": " +
          (environment.release_id
            ? "pinned to this release"
            : "no release selected") +
          " (generation " +
          environment.generation +
          ")",
      )
      .join("; ");
    $("release-identity").textContent =
      "Manifest SHA-256 " + report.release.sha256;
    $("release-export").disabled = false;
  } catch (exception) {
    $("release-status").textContent = "Release check could not start";
    $("release-error").textContent = exception.message;
    $("release-error").hidden = false;
  } finally {
    $("release-run").disabled = false;
    $("release-scenario").disabled = false;
  }
};
$("release-export").onclick = () => {
  if (releaseResult) download("dagrivane-release-check.json", releaseResult);
};

(() => {
  const api = (action, params = {}) =>
    evaluate({ mode: "control", action, ...params });
  const labels = {
    queued: "Queued",
    running: "Running",
    succeeded: "Succeeded",
    failed: "Failed",
    cancel_requested: "Stopping",
    cancelled: "Cancelled",
    orphaned: "Needs reconciliation",
    pending: "Pending",
    blocked: "Blocked",
  };
  const date = (value) =>
    value == null ? "—" : new Date(value * 1000).toLocaleString();
  const state = (value) =>
    element("span", labels[value] || value, "ops-state " + value);
  let overview = null,
    runs = [],
    next = null,
    selected = null,
    detail = null;
  let overviewSerial = 0,
    inspectSerial = 0,
    timer = null,
    busy = false,
    taskID = null;
  let events = new Map(),
    eventCursor = 0,
    eventMore = false;
  let submitKey = null;
  const retryKeys = new Map(),
    working = new Set();
  function showError(message) {
    $("ops-error").hidden = !message;
    $("ops-error").textContent = message || "";
  }
  function controls() {
    for (const id of [
      "ops-init",
      "ops-submit",
      "ops-publish",
      "ops-retry",
      "ops-cancel",
    ])
      $(id).disabled = busy;
    $("ops-submit").disabled =
      busy ||
      !overview?.versions?.some(
        (v) => v.approved && String(v.version) === $("ops-version").value,
      );
    $("ops-work").hidden =
      !detail ||
      !["queued", "running", "cancel_requested"].includes(detail.run.status);
    $("ops-work").disabled =
      !detail || detail.run.status !== "queued" || working.has(selected);
    $("ops-cancel").hidden =
      !detail ||
      !["queued", "running", "cancel_requested"].includes(detail.run.status);
    $("ops-retry").hidden =
      !detail ||
      !["failed", "cancelled", "orphaned"].includes(detail.run.status);
  }
  function renderRuns() {
    clear($("ops-runs"));
    for (const run of runs) {
      const row = element("tr");
      row.dataset.selected = String(run.id === selected);
      const cell = element("td");
      const button = element("button", "Run " + run.cursor);
      button.setAttribute("aria-pressed", String(run.id === selected));
      button.addEventListener("click", () => select(run.id));
      cell.append(button, element("span", run.id.slice(0, 8), "mono notice"));
      cell.append(element("small", date(run.created), "ops-mobile-date"));
      const statusCell = element("td");
      statusCell.append(state(run.status));
      row.append(
        cell,
        element("td", "v" + run.version),
        statusCell,
        element("td", date(run.created), "ops-created"),
      );
      $("ops-runs").append(row);
    }
    $("ops-empty").hidden = runs.length > 0;
    $("ops-empty").textContent = $("ops-filter").value
      ? "No runs in this state."
      : "No runs yet. Queue a workflow version to begin.";
    $("ops-older").hidden = next === null;
  }
  async function refresh(older = false) {
    const serial = ++overviewSerial;
    const filter = $("ops-filter").value;
    const result = await api("overview", {
      status: filter || null,
      before: older ? next : null,
    });
    if (serial !== overviewSerial || filter !== $("ops-filter").value) return;
    overview = result;
    $("ops-init").hidden = result.initialized;
    $("ops-ready").hidden = !result.initialized;
    if (!result.initialized) {
      $("ops-status").textContent =
        "Create a local workspace to save workflow versions and run history.";
      controls();
      return;
    }
    const versionValue = $("ops-version").value;
    clear($("ops-version"));
    for (const version of result.versions) {
      const option = element(
        "option",
        "v" +
          version.version +
          " · " +
          version.label +
          (version.approved ? "" : " · requires review"),
      );
      option.value = version.version;
      option.disabled = !version.approved;
      $("ops-version").append(option);
    }
    if (
      [...$("ops-version").options].some(
        (o) => o.value === versionValue && !o.disabled,
      )
    )
      $("ops-version").value = versionValue;
    const profileValue = $("ops-profile").value;
    clear($("ops-profile"));
    for (const profile of result.profiles) {
      const option = element("option", profile.label);
      option.value = profile.id;
      $("ops-profile").append(option);
    }
    if (profileValue) $("ops-profile").value = profileValue;
    runs = older
      ? [
          ...new Map(
            [...runs, ...result.runs].map((run) => [run.id, run]),
          ).values(),
        ]
      : result.runs;
    next = result.next_cursor;
    const total = Object.values(result.counts).reduce((a, b) => a + b, 0);
    $("ops-status").textContent =
      "Local orders · " +
      total +
      (total === 1
        ? " saved run · Latest workflow v"
        : " saved runs · Latest workflow v") +
      result.versions.at(-1).version;
    renderRuns();
    controls();
    if (!selected && runs.length) await select(runs[0].id);
  }
  function renderDetail() {
    if (!detail) return;
    const run = detail.run;
    $("ops-detail").hidden = false;
    $("ops-title").textContent = "Order checks · v" + run.version;
    $("ops-selection").replaceChildren(
      state(run.status),
      document.createTextNode(" · " + date(run.created)),
    );
    $("ops-identity").textContent =
      run.id + (run.retry_of ? " · Retry of " + run.retry_of : "");
    clear($("ops-tasks"));
    if (!detail.tasks.some((t) => t.id === taskID))
      taskID = detail.tasks[0]?.id;
    for (const task of detail.tasks) {
      const button = element(
        "button",
        task.id + " · " + (labels[task.status] || task.status),
      );
      button.setAttribute("aria-pressed", String(task.id === taskID));
      button.addEventListener("click", () => {
        taskID = task.id;
        renderDetail();
      });
      $("ops-tasks").append(button);
    }
    const task = detail.tasks.find((t) => t.id === taskID);
    $("ops-log-title").textContent = task
      ? task.id +
        " · " +
        task.attempts +
        (task.attempts === 1 ? " attempt" : " attempts")
      : "Task output";
    $("ops-log").textContent =
      (task?.output || "No recorded output.") +
      (task?.output_truncated
        ? "\n[Retained output tail; earlier output omitted]"
        : "");
    $("ops-pins").textContent = JSON.stringify(
      {
        fingerprint: detail.workflow.fingerprint,
        inputs: detail.workflow.inputs,
        complete_task_history: detail.task_history_complete,
      },
      null,
      2,
    );
    clear($("ops-output"));
    if (detail.output) {
      $("ops-output").append(
        element("h3", "Order summary · Checked on refresh"),
        state(detail.output.passed ? "succeeded" : "failed"),
      );
      $("ops-output").append(
        element(
          "pre",
          JSON.stringify(
            detail.output.rows || {
              error: detail.output.error,
              checks: detail.output.checks,
            },
            null,
            2,
          ),
        ),
      );
    }
    clear($("ops-events"));
    for (const event of [...events.values()].sort(
      (a, b) => a.sequence - b.sequence,
    )) {
      const item = element(
        "li",
        event.sequence + " · " + event.kind + " · " + date(event.created),
      );
      const disclosure = element("details"),
        summary = element("summary", "Event data");
      disclosure.append(
        summary,
        element("pre", JSON.stringify(event.payload, null, 2)),
      );
      item.append(disclosure);
      $("ops-events").append(item);
    }
    $("ops-events-more").hidden = !eventMore;
    controls();
  }
  function applyDetail(value) {
    detail = value;
    for (const event of value.events) events.set(event.sequence, event);
    eventCursor = Math.max(eventCursor, value.event_cursor);
    eventMore = value.more_events;
    renderDetail();
  }
  function schedulePoll() {
    clearTimeout(timer);
    if (
      selected &&
      (working.has(selected) ||
        ["running", "cancel_requested"].includes(detail?.run.status))
    ) {
      timer = setTimeout(() => inspect().catch(showError), 500);
    }
  }
  async function inspect() {
    const id = selected,
      serial = ++inspectSerial;
    if (!id) return;
    try {
      const value = await api("inspect", { run_id: id, after: eventCursor });
      if (id !== selected || serial !== inspectSerial) return;
      applyDetail(value);
      const run = runs.find((r) => r.id === id);
      if (run) {
        run.status = value.run.status;
        renderRuns();
      }
    } finally {
      if (id === selected && serial === inspectSerial) schedulePoll();
    }
  }
  async function select(id) {
    clearTimeout(timer);
    selected = id;
    detail = null;
    taskID = null;
    events = new Map();
    eventCursor = 0;
    eventMore = false;
    $("ops-detail").hidden = true;
    $("ops-title").textContent = "Run inspection";
    $("ops-selection").textContent = "Reading selected run…";
    renderRuns();
    controls();
    await inspect();
  }
  async function mutation(action) {
    if (busy) return;
    busy = true;
    showError("");
    controls();
    try {
      await action();
    } catch (error) {
      showError(error.message || String(error));
    } finally {
      busy = false;
      controls();
    }
  }
  const listen = (id, fn) =>
    $(id).addEventListener("click", () =>
      fn().catch((e) => showError(e.message || String(e))),
    );
  listen("ops-init", () =>
    mutation(async () => {
      await api("initialize");
      await refresh();
    }),
  );
  listen("ops-refresh", async () => {
    showError("");
    await refresh();
    await inspect();
  });
  listen("ops-older", () => refresh(true));
  $("ops-filter").addEventListener("change", () =>
    refresh().catch((e) => showError(e.message)),
  );
  for (const id of ["ops-version", "ops-priority"])
    $(id).addEventListener("change", () => {
      submitKey = null;
      controls();
    });
  listen("ops-submit", () =>
    mutation(async () => {
      const version = Number($("ops-version").value),
        priority = Number($("ops-priority").value);
      const signature = version + ":" + priority;
      if (submitKey?.signature !== signature)
        submitKey = { signature, key: crypto.randomUUID() };
      const value = await api("submit", {
        version,
        priority,
        event_key: submitKey.key,
      });
      submitKey = null;
      await refresh();
      await select(value.submission.id);
    }),
  );
  listen("ops-publish", () =>
    mutation(async () => {
      const value = await api("publish", {
        profile: $("ops-profile").value,
        expected_version: overview.versions.at(-1).version,
      });
      await refresh();
      $("ops-version").value = String(value.workflow.version);
      submitKey = null;
    }),
  );
  listen("ops-work", async () => {
    const id = selected;
    if (!id || working.has(id) || detail?.run.status !== "queued") return;
    working.add(id);
    showError("");
    controls();
    schedulePoll();
    try {
      await api("work", { run_id: id });
      await refresh();
      if (selected === id) await inspect();
    } catch (error) {
      showError(error.message);
    } finally {
      working.delete(id);
      controls();
      schedulePoll();
    }
  });
  listen("ops-cancel", () =>
    mutation(async () => {
      const id = selected;
      await api("cancel", { run_id: id });
      await refresh();
      if (selected === id) await inspect();
    }),
  );
  listen("ops-retry", () =>
    mutation(async () => {
      const id = selected;
      if (!retryKeys.has(id)) retryKeys.set(id, crypto.randomUUID());
      const value = await api("retry", {
        run_id: id,
        event_key: retryKeys.get(id),
      });
      retryKeys.delete(id);
      await refresh();
      await select(value.submission.id);
    }),
  );
  listen("ops-events-more", () => inspect());
  $("ops-export").addEventListener("click", () => {
    if (detail)
      download("dagrivane-run-" + detail.run.id + ".json", {
        ...detail,
        events: [...events.values()].sort((a, b) => a.sequence - b.sequence),
        exported_event_cursor: eventCursor,
      });
  });
  function navigation() {
    const target = location.hash || "#queue";
    for (const link of document.querySelectorAll(".sidebar nav a")) {
      if (link.getAttribute("href") === target)
        link.setAttribute("aria-current", "location");
      else link.removeAttribute("aria-current");
    }
  }
  window.addEventListener("hashchange", navigation);
  navigation();
  refresh().catch((e) => showError(e.message));
})();

(() => {
  const api = (action, fields = {}) =>
    evaluate({ mode: "ingestion", action, ...fields });
  let profile = "healthy",
    view = null,
    epoch = 0,
    listSerial = 0,
    detailSerial = 0,
    pageSerial = 0,
    artifactSerial = 0,
    auditSerial = 0;
  let pages = [],
    batches = [],
    pageCursor = null,
    batchCursor = null,
    rowOffset = 0,
    rowHistory = [];
  let pageData = null,
    artifactData = null,
    auditData = null,
    busy = false;
  let operationSerial = 0;
  const requests = new Map();
  const showError = (message) => {
    $("feed-error").hidden = !message;
    $("feed-error").textContent = message || "";
  };
  function bytes(base64) {
    return Uint8Array.from(atob(base64), (char) => char.charCodeAt(0));
  }
  function downloadBytes(name, encoded) {
    const url = URL.createObjectURL(
      new Blob([bytes(encoded)], { type: "application/json" }),
    );
    const link = element("a");
    link.href = url;
    link.download = name;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  function clearPage() {
    pageSerial++;
    pageData = null;
    $("feed-raw-export").disabled = true;
    $("feed-page-export").disabled = true;
    $("feed-raw").textContent = "";
    $("feed-page-summary").textContent = "";
    $("feed-page-status").textContent =
      "Select a retained page to inspect its original entity";
  }
  function clearArtifact() {
    artifactSerial++;
    artifactData = null;
    $("feed-bundle-export").disabled = true;
    $("feed-artifact-export").disabled = true;
    $("feed-artifact").textContent = "";
    $("feed-artifact-status").textContent =
      "Select a published generation to verify its local artifact";
  }
  function controls() {
    const blocked = view?.source.pending?.state === "blocked";
    $("feed-fetch").disabled =
      busy || !view || view.source.lease_active || blocked;
    $("feed-publish").disabled =
      busy || !view || !view.publications.head_batch_id;
    $("feed-profile").disabled = busy;
    const reason = $("feed-reason").value.trim();
    $("feed-repair").disabled =
      busy ||
      !blocked ||
      profile !== "quality" ||
      view.provider.corrected ||
      !reason;
    $("feed-discard").disabled = busy || !blocked || !reason;
  }
  async function refresh(resetRequests = false) {
    const serial = ++listSerial;
    showError("");
    if (resetRequests)
      for (const [key, value] of requests)
        if (value.fields.profile === profile) requests.delete(key);
    const result = await api("overview");
    if (serial !== listSerial) return;
    $("feed-initialize").hidden = result.initialized;
    $("feed-ready").hidden = !result.items.length;
    clear($("feed-profile"));
    for (const item of result.items)
      $("feed-profile").append(new Option(item.label, item.profile));
    if (!result.items.some((item) => item.profile === profile))
      profile = result.items[0]?.profile || "healthy";
    $("feed-profile").value = profile;
    if (result.items.length) await selectProfile(profile);
    else
      $("feed-status").textContent =
        "No ingestion sources. Initialize the three reviewed local controls to begin.";
  }
  async function selectProfile(value) {
    profile = value;
    epoch++;
    detailSerial++;
    operationSerial++;
    auditSerial++;
    rowOffset = 0;
    rowHistory = [];
    view = null;
    pages = [];
    batches = [];
    clearPage();
    clearArtifact();
    auditData = null;
    $("feed-audit-detail").hidden = true;
    $("feed-audit-export").disabled = true;
    $("feed-action").textContent = "";
    $("feed-reason").value = "";
    clear($("feed-rows"));
    clear($("feed-pages"));
    clear($("feed-batches"));
    controls();
    await loadView(false);
  }
  async function loadView(append = false) {
    const current = epoch,
      serial = ++detailSerial,
      source = profile;
    showError("");
    $("feed-status").textContent =
      "Reading source checkpoint and publication state…";
    try {
      const result = await api("inspect", {
        profile: source,
        row_offset: rowOffset,
        ...(append === "pages"
          ? { after_page: pageCursor }
          : append === "batches"
            ? { after_generation: batchCursor }
            : {}),
      });
      if (current !== epoch || serial !== detailSerial) return;
      view = result;
      if (append !== "rows") {
        pages =
          append === "pages"
            ? [...pages, ...result.pages.items]
            : append === "batches"
              ? pages
              : result.pages.items;
        batches =
          append === "batches"
            ? [...batches, ...result.publications.items]
            : append === "pages"
              ? batches
              : result.publications.items;
        if (append !== "batches") pageCursor = result.pages.next_cursor;
        if (append !== "pages")
          batchCursor = result.publications.next_generation;
      }
      render();
    } catch (error) {
      if (current === epoch && serial === detailSerial) {
        view = null;
        controls();
        showError(error.message);
        $("feed-status").textContent =
          "Source inspection failed. Refresh ingestion to retry.";
      }
    }
  }
  function render() {
    const source = view.source,
      data = view.data,
      counts = view.stream.publication_counts;
    $("feed-status").textContent =
      view.label +
      " · checkpoint generation " +
      source.generation +
      ", offset " +
      source.offset;
    $("feed-source-state").textContent = source.pending
      ? "Page " + source.pending.seq + " " + source.pending.state
      : source.retained_pages + " retained pages; no pending entity";
    $("feed-stream-state").textContent =
      data.total_rows +
      " live rows, " +
      data.tombstones +
      " tombstone" +
      (data.tombstones === 1 ? "" : "s");
    $("feed-catalog-state").textContent =
      (counts.published || 0) +
      " published, " +
      ((counts.pending || 0) + (counts.leased || 0)) +
      " awaiting publication";
    $("feed-review").hidden = source.pending?.state !== "blocked";
    $("feed-review-error").textContent = source.pending?.error || "";
    $("feed-repair").hidden = profile !== "quality";
    $("feed-total").textContent = "Exact amount total: " + data.amount_total;
    clear($("feed-rows"));
    for (const item of data.rows) {
      const row = element("tr");
      for (const key of ["id", "amount", "updated_at"])
        row.append(
          element("td", String(item[key]), key === "amount" ? "number" : ""),
        );
      $("feed-rows").append(row);
    }
    $("feed-row-range").textContent = data.total_rows
      ? data.row_offset +
        1 +
        "–" +
        (data.row_offset + data.rows.length) +
        " of " +
        data.total_rows +
        " rows"
      : "No accepted rows";
    $("feed-row-prev").disabled = !rowHistory.length;
    $("feed-row-next").disabled = data.next_offset === null;
    clear($("feed-pages"));
    for (const item of pages) {
      const row = element("tr"),
        cell = element("td"),
        button = element("button", "Page " + item.seq);
      button.onclick = () => inspectPage(item.id);
      cell.append(button);
      row.append(
        cell,
        element(
          "td",
          item.state,
          item.state === "blocked" ? "status failed" : "",
        ),
        element("td", String(item.offset)),
      );
      $("feed-pages").append(row);
    }
    $("feed-more-pages").hidden = pageCursor === null;
    clear($("feed-batches"));
    for (const item of batches) {
      const row = element("tr"),
        cell = element("td"),
        button = element("button", "Generation " + item.generation);
      button.disabled = item.status !== "published";
      button.onclick = () => inspectArtifact(item.generation);
      cell.append(button);
      row.append(
        cell,
        element("td", item.status),
        element("td", String(item.attempts)),
      );
      $("feed-batches").append(row);
      if (item.last_error) {
        const errorRow = element("tr"),
          text = element("td", item.last_error, "status failed");
        text.colSpan = 3;
        errorRow.append(text);
        $("feed-batches").append(errorRow);
      }
    }
    $("feed-more-batches").hidden = batchCursor === null;
    $("feed-checkpoint").textContent = JSON.stringify(
      {
        revision: view.revision,
        source: view.source,
        stream: view.stream,
        provider: view.provider,
        snapshot_scope: view.snapshot_scope,
      },
      null,
      2,
    );
    clear($("feed-operations"));
    for (const item of view.operations) {
      const row = element("tr"),
        cell = element("td"),
        button = element("button", item.action + " #" + item.seq);
      button.onclick = () => readOperation(item.request_key);
      cell.append(button);
      row.append(
        cell,
        element("td", item.status),
        element("td", item.error || "Receipt retained"),
      );
      $("feed-operations").append(row);
    }
    controls();
  }
  async function inspectPage(id) {
    clearPage();
    const serial = ++pageSerial,
      current = epoch,
      source = profile;
    $("feed-page-status").textContent =
      "Reading and verifying retained response entity…";
    try {
      const result = await api("page", { profile: source, page_id: id });
      if (serial !== pageSerial || current !== epoch) return;
      pageData = result;
      $("feed-page-status").textContent =
        "Page " +
        result.seq +
        " · " +
        result.state +
        " · " +
        result.raw_bytes +
        " bytes";
      const { raw_base64, ...summary } = result;
      $("feed-page-summary").textContent = JSON.stringify(summary, null, 2);
      const text = new TextDecoder().decode(bytes(raw_base64));
      $("feed-raw").textContent =
        text.slice(0, 32768) +
        (text.length > 32768
          ? "\n[Preview shortened; export original JSON for full entity]"
          : "");
      $("feed-raw-export").disabled = false;
      $("feed-page-export").disabled = false;
    } catch (error) {
      if (serial === pageSerial && current === epoch) {
        showError(error.message);
        $("feed-page-status").textContent =
          "Page could not be read; select it again to retry.";
      }
    }
  }
  async function inspectArtifact(generation) {
    clearArtifact();
    const serial = ++artifactSerial,
      current = epoch,
      source = profile;
    $("feed-artifact-status").textContent =
      "Verifying the acknowledged catalog artifact…";
    try {
      const result = await api("artifact", { profile: source, generation });
      if (serial !== artifactSerial || current !== epoch) return;
      artifactData = result;
      $("feed-artifact-status").textContent =
        "Generation " + generation + " · local artifact verified";
      const { bundle_base64, ...summary } = result;
      $("feed-artifact").textContent = JSON.stringify(summary, null, 2);
      $("feed-bundle-export").disabled = false;
      $("feed-artifact-export").disabled = false;
    } catch (error) {
      if (serial === artifactSerial && current === epoch) {
        showError(error.message);
        $("feed-artifact-status").textContent =
          "Artifact verification failed; no export is available.";
      }
    }
  }
  async function readOperation(key) {
    const current = epoch,
      serial = ++operationSerial;
    try {
      const result = await api("operation", { profile, request_key: key });
      if (current === epoch && serial === operationSerial)
        $("feed-receipt").textContent = JSON.stringify(result, null, 2);
    } catch (error) {
      if (current === epoch && serial === operationSerial)
        showError(error.message);
    }
  }
  async function mutate(action) {
    if (busy || !view) return;
    const source = profile,
      current = epoch;
    const fields = {
      profile: source,
      ...(["repair", "discard"].includes(action)
        ? {
            page_id: view.source.pending?.id,
            reason: $("feed-reason").value.trim(),
          }
        : {}),
    };
    const signature = JSON.stringify({ action, ...fields });
    let request = requests.get(signature);
    if (!request) {
      request = {
        fields: {
          ...fields,
          request_key: "ingestion-" + crypto.randomUUID(),
          expected_revision: view.revision,
        },
      };
      requests.set(signature, request);
    }
    busy = true;
    listSerial++;
    controls();
    showError("");
    $("feed-action").textContent =
      "Applying " + action + " to the reviewed source state…";
    try {
      const result = await api(action, request.fields);
      if (result.operation.status !== "started") requests.delete(signature);
      if (current !== epoch) return;
      operationSerial++;
      $("feed-receipt").textContent = JSON.stringify(result, null, 2);
      $("feed-action").textContent =
        result.operation.status === "completed"
          ? action +
            ": " +
            (result.outcome?.status ||
              (result.outcome?.published === false
                ? "no pending batch"
                : "completed"))
          : result.operation.status === "started"
            ? result.operation.recovery
            : result.operation.error;
      if (result.operation.status === "failed")
        showError(result.operation.error);
      clearPage();
      clearArtifact();
      auditSerial++;
      auditData = null;
      $("feed-audit-detail").hidden = true;
      rowOffset = 0;
      rowHistory = [];
      await loadView(false);
    } catch (error) {
      if (current === epoch) {
        showError(error.message);
        $("feed-action").textContent =
          "No confirmed receipt. Retry the same action to recover it. If rejected, refresh and review the checkpoint before a new request.";
      }
    } finally {
      busy = false;
      controls();
    }
  }
  $("feed-initialize").onclick = async () => {
    if (busy) return;
    busy = true;
    $("feed-initialize").disabled = true;
    showError("");
    try {
      await api("initialize");
      await refresh();
    } catch (error) {
      showError(error.message);
    } finally {
      busy = false;
      $("feed-initialize").disabled = false;
      controls();
    }
  };
  $("feed-refresh").onclick = () =>
    refresh(true).catch((error) => showError(error.message));
  $("feed-profile").onchange = () => selectProfile($("feed-profile").value);
  for (const [id, action] of [
    ["feed-fetch", "poll"],
    ["feed-publish", "publish"],
    ["feed-repair", "repair"],
    ["feed-discard", "discard"],
  ])
    $(id).onclick = () => mutate(action);
  $("feed-reason").oninput = controls;
  $("feed-more-pages").onclick = () => loadView("pages");
  $("feed-more-batches").onclick = () => loadView("batches");
  $("feed-row-next").onclick = () => {
    if (view?.data.next_offset !== null) {
      rowHistory.push(rowOffset);
      rowOffset = view.data.next_offset;
      loadView("rows");
    }
  };
  $("feed-row-prev").onclick = () => {
    if (rowHistory.length) {
      rowOffset = rowHistory.pop();
      loadView("rows");
    }
  };
  $("feed-raw-export").onclick = () => {
    if (pageData)
      downloadBytes(
        "dagrivane-page-" + pageData.seq + ".json",
        pageData.raw_base64,
      );
  };
  $("feed-page-export").onclick = () => {
    if (pageData) download("dagrivane-page-evidence.json", pageData);
  };
  $("feed-bundle-export").onclick = () => {
    if (artifactData)
      downloadBytes("dagrivane-table.json", artifactData.bundle_base64);
  };
  $("feed-artifact-export").onclick = () => {
    if (artifactData) download("dagrivane-dataset-evidence.json", artifactData);
  };
  $("feed-audit").onclick = async () => {
    const current = epoch,
      serial = ++auditSerial;
    $("feed-audit").disabled = true;
    auditData = null;
    $("feed-audit-detail").hidden = true;
    $("feed-audit-export").disabled = true;
    showError("");
    try {
      const result = await api("audit", { profile });
      if (current !== epoch || serial !== auditSerial) return;
      auditData = result;
      $("feed-audit-detail").hidden = false;
      $("feed-audit-result").textContent = JSON.stringify(result, null, 2);
      $("feed-audit-export").disabled = false;
      $("feed-action").textContent = result.passed
        ? "Retained source and ingestion audits passed"
        : "Audit failed or exceeded its retained-evidence limit; inspect the result";
    } catch (error) {
      if (current === epoch && serial === auditSerial) {
        auditData = null;
        $("feed-audit-export").disabled = true;
        showError(error.message);
      }
    } finally {
      $("feed-audit").disabled = false;
    }
  };
  $("feed-audit-export").onclick = () => {
    if (auditData) download("dagrivane-ingestion-audit.json", auditData);
  };
  refresh().catch((error) => {
    showError(error.message);
    $("feed-status").textContent =
      "Ingestion inspection unavailable. Initialize or refresh to retry.";
  });
})();
