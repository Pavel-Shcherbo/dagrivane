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
