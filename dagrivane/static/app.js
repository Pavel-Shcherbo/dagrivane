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
