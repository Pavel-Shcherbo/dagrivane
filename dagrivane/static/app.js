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
  const ns = "http://www.w3.org/2000/svg";
  tasks.forEach((task, i) => {
    const x = 20 + i * 232;
    if (i) {
      const line = document.createElementNS(ns, "line");
      Object.entries({
        x1: x - 40,
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
  $("runstate").textContent = "Running";
  try {
    const result = await evaluate({ fail_quality: $("fail").checked });
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
  }
};
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
  .catch((e) => error(e.message));
