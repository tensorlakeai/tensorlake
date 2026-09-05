const DEFAULT_LABEL = "tensorlake";

const installLink = document.querySelector("#install-link");
const runnerLabel = document.querySelector("#runner-label");
const profileList = document.querySelector("#profile-list");
const installedBanner = document.querySelector("#installed-banner");
const copyButton = document.querySelector("#copy-button");
const codeCopy = document.querySelector("#code-copy");
const demoPanel = document.querySelector("#demo-panel");
const demoButton = document.querySelector("#demo-button");
const demoStatus = document.querySelector("#demo-status");

let config = {
  default_label: DEFAULT_LABEL,
  profiles: [],
  install_url: "#",
  mode: "demo",
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error?.message || "Tensorlake CI request failed");
  }
  return payload;
}

async function copyLabel(button) {
  await navigator.clipboard.writeText(config.default_label);
  const original = button.textContent;
  button.textContent = "Copied";
  window.setTimeout(() => {
    button.textContent = original;
  }, 1400);
}

function renderProfiles() {
  profileList.replaceChildren();
  for (const profile of config.profiles) {
    const row = document.createElement("div");
    const label = document.createElement("code");
    const resources = document.createElement("span");
    label.textContent = profile.label;
    resources.textContent = `${profile.cpus} vCPU · ${profile.memory_mb / 1024} GB`;
    row.append(label, resources);
    profileList.append(row);
  }
}

function showInstallationResult() {
  const params = new URLSearchParams(window.location.search);
  if (!params.has("installed")) return;
  const account = params.get("account");
  const strong = installedBanner.querySelector("strong");
  strong.textContent = account ? `${account} connected.` : "GitHub connected.";
  installedBanner.hidden = false;
}

async function runDemo() {
  demoButton.disabled = true;
  demoButton.textContent = "Dispatching…";
  demoStatus.textContent = "";
  try {
    const result = await api("/api/demo/jobs", {
      method: "POST",
      body: JSON.stringify({ label: config.default_label }),
    });
    demoStatus.textContent = "Sandbox starting…";
    for (let attempt = 0; attempt < 30; attempt += 1) {
      const run = await api(`/api/runs/${result.run_id}`);
      if (run.status === "completed") {
        demoStatus.textContent = run.conclusion === "success" ? "Job passed ✓" : "Job failed";
        return;
      }
      await new Promise((resolve) => window.setTimeout(resolve, 150));
    }
    demoStatus.textContent = "Still running";
  } catch (error) {
    demoStatus.textContent = error.message;
  } finally {
    demoButton.disabled = false;
    demoButton.textContent = "Run demo job";
  }
}

async function initialize() {
  try {
    config = await api("/api/config");
    installLink.href = config.install_url;
    runnerLabel.textContent = config.default_label;
    renderProfiles();
    demoPanel.hidden = config.mode !== "demo";
  } catch (error) {
    installLink.textContent = "Service unavailable";
    installLink.setAttribute("aria-disabled", "true");
    installLink.removeAttribute("href");
  }
  showInstallationResult();
}

copyButton.addEventListener("click", () => copyLabel(copyButton));
codeCopy.addEventListener("click", () => copyLabel(codeCopy));
demoButton.addEventListener("click", runDemo);
initialize();
