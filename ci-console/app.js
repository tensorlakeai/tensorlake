const onboardingView = document.querySelector('#onboarding-view');
const dashboardView = document.querySelector('#dashboard-view');
const placeholderView = document.querySelector('#placeholder-view');
const navItems = [...document.querySelectorAll('.nav-item')];
const toast = document.querySelector('#toast');
const uiState = {
  repositories: [],
  plan: null,
  pullRequest: null,
  run: null,
  pollTimer: null,
  onboardingComplete: false,
};
let currentStep = 1;
let toastTimer;
let testJobSequence = 1843;

function showToast(message) {
  toast.querySelector('span').textContent = message;
  toast.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove('show'), 2600);
}

function showError(message) {
  const error = document.querySelector('#setup-error');
  error.querySelector('span').textContent = message;
  error.hidden = false;
}

function clearError() {
  document.querySelector('#setup-error').hidden = true;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error?.message || `Request failed (${response.status})`);
  }
  return payload;
}

function setButtonBusy(button, label) {
  if (!button.dataset.originalHtml) button.dataset.originalHtml = button.innerHTML;
  button.disabled = true;
  button.classList.add('loading');
  const existingLabel = button.querySelector('.launch-button-label');
  if (existingLabel) existingLabel.textContent = label;
  else button.innerHTML = `<span class="status-spinner" style="border-color:rgba(255,255,255,.25);border-top-color:#fff"></span>${label}`;
}

function restoreButton(button) {
  button.disabled = false;
  button.classList.remove('loading');
  if (button.dataset.originalHtml) button.innerHTML = button.dataset.originalHtml;
}

function setStep(step) {
  currentStep = step;
  clearError();
  document.querySelectorAll('.setup-content').forEach((panel) => {
    panel.classList.toggle('active', Number(panel.dataset.step) === step);
  });
  document.querySelectorAll('.progress-step').forEach((item) => {
    const position = Number(item.dataset.progress);
    item.classList.toggle('active', position === step);
    item.classList.toggle('complete', position < step);
    const badge = item.querySelector('span');
    badge.innerHTML = position < step
      ? '<svg viewBox="0 0 16 16" style="width:12px;height:12px"><path d="m3 8 3 3 7-7"/></svg>'
      : position;
  });
  document.querySelector('#progress-fill').style.width = `${((step - 1) / 3) * 100}%`;
}

function setView(view) {
  const isOverview = view === 'overview';
  onboardingView.hidden = !isOverview || uiState.onboardingComplete;
  dashboardView.hidden = !isOverview || !uiState.onboardingComplete;
  placeholderView.hidden = isOverview;
  navItems.forEach((item) => {
    const isActive = item.dataset.view === view;
    item.classList.toggle('active', isActive);
    if (isActive) item.setAttribute('aria-current', 'page');
    else item.removeAttribute('aria-current');
  });
  const label = view.charAt(0).toUpperCase() + view.slice(1);
  document.querySelector('#breadcrumb-current').textContent = label;
  if (!isOverview) {
    const copy = {
      jobs: ['Jobs', 'Trace every workflow from queue to completion.', 'Workflow history'],
      runners: ['Runners', 'Configure serverless capacity and machine profiles.', 'Your runner fleet is healthy'],
      repositories: ['Repositories', 'Control which GitHub repositories can use Tensorlake CI.', 'Repositories connected'],
      insights: ['Insights', 'Understand build speed, reliability, and compute usage.', 'Build performance is trending up'],
      secrets: ['Secrets', 'Securely share encrypted values with your workflows.', 'Your secrets stay encrypted'],
      settings: ['Settings', 'Manage your Tensorlake CI workspace.', 'Workspace settings'],
    }[view] || [label, 'Manage your Tensorlake CI workspace.', 'Everything is ready'];
    document.querySelector('#placeholder-title').textContent = copy[0];
    document.querySelector('#placeholder-description').textContent = copy[1];
    document.querySelector('#placeholder-card-title').textContent = copy[2];
  }
  if (window.location.hash !== `#${view}`) history.replaceState(null, '', `#${view}`);
  document.querySelector('.sidebar').classList.remove('open');
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function languageColor(language) {
  if (language === 'Go') return 'violet';
  if (language === 'TypeScript') return 'coral';
  return '';
}

function updateRepoCount() {
  const selected = [...document.querySelectorAll('.repo-row input:checked')];
  const workflows = selected.reduce((total, input) => {
    const repo = uiState.repositories.find((item) => item.full_name === input.value);
    return total + (repo?.workflow_count || 1);
  }, 0);
  document.querySelector('#workflow-count').textContent = workflows;
  const button = document.querySelector('#choose-repos');
  button.disabled = selected.length === 0;
  button.style.opacity = selected.length === 0 ? '.45' : '1';
}

function renderRepositories(repositories) {
  uiState.repositories = repositories;
  const list = document.querySelector('#repo-list');
  list.innerHTML = repositories.map((repo, index) => `
    <label class="repo-row" data-name="${escapeHtml(repo.name.toLowerCase())}" data-full-name="${escapeHtml(repo.full_name)}">
      <input type="checkbox" value="${escapeHtml(repo.full_name)}" ${index < 2 ? 'checked' : ''} />
      <span class="custom-checkbox"><svg viewBox="0 0 16 16"><path d="m3 8 3 3 7-7"/></svg></span>
      <span class="repo-symbol ${languageColor(repo.language)}">${escapeHtml(repo.name[0].toUpperCase())}</span>
      <span><strong>${escapeHtml(repo.name)}</strong><small>${escapeHtml(repo.language)} · ${escapeHtml(repo.workflow_count ?? 'Scanning')} workflow${repo.workflow_count === 1 ? '' : 's'}</small></span>
      <em>${repo.private ? 'Private' : 'Public'}</em>
    </label>`).join('');
  list.querySelectorAll('input').forEach((input) => input.addEventListener('change', updateRepoCount));
  updateRepoCount();
}

function updateOrganization(organization) {
  if (!organization) return;
  document.querySelector('.connected-account strong').textContent = organization.name || organization.login;
  document.querySelector('.connected-account p').textContent = `github.com/${organization.login}`;
  document.querySelector('.connected-avatar').textContent = organization.avatar || organization.login[0].toUpperCase();
}

async function loadRepositories() {
  const payload = await api('/api/repositories');
  renderRepositories(payload.repositories);
  setStep(2);
}

function diffMarkup(diff) {
  const relevantLines = diff.split('\n').filter((line) => !line.startsWith('---') && !line.startsWith('+++') && !line.startsWith('@@'));
  return relevantLines.map((line) => {
    const type = line.startsWith('+') ? 'diff-add' : line.startsWith('-') ? 'diff-remove' : 'diff-context';
    return `<span class="${type}">${escapeHtml(line)}</span>`;
  }).join('\n');
}

function renderMigrationPlan(plan) {
  uiState.plan = plan;
  const first = plan.changes[0];
  document.querySelector('#diff-path').textContent = `${first.repository}/${first.path}`;
  document.querySelector('#diff-file-count').textContent = `1 of ${plan.workflow_count} file${plan.workflow_count === 1 ? '' : 's'}`;
  document.querySelector('#migration-diff').innerHTML = diffMarkup(first.diff);
  const description = document.querySelector('[data-step="3"] .setup-description');
  description.textContent = `Tensorlake found ${plan.replacement_count} runner label${plan.replacement_count === 1 ? '' : 's'} across ${plan.workflow_count} workflow file${plan.workflow_count === 1 ? '' : 's'}. The PR changes only workflow configuration.`;
}

function renderPullRequest(pullRequest) {
  uiState.pullRequest = pullRequest;
  const link = document.querySelector('#migration-pr-link');
  link.href = pullRequest.primary.url;
  link.textContent = `PR #${pullRequest.primary.number} ↗`;
}

function renderRun(run) {
  uiState.run = run;
  const status = document.querySelector('#run-status');
  status.className = 'run-status';
  if (run.status === 'completed') {
    const succeeded = run.conclusion === 'success';
    status.classList.add(succeeded ? 'success' : 'failure');
    status.innerHTML = `<i></i>${succeeded ? 'Passed' : 'Failed'}`;
    document.querySelector('#finish-onboarding').disabled = false;
    if (succeeded) document.querySelector('#finish-onboarding').focus({ preventScroll: true });
  } else {
    status.classList.add('active');
    status.innerHTML = `<i></i>${run.status === 'queued' ? 'Waiting for GitHub' : run.status === 'running' ? 'Running' : 'Provisioning'}`;
  }
  const elapsed = Math.max(0, Math.round((Date.now() / 1000) - run.created_at));
  document.querySelector('#run-elapsed').textContent = elapsed < 2 ? 'Just now' : `${elapsed}s elapsed`;
  document.querySelectorAll('[data-run-step]').forEach((node) => {
    const step = run.steps.find((item) => item.id === node.dataset.runStep);
    node.classList.toggle('active', step?.status === 'active');
    node.classList.toggle('complete', step?.status === 'complete');
    const badge = node.querySelector('span');
    if (step?.status === 'complete') badge.innerHTML = '<svg viewBox="0 0 16 16" style="width:11px;height:11px"><path d="m3 8 3 3 7-7"/></svg>';
  });
  const logs = run.logs.map((entry) => entry.message).join('\n');
  const terminal = document.querySelector('#live-run-logs');
  terminal.textContent = logs || 'Waiting for GitHub to queue the smoke workflow...';
  terminal.scrollTop = terminal.scrollHeight;
  const sandbox = document.querySelector('#live-sandbox');
  if (run.sandbox) {
    sandbox.hidden = false;
    document.querySelector('#live-sandbox-name').textContent = run.sandbox.id;
    document.querySelector('#live-sandbox-machine').textContent = `${run.sandbox.cpus} vCPU · ${Math.round(run.sandbox.memory_mb / 1024)} GB`;
  }
}

function stopPolling() {
  clearInterval(uiState.pollTimer);
  uiState.pollTimer = null;
}

function pollRun(runId) {
  stopPolling();
  const update = async () => {
    try {
      const run = await api(`/api/runs/${encodeURIComponent(runId)}`);
      renderRun(run);
      if (run.status === 'completed') {
        stopPolling();
        showToast(run.conclusion === 'success' ? 'First Tensorlake job passed' : 'The smoke job needs attention');
      }
    } catch (error) {
      stopPolling();
      showError(error.message);
    }
  };
  update();
  uiState.pollTimer = setInterval(update, 350);
}

document.querySelector('#connect-github').addEventListener('click', async () => {
  const button = document.querySelector('#connect-github');
  clearError();
  setButtonBusy(button, 'Connecting securely...');
  try {
    const connection = await api('/api/github/connect', { method: 'POST', body: '{}' });
    if (connection.redirect_url) {
      window.location.assign(connection.redirect_url);
      return;
    }
    updateOrganization(connection.organization);
    await loadRepositories();
    showToast('GitHub App installed');
  } catch (error) {
    showError(`${error.message}. Start the app with “python3 server.py” to use the golden path.`);
  } finally {
    restoreButton(button);
  }
});

document.querySelectorAll('.back-button').forEach((button) => {
  button.addEventListener('click', () => setStep(Number(button.dataset.back)));
});

document.querySelector('#repo-search').addEventListener('input', (event) => {
  const query = event.target.value.toLowerCase().trim();
  document.querySelectorAll('.repo-row').forEach((row) => {
    row.hidden = !row.dataset.name.includes(query) && !row.dataset.fullName.includes(query);
  });
});

document.querySelector('#choose-repos').addEventListener('click', async () => {
  const button = document.querySelector('#choose-repos');
  const repositories = [...document.querySelectorAll('.repo-row input:checked')].map((input) => input.value);
  setButtonBusy(button, 'Scanning workflows...');
  try {
    const plan = await api('/api/migration/plan', {
      method: 'POST',
      body: JSON.stringify({ repositories }),
    });
    renderMigrationPlan(plan);
    setStep(3);
  } catch (error) {
    showError(error.message);
  } finally {
    restoreButton(button);
  }
});

document.querySelector('#open-migration-pr').addEventListener('click', async () => {
  const button = document.querySelector('#open-migration-pr');
  setButtonBusy(button, 'Opening pull request...');
  try {
    const pullRequest = await api('/api/migration/pull-request', { method: 'POST', body: '{}' });
    renderPullRequest(pullRequest);
    setStep(4);
    const run = await api('/api/runs/smoke', { method: 'POST', body: '{}' });
    renderRun(run);
    pollRun(run.id);
  } catch (error) {
    showError(error.message);
  } finally {
    restoreButton(button);
  }
});

document.querySelector('#retain-on-failure').addEventListener('change', async (event) => {
  if (!event.target.checked || !uiState.run) return;
  try {
    await api(`/api/runs/${encodeURIComponent(uiState.run.id)}/retain`, { method: 'POST', body: '{}' });
    showToast('Sandbox retention enabled for this run');
  } catch (error) {
    event.target.checked = false;
    showError(error.message);
  }
});

document.querySelector('#copy-ssh').addEventListener('click', async () => {
  if (!uiState.run?.ssh_command) return;
  try {
    await navigator.clipboard.writeText(uiState.run.ssh_command);
    showToast('SSH command copied');
  } catch {
    showToast(uiState.run.ssh_command);
  }
});

document.querySelector('#finish-onboarding').addEventListener('click', () => {
  stopPolling();
  uiState.onboardingComplete = true;
  setView('overview');
  showToast('Tensorlake CI is ready');
});

document.querySelector('#replay-onboarding').addEventListener('click', async () => {
  stopPolling();
  try {
    await api('/api/session/reset', { method: 'POST', body: '{}' });
  } catch {
    // Live installations stay connected; replay still returns to the first screen.
  }
  uiState.onboardingComplete = false;
  uiState.plan = null;
  uiState.pullRequest = null;
  uiState.run = null;
  document.querySelector('#finish-onboarding').disabled = true;
  document.querySelector('#retain-on-failure').checked = false;
  setStep(1);
  setView('overview');
  userPopover.hidden = true;
  showToast('Onboarding reset');
});

function setRunnerCount(count) {
  document.querySelector('#active-runners').textContent = count;
  document.querySelector('#fleet-active').textContent = count;
}

document.querySelector('#run-test-job').addEventListener('click', () => {
  const button = document.querySelector('#run-test-job');
  const buttonLabel = button.querySelector('span');
  button.disabled = true;
  buttonLabel.textContent = 'Queueing...';
  document.querySelector('#jobs-today').textContent = Number(document.querySelector('#jobs-today').textContent) + 1;
  setRunnerCount(4);
  const row = document.createElement('div');
  row.className = 'job-row new-job';
  row.setAttribute('role', 'row');
  row.innerHTML = `<span class="job-name"><i class="status-spinner"></i><span><strong>Runner smoke test</strong><small>#${testJobSequence++} · tensorlake/setup</small></span></span><span class="repo-name"><i>A</i> api-gateway</span><span><em class="job-status running">Running</em></span><span>Just now</span><button class="row-menu" aria-label="Job actions">•••</button>`;
  document.querySelector('#job-table .table-head').insertAdjacentElement('afterend', row);
  showToast('Test job started in a fresh sandbox');
  setTimeout(() => {
    button.disabled = false;
    buttonLabel.textContent = 'Run test job';
  }, 700);
  setTimeout(() => {
    row.querySelector('.status-spinner').outerHTML = '<i class="status-check"><svg viewBox="0 0 16 16"><path d="m3 8 3 3 7-7"/></svg></i>';
    const status = row.querySelector('.job-status');
    status.className = 'job-status passed';
    status.textContent = 'Passed';
    row.children[3].textContent = '3s';
    row.classList.remove('new-job');
    setRunnerCount(3);
    showToast('Test job passed in 3 seconds');
  }, 3000);
});

navItems.forEach((item) => item.addEventListener('click', (event) => {
  event.preventDefault();
  setView(item.dataset.view);
}));
document.querySelectorAll('[data-view-button]').forEach((button) => button.addEventListener('click', () => setView(button.dataset.viewButton)));
document.querySelectorAll('[data-toast]').forEach((button) => button.addEventListener('click', (event) => {
  event.preventDefault();
  showToast(button.dataset.toast);
}));

const workspaceButton = document.querySelector('#workspace-switcher');
const workspacePopover = document.querySelector('#workspace-popover');
workspaceButton.addEventListener('click', (event) => {
  event.stopPropagation();
  workspacePopover.hidden = !workspacePopover.hidden;
  const rect = workspaceButton.getBoundingClientRect();
  workspacePopover.style.left = `${rect.left + 8}px`;
  workspacePopover.style.top = `${rect.bottom + 7}px`;
});

const userButton = document.querySelector('#user-menu');
const userPopover = document.querySelector('#user-popover');
userButton.addEventListener('click', (event) => {
  event.stopPropagation();
  userPopover.hidden = !userPopover.hidden;
  const rect = userButton.getBoundingClientRect();
  userPopover.style.left = `${rect.right - 150}px`;
  userPopover.style.bottom = `${window.innerHeight - rect.top + 5}px`;
});

document.addEventListener('click', () => {
  workspacePopover.hidden = true;
  userPopover.hidden = true;
});
document.querySelector('#mobile-menu').addEventListener('click', () => document.querySelector('.sidebar').classList.toggle('open'));
document.addEventListener('keydown', (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
    event.preventDefault();
    showToast('Search is ready');
  }
  if (event.key === 'Escape') {
    workspacePopover.hidden = true;
    userPopover.hidden = true;
    document.querySelector('.sidebar').classList.remove('open');
  }
});

async function bootstrap() {
  setStep(1);
  setView(window.location.hash.slice(1) || 'overview');
  try {
    const session = await api('/api/session');
    updateOrganization(session.organization);
    if (session.pull_request && session.latest_run) {
      renderMigrationPlan(session.migration_plan);
      renderPullRequest(session.pull_request);
      setStep(4);
      renderRun(session.latest_run);
      if (session.latest_run.status !== 'completed') pollRun(session.latest_run.id);
    } else if (session.migration_plan) {
      renderMigrationPlan(session.migration_plan);
      setStep(3);
    } else if (session.connected) {
      await loadRepositories();
    }
  } catch {
    showError('The CI control plane is offline. Start this product with “python3 server.py”.');
  }
}

bootstrap();
